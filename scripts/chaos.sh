#!/usr/bin/env bash
# Chaos: inject one fault at a time, measure how long the system takes to detect it and heal, then the next.
#
#   ./scripts/chaos.sh                          # random faults every 60 s until Ctrl-C
#   ./scripts/chaos.sh --rounds 5 --interval 30
#   ./scripts/chaos.sh --faults corrupt,delete --rounds 2
#   ./scripts/chaos.sh --list                   # explain the faults
#
# Faults (one at a time, never two workers down together):
#   stop        docker stop a random worker for --downtime s; the controller marks it DEAD and re-replicates its
#               chunks onto the others; after the restart its stale copies are trimmed
#   pause       freeze a worker for --pause s (below the 15 s heartbeat timeout): it is slow, not dead, and reads
#               are answered by the other copy (hedged reads; visible when scripts/load.py runs)
#   corrupt     overwrite bytes inside one chunk file on disk; the scrubber (or a client read) catches the bad
#               checksum and the copy is rebuilt from the healthy replica
#   delete      remove one chunk file from a worker's disk; found missing and restored the same way
#   controller  docker restart the controller; clients get errors for a few seconds, nothing is lost
#
# Before every fault the cluster must be fully healthy (all running workers HEALTHY, 0 under-replicated chunks,
# no active repair). corrupt/delete only touch a test object this script uploads itself (chaos-<run>.bin), never
# your own objects; it is verified byte for byte after every fault and deleted at the end.
# Every fault is drawn on the Grafana dashboard as a purple region from injection to healed.
# Ctrl-C restores everything (unpauses / starts the worker it touched) before exiting.
set -uo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8090}
GRAFANA=${GRAFANA_URL:-http://localhost:3000}
GRAFANA_AUTH=${GRAFANA_AUTH:-admin:admin}
INTERVAL=60; ROUNDS=0; DOWNTIME=45; PAUSE=8; HEAL_TIMEOUT=240; SCRUB=1
FAULTS="stop,pause,corrupt,delete,controller"

usage() { sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; cat <<EOF

Options:
  --faults LIST     comma-separated subset of: stop,pause,corrupt,delete,controller (default: all)
  --rounds N        number of faults, 0 = until Ctrl-C (default 0)
  --interval S      seconds between "healed" and the next fault (default 60)
  --downtime S      how long a stopped worker stays down (default 45)
  --pause S         how long a paused worker stays frozen (default 8)
  --heal-timeout S  give up (and stop) if not healed within S seconds (default 240)
  --no-scrub        do not trigger the scrubber after corrupt/delete; wait for a client read or the next
                    scheduled pass instead (can take up to SCRUB_INTERVAL)
EOF
}
while [ $# -gt 0 ]; do
  case "$1" in
    --faults) FAULTS=$2; shift ;;  --rounds) ROUNDS=$2; shift ;;  --interval) INTERVAL=$2; shift ;;
    --downtime) DOWNTIME=$2; shift ;;  --pause) PAUSE=$2; shift ;;  --heal-timeout) HEAL_TIMEOUT=$2; shift ;;
    --no-scrub) SCRUB=0 ;;  --list|-h|--help) usage; exit 0 ;;
    *) echo "unknown option $1 (try --help)"; exit 2 ;;
  esac; shift
done
IFS=, read -ra FAULT_LIST <<<"$FAULTS"
for f in "${FAULT_LIST[@]}"; do
  case "$f" in stop|pause|corrupt|delete|controller) ;; *) echo "unknown fault '$f' (try --list)"; exit 2 ;; esac
done

if [ -t 1 ]; then BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'; YEL=$'\033[33m'; MAG=$'\033[35m'; RST=$'\033[0m'
else BOLD= DIM= GREEN= RED= YEL= MAG= RST=; fi
ts()    { date +%H:%M:%S; }
log()   { echo "${DIM}[$(ts)]${RST} $*"; }
ok()    { echo "${DIM}[$(ts)]${RST}   ${GREEN}✔ $*${RST}"; }
warn()  { echo "${DIM}[$(ts)]${RST}   ${YEL}! $*${RST}"; }
fail()  { echo "${DIM}[$(ts)]${RST}   ${RED}✘ $*${RST}"; }
now()   { date +%s.%N; }
since() { python3 -c "print(f'{$(now) - $1:.0f}')"; }
jget()  { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)" 2>/dev/null; }
sql()   { docker exec postgres psql -U objstore -tAc "$1" 2>/dev/null; }
status(){ curl -s -m 5 "$API/internal/status"; }

# ---------------------------------------------------------------------------------------------- grafana markers
ANN_ID=""
grafana_start() {  # region annotation, closed by grafana_end
  ANN_ID=$(curl -s -m 3 -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' -X POST "$GRAFANA/api/annotations" \
    -d "$(python3 -c "import json,sys;print(json.dumps({'text':sys.argv[1],'tags':['objstore-event','chaos',sys.argv[2]]}))" "chaos: $1" "$2")" \
    | jget "d.get('id','')") || ANN_ID=""
}
grafana_end() {
  [ -n "$ANN_ID" ] || return 0
  curl -s -m 3 -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' -X PATCH "$GRAFANA/api/annotations/$ANN_ID" \
    -d "$(python3 -c "import json,sys,time;print(json.dumps({'timeEnd':int(time.time()*1000),'text':sys.argv[1]}))" "chaos: $1")" >/dev/null
  ANN_ID=""
}

# ---------------------------------------------------------------------------------------------- cluster state
running_workers() { docker ps --format '{{.Names}}' | grep -E '^worker[0-9]+$' | sort; }
healthy_workers() {  # running containers that the controller sees HEALTHY and not draining
  local st; st=$(status) || return
  for w in $(running_workers); do
    [ "$(jget "next((x['state'] for x in d['workers'] if x['worker_id']=='$w' and not x['draining']), '')" <<<"$st")" = HEALTHY ] && echo "$w"
  done
}
cluster_ok() {  # every running worker HEALTHY, nothing under-replicated, no repair queued or running
  local st n_run n_ok
  st=$(status) || return 1
  [ "$(jget "d['under_replicated_chunks']" <<<"$st")" = 0 ] || return 1
  [ "$(jget "d['repair_tasks'].get('PENDING',0)+d['repair_tasks'].get('IN_PROGRESS',0)" <<<"$st")" = 0 ] || return 1
  n_run=$(running_workers | wc -l)
  n_ok=$(for w in $(running_workers); do jget "next((x['state'] for x in d['workers'] if x['worker_id']=='$w'), '')" <<<"$st"; done | grep -c '^HEALTHY$')
  [ "$n_run" -ge 2 ] && [ "$n_run" = "$n_ok" ]
}
wait_until() {  # wait_until <timeout s> <condition...>; returns 1 on timeout
  local t=$1; shift; local end; end=$(python3 -c "print($(now)+$t)")
  while ! eval "$@" >/dev/null 2>&1; do
    python3 -c "import sys;sys.exit(0 if $(now) < $end else 1)" || return 1
    sleep 1
  done
}
under() { status | jget "d['under_replicated_chunks']"; }
metric() { curl -s -m 5 "$API/metrics" | awk -v m="objstore_$1" '$1==m {print $2}'; }

# ---------------------------------------------------------------------------------------------- test object
RUN=$(head -c 3 /dev/urandom | od -An -tx1 | tr -d ' \n')
TMP=$(mktemp -d); VICTIM=""; VICTIM_SHA=""
make_victim() {
  head -c $((8 * 1048576 + 4321)) /dev/urandom > "$TMP/chaos.bin"
  VICTIM_SHA=$(sha256sum < "$TMP/chaos.bin" | cut -d' ' -f1)
  VICTIM=$(curl -s -m 60 -F "file=@$TMP/chaos.bin;filename=chaos-$RUN.bin" -F 'metadata={"source":"chaos"}' \
           "$API/api/v1/objects" | jget "d['object_id']")
  [ -n "$VICTIM" ] || { fail "could not upload the test object"; exit 1; }
  log "test object chaos-$RUN.bin ($VICTIM, 9 chunks) for corrupt/delete faults"
}
victim_intact() { [ "$(curl -s -m 60 "$API/api/v1/objects/$VICTIM" | sha256sum | cut -d' ' -f1)" = "$VICTIM_SHA" ]; }
pick_replica() {  # "<chunk_id> <worker_id> <size>": a HEALTHY copy of the test object whose other copy is HEALTHY too
  local ws; ws=$(healthy_workers | sed "s/.*/'&'/" | paste -sd,)
  [ -n "$ws" ] || return 1
  sql "SELECT r.chunk_id||' '||r.worker_id||' '||c.size FROM replicas r JOIN chunks c USING (chunk_id)
       WHERE c.object_id='$VICTIM' AND r.state='HEALTHY' AND r.worker_id IN ($ws)
         AND (SELECT count(*) FROM replicas x WHERE x.chunk_id=r.chunk_id AND x.state='HEALTHY') >= 2
       ORDER BY random() LIMIT 1"
}
chunk_path() { echo "/data/chunks/${1:0:2}/$1"; }

# ---------------------------------------------------------------------------------------------- restore on exit
TOUCHED=""; TOUCHED_HOW=""
restore() {
  if [ -n "$TOUCHED" ]; then
    warn "restoring $TOUCHED ($TOUCHED_HOW)"
    case "$TOUCHED_HOW" in
      paused)  docker unpause "$TOUCHED" >/dev/null 2>&1 ;;
      stopped) docker start "$TOUCHED" >/dev/null 2>&1 ;;
    esac
    TOUCHED=""
  fi
}
summary_and_exit() {
  trap - INT TERM EXIT
  echo
  restore
  grafana_end "interrupted"
  if [ ${#RESULTS[@]} -gt 0 ]; then
    echo "${BOLD}Chaos summary (run $RUN)${RST}"
    printf "  %-3s %-11s %-9s %-10s %-9s %s\n" "#" "fault" "target" "detected" "healed" "result"
    for r in "${RESULTS[@]}"; do echo "  $r"; done
  fi
  if [ -n "$VICTIM" ]; then
    curl -s -o /dev/null -m 30 -X DELETE "$API/api/v1/objects/$VICTIM" && log "test object deleted"
  fi
  rm -rf "$TMP"
  exit "${1:-0}"
}
RESULTS=()
trap 'summary_and_exit 130' INT TERM
trap 'summary_and_exit $?' EXIT

record() {  # record <fault> <target> <detected s> <healed s> <result>
  RESULTS+=("$(printf "%-3s %-11s %-9s %-10s %-9s %s" "$ROUND" "$1" "$2" "$3" "$4" "$5")")
}

# ---------------------------------------------------------------------------------------------- faults
fault_stop() {
  local hw w t0 td th n
  hw=($(healthy_workers))
  [ ${#hw[@]} -ge 3 ] || { warn "only ${#hw[@]} healthy worker(s): skipping stop (needs 3 so 2 copies stay possible)"; return 2; }
  w=${hw[RANDOM % ${#hw[@]}]}
  n=$(sql "SELECT count(*) FROM replicas WHERE worker_id='$w' AND state='HEALTHY'")
  log "${MAG}${BOLD}fault: docker stop $w${RST} ${DIM}($w holds $n healthy copies)${RST}"
  grafana_start "stop $w" stop
  t0=$(now); docker stop "$w" >/dev/null; TOUCHED=$w; TOUCHED_HOW=stopped
  if wait_until 60 "[ \"\$(status | jget \"next(x['state'] for x in d['workers'] if x['worker_id']=='$w')\")\" = DEAD ]"; then
    td=$(since "$t0"); ok "detected: controller marked $w DEAD after ${td}s; $(under) chunk(s) under-replicated"
  else fail "$w was not marked DEAD within 60 s"; record stop "$w" "-" "-" FAILED; return 1; fi
  if wait_until $((DOWNTIME > 30 ? DOWNTIME : 30)) '[ "$(under)" = 0 ]'; then
    ok "re-replicated: every chunk has 2 healthy copies again after $(since "$t0")s, with $w still down"
  else warn "still $(under) under-replicated chunk(s) after ${DOWNTIME}s down"; fi
  local left; left=$(python3 -c "print(max(0, int($DOWNTIME - ($(now) - $t0))))")
  [ "$left" -gt 0 ] && { log "  ${DIM}keeping $w down for another ${left}s (--downtime $DOWNTIME)${RST}"; sleep "$left"; }
  log "docker start $w"
  docker start "$w" >/dev/null; TOUCHED=""
  if wait_until "$HEAL_TIMEOUT" cluster_ok; then
    th=$(since "$t0"); ok "healed: $w HEALTHY again, cluster healthy (${th}s after the fault; its extra copies are trimmed in the background)"
    grafana_end "stop $w — DEAD after ${td}s, healed after ${th}s"; record stop "$w" "${td}s" "${th}s" ok
  else fail "cluster not healthy ${HEAL_TIMEOUT}s after restarting $w"; record stop "$w" "${td}s" "-" FAILED; return 1; fi
}

fault_pause() {
  local hw w t0 h0 h1 d0 d1
  hw=($(healthy_workers))
  [ ${#hw[@]} -ge 2 ] || { warn "fewer than 2 healthy workers: skipping pause"; return 2; }
  w=${hw[RANDOM % ${#hw[@]}]}
  h0=$(metric read_hedge_wins_total); d0=$(metric workers_marked_dead_total)
  log "${MAG}${BOLD}fault: docker pause $w for ${PAUSE}s${RST} ${DIM}(process frozen: it accepts connections, answers nothing)${RST}"
  grafana_start "pause $w ${PAUSE}s" pause
  t0=$(now); docker pause "$w" >/dev/null; TOUCHED=$w; TOUCHED_HOW=paused
  sleep "$PAUSE"
  docker unpause "$w" >/dev/null; TOUCHED=""
  h1=$(metric read_hedge_wins_total); d1=$(metric workers_marked_dead_total)
  local wins; wins=$(python3 -c "print(int(${h1:-0} - ${h0:-0}))")
  if [ "$wins" -gt 0 ]; then ok "reads routed around it: $wins chunk read(s) answered by the other copy (hedged)"
  else log "  ${DIM}no hedged reads (start scripts/load.py to see reads routed around the frozen worker)${RST}"; fi
  if [ "${d1:-0}" = "${d0:-0}" ]; then ok "$w was not marked DEAD (slow is not dead)"
  else warn "$w was marked DEAD during the pause (pause ≥ heartbeat timeout?)"; fi
  if wait_until "$HEAL_TIMEOUT" cluster_ok; then
    local th; th=$(since "$t0"); ok "healthy (${th}s after the fault)"
    grafana_end "pause $w — $wins hedged read(s), healthy after ${th}s"; record pause "$w" "-" "${th}s" "ok, $wins hedged"
  else fail "cluster not healthy after the pause"; record pause "$w" "-" "-" FAILED; return 1; fi
}

fault_bytes() {  # fault_bytes corrupt|delete
  local kind=$1 pick cid w size p t0 td th task
  pick=$(pick_replica) || true
  [ -n "$pick" ] || { warn "no chunk of the test object with 2 healthy copies on healthy workers: skipping $kind"; return 2; }
  read -r cid w size <<<"$pick"; p=$(chunk_path "$cid")
  docker exec "$w" curl -s -X POST localhost:9000/internal/cache/clear >/dev/null 2>&1   # bytes come from disk
  t0=$(now)
  local t0sql; t0sql=$(sql "SELECT now()")
  if [ "$kind" = corrupt ]; then
    local off=$(( size > 200 ? RANDOM % (size - 100) : 0 ))
    log "${MAG}${BOLD}fault: corrupt chunk ${cid: -6} on $w${RST} ${DIM}(overwrite 5 bytes at offset $off of $p)${RST}"
    grafana_start "corrupt chunk on $w" corrupt
    docker exec "$w" sh -c "printf CHAOS | dd of=$p bs=1 seek=$off conv=notrunc 2>/dev/null"
  else
    log "${MAG}${BOLD}fault: delete chunk ${cid: -6} from $w${RST} ${DIM}(rm $p)${RST}"
    grafana_start "delete chunk on $w" delete
    docker exec "$w" rm -f "$p"
  fi
  if [ "$SCRUB" = 1 ]; then
    docker exec "$w" curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null 2>&1
    log "  ${DIM}scrubber triggered on $w (normally it runs every SCRUB_INTERVAL; client reads also detect it)${RST}"
  fi
  if wait_until "$HEAL_TIMEOUT" "[ -n \"\$(sql \"SELECT 1 FROM repair_tasks WHERE chunk_id='$cid' AND created_at >= '$t0sql' LIMIT 1\")\" ]"; then
    td=$(since "$t0")
    local how; how=$(sql "SELECT state FROM replicas WHERE chunk_id='$cid' AND worker_id='$w'")
    if [ "$how" = HEALTHY ]; then ok "detected after ${td}s: repair task queued (and already finished)"
    else ok "detected after ${td}s: replica marked $how, repair task queued"; fi
  else fail "not detected within ${HEAL_TIMEOUT}s"; record "$kind" "$w" "-" "-" FAILED; return 1; fi
  if wait_until "$HEAL_TIMEOUT" "[ \"\$(sql \"SELECT state FROM repair_tasks WHERE chunk_id='$cid' AND created_at >= '$t0sql' ORDER BY created_at DESC LIMIT 1\")\" = DONE ] && cluster_ok"; then
    th=$(since "$t0")
    task=$(sql "SELECT source_worker_id||' -> '||target_worker_id FROM repair_tasks WHERE chunk_id='$cid' AND created_at >= '$t0sql' ORDER BY created_at DESC LIMIT 1")
    ok "healed after ${th}s: copied $task, checksum verified"
  else fail "not repaired within ${HEAL_TIMEOUT}s"; record "$kind" "$w" "${td}s" "-" FAILED; return 1; fi
  if victim_intact; then ok "test object downloads byte-identical to the original"
  else fail "test object differs from the original!"; record "$kind" "$w" "${td}s" "${th}s" "WRONG BYTES"; return 1; fi
  grafana_end "$kind chunk on $w — detected after ${td}s, healed after ${th}s"
  record "$kind" "$w" "${td}s" "${th}s" ok
}

fault_controller() {
  local t0 td th
  log "${MAG}${BOLD}fault: docker restart controller${RST} ${DIM}(the API is down until it is back)${RST}"
  grafana_start "restart controller" controller
  t0=$(now); docker restart controller >/dev/null
  if wait_until 90 "curl -sf -m 2 $API/healthz"; then
    td=$(since "$t0"); ok "controller answering again after ${td}s"
  else fail "controller not back within 90 s"; record controller api "-" "-" FAILED; return 1; fi
  if wait_until "$HEAL_TIMEOUT" cluster_ok; then
    th=$(since "$t0"); ok "all workers re-attached and healthy (${th}s after the fault)"
  else fail "cluster not healthy after the restart"; record controller api "${td}s" "-" FAILED; return 1; fi
  if victim_intact; then ok "test object still byte-identical"; else fail "test object differs!"; return 1; fi
  grafana_end "restart controller — API back after ${td}s, healthy after ${th}s"
  record controller api "${td}s" "${th}s" ok
}

# ---------------------------------------------------------------------------------------------- main loop
echo "${BOLD}chaos: run $RUN, faults: ${FAULT_LIST[*]}, ${ROUNDS/#0/unlimited} round(s), ${INTERVAL}s apart${RST}"
curl -sf -m 5 "$API/healthz" >/dev/null || { fail "controller not reachable at $API"; exit 1; }
log "waiting for a healthy cluster before the first fault…"
wait_until 180 cluster_ok || { fail "cluster not healthy (a worker down or repairs pending); fix that first"; exit 1; }
ok "cluster healthy: $(healthy_workers | paste -sd' ')"
make_victim
ROUND=0; LAST=""
while [ "$ROUNDS" = 0 ] || [ "$ROUND" -lt "$ROUNDS" ]; do
  ROUND=$((ROUND + 1))
  # random fault, not the same one twice in a row (unless only one is allowed)
  F=${FAULT_LIST[RANDOM % ${#FAULT_LIST[@]}]}
  while [ ${#FAULT_LIST[@]} -gt 1 ] && [ "$F" = "$LAST" ]; do F=${FAULT_LIST[RANDOM % ${#FAULT_LIST[@]}]}; done
  LAST=$F
  echo; log "${BOLD}round $ROUND${RST}"
  case "$F" in
    stop) fault_stop ;; pause) fault_pause ;; corrupt) fault_bytes corrupt ;; delete) fault_bytes delete ;;
    controller) fault_controller ;;
  esac
  rc=$?
  if [ $rc = 1 ]; then fail "stopping: the system did not heal; inspect Grafana / docker compose logs"; exit 1; fi
  [ "$ROUNDS" != 0 ] && [ "$ROUND" -ge "$ROUNDS" ] && break
  log "${DIM}next fault in ${INTERVAL}s${RST}"
  sleep "$INTERVAL"
done
exit 0
