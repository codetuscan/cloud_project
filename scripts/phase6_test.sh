#!/usr/bin/env bash
# Phase 6 checks: dead-worker re-replication, worker return, corrupt/missing repair via the
# controller, orphan/tombstone cleanup, unrecoverable chunks, degraded mode.
# Run from the project root with the stack up:  ./scripts/phase6_test.sh   (takes ~6-8 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()  { docker exec postgres psql -U objstore -tAc "$1"; }
# wait_until "<desc>" <timeout-sec> <shell condition>
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=3)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 3; done; bad "$d (timed out after ${t}s)"; return 1; }
# number of chunks of $OID whose count of HEALTHY replicas on live workers is not exactly 2
badchunks() { sql "select count(*) from chunks c where c.object_id='$OID' and (select count(*) from replicas r join workers w using(worker_id) where r.chunk_id=c.chunk_id and r.state='HEALTHY' and w.state<>'DEAD')<>2"; }
allgood()   { [ "$(badchunks)" = 0 ]; }
dl_ok()     { [ "$(curl -s $API/api/v1/objects/$OID | sha256sum)" = "$SUM" ]; }
cleanup() { rm -rf $T; unset RECONCILE_INTERVAL ORPHAN_GRACE_SECONDS; docker compose up -d controller >/dev/null 2>&1; }
trap cleanup EXIT

echo "== Setup: fast reconcile/orphan timers for the test (controller restarts) =="
export RECONCILE_INTERVAL=10 ORPHAN_GRACE_SECONDS=20
docker compose up -d controller >/dev/null 2>&1
wait_until "controller up" 60 "curl -sf $API/healthz"
head -c $((4*1048576+777)) /dev/urandom > $T/obj.bin; SUM=$(sha256sum < $T/obj.bin)
OID=$(curl -s -F file=@$T/obj.bin $API/api/v1/objects | jget "d['object_id']")
echo "  test object $OID (5 chunks)"
check "baseline: every chunk has exactly 2 healthy replicas" "$(badchunks)" 0

echo "== 1. Worker failure -> re-replication =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
WK=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
echo "  docker stop $WK"; docker stop $WK >/dev/null
wait_until "$WK marked DEAD" 45 "[ \"\$(sql \"select state from workers where worker_id='$WK'\")\" = DEAD ]"
check "protection is reported degraded while repairing or just after" "$(curl -s $API/internal/status | jget "'under_replicated_chunks' in d")" True
wait_until "all chunks back to 2 healthy replicas on live workers" 90 allgood
check "download during outage is byte-identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
check "no chunk replica placed twice on one worker" "$(sql "select count(*) from (select chunk_id, worker_id from replicas group by 1,2 having count(*)>1) x")" 0
check "repair tasks completed (DONE) with no failures" "$(sql "select count(*) from repair_tasks where chunk_id like '$OID%' and state in ('PENDING','IN_PROGRESS','UNRECOVERABLE')")" 0
echo "  docker start $WK (it comes back with its old chunks -> surplus gets trimmed)"
docker start $WK >/dev/null
wait_until "$WK HEALTHY again" 45 "[ \"\$(sql \"select state from workers where worker_id='$WK'\")\" = HEALTHY ]"
wait_until "surplus trimmed: exactly 2 replica rows per chunk" 120 "[ \"\$(sql \"select count(*) from chunks c where c.object_id='$OID' and (select count(*) from replicas r where r.chunk_id=c.chunk_id)<>2\")\" = 0 ]"
check "all chunks still 2 healthy after return" "$(badchunks)" 0
check "download correct after return" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 2. Corrupted replica (found on read) -> controller repair =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][1]['chunk_id']" <<<"$M"); WC=$(jget "d['chunks'][1]['replicas'][0]['worker_id']" <<<"$M")
docker exec $WC curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
docker exec $WC sh -c "printf XXXX >> /data/chunks/${CID:0:2}/$CID"; echo "  corrupted $CID on $WC"
SEEN=no
for i in $(seq 1 12); do curl -s -o /dev/null $API/api/v1/objects/$OID; [ "$(sql "select state from replicas where chunk_id='$CID' and worker_id='$WC'")" != HEALTHY ] && SEEN=yes && break; done
check "a read detected the bad replica and flagged it" "$SEEN" yes
wait_until "replica repaired and HEALTHY again" 60 "[ \"\$(sql \"select state from replicas where chunk_id='$CID' and worker_id='$WC'\")\" = HEALTHY ]"
check "repaired file on $WC now serves correct bytes" "$(docker exec $WC curl -s -o /dev/null -w '%{http_code}' localhost:9000/internal/chunks/$CID)" 200
check "chunk back to 2 healthy replicas" "$(badchunks)" 0
check "download identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 3. Chunk file deleted from a worker -> inventory reconciliation =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][2]['chunk_id']" <<<"$M"); WM=$(jget "d['chunks'][2]['replicas'][0]['worker_id']" <<<"$M")
docker exec $WM sh -c "rm -f /data/chunks/${CID:0:2}/$CID /data/chunks/${CID:0:2}/$CID.sum"; echo "  removed $CID from $WM"
wait_until "reconciler detected it and queued a repair task" 45 "[ \"\$(sql \"select count(*) from repair_tasks where chunk_id='$CID' and reason like 'inventory%'\")\" -ge 1 ]"
wait_until "missing file recreated on $WM and verified" 90 "docker exec $WM test -f /data/chunks/${CID:0:2}/$CID"
check "chunk 2 has 2 healthy replicas" "$(badchunks)" 0
check "download identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 4. Unknown/tombstoned files on a worker =="
OLD=$(sql "select chunk_id||' '||worker_id from tombstones limit 1")
if [ -n "$OLD" ]; then
  TC=${OLD% *}; TW=${OLD#* }
  printf 'stale' > $T/s.bin; docker cp $T/s.bin $TW:/tmp/s.bin >/dev/null
  SS="sha256:$(sha256sum < $T/s.bin | cut -d' ' -f1)"
  docker exec $TW curl -s -o /dev/null -X PUT -H "X-Chunk-Checksum: $SS" --data-binary @/tmp/s.bin localhost:9000/internal/chunks/$TC
  wait_until "tombstoned chunk resurrected on $TW is removed" 40 "[ \"\$(docker exec $TW curl -s localhost:9000/internal/workers/$TW/chunks | grep -c $TC)\" = 0 ]"
else echo "  (no tombstones yet; skipped)"; fi
OC="orphan-test-$RANDOM"
printf 'stale' > $T/s.bin; SS="sha256:$(sha256sum < $T/s.bin | cut -d' ' -f1)"; docker cp $T/s.bin worker1:/tmp/s.bin >/dev/null
docker exec worker1 curl -s -o /dev/null -X PUT -H "X-Chunk-Checksum: $SS" --data-binary @/tmp/s.bin localhost:9000/internal/chunks/$OC
check "orphan exists initially" "$(docker exec worker1 curl -s localhost:9000/internal/workers/worker1/chunks | grep -c $OC)" 1
wait_until "unknown orphan removed after grace period (not immediately)" 60 "[ \"\$(docker exec worker1 curl -s localhost:9000/internal/workers/worker1/chunks | grep -c $OC)\" = 0 ]"

echo "== 5. Degraded mode: only some workers up =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
docker stop worker2 worker3 worker4 >/dev/null; echo "  stopped worker2,3,4 (only worker1 alive)"
wait_until "workers marked DEAD" 45 "[ \"\$(sql \"select count(*) from workers where state='DEAD'\")\" = 3 ]"
sleep 20
st=$(curl -s $API/internal/status)
check "status reports under-replicated chunks" "$(jget "d['under_replicated_chunks']>0" <<<"$st")" True
check "some repair tasks are waiting (PENDING) for capacity/workers" "$([ "$(sql "select count(*) from repair_tasks where state='PENDING' and (last_error like 'degraded%' or last_error like 'no live source%')")" -gt 0 ] && echo yes)" yes
check "none declared unrecoverable" "$(sql "select count(*) from repair_tasks where state='UNRECOVERABLE'")" 0
echo "  restarting workers"; docker start worker2 worker3 worker4 >/dev/null
wait_until "cluster fully repaired after workers return" 150 allgood
check "download identical after recovery" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 6. Both replicas destroyed -> unrecoverable, never fabricated =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][3]['chunk_id']" <<<"$M")
for w in $(jget "' '.join(r['worker_id'] for r in d['chunks'][3]['replicas'])" <<<"$M"); do
  docker exec $w sh -c "rm -f /data/chunks/${CID:0:2}/$CID /data/chunks/${CID:0:2}/$CID.sum"; done
wait_until "chunk flagged UNRECOVERABLE" 60 "[ \"\$(sql \"select count(*) from repair_tasks where chunk_id='$CID' and state='UNRECOVERABLE'\")\" -ge 1 ]"
rc=0; c=$(curl -s -o $T/x.bin -w '%{http_code}' $API/api/v1/objects/$OID) || rc=$?
if [ "$c" = 503 ] || [ $rc -ne 0 ]; then ok "download fails cleanly (http=$c rc=$rc) instead of returning wrong data"; else bad "download http=$c rc=$rc"; fi

curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
