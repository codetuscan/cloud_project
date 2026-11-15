#!/usr/bin/env bash
# Plan §17: restarts. Metadata + staged uploads survive a controller restart, worker chunks survive
# worker restarts, PostgreSQL restarts are tolerated, and the whole stack can be bounced.
# Run from the project root with the stack up:  ./scripts/phase10_persistence_test.sh  (~4 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=2)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 2; done; bad "$d (timed out after ${t}s)"; return 1; }
cluster_up() { wait_until "controller healthy + 4 workers HEALTHY" 120 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"; }
trap 'rm -rf $T; docker start worker1 worker2 worker3 worker4 controller postgres >/dev/null 2>&1' EXIT
up() { curl -s -F file=@"$1" ${2:+-F "metadata=$2"} $API/api/v1/objects | jget "d['object_id']"; }
cluster_up

head -c $((3*1048576+321)) /dev/urandom > $T/a.bin; SUM=$(sha256sum < $T/a.bin)
OID=$(up $T/a.bin '{"owner":"alice"}'); echo "  object $OID"
META_BEFORE=$(curl -s $API/api/v1/objects/$OID/metadata | jget "(d['checksum'],d['user_metadata'],[(c['chunk_id'],sorted(r['worker_id'] for r in c['replicas'])) for c in d['chunks']])")

echo "== 1. Controller restart: PostgreSQL metadata survives =="
docker restart controller >/dev/null
wait_until "controller back" 60 "curl -sf $API/healthz"
check "metadata, checksums, chunk placement unchanged" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "(d['checksum'],d['user_metadata'],[(c['chunk_id'],sorted(r['worker_id'] for r in c['replicas'])) for c in d['chunks']])")" "$META_BEFORE"
check "object still downloads identically" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
check "object still listed" "$(curl -s "$API/api/v1/objects?limit=1000" | grep -c $OID)" 1
cluster_up

echo "== 2. Controller restart while an upload is waiting in staging =="
docker stop worker2 worker3 worker4 >/dev/null
R=$(curl -s -w '\n%{http_code}' -F file=@$T/a.bin $API/api/v1/objects); PID=$(sed '$d' <<<"$R" | jget "d['error']['object_id']")
check "upload could not finish (only 1 worker) -> 503, kept in staging" "$(tail -n1 <<<"$R")" 503
check "staged file is on the persistent staging volume" "$(docker exec controller sh -c "ls /staging | grep -c $PID")" 1
docker restart controller >/dev/null
wait_until "controller back" 60 "curl -sf $API/healthz"
check "staged file survived the controller restart (no re-upload needed)" "$(docker exec controller sh -c "ls /staging | grep -c $PID")" 1
check "upload is still unfinished (not visible yet)" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$PID/metadata)" 404
docker start worker2 worker3 worker4 >/dev/null
wait_until "controller resumed the staged upload on its own" 120 "[ \"\$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$PID/metadata)\" = 200 ]"
check "resumed object is byte-identical to the original" "$(curl -s $API/api/v1/objects/$PID | sha256sum)" "$SUM"
check "no duplicate objects: exactly one committed object has this upload id" "$(curl -s "$API/api/v1/objects?limit=1000" | grep -c $PID)" 1
check "staging cleaned up" "$(docker exec controller sh -c "ls /staging | grep -c $PID")" 0
cluster_up

echo "== 3. Each worker restarted: its persistent chunks remain =="
for w in worker1 worker2 worker3 worker4; do
  before=$(docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | jget "sorted((c['chunk_id'],c['checksum']) for c in d['chunks'] if c['chunk_id'].startswith('$OID') or c['chunk_id'].startswith('$PID'))")
  docker restart $w >/dev/null; sleep 6
  after=$(docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | jget "sorted((c['chunk_id'],c['checksum']) for c in d['chunks'] if c['chunk_id'].startswith('$OID') or c['chunk_id'].startswith('$PID'))")
  check "$w: this test's chunk files (ids + checksums) identical after restart" "$after" "$before"
  [ -n "$before" ] && [ "$before" != "[]" ] || note_empty=1
done
cluster_up
check "download after all worker restarts" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 4. PostgreSQL down and back: controller reconnects, answers 503 meanwhile =="
docker stop postgres >/dev/null
check "database down: an upload that needs it fails with 503 (not 500)" "$(curl -s -o /dev/null -w '%{http_code}' -F file=@$T/a.bin $API/api/v1/objects)" 503
check "database down: error body is the standard JSON" "$(curl -s -X DELETE $API/api/v1/objects/00000000-0000-0000-0000-000000000000 | jget "d['error']['code']")" service_unavailable
echo "  keeping the database down for 20 s (longer than the 15 s heartbeat timeout) to prove workers are not wrongly declared DEAD"
sleep 20
docker start postgres >/dev/null
wait_until "controller reports the database healthy again" 90 "curl -sf $API/internal/status"
sleep 8
check "all 4 workers still HEALTHY after the outage (grace period worked)" "$(curl -s $API/internal/status | jget "sum(1 for w in d['workers'] if w['state']=='HEALTHY')")" 4
check "no repair storm was triggered by the outage" "$(curl -s $API/internal/status | jget "d['repair_tasks'].get('PENDING',0)+d['repair_tasks'].get('IN_PROGRESS',0)")" 0
check "metadata intact after the database restarted" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "d['checksum']")" "sha256:${SUM%% *}"
check "download works after the database restarted" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
NEW=$(up $T/a.bin); check "new uploads work after the database restarted" "$([ -n "$NEW" ] && curl -s $API/api/v1/objects/$NEW | sha256sum)" "$SUM"
cluster_up

echo "== 5. Whole stack restart (docker compose restart) =="
docker compose restart >/dev/null 2>&1
cluster_up
check "object intact after full restart" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
check "all chunks still have 2 healthy replicas" "$(curl -s $API/internal/status | jget "d['under_replicated_chunks']")" 0

for id in $OID $PID ${NEW:-}; do curl -s -o /dev/null -X DELETE $API/api/v1/objects/$id; done
echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
