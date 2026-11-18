#!/usr/bin/env bash
# Phase 7: worker scrubbers + controller repair. Walks through the corruption-recovery demo
# (plan section 10.8) and the failure/edge cases around scrub reporting.
# Run from the project root with the stack up:  ./scripts/phase7_test.sh   (takes ~6 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()  { docker exec postgres psql -U objstore -tAc "$1"; }
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=3)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 3; done; bad "$d (timed out after ${t}s)"; return 1; }
badchunks() { sql "select count(*) from chunks c where c.object_id='$OID' and (select count(*) from replicas r join workers w using(worker_id) where r.chunk_id=c.chunk_id and r.state='HEALTHY' and w.state<>'DEAD')<>2"; }
rstate() { sql "select state from replicas where chunk_id='$1' and worker_id='$2'"; }
cleanup() { rm -rf $T; unset SCRUB_INTERVAL RECONCILE_INTERVAL; docker compose up -d >/dev/null 2>&1; }
trap cleanup EXIT
post() { docker exec controller curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d "$2" localhost:8080$1; }
postb() { docker exec controller curl -s -X POST -H 'Content-Type: application/json' -d "$2" localhost:8080$1; }

echo "== Setup: scrub every 40s (staggered), slow reconcile so only the scrubber detects problems =="
export SCRUB_INTERVAL=40 RECONCILE_INTERVAL=600
docker compose up -d >/dev/null 2>&1
sleep 12; wait_until "cluster healthy" 60 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"
head -c $((5*1048576+1234)) /dev/urandom > $T/obj.bin; SUM=$(sha256sum < $T/obj.bin)
OID=$(curl -s -F file=@$T/obj.bin $API/api/v1/objects | jget "d['object_id']")
echo "  test object $OID"

echo "== A. Required demo: silent corruption found by the worker scrubber (no client read involved) =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][2]['chunk_id']" <<<"$M"); EXP=$(jget "d['chunks'][2]['checksum']" <<<"$M")
WBAD=$(jget "d['chunks'][2]['replicas'][0]['worker_id']" <<<"$M"); WGOOD=$(jget "d['chunks'][2]['replicas'][1]['worker_id']" <<<"$M")
echo "  step 1-2: chunk $CID"; echo "            expected checksum $EXP"; echo "            replicas: $WBAD and $WGOOD"
check "two replicas on different workers" "$([ "$WBAD" != "$WGOOD" ] && echo yes)" yes
docker exec $WBAD sh -c "printf 'CORRUPTED' | dd of=/data/chunks/${CID:0:2}/$CID bs=1 seek=100 conv=notrunc 2>/dev/null"
echo "  step 3: modified bytes inside the replica on $WBAD (size unchanged)"
wait_until "step 4-5: scrubber reported CORRUPT with expected/actual checksums" 120 "[ \"\$(sql \"select count(*) from scrub_results where chunk_id='$CID' and worker_id='$WBAD' and status='CORRUPT'\")\" -ge 1 ]"
R=$(sql "select expected_checksum||' | '||actual_checksum from scrub_results where chunk_id='$CID' and worker_id='$WBAD' and status='CORRUPT' limit 1")
echo "            scrub result: expected | actual = $R"
check "reported expected == authoritative checksum" "${R%% |*}" "$EXP"
check "reported actual differs" "$([ "${R##*| }" != "$EXP" ] && echo yes)" yes
wait_until "step 6-9: replica repaired -> HEALTHY again" 90 "[ \"\$(rstate $CID $WBAD)\" = HEALTHY ]"
check "step 6: repair task was created by the scrub report" "$(sql "select count(*) from repair_tasks where chunk_id='$CID' and reason like 'scrub: CORRUPT%'")" 1
check "step 7: healthy replica $WGOOD was the copy source" "$(sql "select source_worker_id from repair_tasks where chunk_id='$CID' and reason like 'scrub: CORRUPT%'")" "$WGOOD"
check "step 8: target $WBAD received it (task target)" "$(sql "select target_worker_id from repair_tasks where chunk_id='$CID' and reason like 'scrub: CORRUPT%'")" "$WBAD"
check "step 9: task DONE" "$(sql "select state from repair_tasks where chunk_id='$CID' and reason like 'scrub: CORRUPT%'")" DONE
check "step 9: 2 healthy replicas on different workers again" "$(badchunks)" 0
check "replaced file serves the original bytes" "$(docker exec $WBAD curl -s localhost:9000/internal/chunks/$CID | sha256sum | cut -d' ' -f1)" "${EXP#sha256:}"
check "step 10: full download matches original" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
docker compose logs controller 2>&1 | grep "repaired $CID" | tail -1 | sed 's/^/            controller log: /'

echo "== B. Missing chunk file (checksum record left behind) -> scrubber reports MISSING =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][3]['chunk_id']" <<<"$M"); WM=$(jget "d['chunks'][3]['replicas'][0]['worker_id']" <<<"$M")
docker exec $WM rm -f /data/chunks/${CID:0:2}/$CID
wait_until "scrubber reported MISSING" 150 "[ \"\$(sql \"select count(*) from scrub_results where chunk_id='$CID' and worker_id='$WM' and status='MISSING'\")\" -ge 1 ]"
wait_until "missing replica restored" 90 "[ \"\$(rstate $CID $WM)\" = HEALTHY ] && docker exec $WM test -f /data/chunks/${CID:0:2}/$CID"
check "download identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== C. Manual trigger + per-worker scrub status =="
for w in worker1 worker2 worker3 worker4; do docker exec $w curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null; done
sleep 8
check "every live worker finished at least one pass" "$(curl -s $API/internal/status | jget "all(w['last_scrub'] and w['last_scrub']['passes']>=1 for w in d['workers'] if w['state']!='DEAD')")" True
curl -s $API/internal/status | jget "'\n'.join('    %s: passes=%d checked=%d bytes=%d corrupt=%d missing=%d (%.2fs)'%(w['worker_id'],w['last_scrub']['passes'],w['last_scrub']['chunks_checked'],w['last_scrub']['bytes_checked'],w['last_scrub']['corrupt_found'],w['last_scrub']['missing_found'],w['last_scrub']['duration_seconds']) for w in d['workers'])"
check "no false positives: only the deliberately corrupted chunk (index 2) has a CORRUPT finding" "$(sql "select string_agg(distinct right(chunk_id,6), ',') from scrub_results where status='CORRUPT' and chunk_id like '$OID%'")" "000002"
S=$(docker exec worker1 curl -s localhost:9000/internal/scrub/status)
check "worker exposes scrub metrics (bytes, duration)" "$(jget "d['bytes_checked']>0 and d['duration_seconds']>=0" <<<"$S")" True

echo "== D. Controller down: reports survive in the durable queue and are delivered later =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][4]['chunk_id']" <<<"$M"); WQ=$(jget "d['chunks'][4]['replicas'][0]['worker_id']" <<<"$M")
docker stop controller >/dev/null
docker exec $WQ sh -c "printf 'CORRUPTED' | dd of=/data/chunks/${CID:0:2}/$CID bs=1 seek=50 conv=notrunc 2>/dev/null"
docker exec $WQ curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null
sleep 8
check "report waits in worker's durable queue (controller down)" "$(docker exec $WQ sh -c 'ls /data/report_queue | grep -c CORRUPT')" 1
check "delivery failures are counted" "$(docker exec $WQ curl -s localhost:9000/internal/scrub/status | jget "d['report_delivery_failures']>0")" True
echo "  restarting the worker too: the queue must survive a worker restart"
docker restart $WQ >/dev/null; sleep 6
check "queued report still on disk after worker restart" "$(docker exec $WQ sh -c 'ls /data/report_queue | grep -c CORRUPT')" 1
docker start controller >/dev/null
wait_until "report delivered after controller returned" 90 "[ \"\$(docker exec $WQ sh -c 'ls /data/report_queue | grep -c CORRUPT')\" = 0 ]"
check "controller recorded the finding" "$(sql "select count(*) from scrub_results where chunk_id='$CID' and worker_id='$WQ' and status='CORRUPT'")" 1
wait_until "and repaired it" 90 "[ \"\$(rstate $CID $WQ)\" = HEALTHY ]"
check "download identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== E. Report validation, idempotency, stale/false reports =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][0]['chunk_id']" <<<"$M"); WV=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ); OLD="2020-01-01T00:00:00Z"
check "unknown worker -> 404" "$(post /internal/workers/scrub-results '{"worker_id":"ghost","chunk_id":"x","status":"CORRUPT","checked_at":"'$NOW'"}')" 404
check "invalid status -> 400" "$(post /internal/workers/scrub-results '{"worker_id":"'$WV'","chunk_id":"'$CID'","status":"BROKEN","checked_at":"'$NOW'"}')" 400
check "missing field -> 400" "$(post /internal/workers/scrub-results '{"worker_id":"'$WV'"}')" 400
R=$(postb /internal/workers/scrub-results '{"worker_id":"'$WV'","chunk_id":"no-such-chunk","status":"CORRUPT","checked_at":"'$NOW'"}')
check "report for unknown chunk is acknowledged but ignored" "$(jget "d['action']" <<<"$R")" ignored
R=$(postb /internal/workers/scrub-results '{"worker_id":"'$WV'","chunk_id":"'$CID'","status":"CORRUPT","expected_checksum":"sha256:aa","actual_checksum":"sha256:bb","checked_at":"'$OLD'"}')
check "stale report (older than replica write) ignored" "$(jget "d['action']" <<<"$R")" ignored
check "replica still HEALTHY after stale report" "$(rstate $CID $WV)" HEALTHY
GOODSUM=$(jget "d['chunks'][0]['checksum']" <<<"$M")
R=$(postb /internal/workers/scrub-results '{"worker_id":"'$WV'","chunk_id":"'$CID'","status":"CORRUPT","expected_checksum":"sha256:aa","actual_checksum":"'$GOODSUM'","checked_at":"'$NOW'"}')
check "false positive (bytes match authoritative checksum) ignored" "$(jget "d['action']" <<<"$R")" ignored
check "replica still HEALTHY after false positive" "$(rstate $CID $WV)" HEALTHY
sleep 2; NOW2=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BODY='{"worker_id":"'$WV'","chunk_id":"'$CID'","status":"CORRUPT","expected_checksum":"'$GOODSUM'","actual_checksum":"sha256:'$(printf 'x%.0s' $(seq 64) | sed 's/x/0/g')'","checked_at":"'$NOW2'"}'
R1=$(postb /internal/workers/scrub-results "$BODY"); R2=$(postb /internal/workers/scrub-results "$BODY")
check "genuine report marks replica corrupt" "$(jget "d['action']" <<<"$R1")" marked_corrupt
check "duplicate delivery is harmless (idempotent)" "$(jget "d['action']" <<<"$R2")" ignored
check "stored once" "$(sql "select count(*) from scrub_results where chunk_id='$CID' and worker_id='$WV' and checked_at='$NOW2'")" 1
check "only one repair task for it" "$(sql "select count(*) from repair_tasks where chunk_id='$CID' and reason like 'scrub:%'")" 1
wait_until "repair (re-copy of already good bytes) finishes, replica HEALTHY" 60 "[ \"\$(rstate $CID $WV)\" = HEALTHY ]"
check "cluster back to 2 healthy per chunk" "$(badchunks)" 0

curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
