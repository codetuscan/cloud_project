#!/usr/bin/env bash
# Phase 2+3 checks: worker chunk storage, registration, heartbeat, dead-worker detection.
# Run from the project root with the stack up:  ./scripts/phase2_3_test.sh
set -u
API=${API:-http://localhost:8090}
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
w() { docker exec worker1 "$@"; }       # run a command inside worker1
W=http://localhost:9000                 # worker1's own address, from inside worker1

echo "== Phase 2: worker chunk storage (worker1) =="
CID="test-chunk-$RANDOM"
head -c 300000 /dev/urandom > /tmp/p23_chunk.bin
SUM="sha256:$(sha256sum /tmp/p23_chunk.bin | cut -d' ' -f1)"
docker cp /tmp/p23_chunk.bin worker1:/tmp/chunk.bin >/dev/null

code=$(w curl -s -o /dev/null -w '%{http_code}' -X PUT -H "X-Chunk-Checksum: $SUM" --data-binary @/tmp/chunk.bin $W/internal/chunks/$CID)
check "PUT new chunk -> 201" "$code" 201
code=$(w curl -s -o /dev/null -w '%{http_code}' -X PUT -H "X-Chunk-Checksum: $SUM" --data-binary @/tmp/chunk.bin $W/internal/chunks/$CID)
check "PUT same chunk again (idempotent) -> 200" "$code" 200

head -c 1000 /dev/urandom > /tmp/p23_other.bin; docker cp /tmp/p23_other.bin worker1:/tmp/other.bin >/dev/null
OTHER="sha256:$(sha256sum /tmp/p23_other.bin | cut -d' ' -f1)"
code=$(w curl -s -o /dev/null -w '%{http_code}' -X PUT -H "X-Chunk-Checksum: $OTHER" --data-binary @/tmp/other.bin $W/internal/chunks/$CID)
check "PUT same id, different content -> 409" "$code" 409

code=$(w curl -s -o /dev/null -w '%{http_code}' -X PUT -H "X-Chunk-Checksum: $SUM" --data-binary @/tmp/other.bin $W/internal/chunks/bad-sum-$RANDOM)
check "PUT body not matching checksum header -> 400" "$code" 400
code=$(w curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary @/tmp/other.bin $W/internal/chunks/nohdr-$RANDOM)
check "PUT without checksum header -> 400" "$code" 400
code=$(w curl -s -o /dev/null -w '%{http_code}' $W/internal/chunks/..%2Fetc)
check "path-traversal chunk id rejected (400/404)" "$([ "$code" = 400 ] || [ "$code" = 404 ] && echo ok)" ok

w curl -s $W/internal/chunks/$CID -o /tmp/got.bin
docker cp worker1:/tmp/got.bin /tmp/p23_got.bin >/dev/null
check "GET returns identical bytes" "$(sha256sum < /tmp/p23_got.bin)" "$(sha256sum < /tmp/p23_chunk.bin)"
n=$(w curl -s $W/internal/workers/worker1/chunks | grep -c "$CID")
check "inventory lists the chunk" "$n" 1

echo "  -- corrupt the file on disk; GET must refuse to return bad bytes"
w sh -c "printf X >> /data/chunks/${CID:0:2}/$CID"
code=$(w curl -s -o /dev/null -w '%{http_code}' $W/internal/chunks/$CID)
check "GET of corrupted chunk -> 500 (not served)" "$code" 500

echo "  -- restart worker1: data must persist"
docker restart worker1 >/dev/null; sleep 8
n=$(w curl -s $W/internal/workers/worker1/chunks | grep -c "$CID")
check "chunk still on disk after restart" "$n" 1

code=$(w curl -s -o /dev/null -w '%{http_code}' -X DELETE $W/internal/chunks/$CID)
check "DELETE chunk -> 204" "$code" 204
code=$(w curl -s -o /dev/null -w '%{http_code}' $W/internal/chunks/$CID)
check "GET deleted chunk -> 404" "$code" 404

echo "== Phase 3: registration, heartbeat, failure detection =="
n=$(curl -s $API/internal/workers | grep -o '"state":"HEALTHY"' | wc -l)
check "4 workers registered HEALTHY" "$n" 4
echo "  -- docker stop worker2; waiting for heartbeat timeout (~20s)"
docker stop worker2 >/dev/null; sleep 22
st=$(curl -s $API/internal/workers | python3 -c "import sys,json;print({w['worker_id']:w['state'] for w in json.load(sys.stdin)['workers']}['worker2'])")
check "worker2 marked DEAD" "$st" DEAD
echo "  -- docker start worker2"
docker start worker2 >/dev/null; sleep 10
st=$(curl -s $API/internal/workers | python3 -c "import sys,json;print({w['worker_id']:w['state'] for w in json.load(sys.stdin)['workers']}['worker2'])")
check "worker2 back to HEALTHY" "$st" HEALTHY

echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
