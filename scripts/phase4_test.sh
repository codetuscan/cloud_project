#!/usr/bin/env bash
# Phase 4 checks: upload path (staging, chunking, 2-replica placement, commit, resume).
# Run from the project root with the stack up:  ./scripts/phase4_test.sh
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d); trap 'rm -rf $T' EXIT
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
upload() { curl -s -w '\n%{http_code}' "$@" "$API/api/v1/objects"; }   # body, then status on last line
body()   { sed '$d' <<<"$1"; }
status() { tail -n1 <<<"$1"; }

echo "== Upload 5.5 MiB (expect 6 chunks, last = 0.5 MiB) =="
head -c $((5*1048576+524288)) /dev/urandom > $T/big.bin
BIGSUM=$(sha256sum < $T/big.bin | cut -d' ' -f1)
R=$(upload -F file=@$T/big.bin -F 'metadata={"owner":"alice","project":"cloud"}')
check "POST -> 201" "$(status "$R")" 201
OID=$(body "$R" | jget "d['object_id']")
check "state COMMITTED" "$(body "$R" | jget "d['state']")" COMMITTED
check "chunk_count 6" "$(body "$R" | jget "d['chunk_count']")" 6
check "object checksum = sha256 of file" "$(body "$R" | jget "d['checksum']")" "sha256:$BIGSUM"
check "size correct" "$(body "$R" | jget "d['size']")" $((5*1048576+524288))
check "name taken from filename" "$(body "$R" | jget "d['name']")" big.bin
check "custom metadata stored" "$(body "$R" | jget "d['user_metadata']['owner']")" alice

M=$(curl -s $API/api/v1/objects/$OID/metadata)
check "chunk order 0..5" "$(jget "[c['index'] for c in d['chunks']]" <<<"$M")" "[0, 1, 2, 3, 4, 5]"
check "last chunk is 524288 bytes" "$(jget "d['chunks'][-1]['size']" <<<"$M")" 524288
check "other chunks are 1048576 bytes" "$(jget "set(c['size'] for c in d['chunks'][:-1])" <<<"$M")" "{1048576}"
check "every chunk has 2 HEALTHY replicas" "$(jget "all(len([r for r in c['replicas'] if r['state']=='HEALTHY'])==2 for c in d['chunks'])" <<<"$M")" True
check "replicas are on different workers" "$(jget "all(len({r['worker_id'] for r in c['replicas']})==2 for c in d['chunks'])" <<<"$M")" True
echo "  chunk -> workers:"; jget "'\n'.join('    chunk %d (%d B): %s'%(c['index'],c['size'],', '.join(r['worker_id'] for r in c['replicas'])) for c in d['chunks'])" <<<"$M"

echo "== Bytes really on the workers' disks =="
total=0
for w in worker1 worker2 worker3 worker4; do
  n=$(docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | python3 -c "import sys,json;print(sum(1 for c in json.load(sys.stdin)['chunks'] if c['chunk_id'].startswith('$OID')))")
  echo "    $w holds $n chunks of this object"; total=$((total+n))
done
check "12 replica files total on workers" "$total" 12
# reassemble from workers using manifest and compare with original
python3 - "$OID" <<'PY' > $T/manifest.txt
import sys,json,urllib.request
d=json.load(urllib.request.urlopen(f"http://localhost:8090/api/v1/objects/{sys.argv[1]}/metadata"))
for c in d['chunks']: print(c['chunk_id'], c['replicas'][0]['worker_id'])
PY
: > $T/reassembled.bin
while read cid w; do docker exec $w curl -s localhost:9000/internal/chunks/$cid >> $T/reassembled.bin; done < $T/manifest.txt
check "chunks reassembled from workers == original" "$(sha256sum < $T/reassembled.bin | cut -d' ' -f1)" "$BIGSUM"
check "staging dir is empty after commit" "$(docker exec controller sh -c 'ls /staging | wc -l')" 0

echo "== Edge sizes =="
head -c 2097152 /dev/urandom > $T/exact.bin
R=$(upload -F file=@$T/exact.bin); check "exact multiple of chunk size (2 MiB) -> 2 chunks" "$(body "$R" | jget "d['chunk_count']")" 2
printf 'hello' > $T/tiny.txt
R=$(upload -F "file=@$T/tiny.txt;type=text/plain"); check "tiny file -> 1 chunk" "$(body "$R" | jget "d['chunk_count']")" 1
check "content type kept" "$(body "$R" | jget "d['content_type']")" text/plain

echo "== Validation errors =="
: > $T/empty.bin
check "empty file -> 400" "$(status "$(upload -F file=@$T/empty.bin)")" 400
check "missing file field -> 400" "$(status "$(upload -F 'metadata={}')")" 400
check "bad metadata JSON -> 400" "$(status "$(upload -F file=@$T/tiny.txt -F 'metadata={nope')")" 400
check "metadata not an object -> 400" "$(status "$(upload -F file=@$T/tiny.txt -F 'metadata=[1,2]')")" 400
check "non-multipart body -> 400" "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{}' $API/api/v1/objects)" 400
check "error body is JSON with message" "$(curl -s -X POST -d x $API/api/v1/objects | jget "'message' in d['error']")" True
check "metadata of unknown id -> 404" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/00000000-0000-0000-0000-000000000000/metadata)" 404

echo "== Idempotency-Key =="
KEY="key-$RANDOM"
R1=$(upload -H "Idempotency-Key: $KEY" -F file=@$T/tiny.txt); R2=$(upload -H "Idempotency-Key: $KEY" -F file=@$T/tiny.txt)
check "first -> 201" "$(status "$R1")" 201
check "retry -> 200 (no duplicate)" "$(status "$R2")" 200
check "retry returns same object_id" "$(body "$R2" | jget "d['object_id']")" "$(body "$R1" | jget "d['object_id']")"

echo "== Not enough workers: 503, upload kept in staging, auto-resumed =="
docker stop worker2 worker3 worker4 >/dev/null
R=$(upload -F file=@$T/exact.bin)
check "only 1 worker reachable -> 503" "$(status "$R")" 503
PID=$(body "$R" | jget "d['error']['object_id']")
check "unfinished object is NOT visible yet (404)" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$PID/metadata)" 404
check "staged file retained" "$(docker exec controller sh -c "ls /staging | grep -c $PID")" 1
docker start worker2 worker3 worker4 >/dev/null
echo "  waiting for workers to return and the resume loop (up to ~75s)..."
code=404; for i in $(seq 1 25); do sleep 3; code=$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$PID/metadata); [ "$code" = 200 ] && break; done
check "upload auto-completed after workers returned" "$code" 200
M=$(curl -s $API/api/v1/objects/$PID/metadata)
check "resumed object has 2 healthy replicas per chunk on different workers" "$(jget "all(len({r['worker_id'] for r in c['replicas'] if r['state']=='HEALTHY'})==2 for c in d['chunks'])" <<<"$M")" True
check "staging cleaned after resume" "$(docker exec controller sh -c "ls /staging | grep -c $PID")" 0

echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
