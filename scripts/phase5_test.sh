#!/usr/bin/env bash
# Phase 5 checks: download (with replica fallback), list, metadata PATCH, delete lifecycle.
# Run from the project root with the stack up:  ./scripts/phase5_test.sh
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d); trap 'rm -rf $T' EXIT
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()  { docker exec postgres psql -U objstore -tAc "$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
upload_obj() { curl -s -F file=@"$1" ${2:+-F "metadata=$2"} $API/api/v1/objects | jget "d['object_id']"; }

head -c $((3*1048576+12345)) /dev/urandom > $T/obj.bin
SUM=$(sha256sum < $T/obj.bin)
OID=$(upload_obj $T/obj.bin '{"owner":"alice","tag":"demo"}')
echo "uploaded test object $OID"

echo "== Download =="
curl -s -D $T/h.txt -o $T/dl.bin $API/api/v1/objects/$OID
check "200 OK" "$(head -1 $T/h.txt | tr -d '\r' | cut -d' ' -f2)" 200
check "download is byte-identical to original" "$(sha256sum < $T/dl.bin)" "$SUM"
check "Content-Length header" "$(grep -i '^content-length' $T/h.txt | tr -d '\r' | cut -d' ' -f2)" $((3*1048576+12345))
check "Content-Type header" "$(grep -i '^content-type' $T/h.txt | tr -d '\r' | cut -d' ' -f2)" application/octet-stream
check "Content-Disposition has filename" "$(grep -ci 'filename=\"obj.bin\"' $T/h.txt)" 1
check "unknown object -> 404" "$(code $API/api/v1/objects/00000000-0000-0000-0000-000000000000)" 404
check "malformed id -> 404" "$(code $API/api/v1/objects/not-a-uuid)" 404

echo "== Read fallback: one replica worker stopped =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
W1=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
echo "  stopping $W1 (holds a replica of chunk 0)"; docker stop $W1 >/dev/null
for i in 1 2 3; do curl -s -o $T/dl2.bin $API/api/v1/objects/$OID; [ "$(sha256sum < $T/dl2.bin)" = "$SUM" ] || break; done
check "download still correct (3 tries) with $W1 down" "$(sha256sum < $T/dl2.bin)" "$SUM"
docker start $W1 >/dev/null; sleep 8

echo "== Read fallback: corrupted replica on disk =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][1]['chunk_id']" <<<"$M"); W2=$(jget "d['chunks'][1]['replicas'][0]['worker_id']" <<<"$M")
docker exec $W2 sh -c "printf XXXX >> /data/chunks/${CID:0:2}/$CID"
echo "  corrupted chunk 1 ($CID) on $W2"
for i in 1 2 3 4 5 6; do curl -s -o $T/dl3.bin $API/api/v1/objects/$OID; [ "$(sha256sum < $T/dl3.bin)" = "$SUM" ] || break; done
check "download correct despite corrupt replica (6 tries)" "$(sha256sum < $T/dl3.bin)" "$SUM"
check "corrupt replica reported -> state CORRUPT" "$(sql "select state from replicas where chunk_id='$CID' and worker_id='$W2'")" CORRUPT
check "repair task queued" "$(sql "select count(*) from repair_tasks where chunk_id='$CID' and state='PENDING'")" 1

echo "== Both replicas of a chunk gone: must fail, never truncate silently =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
WA=$(jget "d['chunks'][2]['replicas'][0]['worker_id']" <<<"$M"); WB=$(jget "d['chunks'][2]['replicas'][1]['worker_id']" <<<"$M")
docker stop $WA $WB >/dev/null
rc=0; c=$(curl -s -o $T/dl4.bin -w '%{http_code}' $API/api/v1/objects/$OID) || rc=$?
if [ "$c" = 503 ] || [ $rc -ne 0 ]; then ok "request failed (http=$c curl_rc=$rc), not a quiet short file"; else bad "got http=$c rc=$rc"; fi
docker start $WA $WB >/dev/null; sleep 8

echo "== List =="
S1=$(upload_obj $T/obj.bin); S2=$(upload_obj $T/obj.bin); S3=$(upload_obj $T/obj.bin)
L=$(curl -s "$API/api/v1/objects?limit=2")
check "limit=2 returns 2 items" "$(jget "len(d['items'])" <<<"$L")" 2
check "next_cursor present" "$(jget "d['next_cursor'] is not None" <<<"$L")" True
check "items have id,name,size,content_type" "$(jget "all(k in d['items'][0] for k in ('object_id','name','size','content_type'))" <<<"$L")" True
ALL=""; CUR=""
while :; do
  P=$(curl -s "$API/api/v1/objects?limit=2${CUR:+&cursor=$CUR}")
  ALL="$ALL $(jget "' '.join(i['object_id'] for i in d['items'])" <<<"$P")"
  CUR=$(jget "d['next_cursor'] or ''" <<<"$P"); [ -z "$CUR" ] && break
done
check "pagination: no duplicate ids" "$(tr ' ' '\n' <<<"$ALL" | grep -v '^$' | sort | uniq -d | wc -l)" 0
for id in $OID $S1 $S2 $S3; do check "listed: $id" "$(grep -c $id <<<"$ALL")" 1; done
check "limit=0 -> 400" "$(code "$API/api/v1/objects?limit=0")" 400
check "limit=abc -> 400" "$(code "$API/api/v1/objects?limit=abc")" 400
check "bad cursor -> 400" "$(code "$API/api/v1/objects?cursor=zzzz")" 400

echo "== PATCH metadata (no re-upload) =="
B=$(curl -s $API/api/v1/objects/$OID/metadata); T0=$(jget "d['last_modified']" <<<"$B")
sleep 1
check "PATCH -> 200" "$(code -X PATCH -H 'Content-Type: application/json' -d '{"owner":"bob","new":"v","n":5,"tag":null}' $API/api/v1/objects/$OID/metadata)" 200
A=$(curl -s $API/api/v1/objects/$OID/metadata)
check "key updated" "$(jget "d['user_metadata']['owner']" <<<"$A")" bob
check "key added" "$(jget "d['user_metadata']['new']" <<<"$A")" v
check "number kept" "$(jget "d['user_metadata']['n']" <<<"$A")" 5
check "null removes key" "$(jget "'tag' in d['user_metadata']" <<<"$A")" False
check "last_modified advanced" "$(jget "d['last_modified'] > '$T0'" <<<"$A")" True
check "data untouched (same checksum, chunks)" "$(jget "(d['checksum'],len(d['chunks']))" <<<"$A")" "$(jget "(d['checksum'],len(d['chunks']))" <<<"$B")"
check "download after PATCH still identical" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
check "PATCH unknown id -> 404" "$(code -X PATCH -d '{"a":1}' $API/api/v1/objects/00000000-0000-0000-0000-000000000000/metadata)" 404
check "PATCH invalid JSON -> 400" "$(code -X PATCH -d '{bad' $API/api/v1/objects/$OID/metadata)" 400
check "PATCH non-object -> 400" "$(code -X PATCH -d '[1]' $API/api/v1/objects/$OID/metadata)" 400
check "PATCH nested value -> 400" "$(code -X PATCH -d '{"a":{"b":1}}' $API/api/v1/objects/$OID/metadata)" 400

echo "== DELETE =="
M=$(curl -s $API/api/v1/objects/$S1/metadata); CIDS=$(jget "' '.join(c['chunk_id'] for c in d['chunks'])" <<<"$M")
check "DELETE -> 204" "$(code -X DELETE $API/api/v1/objects/$S1)" 204
check "GET after delete -> 404" "$(code $API/api/v1/objects/$S1)" 404
check "metadata after delete -> 404" "$(code $API/api/v1/objects/$S1/metadata)" 404
check "not in list" "$(curl -s "$API/api/v1/objects?limit=1000" | grep -c $S1)" 0
check "second DELETE -> 404" "$(code -X DELETE $API/api/v1/objects/$S1)" 404
check "DELETE unknown -> 404" "$(code -X DELETE $API/api/v1/objects/00000000-0000-0000-0000-000000000000)" 404
sleep 8
left=0; for w in worker1 worker2 worker3 worker4; do n=$(docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | grep -c "$S1"); left=$((left+n)); done
check "chunk files removed from all workers" "$left" 0
check "object state DELETED, no replica rows" "$(sql "select state from objects where object_id='$S1'")|$(sql "select count(*) from replicas r join chunks c using(chunk_id) where c.object_id='$S1'")" "DELETED|0"
check "tombstones kept" "$([ "$(sql "select count(*) from tombstones where chunk_id like '$S1%'")" -gt 0 ] && echo yes)" yes

echo "  -- delete while a replica worker is down: retried until it returns"
M=$(curl -s $API/api/v1/objects/$S2/metadata); WD=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
docker stop $WD >/dev/null
check "DELETE with $WD down -> 204" "$(code -X DELETE $API/api/v1/objects/$S2)" 204
sleep 20
check "object stays DELETING while $WD is down" "$(sql "select state from objects where object_id='$S2'")" DELETING
docker start $WD >/dev/null; sleep 12
check "after $WD returns -> DELETED" "$(sql "select state from objects where object_id='$S2'")" DELETED
check "its chunk files are gone" "$(docker exec $WD curl -s localhost:9000/internal/workers/$WD/chunks | grep -c $S2)" 0

# cleanup
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID; curl -s -o /dev/null -X DELETE $API/api/v1/objects/$S3
echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
