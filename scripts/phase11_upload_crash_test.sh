#!/usr/bin/env bash
# Upload crash safety ("database first" ordering). Simulates the states a controller crash can leave behind at each
# step of an upload, restarts the controller, and checks that staging and PostgreSQL agree afterwards. Also covers a
# client disconnect mid-upload and retries that reuse an Idempotency-Key.
# Run from the project root with the stack up:  ./scripts/phase11_upload_crash_test.sh  (~2 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()  { docker exec postgres psql -U objstore -tAc "$1"; }
staging_has() { docker exec controller sh -c "ls /staging 2>/dev/null | grep -c '$1'; true"; }
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=2)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 2; done; bad "$d (timed out after ${t}s)"; return 1; }
cluster_up() { wait_until "controller healthy + 4 workers HEALTHY" 120 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"; }
trap 'jobs -p | xargs -r kill 2>/dev/null; rm -rf $T; docker start controller >/dev/null 2>&1' EXIT
new_id() { python3 -c 'import uuid;print(uuid.uuid4())'; }
cluster_up
CHUNK=$(curl -s $API/internal/status | jget "d['chunk_size']")

echo "== 1. Normal upload still works and leaves nothing behind =="
head -c $((2*CHUNK+777)) /dev/urandom > $T/n.bin
R=$(curl -s -w '\n%{http_code}' -F file=@$T/n.bin -F 'metadata={"case":"normal"}' $API/api/v1/objects)
check "upload -> 201" "$(echo "$R" | tail -1)" 201
NID=$(echo "$R" | head -1 | jget "d['object_id']")
check "name and metadata are the real ones, not placeholders" "$(echo "$R" | head -1 | jget "(d['name'],d['user_metadata'])")" "('n.bin', {'case': 'normal'})"
check "download identical" "$(curl -s $API/api/v1/objects/$NID | sha256sum)" "$(sha256sum < $T/n.bin)"
check "no upload row left" "$(sql "select count(*) from uploads where object_id='$NID'")" 0
check "no staging file left" "$(staging_has $NID)" 0

echo "== 2. Crash states left behind, then a controller restart =="
docker stop controller >/dev/null
# (a) the old leak: complete .staged file, no database row at all
ORPH=$(new_id); head -c 1000 /dev/urandom > $T/orph.bin
docker cp $T/orph.bin controller:/staging/$ORPH.staged
# (b) crash while streaming: STREAMING row (placeholders) + partial .uploading
STRM=$(new_id); head -c 5000 /dev/urandom > $T/strm.bin
sql "insert into objects (object_id,name,chunk_size,state,idempotency_key) values ('$STRM','(upload in progress)',$CHUNK,'PENDING','key-strm-$STRM');
     insert into uploads (object_id,staged_path,state) values ('$STRM','/staging/$STRM.staged','STREAMING');" >/dev/null
docker cp $T/strm.bin controller:/staging/$STRM.uploading
# (c) crash after the commit point but before the rename: STAGED row with full details + complete .uploading
STGD=$(new_id); head -c $((2*CHUNK+4242)) /dev/urandom > $T/stgd.bin
SZ=$(stat -c %s $T/stgd.bin); SUM=sha256:$(sha256sum $T/stgd.bin | cut -d' ' -f1); NC=$(( (SZ+CHUNK-1)/CHUNK ))
sql "insert into objects (object_id,name,size,content_type,checksum,chunk_count,chunk_size,state,user_metadata,idempotency_key)
     values ('$STGD','crashed.bin',$SZ,'application/octet-stream','$SUM',$NC,$CHUNK,'PENDING','{\"case\":\"crash-after-commit-point\"}','key-stgd-$STGD');
     insert into uploads (object_id,staged_path,state) values ('$STGD','/staging/$STGD.staged','STAGED');" >/dev/null
docker cp $T/stgd.bin controller:/staging/$STGD.uploading
docker start controller >/dev/null
wait_until "controller back" 60 "curl -sf $API/healthz"
check "(a) orphan .staged file without a row was removed" "$(staging_has $ORPH)" 0
check "(b) STREAMING row was dropped" "$(sql "select count(*) from objects where object_id='$STRM'")" 0
check "(b) its partial file was removed" "$(staging_has $STRM)" 0
check "(b) its Idempotency-Key is free again" "$(sql "select count(*) from objects where idempotency_key='key-strm-$STRM'")" 0
wait_until "(c) upload interrupted after the commit point was finished by the controller" 90 "[ \"\$(sql \"select state from objects where object_id='$STGD'\")\" = COMMITTED ]"
check "(c) download identical to the original" "$(curl -s $API/api/v1/objects/$STGD | sha256sum)" "$(sha256sum < $T/stgd.bin)"
check "(c) name and metadata survived the crash" "$(curl -s $API/api/v1/objects/$STGD/metadata | jget "(d['name'],d['user_metadata'])")" "('crashed.bin', {'case': 'crash-after-commit-point'})"
check "(c) staging cleaned" "$(staging_has $STGD)" 0
check "(c) client retry with the same key gets the finished object (200), no re-upload" "$(curl -s -o /dev/null -w '%{http_code}' -H "Idempotency-Key: key-stgd-$STGD" -F file=@$T/stgd.bin $API/api/v1/objects)" 200

echo "== 3. Retry that finds a dead STREAMING row with its key is not blocked =="
DEAD=$(new_id); K5="key-dead-$DEAD"
sql "insert into objects (object_id,name,chunk_size,state,idempotency_key) values ('$DEAD','(upload in progress)',$CHUNK,'PENDING','$K5');
     insert into uploads (object_id,staged_path,state) values ('$DEAD','/staging/$DEAD.staged','STREAMING');" >/dev/null
head -c 3000 /dev/urandom > $T/k5.bin
check "upload with the reused key -> 201 (not 409)" "$(curl -s -o /dev/null -w '%{http_code}' -H "Idempotency-Key: $K5" -F file=@$T/k5.bin $API/api/v1/objects)" 201
check "the dead row is gone" "$(sql "select count(*) from objects where object_id='$DEAD'")" 0

echo "== 4. Client disconnects mid-upload =="
head -c $((20*CHUNK)) /dev/urandom > $T/slow.bin
curl -s -o /dev/null --limit-rate 1M -F file=@$T/slow.bin $API/api/v1/objects & CP=$!
sleep 3
check "while streaming, the database already knows the upload (STREAMING row)" "$(sql "select count(*) from uploads where state='STREAMING'")" 1
kill $CP 2>/dev/null; wait $CP 2>/dev/null
wait_until "after the disconnect the STREAMING row is removed" 20 "[ \"\$(sql \"select count(*) from uploads where state='STREAMING'\")\" = 0 ]"
check "and the partial file is removed" "$(docker exec controller sh -c 'ls /staging | grep -c uploading$' 2>/dev/null)" 0

echo "== 5. Two simultaneous uploads with the same Idempotency-Key =="
K7="key-same-$(new_id)"
curl -s -o /dev/null --limit-rate 1M -H "Idempotency-Key: $K7" -F file=@$T/slow.bin $API/api/v1/objects & CP=$!
sleep 2
check "the second one gets 409 while the first is still streaming" "$(curl -s -o /dev/null -w '%{http_code}' -H "Idempotency-Key: $K7" -F file=@$T/k5.bin $API/api/v1/objects)" 409
kill $CP 2>/dev/null; wait $CP 2>/dev/null
wait_until "after the first one disconnects, its key is released" 20 "[ \"\$(sql \"select count(*) from objects where idempotency_key='$K7'\")\" = 0 ]"

for o in $NID $STGD; do curl -s -o /dev/null -X DELETE $API/api/v1/objects/$o; done
echo; echo "Result: $pass passed, $fail failed"
[ $fail -eq 0 ]
