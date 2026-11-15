#!/usr/bin/env bash
# PDF §10 "Metadata": upload custom metadata; read and modify it without re-uploading.
source "$(dirname "$0")/lib.sh"
banner "4. METADATA — system + custom metadata, patched without re-uploading"
printf 'quarterly report contents\n' > $DEMO_TMP/report.txt
step "Upload with custom metadata"; show "curl -F file=@report.txt -F 'metadata={\"owner\":\"alice\",\"project\":\"cloud\"}' ..."
R=$(upload $DEMO_TMP/report.txt '{"owner":"alice","project":"cloud","year":2026}'); OID=$(jget "d['object_id']" <<<"$R")
step "GET metadata (no data transferred)"; curl -s $API/api/v1/objects/$OID/metadata | python3 -m json.tool | sed 's/^/    /' | head -32
M1=$(curl -s $API/api/v1/objects/$OID/metadata)
expect_eq "system fields present (name, size, content_type, checksum, chunk_count, created, last_modified)" "$(jget "all(d[k] not in (None,'') for k in ('name','size','content_type','checksum','chunk_count','created_at','last_modified'))" <<<"$M1")" True
expect_eq "custom metadata returned" "$(jget "d['user_metadata']['owner']" <<<"$M1")" alice
sleep 1
step "PATCH: change owner, add a key, remove 'year' (null)"
show "curl -X PATCH -d '{\"owner\":\"bob\",\"status\":\"reviewed\",\"year\":null}' $API/api/v1/objects/$OID/metadata"
curl -s -X PATCH -d '{"owner":"bob","status":"reviewed","year":null}' $API/api/v1/objects/$OID/metadata | jget "d['user_metadata']" | sed 's/^/    new user_metadata: /'
M2=$(curl -s $API/api/v1/objects/$OID/metadata)
expect_eq "owner updated" "$(jget "d['user_metadata']['owner']" <<<"$M2")" bob
expect_eq "new key added" "$(jget "d['user_metadata']['status']" <<<"$M2")" reviewed
expect_eq "null removed the key" "$(jget "'year' in d['user_metadata']" <<<"$M2")" False
expect_eq "untouched key kept" "$(jget "d['user_metadata']['project']" <<<"$M2")" cloud
expect_eq "last_modified moved forward" "$(jget "d['last_modified'] > '$(jget "d['last_modified']" <<<"$M1")'" <<<"$M2")" True
expect_eq "no re-upload: same chunks, same data checksum" "$(jget "(d['checksum'], [c['chunk_id'] for c in d['chunks']])" <<<"$M2")" "$(jget "(d['checksum'], [c['chunk_id'] for c in d['chunks']])" <<<"$M1")"
expect_eq "PATCH on unknown object -> 404" "$(curl -s -o /dev/null -w '%{http_code}' -X PATCH -d '{"a":1}' $API/api/v1/objects/00000000-0000-0000-0000-000000000000/metadata)" 404
expect_eq "invalid JSON -> 400" "$(curl -s -o /dev/null -w '%{http_code}' -X PATCH -d '{oops' $API/api/v1/objects/$OID/metadata)" 400
expect_eq "data still downloads" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$(sha256sum < $DEMO_TMP/report.txt)"
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
