#!/usr/bin/env bash
# PDF §10 "Object Storage": upload and retrieve an object; verify it matches the original.
source "$(dirname "$0")/lib.sh"
banner "1. OBJECT STORAGE — upload, retrieve, verify"
head -c 700000 /dev/urandom > $DEMO_TMP/photo.bin
ORIG=$(sha256sum < $DEMO_TMP/photo.bin | cut -d' ' -f1)
step "Upload a 700 KB binary object"; show "curl -F file=@photo.bin $API/api/v1/objects"
R=$(upload $DEMO_TMP/photo.bin); echo "$R" | python3 -m json.tool | sed 's/^/    /'
OID=$(jget "d['object_id']" <<<"$R")
step "Download it by object id (the client never learns where the chunks live)"
show "curl $API/api/v1/objects/$OID -o downloaded.bin"
curl -s -D $DEMO_TMP/h.txt -o $DEMO_TMP/down.bin $API/api/v1/objects/$OID
grep -iE "^(HTTP|content-type|content-length|content-disposition)" $DEMO_TMP/h.txt | tr -d '\r' | sed 's/^/    /'
step "Compare with the original"
echo "    original   sha256: $ORIG"; echo "    downloaded sha256: $(sha256sum < $DEMO_TMP/down.bin | cut -d' ' -f1)"
expect_eq "downloaded bytes are identical to the original" "$(sha256sum < $DEMO_TMP/down.bin | cut -d' ' -f1)" "$ORIG"
step "Other behaviour from the spec"
expect_eq "unknown object id -> 404" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/00000000-0000-0000-0000-000000000000)" 404
expect_eq "upload without a file -> 400" "$(curl -s -o /dev/null -w '%{http_code}' -F 'metadata={}' $API/api/v1/objects)" 400
curl -s $API/api/v1/objects/00000000-0000-0000-0000-000000000000 | sed 's/^/    error body: /'
step "List objects"; curl -s "$API/api/v1/objects?limit=3" | python3 -m json.tool | sed 's/^/    /'
step "Delete the object"; expect_eq "DELETE -> 204" "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE $API/api/v1/objects/$OID)" 204
expect_eq "then GET -> 404" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$OID)" 404
finish
