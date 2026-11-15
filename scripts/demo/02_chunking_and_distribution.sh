#!/usr/bin/env bash
# PDF §10 "Chunking and Distribution": upload an object larger than 1 MB; show chunking + spread over workers.
source "$(dirname "$0")/lib.sh"
banner "2. CHUNKING AND DISTRIBUTION — a 5.5 MiB object"
head -c $((5*1048576 + 524288)) /dev/urandom > $DEMO_TMP/big.bin
step "Upload (chunk size is 1 MiB = 1,048,576 bytes; the last chunk may be smaller)"
R=$(upload $DEMO_TMP/big.bin); OID=$(jget "d['object_id']" <<<"$R")
echo "    object $OID: size=$(jget "d['size']" <<<"$R") bytes, chunk_count=$(jget "d['chunk_count']" <<<"$R")"
expect_eq "5.5 MiB -> 6 chunks" "$(jget "d['chunk_count']" <<<"$R")" 6
step "Chunk manifest: order, size, checksum, and the workers holding each chunk"
placement_table $OID
M=$(curl -s $API/api/v1/objects/$OID/metadata)
expect_eq "chunks are ordered 0..5" "$(jget "[c['index'] for c in d['chunks']]" <<<"$M")" "[0, 1, 2, 3, 4, 5]"
expect_eq "first five chunks are exactly 1 MiB" "$(jget "[c['size'] for c in d['chunks'][:5]]" <<<"$M")" "[1048576, 1048576, 1048576, 1048576, 1048576]"
expect_eq "last chunk is 0.5 MiB" "$(jget "d['chunks'][5]['size']" <<<"$M")" 524288
step "Chunk files physically present on each worker's own volume"
for w in $WORKERS; do
  n=$(docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | jget "sum(1 for c in d['chunks'] if c['chunk_id'].startswith('$OID'))")
  echo "    $w holds $n chunk file(s) of this object"
done
expect_eq "6 chunks x 2 copies = 12 chunk files in total" "$(for w in $WORKERS; do docker exec $w curl -s localhost:9000/internal/workers/$w/chunks | jget "sum(1 for c in d['chunks'] if c['chunk_id'].startswith('$OID'))"; done | paste -sd+ | bc)" 12
expect_eq "chunks are spread over several workers" "$(jget "len({r['worker_id'] for c in d['chunks'] for r in c['replicas']})>=3" <<<"$M")" True
step "Reassemble the file from the workers' chunk files and compare"
: > $DEMO_TMP/re.bin
for row in $(jget "' '.join(c['chunk_id']+':'+c['replicas'][0]['worker_id'] for c in d['chunks'])" <<<"$M"); do
  docker exec ${row#*:} curl -s localhost:9000/internal/chunks/${row%%:*} >> $DEMO_TMP/re.bin; done
expect_eq "concatenated chunks == original upload" "$(sha256sum < $DEMO_TMP/re.bin)" "$(sha256sum < $DEMO_TMP/big.bin)"
echo "$OID" > $DEMO_TMP/last_oid
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
