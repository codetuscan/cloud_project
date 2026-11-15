#!/usr/bin/env bash
# PDF §10 "Replication": every chunk has two copies; show where they are stored.
source "$(dirname "$0")/lib.sh"
banner "3. REPLICATION — two copies of every chunk on different workers"
head -c $((3*1048576 + 100000)) /dev/urandom > $DEMO_TMP/r.bin
R=$(upload $DEMO_TMP/r.bin); OID=$(jget "d['object_id']" <<<"$R")
step "Replica locations as recorded by the controller (PostgreSQL)"; placement_table $OID
M=$(curl -s $API/api/v1/objects/$OID/metadata)
expect_eq "every chunk has exactly 2 replicas" "$(jget "all(len(c['replicas'])==2 for c in d['chunks'])" <<<"$M")" True
expect_eq "all replicas HEALTHY" "$(jget "all(r['state']=='HEALTHY' for c in d['chunks'] for r in c['replicas'])" <<<"$M")" True
expect_eq "the two copies are always on different workers" "$(jget "all(len({r['worker_id'] for r in c['replicas']})==2 for c in d['chunks'])" <<<"$M")" True
step "Verify against the workers themselves (files on disk, checksums)"
CID=$(jget "d['chunks'][0]['chunk_id']" <<<"$M"); EXP=$(jget "d['chunks'][0]['checksum']" <<<"$M")
echo "    chunk 0 = $CID"; echo "    expected checksum: $EXP"
for w in $(jget "' '.join(r['worker_id'] for r in d['chunks'][0]['replicas'])" <<<"$M"); do
  p=/data/chunks/${CID:0:2}/$CID
  echo "    on $w: $(docker exec $w stat -c '%s bytes' $p), sha256 $(docker exec $w sha256sum $p | cut -c1-16)…"
  expect_eq "$w stores a copy with the right checksum" "sha256:$(docker exec $w sha256sum $p | cut -d' ' -f1)" "$EXP"
done
step "Cluster overview"; cluster_summary
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
