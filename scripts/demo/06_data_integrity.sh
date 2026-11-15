#!/usr/bin/env bash
# PDF §10 "Data Integrity" + plan §10.8: corrupt stored data on purpose; show detection and repair.
source "$(dirname "$0")/lib.sh"
banner "6. DATA INTEGRITY — deliberate corruption, detection, repair"
head -c $((4*1048576 + 999)) /dev/urandom > $DEMO_TMP/i.bin; ORIG=$(sha256sum < $DEMO_TMP/i.bin | cut -d' ' -f1)
R=$(upload $DEMO_TMP/i.bin); OID=$(jget "d['object_id']" <<<"$R"); M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][2]['chunk_id']" <<<"$M"); EXP=$(jget "d['chunks'][2]['checksum']" <<<"$M")
BAD=$(jget "d['chunks'][2]['replicas'][0]['worker_id']" <<<"$M"); GOOD=$(jget "d['chunks'][2]['replicas'][1]['worker_id']" <<<"$M")
P=/data/chunks/${CID:0:2}/$CID
step "1) Pick a chunk and record the facts"
echo "    chunk          $CID"; echo "    expected sha256 $EXP"; echo "    replicas       $BAD and $GOOD"
step "2) Deliberately corrupt the copy on $BAD (flip bytes inside the file on disk)"
docker exec $BAD curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
show "docker exec $BAD sh -c \"printf CORRUPTED | dd of=$P bs=1 seek=100 conv=notrunc\""
docker exec $BAD sh -c "printf CORRUPTED | dd of=$P bs=1 seek=100 conv=notrunc 2>/dev/null"
echo "    sha256 now on $BAD: sha256:$(docker exec $BAD sha256sum $P | cut -d' ' -f1)  (differs from expected)"
step "3) Detection by the worker's background scrubber (triggered manually instead of waiting for the interval)"
docker exec $BAD curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null
wait_for "scrubber reported the corruption to the controller" 40 "[ \"\$(sql \"select count(*) from scrub_results where chunk_id='$CID' and worker_id='$BAD' and status='CORRUPT'\")\" -ge 1 ]"
sql "select '    scrub result: status='||status||E'\n      expected='||expected_checksum||E'\n      actual  ='||actual_checksum from scrub_results where chunk_id='$CID' and worker_id='$BAD' and status='CORRUPT' order by id desc limit 1"
step "4) Controller reaction: replica marked CORRUPT, repair task created, healthy source chosen"
wait_for "repair finished and the replica is HEALTHY again" 60 "[ \"\$(sql \"select state from replicas where chunk_id='$CID' and worker_id='$BAD'\")\" = HEALTHY ]"
sql "select '    repair task: reason='||left(reason,20)||'…  source='||source_worker_id||'  target='||target_worker_id||'  state='||state from repair_tasks where chunk_id='$CID' order by created_at desc limit 1"
docker compose logs --since 2m controller 2>&1 | grep "repaired $CID" | tail -1 | sed 's/^[^|]*|/    controller log:/'
expect_eq "the healthy copy ($GOOD) was the repair source" "$(sql "select source_worker_id from repair_tasks where chunk_id='$CID' order by created_at desc limit 1")" "$GOOD"
step "5) Verify the replacement"
expect_eq "$BAD's file matches the expected checksum again" "sha256:$(docker exec $BAD sha256sum $P | cut -d' ' -f1)" "$EXP"
placement_table $OID
expect_eq "two healthy replicas on different workers" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "len({r['worker_id'] for r in d['chunks'][2]['replicas'] if r['state']=='HEALTHY'})")" 2
step "6) Corruption met by a client READ: the bad copy is skipped, the data is still correct, and the copy is repaired"
M=$(curl -s $API/api/v1/objects/$OID/metadata); CID2=$(jget "d['chunks'][0]['chunk_id']" <<<"$M"); W2=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
docker exec $W2 curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
docker exec $W2 sh -c "printf XXXX >> /data/chunks/${CID2:0:2}/$CID2"
ALLOK=yes; for i in 1 2 3 4 5 6 7 8; do [ "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" = "$ORIG" ] || ALLOK=no; done
expect_eq "8 downloads in a row were all byte-identical to the original (bad replica skipped)" "$ALLOK" yes
wait_for "the bad replica found by reads was repaired" 60 "[ \"\$(sql \"select count(*) from replicas where chunk_id='$CID2' and state<>'HEALTHY'\")\" = 0 ]"
step "7) Also: a chunk file deleted from a worker is found by inventory reconciliation / scrubbing"
M=$(curl -s $API/api/v1/objects/$OID/metadata); CID3=$(jget "d['chunks'][3]['chunk_id']" <<<"$M"); W3=$(jget "d['chunks'][3]['replicas'][0]['worker_id']" <<<"$M")
docker exec $W3 rm -f /data/chunks/${CID3:0:2}/$CID3
wait_for "missing chunk file restored on $W3" 120 "docker exec $W3 test -f /data/chunks/${CID3:0:2}/$CID3"
step "8) Final: full download compared with the original upload"
expect_eq "sha256 of the downloaded object equals the original" "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" "$ORIG"
cluster_summary
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
