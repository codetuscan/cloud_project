#!/usr/bin/env bash
# PDF §10 "Worker Failure": stop a worker container, show handling, restart, show replication restored.
source "$(dirname "$0")/lib.sh"
banner "5. WORKER FAILURE — docker stop worker2 / docker start worker2"
head -c $((4*1048576 + 4242)) /dev/urandom > $DEMO_TMP/w.bin; ORIG=$(sha256sum < $DEMO_TMP/w.bin)
R=$(upload $DEMO_TMP/w.bin); OID=$(jget "d['object_id']" <<<"$R")
M=$(curl -s $API/api/v1/objects/$OID/metadata)
V=worker2
step "BEFORE the failure"; cluster_summary; placement_table $OID
ONV=$(jget "sum(1 for c in d['chunks'] for r in c['replicas'] if r['worker_id']=='$V')" <<<"$M")
note "$V holds $ONV chunk replica(s) of this object (the demo is most interesting when this is > 0)"
step "DURING: docker stop $V"; show "docker stop $V"; docker stop $V >/dev/null
note "The controller notices missing heartbeats (timeout 15s)…"
wait_for "$V declared DEAD" 60 "[ \"\$(curl -s $API/internal/status | jget \"[w['state'] for w in d['workers'] if w['worker_id']=='$V'][0]\")\" = DEAD ]"
expect_eq "download still works while $V is down (other replicas serve it)" "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" "${ORIG%% *}"
note "Replication factor 2 is no longer met for chunks that were on $V -> the controller re-replicates them to other workers"
wait_for "every chunk has 2 healthy replicas on LIVE workers again" 90 "[ \"\$(sql \"select count(*) from chunks c where c.object_id='$OID' and (select count(*) from replicas r join workers w using(worker_id) where r.chunk_id=c.chunk_id and r.state='HEALTHY' and w.state<>'DEAD')<>2\")\" = 0 ]"
step "Controller log (repair activity)"; docker compose logs --since 3m controller 2>&1 | grep -E "DEAD|repaired|below 2" | tail -5 | sed 's/^/    /'
step "WITH $V STILL DOWN: placement"; cluster_summary
curl -s $API/api/v1/objects/$OID/metadata | jget "'\n'.join('    chunk %d -> %s'%(c['index'], ', '.join(r['worker_id']+('(DEAD)' if r['worker_id']=='$V' else '') for r in c['replicas'])) for c in d['chunks'])"
step "AFTER: docker start $V"; show "docker start $V"; docker start $V >/dev/null
wait_for "$V HEALTHY again" 60 "[ \"\$(curl -s $API/internal/status | jget \"[w['state'] for w in d['workers'] if w['worker_id']=='$V'][0]\")\" = HEALTHY ]"
note "$V returns with the chunks it kept on its persistent volume; the controller trims any chunk that is now over-replicated"
wait_for "exactly 2 replicas per chunk again (surplus trimmed)" 120 "[ \"\$(sql \"select count(*) from chunks c where c.object_id='$OID' and (select count(*) from replicas r where r.chunk_id=c.chunk_id)<>2\")\" = 0 ]"
step "AFTER the recovery"; cluster_summary; placement_table $OID
expect_eq "replication factor 2 maintained for every chunk" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "all(len([r for r in c['replicas'] if r['state']=='HEALTHY'])==2 and len({r['worker_id'] for r in c['replicas']})==2 for c in d['chunks'])")" True
expect_eq "final download identical to the original" "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" "${ORIG%% *}"
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
