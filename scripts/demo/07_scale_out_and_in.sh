#!/usr/bin/env bash
# Extra (beyond §10): elastic scaling. A new empty worker joins and existing replicas are rebalanced onto it;
# then it is drained and removed without the cluster ever being under-replicated.
source "$(dirname "$0")/lib.sh"
cd "$(dirname "$0")/../.."          # docker compose needs the project directory
V=worker5
banner "7. SCALE OUT / SCALE IN — rebalancing onto a new worker, draining before removal"

worker_state() { curl -s $API/internal/status | jget "([w['state'] for w in d['workers'] if w['worker_id']=='$V'] or ['ABSENT'])[0]"; }
counts() { note "replicas per worker: $(sql "select string_agg(worker_id||'='||n, '  ' order by worker_id) from (select worker_id, count(*) n from replicas where state='HEALTHY' group by 1) s")"; }
not_two() { sql "select count(*) from chunks c join objects o using(object_id) where o.state='COMMITTED' and (select count(*) from replicas r join workers w using(worker_id) where r.chunk_id=c.chunk_id and r.state='HEALTHY' and w.state<>'DEAD')<>2"; }

if [ "$(worker_state)" != ABSENT ]; then   # leftover from an earlier run: remove it the safe way first
  note "cleaning up a $V left over from an earlier run…"
  curl -s -X POST $API/internal/workers/$V/drain >/dev/null
  for i in $(seq 90); do [ "$(sql "select count(*) from replicas where worker_id='$V'")" = 0 ] && break; sleep 2; done
  docker compose rm -sf $V >/dev/null 2>&1
  for i in $(seq 30); do [ "$(worker_state)" = DEAD ] && break; sleep 2; done
  curl -s -X DELETE $API/internal/workers/$V >/dev/null
fi
wait_cluster

head -c $((12*1048576)) /dev/urandom > $DEMO_TMP/s.bin; ORIG=$(sha256sum < $DEMO_TMP/s.bin)
OID=$(upload $DEMO_TMP/s.bin | jget "d['object_id']")

step "BEFORE: 4 workers, data spread evenly by placement"
counts

step "SCALE OUT: a fifth, empty worker joins"
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
show "docker compose up -d $V"; docker compose up -d $V >/dev/null 2>&1
wait_for "$V registered and HEALTHY" 90 "[ \"\$(worker_state)\" = HEALTHY ]"
note "New uploads would go to $V anyway (least-loaded placement), but existing data stays put unless it is moved."
note "The rebalancer moves replicas from the fullest to the emptiest worker: copy -> verify -> retire the old copy."
wait_for "rebalanced: $V holds data and the replica counts differ by at most 2" 180 \
  "[ \"\$(curl -s $API/internal/status | jget \"d['rebalance']['state']=='balanced' and d['rebalance']['loads'].get('$V',0)>0\")\" = True ]"
counts
docker compose logs --since $T0 controller 2>&1 | grep -E "rebalancing:|rebalanced " | head -3 | sed 's/^/    /'
expect_eq "every chunk still has exactly 2 healthy copies" "$(not_two)" 0
wait_for "old copies removed from the donor workers' disks (inventories match the metadata)" 60 \
  "[ \"\$(for w in worker1 worker2 worker3 worker4 $V; do echo \$(docker exec \$w curl -s localhost:9000/internal/workers/\$w/chunks | jget \"len(d['chunks'])\")=\$(sql \"select count(*) from replicas where worker_id='\$w'\"); done | awk -F= '\$1!=\$2' | wc -l)\" = 0 ]"
expect_eq "download identical after the move" "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" "${ORIG%% *}"

step "SCALE IN: drain $V, then remove it"
show "curl -X POST $API/internal/workers/$V/drain"
curl -s -X POST $API/internal/workers/$V/drain | sed 's/^/    /'
note "Placement no longer chooses $V; the rebalancer moves its replicas to the other workers while it is still up."
wait_for "$V holds no replicas" 180 "[ \"\$(sql \"select count(*) from replicas where worker_id='$V'\")\" = 0 ]"
counts
show "docker compose rm -sf $V"; docker compose rm -sf $V >/dev/null 2>&1
wait_for "$V marked DEAD (heartbeat timeout)" 60 "[ \"\$(worker_state)\" = DEAD ]"
expect_eq "no chunk lost a copy when $V stopped (nothing to repair)" "$(not_two)" 0
show "curl -X DELETE $API/internal/workers/$V"
curl -s -X DELETE $API/internal/workers/$V | sed 's/^/    /'
expect_eq "$V is no longer part of the cluster" "$(worker_state)" ABSENT
step "AFTER"; cluster_summary
expect_eq "final download identical to the original" "$(curl -s $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)" "${ORIG%% *}"
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
