#!/usr/bin/env bash
# Compares read latency: caches ON (cold vs warm) and caches OFF.  ./scripts/cache_benchmark.sh
# Everything runs on localhost, so absolute numbers are small; the point is the relative difference
# and the hit/miss counters. N and SIZE_MB can be overridden: N=10 SIZE_MB=32 ./scripts/cache_benchmark.sh
set -u
API=${API:-http://localhost:8090}; N=${N:-8}; SIZE_MB=${SIZE_MB:-16}
T=$(mktemp -d)
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
cleanup() { rm -rf $T; unset META_CACHE_ENABLED CHUNK_CACHE_ENABLED; docker compose up -d >/dev/null 2>&1; }
trap cleanup EXIT
clear_all() { curl -s -X POST $API/internal/cache/clear >/dev/null; for w in worker1 worker2 worker3 worker4; do docker exec $w curl -s -X POST localhost:9000/internal/cache/clear >/dev/null; done; }
avg_ms() { python3 -c "import sys;v=[float(x) for x in sys.stdin.read().split()];print('%.1f'%(1000*sum(v)/len(v)))"; }
time_dl()  { curl -s -o /dev/null -w '%{time_total}\n' $API/api/v1/objects/$OID; }
time_md()  { curl -s -o /dev/null -w '%{time_total}\n' $API/api/v1/objects/$OID/metadata; }
wait_up() { for i in $(seq 1 40); do [ "$(curl -s $API/internal/status 2>/dev/null | jget "sum(1 for w in d['workers'] if w['state']=='HEALTHY')" 2>/dev/null)" = 4 ] && return; sleep 2; done; }

echo "Preparing a ${SIZE_MB} MiB object (N=$N timed runs per scenario)..."
docker compose up -d >/dev/null 2>&1; wait_up
head -c $((SIZE_MB*1048576)) /dev/urandom > $T/o.bin
OID=$(curl -s -F file=@$T/o.bin $API/api/v1/objects | jget "d['object_id']")

# ---- caches ON
cold_dl=$(for i in $(seq $N); do clear_all; time_dl; done | avg_ms)
cold_md=$(for i in $(seq $N); do clear_all; time_md; done | avg_ms)
time_dl >/dev/null; time_md >/dev/null
warm_dl=$(for i in $(seq $N); do time_dl; done | avg_ms)
warm_md=$(for i in $(seq $N); do time_md; done | avg_ms)
mh=$(curl -s $API/internal/cache/stats | jget "d['metadata_cache']['hits']")
wh=0; wd=0
for w in worker1 worker2 worker3 worker4; do
  s=$(docker exec $w curl -s localhost:9000/internal/cache/stats)
  wh=$((wh + $(jget "d['chunk_cache']['hits']" <<<"$s"))); wd=$((wd + $(jget "d['chunk_cache']['disk_reads']" <<<"$s")))
done

# ---- caches OFF
export META_CACHE_ENABLED=false CHUNK_CACHE_ENABLED=false
docker compose up -d >/dev/null 2>&1; sleep 8; wait_up
time_dl >/dev/null
off_dl=$(for i in $(seq $N); do time_dl; done | avg_ms)
off_md=$(for i in $(seq $N); do time_md; done | avg_ms)
off_hits=$(curl -s $API/internal/cache/stats | jget "d['metadata_cache']['hits']")

echo
printf '%-34s %12s %12s %12s\n' "scenario (avg of $N, ms)" "cold" "warm" "caches OFF"
printf '%-34s %12s %12s %12s\n' "download ${SIZE_MB} MiB object" "$cold_dl" "$warm_dl" "$off_dl"
printf '%-34s %12s %12s %12s\n' "metadata GET (manifest)" "$cold_md" "$warm_md" "$off_md"
echo
echo "counters while ON: controller metadata hits=$mh; worker chunk-cache hits=$wh, disk reads=$wd"
echo "counter while OFF: controller metadata hits=$off_hits (must be 0)"
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
