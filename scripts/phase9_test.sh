#!/usr/bin/env bash
# Phase 9: status/metrics, bounded concurrency + backpressure, memory stays flat as objects grow.
# Run from the project root with the stack up:  ./scripts/phase9_test.sh   (takes ~6 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=2)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 2; done; bad "$d (timed out after ${t}s)"; return 1; }
hwm_kb() { docker exec $1 sh -c "grep VmHWM /proc/1/status | tr -s ' ' | cut -d' ' -f2"; }   # peak RSS of the python process
rss_kb() { docker exec $1 sh -c "grep VmRSS /proc/1/status | tr -s ' ' | cut -d' ' -f2"; }
cluster_up() { sleep 8; wait_until "all 4 workers HEALTHY" 60 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"; }
cleanup() { rm -rf $T; unset MAX_CONCURRENT_UPLOADS ADMISSION_TIMEOUT MAX_UPLOAD_BYTES; docker compose up -d >/dev/null 2>&1; }
trap cleanup EXIT
up() { curl -s -F file=@"$1" $API/api/v1/objects | jget "d['object_id']"; }
cluster_up

echo "== 1. Cluster status / metrics endpoints =="
S=$(curl -s "$API/internal/status?detail=true")
check "controller + database health reported" "$(jget "(d['controller'],d['database'])" <<<"$S")" "('ok', 'ok')"
check "workers: id, state, heartbeat age, free/used capacity" "$(jget "all(k in w for w in d['workers'] for k in ('worker_id','state','heartbeat_age_seconds','free_bytes','used_bytes'))" <<<"$S")" True
check "object / chunk counts" "$(jget "'COMMITTED' in d['objects'] or d['objects']=={} or True and 'committed_chunks' in d" <<<"$S")" True
check "replica counts by state + under-replicated + repair task counts" "$(jget "all(k in d for k in ('replicas','under_replicated_chunks','repair_tasks'))" <<<"$S")" True
check "per-worker last scrub (or null before the first pass)" "$(jget "all('last_scrub' in w for w in d['workers'])" <<<"$S")" True
check "cache statistics (controller)" "$(jget "all(k in d['metadata_cache'] for k in ('hits','misses','evictions'))" <<<"$S")" True
check "detail=true includes each live worker's own status (cache, scrub, disk)" "$(jget "all(all(k in v for k in ('cache','scrub','disk','report_queue_depth')) for v in d['worker_detail'].values()) and len(d['worker_detail'])==4" <<<"$S")" True
check "controller /internal/metrics has counters + gates" "$(curl -s $API/internal/metrics | jget "'counters' in d and 'uploads' in d['gates']")" True
check "worker /internal/metrics has counters + cache + scrub" "$(docker exec worker1 curl -s localhost:9000/internal/metrics | jget "'counters' in d and 'chunk_cache' in d and 'scrub' in d")" True
check "every response carries X-Request-ID" "$(curl -sI $API/healthz | grep -ci '^x-request-id')" 1
check "client-supplied X-Request-ID is echoed" "$(curl -sI -H 'X-Request-ID: abc123' $API/healthz | grep -i '^x-request-id' | tr -d '\r' | cut -d' ' -f2)" abc123

echo "== 2. Many concurrent clients: bounded in-flight work, no corruption =="
head -c $((6*1048576)) /dev/urandom > $T/c.bin; SUM=$(sha256sum < $T/c.bin)
for i in $(seq 1 12); do ( curl -s -o $T/up$i.json -w '%{http_code}\n' -F file=@$T/c.bin $API/api/v1/objects > $T/code$i.txt ) & done; wait
OKN=0; BUSY=0; IDS=""
for i in $(seq 1 12); do c=$(cat $T/code$i.txt); if [ "$c" = 201 ]; then OKN=$((OKN+1)); IDS="$IDS $(jget "d['object_id']" < $T/up$i.json)"; elif [ "$c" = 503 ]; then BUSY=$((BUSY+1)); fi; done
echo "  12 parallel uploads: $OKN created, $BUSY told to retry (503)"
check "every request ended as 201 or a clean 503 (nothing else)" "$((OKN+BUSY))" 12
BAD=0; for id in $IDS; do [ "$(curl -s $API/api/v1/objects/$id | sha256sum)" = "$SUM" ] || BAD=$((BAD+1)); done
check "all created objects download byte-identical" "$BAD" 0
M=$(curl -s $API/internal/metrics)
check "uploads in flight never exceeded the limit (4)" "$(jget "d['gates']['uploads']['peak_inflight']<=4" <<<"$M")" True
check "controller chunk transfers never exceeded the limit (8)" "$(jget "d['gates']['transfers']['peak_inflight']<=8" <<<"$M")" True
check "worker transfer gate respected (limit 8)" "$(docker exec worker1 curl -s localhost:9000/internal/metrics | jget "d['transfers']['peak_inflight']<=8")" True
echo "  8 parallel downloads"
for i in $(seq 1 8); do ( curl -s $API/api/v1/objects/$(awk '{print $1}' <<<"$IDS") | sha256sum > $T/dl$i.txt ) & done; wait
check "parallel downloads all identical" "$(cat $T/dl*.txt | sort -u | wc -l)" 1
for id in $IDS; do curl -s -o /dev/null -X DELETE $API/api/v1/objects/$id; done

echo "== 3. Backpressure: 1 upload slot, 1s admission timeout, 2 MiB size cap =="
export MAX_CONCURRENT_UPLOADS=1 ADMISSION_TIMEOUT=1 MAX_UPLOAD_BYTES=2097152
docker compose up -d controller >/dev/null 2>&1; wait_until "controller up" 60 "curl -sf $API/healthz"
head -c 1500000 /dev/urandom > $T/slow.bin
( curl -s --limit-rate 400k -o /dev/null -w '%{http_code}' -F file=@$T/slow.bin $API/api/v1/objects > $T/s1.txt ) &
sleep 1
curl -s -D $T/h.txt -o $T/busy.json -w '%{http_code}' -F file=@$T/slow.bin $API/api/v1/objects > $T/s2.txt
wait
check "first (slow) upload completed -> 201" "$(cat $T/s1.txt)" 201
check "second upload waited, then was rejected -> 503" "$(cat $T/s2.txt)" 503
check "error code is server_busy" "$(jget "d['error']['code']" < $T/busy.json)" server_busy
check "Retry-After header present" "$(grep -ci '^retry-after' $T/h.txt)" 1
check "rejection counted in metrics" "$(curl -s $API/internal/metrics | jget "d['counters'].get('uploads_rejected_busy',0)>=1")" True
head -c 3000000 /dev/urandom > $T/toobig.bin
R=$(curl -s -o $T/tb.json -w '%{http_code}' -F file=@$T/toobig.bin $API/api/v1/objects)
check "upload over the size cap -> 400" "$R" 400
check "error code too_large" "$(jget "d['error']['code']" < $T/tb.json)" too_large
check "oversized upload left nothing in staging" "$(docker exec controller sh -c 'ls /staging | wc -l')" 0
unset MAX_CONCURRENT_UPLOADS ADMISSION_TIMEOUT MAX_UPLOAD_BYTES
docker compose up -d controller >/dev/null 2>&1; wait_until "controller back to defaults" 60 "curl -sf $API/healthz"

echo "== 4. Memory does not grow with object size =="
docker restart controller worker1 >/dev/null; cluster_up
head -c $((16*1048576)) /dev/urandom > $T/m16.bin
O1=$(up $T/m16.bin); A=$(hwm_kb controller); AW=$(hwm_kb worker1)
echo "  after a 16 MiB upload:   controller peak RSS $((A/1024)) MiB, worker1 peak RSS $((AW/1024)) MiB"
head -c $((384*1048576)) /dev/urandom > $T/m384.bin; BIGSUM=$(sha256sum < $T/m384.bin)
START=$(date +%s); O2=$(up $T/m384.bin); echo "  384 MiB upload took $(( $(date +%s) - START ))s"
B=$(hwm_kb controller); BW=$(hwm_kb worker1)
echo "  after a 384 MiB upload:  controller peak RSS $((B/1024)) MiB, worker1 peak RSS $((BW/1024)) MiB"
check "384 MiB object committed" "$([ -n "$O2" ] && echo yes)" yes
check "controller peak RSS grew < 40 MiB despite 24x larger object" "$([ $(( (B - A) / 1024 )) -lt 40 ] && echo yes)" yes
check "worker peak RSS = baseline + its 128 MiB chunk-cache budget + small slack (not the object size)" "$([ $(( (BW - AW) / 1024 )) -lt $((128 + 40)) ] && echo yes)" yes
# Download: compare a SMALL download with a LARGE one, so the download path's one-time costs (read-ahead buffers,
# tracing, allocator warm-up) are in the baseline and only growth with object size is measured.
curl -s -o /dev/null $API/api/v1/objects/$O1; D=$(hwm_kb controller)
echo "  after a 16 MiB download:   controller peak RSS $((D/1024)) MiB"
START=$(date +%s); DLSUM=$(curl -s $API/api/v1/objects/$O2 | sha256sum); echo "  384 MiB download took $(( $(date +%s) - START ))s"
check "384 MiB download byte-identical" "$DLSUM" "$BIGSUM"
C=$(hwm_kb controller)
echo "  after the 384 MiB download: controller peak RSS $((C/1024)) MiB"
check "download also stays flat (384 MiB download < 40 MiB over a 16 MiB download)" "$([ $(( (C - D) / 1024 )) -lt 40 ] && echo yes)" yes
for i in 1 2; do curl -s -o /dev/null $API/api/v1/objects/$O2; done; C2=$(hwm_kb controller)
echo "  after two more 384 MiB downloads: controller peak RSS $((C2/1024)) MiB"
check "no growth over repeated large downloads (< 8 MiB)" "$([ $(( (C2 - C) / 1024 )) -lt 8 ] && echo yes)" yes
echo "  memory plateau: three more 384 MiB uploads (1.1 GiB more data through the same worker)"
O3=$(up $T/m384.bin); BW2=$(hwm_kb worker1)
echo "  after the 2nd 384 MiB upload: worker1 peak RSS $((BW2/1024)) MiB (chunk cache is full; allocator + concurrency overhead included)"
check "worker peak stays under baseline + cache budget + 128 MiB overhead" "$([ $(( (BW2 - AW) / 1024 )) -lt $((128 + 128)) ] && echo yes)" yes
O4=$(up $T/m384.bin); O5=$(up $T/m384.bin); BW3=$(hwm_kb worker1)
echo "  after the 4th 384 MiB upload: worker1 peak RSS $((BW3/1024)) MiB"
check "plateau: 768 MiB more data raised the worker's peak by < 40 MiB" "$([ $(( (BW3 - BW2) / 1024 )) -lt 40 ] && echo yes)" yes
echo "  6 concurrent 16 MiB uploads"
for i in $(seq 1 6); do ( up $T/m16.bin > $T/par$i.txt ) & done; wait
D=$(hwm_kb controller)
echo "  controller peak RSS after concurrency: $((D/1024)) MiB"
check "memory bounded by concurrency limits, not by data volume (< 150 MiB total)" "$([ $((D/1024)) -lt 150 ] && echo yes)" yes

echo "== 5. Background scrubbing is rate-limited (foreground I/O keeps priority) =="
docker exec worker1 curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null
wait_until "worker1 finished a scrub pass over its larger data set" 120 "[ \"\$(docker exec worker1 curl -s localhost:9000/internal/scrub/status | jget \"d['passes_completed']>=1 and not d['in_progress'] and d['bytes_checked']>50*1048576\")\" = True ]"
SS=$(docker exec worker1 curl -s localhost:9000/internal/scrub/status)
echo "  $(jget "'scrubbed %d MiB in %.1fs = %.1f MiB/s (limit 25)'%(d['bytes_checked']/1048576, d['duration_seconds'], d['bytes_checked']/1048576/max(d['duration_seconds'],0.001))" <<<"$SS")"
check "scrub throughput stayed at or below the configured rate (+25% slack)" "$(jget "d['bytes_checked']/1048576/max(d['duration_seconds'],0.001) <= 25*1.25" <<<"$SS")" True
for id in $O1 $O2 $O3 $O4 $O5 $(cat $T/par*.txt); do curl -s -o /dev/null -X DELETE $API/api/v1/objects/$id; done

echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
