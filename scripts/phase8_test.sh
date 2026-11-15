#!/usr/bin/env bash
# Phase 8: controller metadata cache + worker chunk cache (LRU/TTL, invalidation, stats, on/off).
# Run from the project root with the stack up:  ./scripts/phase8_test.sh   (takes ~5 minutes)
set -u
API=${API:-http://localhost:8090}
T=$(mktemp -d)
pass=0; fail=0
ok()   { echo "  PASS: $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()  { docker exec postgres psql -U objstore -tAc "$1"; }
wait_until() { local d="$1" t="$2"; shift 2; local i; for ((i=0;i<t;i+=2)); do if eval "$@" >/dev/null 2>&1; then ok "$d (after ~${i}s)"; return 0; fi; sleep 2; done; bad "$d (timed out after ${t}s)"; return 1; }
cstat() { curl -s $API/internal/cache/stats | jget "d['metadata_cache']['$1']"; }
wstat() { docker exec $1 curl -s localhost:9000/internal/cache/stats | jget "d['chunk_cache']['$2']"; }
# worker chunk GET -> "<http code> <HIT|MISS|>" ; body sha256 left in /tmp/b on that worker
wget_chunk() { docker exec $1 sh -c "curl -s -D /tmp/h -o /tmp/b -w '%{http_code}' localhost:9000/internal/chunks/$2; echo -n ' '; grep -i '^x-cache' /tmp/h | tr -d '\r' | cut -d' ' -f2"; }
wbody_sha() { docker exec $1 sha256sum /tmp/b | cut -d' ' -f1; }
cleanup() { rm -rf $T; unset META_CACHE_TTL META_CACHE_SIZE CHUNK_CACHE_BYTES SCRUB_INTERVAL RECONCILE_INTERVAL META_CACHE_ENABLED CHUNK_CACHE_ENABLED; docker compose up -d >/dev/null 2>&1; }
trap cleanup EXIT
up() { curl -s -F file=@"$1" $API/api/v1/objects | jget "d['object_id']"; }
cluster_up() { sleep 10; wait_until "all 4 workers HEALTHY" 60 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"; }

echo "== Setup: small caches so limits are visible (meta: 3 entries, 5s TTL; worker: 3 MiB) =="
export META_CACHE_TTL=5 META_CACHE_SIZE=3 CHUNK_CACHE_BYTES=3145728 SCRUB_INTERVAL=3600 RECONCILE_INTERVAL=600
docker compose up -d >/dev/null 2>&1; cluster_up
head -c $((8*1048576)) /dev/urandom > $T/obj.bin; SUM=$(sha256sum < $T/obj.bin)
OID=$(up $T/obj.bin); echo "  test object $OID (8 chunks)"

echo "== 1. Controller metadata cache: hit/miss =="
curl -s -X POST $API/internal/cache/clear >/dev/null
H0=$(cstat hits); M0=$(cstat misses)
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata; curl -s -o /dev/null $API/api/v1/objects/$OID/metadata
curl -s -o $T/dl.bin $API/api/v1/objects/$OID
check "first read = 1 miss" "$(( $(cstat misses) - M0 ))" 1
check "next two reads (metadata + download) = 2 hits" "$(( $(cstat hits) - H0 ))" 2
check "download via cached manifest is byte-identical" "$(sha256sum < $T/dl.bin)" "$SUM"

echo "== 2. Invalidation on metadata PATCH (no stale metadata) =="
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata   # warm
I0=$(cstat invalidations)
curl -s -o /dev/null -X PATCH -d '{"stage":"patched"}' $API/api/v1/objects/$OID/metadata
check "PATCH invalidated the entry" "$(( $(cstat invalidations) - I0 ))" 1
check "next read shows the new value immediately" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "d['user_metadata'].get('stage')")" patched

echo "== 3. Invalidation on replica health change + repair completion =="
M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][3]['chunk_id']" <<<"$M"); WC=$(jget "d['chunks'][3]['replicas'][0]['worker_id']" <<<"$M")
docker exec $WC curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
docker exec $WC sh -c "printf 'CORRUPTED' | dd of=/data/chunks/${CID:0:2}/$CID bs=1 seek=10 conv=notrunc 2>/dev/null"
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata    # manifest cached while replica still looks HEALTHY
I0=$(cstat invalidations)
for i in $(seq 1 15); do curl -s -o /dev/null $API/api/v1/objects/$OID; [ "$(( $(cstat invalidations) - I0 ))" -ge 1 ] && break; done
check "read-time corruption detection invalidated the cached manifest" "$([ $(( $(cstat invalidations) - I0 )) -ge 1 ] && echo yes)" yes
S_DB=$(sql "select state from replicas where chunk_id='$CID' and worker_id='$WC'")
S_API=$(curl -s $API/api/v1/objects/$OID/metadata | jget "[r['state'] for r in d['chunks'][3]['replicas'] if r['worker_id']=='$WC'][0]")
S_DB2=$(sql "select state from replicas where chunk_id='$CID' and worker_id='$WC'")
check "metadata API never shows a state the DB did not have (db=$S_DB api=$S_API db=$S_DB2)" "$([ "$S_API" = "$S_DB" ] || [ "$S_API" = "$S_DB2" ] && echo yes)" yes
wait_until "repair finished" 60 "[ \"\$(sql \"select state from replicas where chunk_id='$CID' and worker_id='$WC'\")\" = HEALTHY ]"
check "after repair the API reports the replica HEALTHY" "$(curl -s $API/api/v1/objects/$OID/metadata | jget "[r['state'] for r in d['chunks'][3]['replicas'] if r['worker_id']=='$WC'][0]")" HEALTHY
check "download identical after repair" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 4. TTL expiry =="
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata; E0=$(cstat expirations); sleep 7
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata
check "entry expired after the 5s TTL" "$(( $(cstat expirations) - E0 ))" 1

echo "== 5. Size bound (3 entries) + LRU eviction =="
IDS=""; printf 'tiny' > $T/t.bin
for i in 1 2 3 4 5; do IDS="$IDS $(up $T/t.bin)"; done
EV0=$(cstat evictions)
for id in $IDS; do curl -s -o /dev/null $API/api/v1/objects/$id/metadata; done
check "never more than 3 entries" "$([ "$(cstat entries)" -le 3 ] && echo yes)" yes
check "older entries were evicted (>=2)" "$([ $(( $(cstat evictions) - EV0 )) -ge 2 ] && echo yes)" yes
for id in $IDS; do curl -s -o /dev/null -X DELETE $API/api/v1/objects/$id; done

echo "== 6. Invalidation on worker state change =="
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata
I0=$(cstat invalidations); WK=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
docker stop $WK >/dev/null
wait_until "$WK DEAD" 45 "[ \"\$(sql \"select state from workers where worker_id='$WK'\")\" = DEAD ]"
check "worker going DEAD cleared the cache" "$([ $(( $(cstat invalidations) - I0 )) -ge 1 ] && echo yes)" yes
check "download still correct with $WK down" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
docker start $WK >/dev/null; sleep 10

echo "== 7. Controller restart clears its cache (data is in PostgreSQL) =="
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata
docker restart controller >/dev/null; wait_until "controller back" 60 "curl -sf $API/healthz"
check "cache empty after restart" "$(cstat entries)" 0
check "object still served after restart" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"

echo "== 8. Delete: no stale manifest =="
curl -s -o /dev/null $API/api/v1/objects/$OID/metadata   # warm
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
check "metadata -> 404 right after DELETE" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$OID/metadata)" 404
check "download -> 404 right after DELETE" "$(curl -s -o /dev/null -w '%{http_code}' $API/api/v1/objects/$OID)" 404

echo "== 9. Worker chunk cache: write-through, hit/miss, stats =="
OID=$(up $T/obj.bin); M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][7]['chunk_id']" <<<"$M"); W=$(jget "d['chunks'][7]['replicas'][0]['worker_id']" <<<"$M"); EXP=$(jget "d['chunks'][7]['checksum']" <<<"$M")
check "just-written chunk is already cached (write-through) -> HIT" "$(wget_chunk $W $CID)" "200 HIT"
docker exec $W curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
DR0=$(wstat $W disk_reads); H0=$(wstat $W hits); B0=$(wstat $W bytes_served_from_cache)
check "cold read -> MISS (from disk)" "$(wget_chunk $W $CID)" "200 MISS"
check "warm read -> HIT" "$(wget_chunk $W $CID)" "200 HIT"
check "exactly 1 disk read for the two reads" "$(( $(wstat $W disk_reads) - DR0 ))" 1
check "hit counted" "$(( $(wstat $W hits) - H0 ))" 1
check "bytes_served_from_cache grew by one chunk" "$(( $(wstat $W bytes_served_from_cache) - B0 ))" "$(jget "d['chunks'][7]['size']" <<<"$M")"
check "cached bytes verify against authoritative checksum" "sha256:$(wbody_sha $W)" "$EXP"

echo "== 10. Byte budget + eviction never deletes files =="
BIG=$(jget "max(set(r['worker_id'] for c in d['chunks'] for r in c['replicas']), key=lambda w: sum(1 for c in d['chunks'] for r in c['replicas'] if r['worker_id']==w))" <<<"$M")
MYC=$(jget "' '.join(c['chunk_id'] for c in d['chunks'] if any(r['worker_id']=='$BIG' for r in c['replicas']))" <<<"$M")
echo "  $BIG holds $(wc -w <<<"$MYC") chunks of this object (budget 3 MiB)"
docker exec $BIG curl -s -X POST localhost:9000/internal/cache/clear >/dev/null; EV0=$(wstat $BIG evictions)
for c in $MYC; do wget_chunk $BIG $c >/dev/null; done
check "cache bytes <= 3 MiB budget" "$([ "$(wstat $BIG bytes)" -le 3145728 ] && echo yes)" yes
check "evictions happened" "$([ $(( $(wstat $BIG evictions) - EV0 )) -ge 1 ] && echo yes)" yes
ALLOK=yes; for c in $MYC; do exp=$(jget "[x['checksum'] for x in d['chunks'] if x['chunk_id']=='$c'][0]" <<<"$M"); r=$(wget_chunk $BIG $c); [ "sha256:$(wbody_sha $BIG)" = "$exp" ] || ALLOK=no; done
check "every chunk still readable from disk after evictions" "$ALLOK" yes
check "chunk files still all on disk (eviction never deletes)" "$(docker exec $BIG curl -s localhost:9000/internal/workers/$BIG/chunks | jget "sum(1 for c in d['chunks'] if c['chunk_id'].startswith('$OID'))")" "$(wc -w <<<"$MYC")"

echo "== 11. Corruption: cached copy evicted when the scrubber finds the damage =="
docker exec $W curl -s -X POST localhost:9000/internal/cache/clear >/dev/null
wget_chunk $W $CID >/dev/null   # load into cache (MISS)
check "chunk is cached" "$(wget_chunk $W $CID)" "200 HIT"
docker exec $W sh -c "printf 'CORRUPTED' | dd of=/data/chunks/${CID:0:2}/$CID bs=1 seek=10 conv=notrunc 2>/dev/null"
R=$(wget_chunk $W $CID); echo "  after silent disk corruption a read returns: $R (cache serves the previously verified bytes)"
check "cached good bytes still served (documented trade-off, scrubber closes the gap)" "$R" "200 HIT"
INV0=$(wstat $W invalidations)
docker exec $W curl -s -X POST localhost:9000/internal/scrub/trigger >/dev/null; sleep 4
check "scrub finding evicted the cached entry (invalidation counter +1)" "$(( $(wstat $W invalidations) - INV0 ))" 1
wait_until "repaired replica serves correct bytes again" 60 "[ \"\$(sql \"select state from replicas where chunk_id='$CID' and worker_id='$W'\")\" = HEALTHY ]"
wget_chunk $W $CID >/dev/null
check "repaired bytes are the original chunk" "sha256:$(wbody_sha $W)" "$EXP"

echo "== 12. Delete evicts cached chunks =="
wget_chunk $W $CID >/dev/null
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
wait_until "chunk gone from worker" 40 "[ \"\$(wget_chunk $W $CID | cut -d' ' -f1)\" = 404 ]"
check "deleted chunk is not served from cache" "$(wget_chunk $W $CID | cut -d' ' -f1)" 404

echo "== 13. Worker restart clears RAM cache, disk survives =="
OID=$(up $T/obj.bin); M=$(curl -s $API/api/v1/objects/$OID/metadata)
CID=$(jget "d['chunks'][0]['chunk_id']" <<<"$M"); W=$(jget "d['chunks'][0]['replicas'][0]['worker_id']" <<<"$M")
docker restart $W >/dev/null; sleep 8
check "cache empty after restart" "$(wstat $W entries)" 0
check "chunk still readable (MISS from disk)" "$(wget_chunk $W $CID)" "200 MISS"

echo "== 14. Caches disabled: correct, and counters stay at zero =="
export META_CACHE_ENABLED=false CHUNK_CACHE_ENABLED=false
docker compose up -d >/dev/null 2>&1; cluster_up
check "metadata cache reports disabled" "$(cstat enabled)" False
for i in 1 2 3; do curl -s -o /dev/null $API/api/v1/objects/$OID/metadata; done
check "no metadata hits when disabled" "$(cstat hits)" 0
check "download identical with caches off" "$(curl -s $API/api/v1/objects/$OID | sha256sum)" "$SUM"
check "worker read -> MISS" "$(wget_chunk $W $CID)" "200 MISS"
check "worker read again -> still MISS (no caching)" "$(wget_chunk $W $CID)" "200 MISS"
check "worker hits stay 0" "$(wstat $W hits)" 0
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID

echo; echo "Result: $pass passed, $fail failed"; [ $fail -eq 0 ]
