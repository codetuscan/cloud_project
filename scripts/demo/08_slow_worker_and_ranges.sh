#!/usr/bin/env bash
# Extra (beyond §10): a hung/slow worker does not slow down downloads (hedged reads), and partial downloads (Range).
source "$(dirname "$0")/lib.sh"
V=worker2
banner "8. SLOW WORKER (hedged reads) and PARTIAL DOWNLOADS (Range)"
metric() { curl -s $API/metrics | awk -v m="objstore_$1" '$1==m {print $2}'; }
wait_cluster

head -c $((5*1048576 + 524288)) /dev/urandom > $DEMO_TMP/h.bin; ORIG=$(sha256sum < $DEMO_TMP/h.bin)
OID=$(upload $DEMO_TMP/h.bin | jget "d['object_id']")
ON=$(curl -s $API/api/v1/objects/$OID/metadata | jget "sum(any(r['worker_id']=='$V' for r in c['replicas']) for c in d['chunks'])")
note "object: 6 chunks, $ON of them have a copy on $V"
for i in 1 2 3; do curl -s -o /dev/null $API/api/v1/objects/$OID; done

step "A worker that is up but does not answer"
show "docker pause $V      # process frozen: connections are accepted, requests never answered"
H0=$(metric read_hedge_wins_total)
docker pause $V >/dev/null
note "hedge delay now: $(metric read_hedge_delay_seconds)s (p95 of recent chunk reads)"
SLOWEST=0
for i in 1 2 3 4 5; do
  T=$( { /usr/bin/time -f %e curl -s -o $DEMO_TMP/out.bin $API/api/v1/objects/$OID; } 2>&1 )
  SUM=$(sha256sum < $DEMO_TMP/out.bin)
  note "download $i: ${T}s  $( [ "$SUM" = "$ORIG" ] && echo identical || echo DIFFERENT)"
  [ "$SUM" = "$ORIG" ] || badmsg "download $i differs"
  SLOWEST=$(python3 -c "print(max($SLOWEST, $T))")
done
H1=$(metric read_hedge_wins_total)
docker unpause $V >/dev/null
expect_eq "every download finished in under 2 s although $V never answered" "$(python3 -c "print($SLOWEST < 2)")" True
expect_eq "the other replica answered the hedged requests" "$(python3 -c "print(${H1:-0} > ${H0:-0})")" True
note "hedge wins during the pause: $(python3 -c "print(int(${H1:-0} - ${H0:-0}))") (without hedging each such read waits for the 30 s timeout)"
docker compose logs --since 1m controller 2>&1 | grep "hedging to" | head -2 | sed 's/^/    /'
expect_eq "$V was never marked DEAD or bad (it was only slow, for a few seconds)" \
  "$(sql "select count(*) from replicas where worker_id='$V' and state<>'HEALTHY'")" 0

step "Partial downloads (HTTP Range)"
rng() {  # rng <curl range> <python slice> <label>
  local got want
  got=$(curl -s -r "$1" $API/api/v1/objects/$OID | sha256sum | cut -d' ' -f1)
  want=$(python3 -c "import hashlib;print(hashlib.sha256(open('$DEMO_TMP/h.bin','rb').read()[$2]).hexdigest())")
  expect_eq "$3: $(curl -s -o /dev/null -w '%{http_code} %header{content-range}' -r "$1" $API/api/v1/objects/$OID)" "$got" "$want"
}
rng 0-99 0:100 "first 100 bytes"
rng 1048570-1048585 1048570:1048586 "16 bytes across the chunk 0/1 boundary"
rng -1000 -1000: "last 1000 bytes"
rng 3000000- 3000000: "from byte 3000000 to the end"
expect_eq "range past the end -> 416" "$(curl -s -o /dev/null -w '%{http_code}' -r 9999999- $API/api/v1/objects/$OID)" 416
curl -s -o /dev/null -X DELETE $API/api/v1/objects/$OID
finish
