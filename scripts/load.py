#!/usr/bin/env python3
"""Steady background traffic for the object store, with end-to-end verification.

    python3 scripts/load.py                      # 3 clients until Ctrl-C
    python3 scripts/load.py --clients 6 --duration 300 --max-size 8
    python3 scripts/load.py --cleanup            # only delete objects left behind by an earlier run

Each client loops over a weighted mix: upload a random object (64 KiB to --max-size MiB), download a random one in
full, download a byte range, read its metadata, and delete the oldest objects once more than --objects exist.
Every download is checked: the full body against the SHA-256 computed before the upload, a range against the
exact bytes it should contain. A mismatch (wrong bytes reaching a client) is the one thing that must never
happen; it is reported in red and makes the exit code 2.

Failures that are expected while the system heals (connection refused while the controller restarts, 503 while
fewer than 2 workers are up) are counted separately, so you can watch them appear during chaos and stop again.

Only the Python standard library is used. Objects are named load-<run>-<n>.bin and deleted when the run ends
(Ctrl-C included) unless --keep is given. Start and stop are posted to Grafana as event markers when reachable.
"""
import argparse
import base64
import hashlib
import json
import os
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

GREEN, RED, YEL, DIM, BOLD, RST = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m"
if not sys.stdout.isatty():
    GREEN = RED = YEL = DIM = BOLD = RST = ""

MIB = 1024 * 1024


# ------------------------------------------------------------------------------------------------ http
class Http:
    def __init__(self, api, timeout):
        self.api, self.timeout = api.rstrip("/"), timeout

    def request(self, method, path, body=None, headers=None):
        """Returns (status, body bytes, headers). Raises OSError for connection-level failures."""
        req = urllib.request.Request(self.api + path, data=body, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers

    def upload(self, name, data, metadata):
        boundary = uuid.uuid4().hex
        parts = [
            f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\n\r\n{json.dumps(metadata)}\r\n'.encode(),
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
            f'Content-Type: application/octet-stream\r\n\r\n'.encode(), data, f"\r\n--{boundary}--\r\n".encode()]
        return self.request("POST", "/api/v1/objects", b"".join(parts),
                            {"Content-Type": f"multipart/form-data; boundary={boundary}"})


def grafana_event(url, auth, text, tags):
    """Best effort: a marker on the dashboard's graphs (needs Grafana's admin login, default admin:admin)."""
    try:
        req = urllib.request.Request(
            url.rstrip("/") + "/api/annotations", method="POST",
            data=json.dumps({"text": text, "tags": ["objstore-event"] + tags}).encode(),
            headers={"Content-Type": "application/json",
                     "Authorization": "Basic " + base64.b64encode(auth.encode()).decode()})
        urllib.request.urlopen(req, timeout=3).read()
    except Exception:
        pass


# ------------------------------------------------------------------------------------------------ objects
def make_bytes(seed, size):
    """Deterministic content: any byte range can be recomputed later without keeping the object in memory."""
    return random.Random(seed).randbytes(size)


class Pool:
    """Objects this run uploaded and believes are COMMITTED: object_id -> (seed, size, sha256)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.objects = {}           # insertion order = upload order
        self.deleting = set()

    def add(self, oid, info):
        with self.lock:
            self.objects[oid] = info

    def pick(self, rng):
        with self.lock:
            ids = [o for o in self.objects if o not in self.deleting]
            if not ids:
                return None, None
            oid = rng.choice(ids)
            return oid, self.objects[oid]

    def take_oldest(self, keep):
        with self.lock:
            live = [o for o in self.objects if o not in self.deleting]
            if len(live) <= keep:
                return None
            self.deleting.add(live[0])
            return live[0]

    def forget(self, oid):
        with self.lock:
            self.objects.pop(oid, None)
            self.deleting.discard(oid)

    def __len__(self):
        with self.lock:
            return len(self.objects)


# ------------------------------------------------------------------------------------------------ stats
class Stats:
    KINDS = ("upload", "download", "range", "metadata", "delete")

    def __init__(self):
        self.lock = threading.Lock()
        self.ok = {k: 0 for k in self.KINDS}
        self.fail = {k: 0 for k in self.KINDS}
        self.reasons = {}           # "503 storage_unavailable" -> n
        self.mismatches = 0
        self.lat = {k: [] for k in self.KINDS}
        self.bytes_up = self.bytes_down = 0
        self.window = {k: [] for k in self.KINDS}   # latencies since the last report line

    def record(self, kind, ok, seconds, reason=None, up=0, down=0):
        with self.lock:
            (self.ok if ok else self.fail)[kind] += 1
            if ok:
                self.lat[kind].append(seconds)
                self.window[kind].append(seconds)
            elif reason:
                self.reasons[reason] = self.reasons.get(reason, 0) + 1
            self.bytes_up += up
            self.bytes_down += down

    def mismatch(self):
        with self.lock:
            self.mismatches += 1

    @staticmethod
    def pct(values, p):
        if not values:
            return None
        v = sorted(values)
        return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]

    def snapshot(self):
        with self.lock:
            snap = (dict(self.ok), dict(self.fail), dict(self.reasons), self.mismatches,
                    {k: list(v) for k, v in self.window.items()}, self.bytes_up, self.bytes_down)
            self.window = {k: [] for k in self.KINDS}
            return snap


def reason_of(status, body):
    try:
        code = json.loads(body).get("error", {}).get("code", "")
    except Exception:
        code = ""
    return f"{status} {code}".strip()


# ------------------------------------------------------------------------------------------------ client loop
def client_loop(idx, args, http, pool, stats, stop, run_id, counter):
    rng = random.Random(f"{run_id}-{idx}")
    weights = {"upload": args.upload_weight, "download": 45, "range": 15, "metadata": 10}
    kinds, w = list(weights), list(weights.values())
    while not stop.is_set():
        kind = "upload" if len(pool) < 3 else rng.choices(kinds, w)[0]
        try:
            if kind == "upload":
                do_upload(rng, args, http, pool, stats, run_id, counter)
            elif kind in ("download", "range", "metadata"):
                do_read(kind, rng, http, pool, stats)
            oid = pool.take_oldest(args.objects)
            if oid:
                do_delete(oid, http, pool, stats)
        except Exception as e:      # never let one client die; count it
            stats.record(kind, False, 0, type(e).__name__)
        stop.wait(args.pause * rng.uniform(0.5, 1.5))


def do_upload(rng, args, http, pool, stats, run_id, counter):
    size = int(min(args.max_size * MIB, max(1, rng.choice([64 * 1024, 300 * 1024, MIB + 12345,
                                                            int(3.5 * MIB), 6 * MIB]))))
    seed = rng.getrandbits(64)
    data = make_bytes(seed, size)
    sha = hashlib.sha256(data).hexdigest()
    with counter["lock"]:
        counter["n"] += 1
        n = counter["n"]
    t = time.monotonic()
    try:
        status, body, _ = http.upload(f"load-{run_id}-{n}.bin", data, {"source": "load", "run": run_id})
    except OSError as e:
        stats.record("upload", False, 0, f"connection ({type(e).__name__})")
        return
    dt = time.monotonic() - t
    if status == 201:
        d = json.loads(body)
        if d.get("checksum") != "sha256:" + sha:
            stats.mismatch()
            print(f"{RED}{BOLD}MISMATCH: upload {d.get('object_id')} stored checksum {d.get('checksum')} "
                  f"!= sent sha256:{sha}{RST}", flush=True)
        pool.add(d["object_id"], (seed, size, sha))
        stats.record("upload", True, dt, up=size)
    else:
        stats.record("upload", False, dt, reason_of(status, body))


def do_read(kind, rng, http, pool, stats):
    oid, info = pool.pick(rng)
    if oid is None:
        return
    seed, size, sha = info
    headers, start, end = {}, 0, size - 1
    path = f"/api/v1/objects/{oid}" + ("/metadata" if kind == "metadata" else "")
    if kind == "range":
        start = rng.randrange(size)
        end = min(size - 1, start + rng.randrange(1, 2 * MIB))
        headers["Range"] = f"bytes={start}-{end}"
    t = time.monotonic()
    try:
        status, body, _ = http.request("GET", path, headers=headers)
    except OSError as e:
        stats.record(kind, False, 0, f"connection ({type(e).__name__})")
        return
    dt = time.monotonic() - t
    if status == 404 and pool.objects.get(oid) is None:
        return                      # deleted by another client meanwhile
    want = 206 if kind == "range" else 200
    if status != want:
        stats.record(kind, False, dt, reason_of(status, body))
        return
    if kind == "download":
        good = hashlib.sha256(body).hexdigest() == sha
    elif kind == "range":
        good = body == make_bytes(seed, size)[start:end + 1]
    else:
        good = json.loads(body).get("checksum") == "sha256:" + sha
    if not good:
        stats.mismatch()
        print(f"{RED}{BOLD}MISMATCH: {kind} of {oid} returned wrong bytes "
              f"(range {start}-{end}, got {len(body)} bytes){RST}", flush=True)
    stats.record(kind, good, dt, None if good else "WRONG BYTES", down=len(body) if kind != "metadata" else 0)


def do_delete(oid, http, pool, stats):
    t = time.monotonic()
    try:
        status, body, _ = http.request("DELETE", f"/api/v1/objects/{oid}")
    except OSError as e:
        pool.deleting.discard(oid)  # try again later
        stats.record("delete", False, 0, f"connection ({type(e).__name__})")
        return
    ok = status in (204, 404)
    if ok:
        pool.forget(oid)
    else:
        with pool.lock:
            pool.deleting.discard(oid)
    stats.record("delete", ok, time.monotonic() - t, None if ok else reason_of(status, body))


# ------------------------------------------------------------------------------------------------ reporting
def fmt_ms(v):
    return "   -  " if v is None else f"{v * 1000:5.0f}ms"


def reporter(args, stats, pool, stop, started):
    prev_ok = prev_fail = None
    last_reasons = {}
    print(f"{DIM}  time    ok/s  fail/s  objects   upload p50/p95     download p50/p95   last failures{RST}",
          flush=True)
    while not stop.wait(args.interval):
        ok, fail, reasons, mism, win, _, _ = stats.snapshot()
        tot_ok, tot_fail = sum(ok.values()), sum(fail.values())
        d_ok = tot_ok - (prev_ok or 0)
        d_fail = tot_fail - (prev_fail or 0)
        if prev_fail is not None and d_fail:
            new = {k: v - last_reasons.get(k, 0) for k, v in reasons.items() if v - last_reasons.get(k, 0)}
            why = ", ".join(f"{k} ×{v}" for k, v in sorted(new.items(), key=lambda kv: -kv[1]))
        else:
            why = ""
        last_reasons = reasons
        prev_ok, prev_fail = tot_ok, tot_fail
        el = time.monotonic() - started
        dl = win["download"] + win["range"]
        color = RED if mism else (YEL if d_fail else "")
        print(f"{color}{int(el // 60):3d}:{int(el % 60):02d}  {d_ok / args.interval:6.1f}  {d_fail / args.interval:6.1f}"
              f"  {len(pool):7d}   {fmt_ms(stats.pct(win['upload'], 50))} {fmt_ms(stats.pct(win['upload'], 95))}"
              f"   {fmt_ms(stats.pct(dl, 50))} {fmt_ms(stats.pct(dl, 95))}   {why}{RST}", flush=True)


def summary(stats, started):
    ok, fail, reasons = stats.ok, stats.fail, stats.reasons
    el = time.monotonic() - started
    print(f"\n{BOLD}Summary after {el:.0f}s{RST}")
    print(f"  {'':10} {'ok':>7} {'failed':>7} {'p50':>8} {'p95':>8} {'max':>8}")
    for k in Stats.KINDS:
        lat = stats.lat[k]
        print(f"  {k:10} {ok[k]:7d} {fail[k]:7d} {fmt_ms(stats.pct(lat, 50)):>8} {fmt_ms(stats.pct(lat, 95)):>8} "
              f"{fmt_ms(max(lat) if lat else None):>8}")
    print(f"  uploaded {stats.bytes_up / MIB:.1f} MiB, downloaded {stats.bytes_down / MIB:.1f} MiB (all verified)")
    if reasons:
        print("  failures by reason: " + ", ".join(f"{k} ×{v}" for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])))
    if stats.mismatches:
        print(f"  {RED}{BOLD}{stats.mismatches} response(s) carried WRONG BYTES{RST}")
    else:
        print(f"  {GREEN}no wrong bytes: every successful download matched what was uploaded{RST}")


def cleanup(http, prefix, verbose=True):
    """Delete every object whose name starts with prefix (objects of this or an earlier run)."""
    deleted, cursor = 0, None
    try:
        while True:
            q = "/api/v1/objects?limit=1000" + (f"&cursor={cursor}" if cursor else "")
            status, body, _ = http.request("GET", q)
            if status != 200:
                break
            d = json.loads(body)
            for it in d["items"]:
                if it["name"].startswith(prefix):
                    if http.request("DELETE", f"/api/v1/objects/{it['object_id']}")[0] in (204, 404):
                        deleted += 1
            cursor = d.get("next_cursor")
            if not cursor:
                break
    except OSError as e:
        print(f"{YEL}cleanup incomplete ({e}); run again with --cleanup{RST}")
    if verbose:
        print(f"  deleted {deleted} object(s) named {prefix}*")
    return deleted


# ------------------------------------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default=os.environ.get("API", "http://localhost:8090"))
    ap.add_argument("--clients", type=int, default=3, help="concurrent clients (default 3)")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = until Ctrl-C (default)")
    ap.add_argument("--pause", type=float, default=0.3, help="average pause between operations per client, s")
    ap.add_argument("--max-size", type=float, default=6, help="largest object, MiB (default 6)")
    ap.add_argument("--objects", type=int, default=25, help="objects kept alive; older ones are deleted (default 25)")
    ap.add_argument("--upload-weight", type=int, default=30, help="relative share of uploads (reads: 70)")
    ap.add_argument("--interval", type=float, default=5, help="seconds between report lines")
    ap.add_argument("--timeout", type=float, default=60, help="per-request timeout, s")
    ap.add_argument("--keep", action="store_true", help="do not delete this run's objects at the end")
    ap.add_argument("--cleanup", action="store_true", help="only delete objects named load-* and exit")
    ap.add_argument("--grafana", default=os.environ.get("GRAFANA_URL", "http://localhost:3000"))
    ap.add_argument("--grafana-auth", default=os.environ.get("GRAFANA_AUTH", "admin:admin"))
    args = ap.parse_args()

    http = Http(args.api, args.timeout)
    if args.cleanup:
        cleanup(http, "load-")
        return 0
    try:
        http.request("GET", "/healthz")
    except OSError as e:
        print(f"{RED}controller not reachable at {args.api}: {e}{RST}")
        return 1

    run_id = uuid.uuid4().hex[:6]
    pool, stats, stop = Pool(), Stats(), threading.Event()
    counter = {"n": 0, "lock": threading.Lock()}
    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, lambda *_: stop.set())

    print(f"{BOLD}load: run {run_id}, {args.clients} client(s) against {args.api}"
          f"{f' for {args.duration:.0f}s' if args.duration else ' (Ctrl-C to stop)'}{RST}")
    print(f"{DIM}  mix: uploads {args.upload_weight}, downloads 45, ranges 15, metadata 10; objects up to "
          f"{args.max_size:g} MiB, {args.objects} kept alive; every download is verified byte for byte{RST}")
    grafana_event(args.grafana, args.grafana_auth, f"load started: {args.clients} clients (run {run_id})", ["load"])
    started = time.monotonic()
    threads = [threading.Thread(target=client_loop, args=(i, args, http, pool, stats, stop, run_id, counter),
                                daemon=True) for i in range(args.clients)]
    threads.append(threading.Thread(target=reporter, args=(args, stats, pool, stop, started), daemon=True))
    for t in threads:
        t.start()
    while not stop.wait(0.5):
        if args.duration and time.monotonic() - started >= args.duration:
            stop.set()
    print(f"{DIM}  stopping: waiting for in-flight requests…{RST}")
    for t in threads:
        t.join(timeout=args.timeout)
    summary(stats, started)
    grafana_event(args.grafana, args.grafana_auth, f"load stopped (run {run_id})", ["load"])
    if not args.keep:
        cleanup(http, f"load-{run_id}-")
    return 2 if stats.mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
