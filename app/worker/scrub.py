"""Background checksum scrubber + durable report queue.

The worker only DETECTS and REPORTS. It never changes global replica state: findings go to the
controller (POST /internal/workers/scrub-results), which decides and coordinates the repair.

* Walks local chunk files with 64 KiB reads, one chunk at a time, rate limited (SCRUB_RATE_MB_S)
  so it can't starve foreground reads/writes.
* Start time is staggered per worker (hash of worker id) so the four workers don't scan together.
* Findings are written to a durable on-disk queue (one fsynced file per report) and removed only
  after the controller acknowledges them, so a controller outage loses nothing.
* Progress/counters are persisted to DATA_DIR/scrub_state.json.
"""
import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
import zlib
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app.common import checksum as cs
from app.common import tracing
from app.common import metrics
from app.common.config import settings
from app.worker import cache as wcache
from app.worker import disk

log = logging.getLogger("worker.scrub")
BUF = 64 * 1024

trigger = asyncio.Event()
state: dict = {
    "in_progress": False, "passes_completed": 0, "last_scrub_started": None, "last_scrub_finished": None,
    "chunks_total": 0, "chunks_checked": 0, "bytes_checked": 0, "healthy": 0,
    "corrupt_found": 0, "missing_found": 0, "unreadable_found": 0, "skipped": 0,
    "duration_seconds": 0.0, "report_delivery_failures": 0, "reports_delivered": 0,
}


# ---------------------------------------------------------------- persistence
def _state_file() -> Path:
    return Path(settings.data_dir) / "scrub_state.json"


def _queue_dir() -> Path:
    return Path(settings.data_dir) / "report_queue"


def load_state() -> None:
    _queue_dir().mkdir(parents=True, exist_ok=True)
    try:
        state.update(json.loads(_state_file().read_text()))
        state["in_progress"] = False
    except (FileNotFoundError, ValueError):
        pass


def save_state() -> None:
    tmp = Path(settings.data_dir) / f"tmp/scrub_state.{uuid.uuid4().hex}"
    with open(tmp, "w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, _state_file())


def queue_depth() -> int:
    try:
        return sum(1 for _ in _queue_dir().glob("*.json"))
    except FileNotFoundError:
        return 0


def enqueue(name: str, endpoint: str, payload: dict) -> None:
    """Durably queue a report. Same name = same report, so repeats overwrite instead of piling up."""
    q = _queue_dir()
    q.mkdir(parents=True, exist_ok=True)
    tmp = Path(settings.data_dir) / f"tmp/q.{uuid.uuid4().hex}"
    with open(tmp, "w") as f:
        json.dump({"endpoint": endpoint, "payload": payload}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, q / f"{name}.json")


# ---------------------------------------------------------------- local checking (runs in a thread)
def _throttle(nbytes: int, started: float) -> None:
    rate = settings.scrub_rate_mb_s * 1024 * 1024
    if rate > 0:
        ahead = nbytes / rate - (time.monotonic() - started)
        if ahead > 0:
            time.sleep(ahead)


def check_local(chunk_id: str) -> dict | None:
    """Hash one chunk file against its local checksum record. None = nothing to report/skip."""
    cp = disk.chunk_path(chunk_id)
    expected = disk.read_sum(chunk_id)
    if not cp.exists():
        age = disk.sum_age_seconds(chunk_id)
        # a young .sum with no chunk may just be a write in flight (sum is written first)
        if expected is not None and age is not None and age >= settings.scrub_missing_min_age:
            return {"status": "MISSING", "expected": expected, "actual": None, "bytes": 0}
        return None
    h = hashlib.sha256()
    n = 0
    t0 = time.monotonic()
    try:
        with open(cp, "rb") as f:
            while True:
                buf = f.read(BUF)
                if not buf:
                    break
                h.update(buf)
                n += len(buf)
                _throttle(n, t0)
    except FileNotFoundError:
        return None  # deleted while scanning
    except OSError as e:
        return {"status": "UNREADABLE", "expected": expected, "actual": None, "bytes": n, "error": str(e)}
    actual = cs.fmt(h.hexdigest())
    if expected is None:
        return {"status": "NOSUM", "expected": None, "actual": actual, "bytes": n}
    return {"status": "HEALTHY" if actual == expected else "MISMATCH",
            "expected": expected, "actual": actual, "bytes": n}


# ---------------------------------------------------------------- one pass
async def _authoritative(client: httpx.AsyncClient, chunk_id: str) -> str | None:
    """Ask the controller for the authoritative checksum (the local record could be the damaged part)."""
    try:
        r = await client.get(f"/internal/chunks/{chunk_id}/expected")
        if r.status_code == 200:
            return r.json()["checksum"]
    except httpx.HTTPError:
        pass
    return None


async def _report(chunk_id: str, status: str, expected, actual) -> None:
    wcache.chunk_cache.invalidate(chunk_id)  # never keep serving a replica we just found damaged
    metrics.inc(f"scrub_findings_{status.lower()}")
    payload = {
        "worker_id": settings.worker_id, "chunk_id": chunk_id, "status": status,
        "expected_checksum": expected, "actual_checksum": actual,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    await asyncio.to_thread(enqueue, f"{chunk_id}__{status}", "/internal/workers/scrub-results", payload)
    log.warning("scrub finding: %s %s (expected=%s actual=%s)", status, chunk_id, expected, actual)


async def run_pass(client: httpx.AsyncClient) -> None:
    ids = await asyncio.to_thread(disk.scrub_candidates)
    t0 = time.monotonic()
    state.update(in_progress=True, last_scrub_started=datetime.now(timezone.utc).isoformat(),
                 chunks_total=len(ids), chunks_checked=0, bytes_checked=0, healthy=0,
                 corrupt_found=0, missing_found=0, unreadable_found=0, skipped=0)
    log.info("scrub pass started: %d chunk(s)", len(ids))
    for i, cid in enumerate(ids, 1):
        res = await asyncio.to_thread(check_local, cid)
        if res is None:
            state["skipped"] += 1
            continue
        state["chunks_checked"] += 1
        state["bytes_checked"] += res["bytes"]
        metrics.inc("scrub_chunks_checked")  # cumulative (state[] holds only the current/last pass)
        metrics.inc("scrub_bytes_checked", res["bytes"])
        st = res["status"]
        if st in ("MISMATCH", "NOSUM"):
            auth = await _authoritative(client, cid)
            if auth is not None and auth == res["actual"]:
                # file is fine; only the local checksum record was wrong/missing -> repair the record
                await asyncio.to_thread(disk.write_sum, cid, auth)
                st = "HEALTHY"
            elif auth is not None:
                await _report(cid, "CORRUPT", auth, res["actual"])
                state["corrupt_found"] += 1
                continue
            elif st == "MISMATCH":  # controller unreachable: report with the local expectation
                await _report(cid, "CORRUPT", res["expected"], res["actual"])
                state["corrupt_found"] += 1
                continue
            else:
                state["skipped"] += 1  # NOSUM and no authority (orphan/unknown): cannot judge
                continue
        if st == "HEALTHY":
            state["healthy"] += 1
        elif st == "MISSING":
            await _report(cid, "MISSING", res["expected"], None)
            state["missing_found"] += 1
        elif st == "UNREADABLE":
            await _report(cid, "UNREADABLE", res["expected"], None)
            state["unreadable_found"] += 1
        if i % 100 == 0:
            await asyncio.to_thread(save_state)
        await asyncio.sleep(0)  # yield to foreground requests between chunks

    state["duration_seconds"] = round(time.monotonic() - t0, 3)
    state["last_scrub_finished"] = datetime.now(timezone.utc).isoformat()
    state["passes_completed"] += 1
    state["in_progress"] = False
    summary = {k: state[k] for k in ("chunks_checked", "bytes_checked", "corrupt_found", "missing_found",
                                      "unreadable_found", "duration_seconds", "passes_completed")}
    summary.update(worker_id=settings.worker_id, last_scrub_at=state["last_scrub_finished"])
    await asyncio.to_thread(enqueue, "__summary__", "/internal/workers/scrub-summary", summary)
    await asyncio.to_thread(save_state)
    log.info("scrub pass done: checked=%d healthy=%d corrupt=%d missing=%d unreadable=%d in %.2fs",
             state["chunks_checked"], state["healthy"], state["corrupt_found"], state["missing_found"],
             state["unreadable_found"], state["duration_seconds"])


async def scrub_loop() -> None:
    await asyncio.to_thread(load_state)
    # stagger: each worker starts at a different offset inside the interval
    offset = zlib.crc32(settings.worker_id.encode()) % max(1, int(settings.scrub_interval))
    log.info("scrubber: interval=%.0fs, first pass in %ds (staggered)", settings.scrub_interval, offset)
    async with httpx.AsyncClient(base_url=settings.controller_url, timeout=5.0) as client:
        wait = float(offset)
        while True:
            try:
                await asyncio.wait_for(trigger.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
            trigger.clear()
            try:
                await run_pass(client)
            except asyncio.CancelledError:
                raise
            except Exception:
                state["in_progress"] = False
                log.exception("scrub pass crashed")
            wait = settings.scrub_interval


# ---------------------------------------------------------------- report delivery
async def delivery_loop() -> None:
    """Send queued reports; delete a file only after the controller acknowledges it."""
    last_warn = 0.0
    async with httpx.AsyncClient(base_url=settings.controller_url, timeout=5.0) as client:
        tracing.instrument_client(client)  # a report and the controller's reaction form one trace
        while True:
            try:
                files = sorted(_queue_dir().glob("*.json"))
                for f in files:
                    try:
                        item = json.loads(f.read_text())
                    except (ValueError, FileNotFoundError):
                        f.unlink(missing_ok=True)
                        continue
                    try:
                        p = item["payload"]
                        if "chunk_id" in p:  # a finding (summaries are not traced)
                            with tracing.operation(f"scrub report {p.get('status')}", chunk_id=p["chunk_id"],
                                                   worker=p.get("worker_id"), expected=p.get("expected_checksum"),
                                                   actual=p.get("actual_checksum")):
                                r = await client.post(item["endpoint"], json=p)
                        else:
                            r = await client.post(item["endpoint"], json=p)
                    except httpx.HTTPError as e:
                        state["report_delivery_failures"] += 1
                        if time.monotonic() - last_warn > 30:
                            log.warning("controller unreachable; %d report(s) kept in durable queue (%r)", len(files), e)
                            last_warn = time.monotonic()
                        break  # controller down: retry the whole queue later
                    if r.status_code < 300 or r.status_code in (400, 422):
                        # acknowledged (or permanently invalid: retrying can never succeed)
                        f.unlink(missing_ok=True)
                        state["reports_delivered"] += 1
                        if r.status_code >= 400:
                            log.error("controller rejected report %s: %s", f.name, r.text.strip())
                    else:
                        state["report_delivery_failures"] += 1  # 404 unknown worker / 5xx: keep and retry
                        break
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("report delivery error")
            await asyncio.sleep(2.0)
