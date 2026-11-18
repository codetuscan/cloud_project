"""Read path for downloads: hedged chunk reads, read-ahead, and HTTP Range parsing.

Hedged reads ("The Tail at Scale"): ask one replica; if it has not answered within the hedge delay, ask the
second one too and use whichever returns VERIFIED bytes first, cancelling the other. A replica that fails outright
(unreachable, 404, corrupt) is replaced at once, as before. The delay adapts to the cluster: it is the p95 of recent
successful chunk reads (clamped to READ_HEDGE_MIN_DELAY..READ_HEDGE_MAX_DELAY), so only the slowest ~5% of reads send
a second request; a fixed READ_HEDGE_DELAY can be configured instead. No hedge is sent while the transfer gate is
full, so hedging never adds load to an already busy controller.

Read-ahead: while chunk i is being sent to the client, chunks i+1..i+READ_AHEAD_CHUNKS are already being fetched,
which keeps a large download from paying one worker round trip per chunk. Memory stays bounded at
(1 + READ_AHEAD_CHUNKS) chunks per download.

Every chunk is still verified against its authoritative checksum in full before any byte of it is sent, also for
Range requests (the needed chunks are fetched whole and sliced after verification).
"""
import asyncio
import logging
import time
from collections import deque

import httpx

from app.common import checksum as cs
from app.common import metrics
from app.common.config import settings
from app.controller import replicas

log = logging.getLogger("controller.reads")


# ------------------------------------------------------------------ adaptive hedge delay
class LatencyTracker:
    """Recent successful chunk-read latencies; the hedge delay is their p95."""

    def __init__(self, size: int = 256, min_samples: int = 20):
        self.samples: deque[float] = deque(maxlen=size)
        self.min_samples = min_samples
        self._cached: float | None = None
        self._since = 0

    def add(self, seconds: float) -> None:
        self.samples.append(seconds)
        self._since += 1
        if self._since >= 16:  # recompute the percentile every 16 samples, not on every chunk
            self._cached, self._since = None, 0

    def p95(self) -> float | None:
        if len(self.samples) < self.min_samples:
            return None
        if self._cached is None:
            s = sorted(self.samples)
            self._cached = s[min(len(s) - 1, int(0.95 * len(s)))]
        return self._cached


tracker = LatencyTracker()


def hedge_delay() -> float:
    if settings.read_hedge_delay > 0:
        return settings.read_hedge_delay
    p95 = tracker.p95()
    if p95 is None:  # not enough history yet
        p95 = 0.1
    return min(settings.read_hedge_max_delay, max(settings.read_hedge_min_delay, p95))


# ------------------------------------------------------------------ one replica
async def _mark(pool, chunk_id, worker_id, state, reason) -> None:
    # shielded: a hedge that is cancelled must not abort a half-written state change
    await asyncio.shield(replicas.mark_bad(pool, chunk_id, worker_id, state, reason))


async def try_replica(state, chunk, rep) -> bytes | None:
    """Verified bytes from one replica, or None (problems are logged and the replica marked). Never raises,
    except CancelledError."""
    t0 = time.monotonic()
    try:
        slot = await state.transfer_sem.enter()
        try:
            r = await state.http.get(f"{rep['address']}/internal/chunks/{chunk['chunk_id']}")
        finally:
            slot.release()
    except httpx.HTTPError as e:
        log.warning("replica %s on %s unreachable: %r", chunk["chunk_id"], rep["worker_id"], e)
        return None
    if r.status_code == 404:
        await _mark(state.pool, chunk["chunk_id"], rep["worker_id"], "MISSING", "read: 404")
        return None
    if r.status_code != 200:
        if "chunk_corrupt" in r.text:
            await _mark(state.pool, chunk["chunk_id"], rep["worker_id"], "CORRUPT", "read: worker self-check failed")
        else:
            log.warning("replica %s on %s returned %s", chunk["chunk_id"], rep["worker_id"], r.status_code)
        return None
    data = r.content
    if len(data) != chunk["size"] or cs.sha256_bytes(data) != chunk["checksum"]:
        await _mark(state.pool, chunk["chunk_id"], rep["worker_id"], "CORRUPT", "read: checksum mismatch at controller")
        return None
    tracker.add(time.monotonic() - t0)
    return data


# ------------------------------------------------------------------ hedged fetch of one chunk
async def fetch_chunk(state, chunk, candidates: list[dict]) -> bytes:
    """Verified bytes of one chunk from the first replica that delivers them (see module docstring)."""
    queue = list(candidates)
    pending: dict[asyncio.Task, dict] = {}
    hedge_tasks: set[asyncio.Task] = set()
    hedged = False

    def launch() -> asyncio.Task:
        rep = queue.pop(0)
        t = asyncio.create_task(try_replica(state, chunk, rep))
        pending[t] = rep
        return t

    launch()
    try:
        while pending:
            can_hedge = (settings.read_hedge_enabled and not hedged and queue and len(pending) == 1)
            done, _ = await asyncio.wait(pending, timeout=hedge_delay() if can_hedge else None,
                                         return_when=asyncio.FIRST_COMPLETED)
            if not done:  # the replica is slow: ask the next one too
                hedged = True
                gate = state.transfer_sem
                if gate.inflight >= gate.limit:  # busy controller: never add load, keep waiting instead
                    metrics.inc("read_hedges_skipped_busy")
                    continue
                metrics.inc("read_hedges")
                log.info("chunk %s: %s slower than %.0f ms, hedging to %s", chunk["chunk_id"],
                         next(iter(pending.values()))["worker_id"], hedge_delay() * 1000, queue[0]["worker_id"])
                hedge_tasks.add(launch())
                continue
            for t in done:
                pending.pop(t)
                data = t.result()
                if data is not None:
                    if t in hedge_tasks:
                        metrics.inc("read_hedge_wins")
                    return data
            if not pending and queue:  # every request so far failed: fall back to the next replica
                metrics.inc("replica_read_fallbacks")
                launch()
    finally:
        for t in pending:
            t.cancel()
    raise RuntimeError(f"no readable replica for chunk {chunk['chunk_id']}")


# ------------------------------------------------------------------ read-ahead
async def stream_chunks(fetch, items: list, ahead: int):
    """Yield fetch(item) results in order, with up to `ahead` later items already being fetched."""
    it = iter(items)
    window: deque[asyncio.Task] = deque()

    def schedule() -> None:
        item = next(it, None)
        if item is not None:
            window.append(asyncio.create_task(fetch(item)))

    for _ in range(1 + max(0, ahead)):
        schedule()
    try:
        while window:
            data = await window.popleft()
            schedule()
            yield data
    finally:
        for t in window:
            t.cancel()
        for t in window:  # retrieve outcomes so a failed prefetch is not reported as "never retrieved"
            try:
                await t
            except BaseException:
                pass


# ------------------------------------------------------------------ HTTP Range
class RangeNotSatisfiable(Exception):
    pass


def parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Inclusive (start, end) for a single `bytes=` range, or None to serve the whole object.

    Malformed headers and multi-range requests are ignored (full 200 response, as RFC 9110 allows).
    Raises RangeNotSatisfiable (-> 416) when the range lies entirely outside the object.
    """
    if not header:
        return None
    unit, _, spec = header.strip().partition("=")
    if unit.strip().lower() != "bytes" or "," in spec:
        return None
    first, dash, last = spec.strip().partition("-")
    if not dash:
        return None
    try:
        if first == "":                       # suffix: the last N bytes
            n = int(last)
            if n <= 0:
                raise RangeNotSatisfiable()
            return max(0, size - n), size - 1
        start = int(first)
        end = int(last) if last != "" else size - 1
    except ValueError:
        return None
    if start >= size:
        raise RangeNotSatisfiable()
    if start < 0 or end < start:
        return None
    return start, min(end, size - 1)


def chunk_span(start: int, end: int, chunk_size: int) -> tuple[int, int]:
    """Indexes of the first and last chunk that hold bytes start..end."""
    return start // chunk_size, end // chunk_size
