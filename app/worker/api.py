import asyncio
import hashlib
import logging

import httpx
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, Field

from app.common import checksum as cs
from app.common import metrics, prom
from app.common.gate import Gate
from app.common.config import settings
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
from app.worker import cache as wcache
from app.worker import disk, prom_metrics, scrub

log = logging.getLogger("worker.api")
router = APIRouter()
_gate: Gate | None = None


def _transfers() -> Gate:
    """Per-worker bound on concurrent chunk transfers (backpressure)."""
    global _gate
    if _gate is None:
        _gate = Gate(settings.max_concurrent_transfers, "transfers")
    return _gate


def _check_id(chunk_id: str) -> None:
    if not disk.ID_RE.match(chunk_id):
        raise ApiError(400, "invalid chunk id")


@router.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    _check_id(chunk_id)
    expected = request.headers.get("x-chunk-checksum")
    if not cs.valid(expected):
        raise ApiError(400, "X-Chunk-Checksum header required, format sha256:<64 hex>")

    # repair=true: controller-directed replacement of a bad/missing replica. The body is still
    # verified against the controller-supplied checksum before it replaces anything.
    repair = request.query_params.get("repair") == "true"

    # Chain replication: the controller sends the chunk to ONE worker and names the next one. This worker
    # stores it, forwards it, and acknowledges only when it knows the outcome of both copies.
    forward = None
    fwd_addr = request.headers.get("x-forward-to")
    if fwd_addr:
        if repair or not fwd_addr.startswith("http://"):
            raise ApiError(400, "X-Forward-To must be an http:// address and cannot be combined with repair=true")
        forward = (request.headers.get("x-forward-worker-id", ""), fwd_addr.rstrip("/"))

    async with _transfers():
        existing = None if repair else await asyncio.to_thread(disk.stored_checksum, chunk_id)
        if existing is not None and existing != expected:
            metrics.inc("chunk_put_conflicts")
            raise ApiError(409, "chunk exists with different content", "chunk_conflict")

        writer = None if existing else await asyncio.to_thread(disk.ChunkWriter, chunk_id)
        if repair:
            wcache.chunk_cache.invalidate(chunk_id)
        # write-through: keep the bytes (<= one chunk) so the cache can be warmed AFTER the durable write
        # (the same bytes are forwarded to the next worker when chain replication was requested)
        keep: list[bytes] | None = [] if (forward or (writer is not None and wcache.chunk_cache.enabled)) else None
        total = 0
        try:
            hasher_h = None
            if writer is None:  # idempotent retry: still verify the body matches
                hasher_h = hashlib.sha256()
            async for part in request.stream():
                total += len(part)
                if total > settings.chunk_size:
                    raise ApiError(400, f"chunk larger than {settings.chunk_size} bytes")
                if writer is not None:
                    await asyncio.to_thread(writer.write, part)
                else:
                    hasher_h.update(part)
                if keep is not None:
                    keep.append(part)
            actual = writer.checksum if writer else cs.fmt(hasher_h.hexdigest())
            if actual != expected:
                metrics.inc("chunk_put_checksum_mismatches")
                raise ApiError(400, f"checksum mismatch: expected {expected} got {actual}",
                               "checksum_mismatch")
            if writer is not None:
                await asyncio.to_thread(writer.commit)
                if keep is not None and wcache.chunk_cache.enabled:  # durable + verified: now safe to cache
                    wcache.chunk_cache.put(chunk_id, (b"".join(keep), expected), size=total)
        except BaseException:
            if writer is not None:
                await asyncio.to_thread(writer.abort)
            raise

    metrics.inc("chunk_puts")
    metrics.inc("chunk_put_bytes", total)
    body = {"chunk_id": chunk_id, "size": total, "checksum": expected}
    if forward is not None:
        # our transfer slot is released first, so two workers forwarding to each other can never deadlock
        body["replicas"] = [{"worker_id": settings.worker_id, "ok": True},
                            await _forward(chunk_id, expected, b"".join(keep), forward)]
    status = 200 if existing else 201
    return JSONResponseNL(status_code=status, content=body)


_client: httpx.AsyncClient | None = None
FORWARD_ATTEMPTS = 2


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0))
    return _client


async def close_client() -> None:
    if _client is not None:
        await _client.aclose()


async def _forward(chunk_id: str, checksum: str, data: bytes, target: tuple[str, str]) -> dict:
    """Send the chunk we just stored to the next worker. Never raises: the outcome goes back to the controller."""
    wid, addr = target
    last = "unknown error"
    for attempt in range(FORWARD_ATTEMPTS):
        try:
            r = await _http().put(f"{addr}/internal/chunks/{chunk_id}", content=data,
                                  headers={"X-Chunk-Checksum": checksum, "Content-Type": "application/octet-stream"})
            if r.status_code in (200, 201):
                metrics.inc("chunk_forwards_ok")
                return {"worker_id": wid, "ok": True}
            last = f"{r.status_code} {r.text.strip()[:200]}"
            if r.status_code in (400, 409):  # deterministic rejection: retrying cannot help
                break
        except httpx.HTTPError as e:
            last = repr(e)
        await asyncio.sleep(0.2 * (2 ** attempt))
    metrics.inc("chunk_forwards_failed")
    log.warning("forward of %s to %s failed: %s", chunk_id, wid or addr, last)
    return {"worker_id": wid, "ok": False, "error": last}


class SendCommand(BaseModel):
    target_address: str = Field(min_length=8, max_length=256)
    checksum: str  # the controller's authoritative checksum for this chunk
    size: int = Field(ge=1)


@router.post("/internal/chunks/{chunk_id}/send")
async def send_chunk(chunk_id: str, cmd: SendCommand):
    """Controller-directed copy: push one of OUR chunks straight to another worker (repair / re-replication).

    The bytes never pass through the controller. We verify them against both our own record and the
    controller's checksum before sending; the target verifies again before it acknowledges. Always answers 200
    with {"ok": bool, "reason"?} so the controller can tell a bad source from a bad target.
    """
    _check_id(chunk_id)
    if not cs.valid(cmd.checksum) or not cmd.target_address.startswith("http://"):
        raise ApiError(400, "need a valid sha256 checksum and an http:// target_address")

    def fail(reason: str, detail: str = ""):
        metrics.inc("chunk_sends_failed")
        log.warning("send of %s to %s failed: %s %s", chunk_id, cmd.target_address, reason, detail)
        return JSONResponseNL(content={"ok": False, "reason": reason, "detail": detail})

    async with _transfers():  # released before the push so two workers sending to each other cannot deadlock
        try:
            data, _ = await asyncio.to_thread(disk.read_verified, chunk_id)  # disk, never the cache: repairs must see the truth
        except disk.ChunkMissing:
            wcache.chunk_cache.invalidate(chunk_id)
            return fail("source_missing")
        except disk.ChunkCorrupt as e:
            wcache.chunk_cache.invalidate(chunk_id)
            return fail("source_corrupt", str(e))
    if len(data) != cmd.size or cs.sha256_bytes(data) != cmd.checksum:
        wcache.chunk_cache.invalidate(chunk_id)
        return fail("source_corrupt", "differs from the controller's checksum")

    last = "unknown error"
    for attempt in range(FORWARD_ATTEMPTS):
        try:
            r = await _http().put(f"{cmd.target_address.rstrip('/')}/internal/chunks/{chunk_id}?repair=true",
                                  content=data, headers={"X-Chunk-Checksum": cmd.checksum,
                                                         "Content-Type": "application/octet-stream"})
            if r.status_code in (200, 201):
                metrics.inc("chunk_sends_ok")
                return JSONResponseNL(content={"ok": True})
            last = f"{r.status_code} {r.text.strip()[:200]}"
        except httpx.HTTPError as e:
            last = repr(e)
        await asyncio.sleep(0.2 * (2 ** attempt))
    return fail("target_failed", last)


@router.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    _check_id(chunk_id)
    hit = wcache.chunk_cache.get(chunk_id)
    if hit is not None:  # immutable, previously verified bytes
        data, checksum = hit
        metrics.inc("chunk_gets")
        metrics.inc("chunk_get_bytes", len(data))
        return Response(content=data, media_type="application/octet-stream",
                        headers={"X-Chunk-Checksum": checksum, "X-Cache": "HIT"})
    async with _transfers():
        token = wcache.chunk_cache.token()
        try:
            wcache.count_disk_read()
            data, checksum = await asyncio.to_thread(disk.read_verified, chunk_id)
        except disk.ChunkMissing:
            wcache.chunk_cache.invalidate(chunk_id)
            metrics.inc("chunk_get_missing")
            raise ApiError(404, "chunk not found")
        except disk.ChunkCorrupt as e:
            wcache.chunk_cache.invalidate(chunk_id)
            metrics.inc("chunk_get_corrupt")
            log.error("chunk %s failed verification: %s", chunk_id, e)
            raise ApiError(500, f"chunk corrupt: expected {e.expected} got {e.actual}", "chunk_corrupt")
    wcache.chunk_cache.put(chunk_id, (data, checksum), size=len(data), token=token)
    metrics.inc("chunk_gets")
    metrics.inc("chunk_get_bytes", len(data))
    return Response(content=data, media_type="application/octet-stream",
                    headers={"X-Chunk-Checksum": checksum, "X-Cache": "MISS"})


@router.delete("/internal/chunks/{chunk_id}", status_code=204)
async def delete_chunk(chunk_id: str):
    _check_id(chunk_id)
    wcache.chunk_cache.invalidate(chunk_id)
    await asyncio.to_thread(disk.delete, chunk_id)
    wcache.chunk_cache.invalidate(chunk_id)
    metrics.inc("chunk_deletes")
    return Response(status_code=204)


@router.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str):
    if worker_id != settings.worker_id:
        raise ApiError(404, "unknown worker id")
    chunks = await asyncio.to_thread(disk.list_chunks)
    return {"worker_id": worker_id, "count": len(chunks), "chunks": chunks}


@router.post("/internal/scrub/trigger", status_code=202)
async def trigger_scrub():
    """Start a scrub pass now (used by demos/tests instead of waiting for the interval)."""
    scrub.trigger.set()
    return JSONResponseNL(status_code=202, content={"status": "scrub requested"})


@router.get("/internal/scrub/status")
async def scrub_status():
    return {**scrub.state, "report_queue_depth": await asyncio.to_thread(scrub.queue_depth),
            "scrub_interval": settings.scrub_interval}


@router.get("/internal/cache/stats")
async def cache_stats():
    return {"chunk_cache": wcache.stats()}


@router.post("/internal/cache/clear")
async def cache_clear():
    """Drop all cached chunk bytes (RAM only; files on disk are untouched). For demos/tests."""
    wcache.chunk_cache.clear()
    return {"status": "cleared"}


@router.get("/internal/status")
async def worker_status():
    """Component health/readiness for this worker."""
    chunks = await asyncio.to_thread(lambda: len(disk.list_chunks()))
    return {
        "worker_id": settings.worker_id, "status": "ok", "chunks_on_disk": chunks,
        "disk": await asyncio.to_thread(disk.usage),
        "transfers": _transfers().stats(),
        "cache": wcache.stats(),
        "scrub": {k: scrub.state[k] for k in ("in_progress", "passes_completed", "last_scrub_finished",
                                              "chunks_checked", "corrupt_found", "missing_found",
                                              "unreadable_found", "duration_seconds")},
        "report_queue_depth": await asyncio.to_thread(scrub.queue_depth),
    }


@router.get("/metrics", include_in_schema=False)
async def prometheus_metrics():
    """Prometheus scrape endpoint (text format). /internal/metrics stays the JSON view."""
    return Response(await prom_metrics.render(_transfers()), media_type=prom.CONTENT_TYPE)


@router.get("/internal/metrics")
async def worker_metrics():
    return {**metrics.snapshot(), "transfers": _transfers().stats(), "chunk_cache": wcache.stats(),
            "scrub": {**scrub.state, "report_queue_depth": await asyncio.to_thread(scrub.queue_depth)}}
