import asyncio
import hashlib
import logging

from fastapi import APIRouter, Request, Response

from app.common import checksum as cs
from app.common import metrics
from app.common.gate import Gate
from app.common.config import settings
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
from app.worker import cache as wcache
from app.worker import disk, scrub

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
    async with _transfers():
        existing = None if repair else await asyncio.to_thread(disk.stored_checksum, chunk_id)
        if existing is not None and existing != expected:
            metrics.inc("chunk_put_conflicts")
            raise ApiError(409, "chunk exists with different content", "chunk_conflict")

        writer = None if existing else await asyncio.to_thread(disk.ChunkWriter, chunk_id)
        if repair:
            wcache.chunk_cache.invalidate(chunk_id)
        # write-through: keep the bytes (<= one chunk) so the cache can be warmed AFTER the durable write
        keep: list[bytes] | None = [] if (writer is not None and wcache.chunk_cache.enabled) else None
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
                    if keep is not None:
                        keep.append(part)
                else:
                    hasher_h.update(part)
            actual = writer.checksum if writer else cs.fmt(hasher_h.hexdigest())
            if actual != expected:
                metrics.inc("chunk_put_checksum_mismatches")
                raise ApiError(400, f"checksum mismatch: expected {expected} got {actual}",
                               "checksum_mismatch")
            if writer is not None:
                await asyncio.to_thread(writer.commit)
                if keep is not None:  # durable + verified: now it is safe to cache
                    wcache.chunk_cache.put(chunk_id, (b"".join(keep), expected), size=total)
        except BaseException:
            if writer is not None:
                await asyncio.to_thread(writer.abort)
            raise

    metrics.inc("chunk_puts")
    metrics.inc("chunk_put_bytes", total)
    status = 200 if existing else 201
    return JSONResponseNL(status_code=status,
                          content={"chunk_id": chunk_id, "size": total, "checksum": expected})


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
            raise ApiError(404, "chunk not found")
        except disk.ChunkCorrupt as e:
            wcache.chunk_cache.invalidate(chunk_id)
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


@router.get("/internal/metrics")
async def worker_metrics():
    return {**metrics.snapshot(), "transfers": _transfers().stats(), "chunk_cache": wcache.stats(),
            "scrub": {**scrub.state, "report_queue_depth": await asyncio.to_thread(scrub.queue_depth)}}
