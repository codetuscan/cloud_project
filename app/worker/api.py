import asyncio
import hashlib
import logging

from fastapi import APIRouter, Request, Response

from app.common import checksum as cs
from app.common.config import settings
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
from app.worker import disk

log = logging.getLogger("worker.api")
router = APIRouter()
_sem: asyncio.Semaphore | None = None


def _transfers() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(settings.max_concurrent_transfers)
    return _sem


def _check_id(chunk_id: str) -> None:
    if not disk.ID_RE.match(chunk_id):
        raise ApiError(400, "invalid chunk id")


@router.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    _check_id(chunk_id)
    expected = request.headers.get("x-chunk-checksum")
    if not cs.valid(expected):
        raise ApiError(400, "X-Chunk-Checksum header required, format sha256:<64 hex>")

    async with _transfers():
        existing = await asyncio.to_thread(disk.stored_checksum, chunk_id)
        if existing is not None and existing != expected:
            raise ApiError(409, "chunk exists with different content", "chunk_conflict")

        writer = None if existing else await asyncio.to_thread(disk.ChunkWriter, chunk_id)
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
            actual = writer.checksum if writer else cs.fmt(hasher_h.hexdigest())
            if actual != expected:
                raise ApiError(400, f"checksum mismatch: expected {expected} got {actual}",
                               "checksum_mismatch")
            if writer is not None:
                await asyncio.to_thread(writer.commit)
        except BaseException:
            if writer is not None:
                await asyncio.to_thread(writer.abort)
            raise

    status = 200 if existing else 201
    return JSONResponseNL(status_code=status,
                          content={"chunk_id": chunk_id, "size": total, "checksum": expected})


@router.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    _check_id(chunk_id)
    async with _transfers():
        try:
            data, checksum = await asyncio.to_thread(disk.read_verified, chunk_id)
        except disk.ChunkMissing:
            raise ApiError(404, "chunk not found")
        except disk.ChunkCorrupt as e:
            log.error("chunk %s failed verification: %s", chunk_id, e)
            raise ApiError(500, f"chunk corrupt: expected {e.expected} got {e.actual}", "chunk_corrupt")
    return Response(content=data, media_type="application/octet-stream",
                    headers={"X-Chunk-Checksum": checksum})


@router.delete("/internal/chunks/{chunk_id}", status_code=204)
async def delete_chunk(chunk_id: str):
    _check_id(chunk_id)
    await asyncio.to_thread(disk.delete, chunk_id)
    return Response(status_code=204)


@router.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str):
    if worker_id != settings.worker_id:
        raise ApiError(404, "unknown worker id")
    chunks = await asyncio.to_thread(disk.list_chunks)
    return {"worker_id": worker_id, "count": len(chunks), "chunks": chunks}
