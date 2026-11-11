"""Public client API."""
import asyncio
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request

from app.common.config import settings
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
from app.controller import placement, upload
from app.controller.multipart_stream import StreamingUpload, UploadError

log = logging.getLogger("controller.api")
router = APIRouter()


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _object_json(o) -> dict:
    return {
        "object_id": str(o["object_id"]),
        "name": o["name"],
        "size": o["size"],
        "content_type": o["content_type"],
        "checksum": o["checksum"],
        "chunk_count": o["chunk_count"],
        "chunk_size": o["chunk_size"],
        "state": o["state"],
        "created_at": _iso(o["created_at"]),
        "last_modified": _iso(o["updated_at"]),
        "user_metadata": o["user_metadata"],
    }


def _parse_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise ApiError(404, "object not found")


def _parse_metadata(raw: bytes) -> dict:
    if not raw.strip():
        return {}
    try:
        meta = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ApiError(400, "metadata must be valid JSON")
    if not isinstance(meta, dict):
        raise ApiError(400, "metadata must be a JSON object of key-value pairs")
    for k, v in meta.items():
        if not k or len(k) > 128:
            raise ApiError(400, "metadata keys must be 1-128 characters")
        if not isinstance(v, (str, int, float, bool)):
            raise ApiError(400, f"metadata value for '{k}' must be a string, number or boolean")
        if isinstance(v, str) and len(v) > 4096:
            raise ApiError(400, f"metadata value for '{k}' too long")
    return meta


@router.post("/api/v1/objects", status_code=201)
async def upload_object(request: Request):
    state = request.app.state
    ctype = request.headers.get("content-type", "")
    if not ctype.lower().startswith("multipart/form-data"):
        raise ApiError(400, "use multipart/form-data with a 'file' field")

    idem = request.headers.get("idempotency-key")
    if idem:
        existing = await state.pool.fetchrow("SELECT * FROM objects WHERE idempotency_key=$1", idem)
        if existing is not None:
            if existing["state"] == "COMMITTED":
                return JSONResponseNL(status_code=200, content=_object_json(existing))
            raise ApiError(409, "an upload with this Idempotency-Key is still in progress",
                           extra={"object_id": str(existing["object_id"])})

    if await placement.healthy_count(state.pool) < 2:
        raise ApiError(503, "fewer than 2 healthy workers available; cannot store 2 replicas")

    object_id = uuid.uuid4()
    staging = Path(settings.staging_dir)
    part_path = staging / f"{object_id}.uploading"
    su = None
    try:
        su = StreamingUpload(ctype, part_path)
        async for data in request.stream():
            await asyncio.to_thread(su.feed, data)
        await asyncio.to_thread(su.finish)
        if su.file_parts == 0:
            raise UploadError("multipart field 'file' is required")
        if su.size == 0:
            raise UploadError("empty file; cannot store an object with no data")
        user_meta = _parse_metadata(bytes(su.meta))
    except UploadError as e:
        _discard(su, part_path)
        raise ApiError(400, str(e))
    except BaseException:
        _discard(su, part_path)
        raise

    staged_path = staging / f"{object_id}.staged"
    await asyncio.to_thread(part_path.replace, staged_path)

    chunk_size = settings.chunk_size
    chunk_count = -(-su.size // chunk_size)
    name = (su.filename or "unnamed")[:255]
    async with state.pool.acquire() as c:
        async with c.transaction():
            await c.execute(
                """INSERT INTO objects (object_id,name,size,content_type,checksum,chunk_count,chunk_size,
                                        state,user_metadata,idempotency_key)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING',$8,$9)""",
                object_id, name, su.size, su.content_type, su.checksum, chunk_count, chunk_size,
                user_meta, idem)
            await c.execute("INSERT INTO uploads (object_id, staged_path, state) VALUES ($1,$2,'STAGED')",
                            object_id, str(staged_path))

    state.active_uploads.add(object_id)
    try:
        await upload.commit_upload(state, object_id)
    except upload.UploadUnavailable as e:
        log.warning("upload %s not committed yet: %s", object_id, e)
        raise ApiError(503, "not enough healthy workers to store 2 replicas right now; the upload is "
                            "kept in staging and will be completed automatically when workers recover",
                       code="storage_unavailable",
                       extra={"object_id": str(object_id), "state": "PENDING"})
    finally:
        state.active_uploads.discard(object_id)

    obj = await state.pool.fetchrow("SELECT * FROM objects WHERE object_id=$1", object_id)
    return JSONResponseNL(status_code=201, content=_object_json(obj))


def _discard(su, path: Path) -> None:
    if su is not None:
        su.close()
    path.unlink(missing_ok=True)


@router.get("/api/v1/objects/{object_id}/metadata")
async def get_metadata(object_id: str, request: Request):
    oid = _parse_uuid(object_id)
    pool = request.app.state.pool
    obj = await pool.fetchrow("SELECT * FROM objects WHERE object_id=$1 AND state='COMMITTED'", oid)
    if obj is None:
        raise ApiError(404, "object not found")
    chunks = await pool.fetch(
        "SELECT chunk_index, chunk_id, size, checksum FROM chunks WHERE object_id=$1 ORDER BY chunk_index", oid)
    reps = await pool.fetch(
        """SELECT r.chunk_id, r.worker_id, r.state FROM replicas r JOIN chunks c USING (chunk_id)
           WHERE c.object_id=$1 ORDER BY r.worker_id""", oid)
    by_chunk: dict[str, list] = {}
    for r in reps:
        by_chunk.setdefault(r["chunk_id"], []).append({"worker_id": r["worker_id"], "state": r["state"]})
    body = _object_json(obj)
    body["chunks"] = [
        {"index": c["chunk_index"], "chunk_id": c["chunk_id"], "size": c["size"], "checksum": c["checksum"],
         "replicas": by_chunk.get(c["chunk_id"], [])} for c in chunks]
    return body
