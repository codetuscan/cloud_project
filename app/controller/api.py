"""Public client API."""
import asyncio
import json
import logging
import uuid
import base64
import random
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse

from app.common.config import settings
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
import httpx

from app.common import checksum as cs
from app.controller import placement, reaper, replicas, upload
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


def _parse_metadata(raw: bytes, allow_null: bool = False) -> dict:
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
        if v is None and allow_null:
            continue
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


# ---------------------------------------------------------------- download
async def _fetch_chunk(state, chunk, candidates: list[dict]) -> bytes:
    """Return verified bytes for one chunk, trying each healthy replica; report bad ones."""
    for rep in candidates:
        try:
            async with state.transfer_sem:
                r = await state.http.get(f"{rep['address']}/internal/chunks/{chunk['chunk_id']}")
        except httpx.HTTPError as e:
            log.warning("replica %s on %s unreachable: %r", chunk["chunk_id"], rep["worker_id"], e)
            continue
        if r.status_code == 404:
            await replicas.mark_bad(state.pool, chunk["chunk_id"], rep["worker_id"], "MISSING", "read: 404")
            continue
        if r.status_code != 200:
            if "chunk_corrupt" in r.text:
                await replicas.mark_bad(state.pool, chunk["chunk_id"], rep["worker_id"], "CORRUPT",
                                        "read: worker self-check failed")
            else:
                log.warning("replica %s on %s returned %s", chunk["chunk_id"], rep["worker_id"], r.status_code)
            continue
        data = r.content
        if len(data) != chunk["size"] or cs.sha256_bytes(data) != chunk["checksum"]:
            await replicas.mark_bad(state.pool, chunk["chunk_id"], rep["worker_id"], "CORRUPT",
                                    "read: checksum mismatch at controller")
            continue
        return data
    raise RuntimeError(f"no readable replica for chunk {chunk['chunk_id']}")


@router.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str, request: Request):
    oid = _parse_uuid(object_id)
    state = request.app.state
    pool = state.pool
    obj = await pool.fetchrow("SELECT * FROM objects WHERE object_id=$1 AND state='COMMITTED'", oid)
    if obj is None:
        raise ApiError(404, "object not found")
    chunks = await pool.fetch(
        "SELECT chunk_id, chunk_index, size, checksum FROM chunks WHERE object_id=$1 ORDER BY chunk_index", oid)
    reps = await pool.fetch(
        """SELECT r.chunk_id, r.worker_id, w.address FROM replicas r JOIN workers w USING (worker_id)
           WHERE r.state='HEALTHY' AND w.state <> 'DEAD'
             AND r.chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=$1)""", oid)
    by_chunk: dict[str, list[dict]] = {}
    for r in reps:
        by_chunk.setdefault(r["chunk_id"], []).append(dict(r))
    if len(chunks) != obj["chunk_count"]:
        raise ApiError(500, "object manifest is incomplete")
    # fail before sending headers if some chunk has no replica at all
    for c in chunks:
        if not by_chunk.get(c["chunk_id"]):
            raise ApiError(503, f"no healthy replica available for chunk {c['chunk_index']}")

    async def body():
        for c in chunks:
            cands = by_chunk[c["chunk_id"]][:]
            random.shuffle(cands)
            # raising here aborts the connection: the client never gets a silently truncated object
            yield await _fetch_chunk(state, c, cands)

    fname = obj["name"]
    ascii_name = fname.encode("ascii", "replace").decode().replace('"', "'").replace("?", "_")
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(fname)}",
        "X-Object-Checksum": obj["checksum"] or "",
        "ETag": f'"{(obj["checksum"] or "")[7:]}"',
    }
    return StreamingResponse(body(), media_type=obj["content_type"], headers=headers)


# ---------------------------------------------------------------- list
def _encode_cursor(created_at: datetime, oid) -> str:
    return base64.urlsafe_b64encode(f"{created_at.isoformat()}|{oid}".encode()).decode()


def _decode_cursor(cur: str):
    try:
        ts, oid = base64.urlsafe_b64decode(cur.encode()).decode().split("|")
        return datetime.fromisoformat(ts), uuid.UUID(oid)
    except Exception:
        raise ApiError(400, "invalid cursor")


@router.get("/api/v1/objects")
async def list_objects(request: Request, limit: int = 100, cursor: str | None = None):
    if not 1 <= limit <= 1000:
        raise ApiError(400, "limit must be between 1 and 1000")
    pool = request.app.state.pool
    if cursor:
        ts, oid = _decode_cursor(cursor)
        rows = await pool.fetch(
            """SELECT object_id,name,size,content_type,created_at FROM objects
               WHERE state='COMMITTED' AND (created_at, object_id) > ($1, $2)
               ORDER BY created_at, object_id LIMIT $3""", ts, oid, limit + 1)
    else:
        rows = await pool.fetch(
            """SELECT object_id,name,size,content_type,created_at FROM objects
               WHERE state='COMMITTED' ORDER BY created_at, object_id LIMIT $1""", limit + 1)
    page = rows[:limit]
    nxt = _encode_cursor(page[-1]["created_at"], page[-1]["object_id"]) if len(rows) > limit else None
    return {
        "items": [{"object_id": str(r["object_id"]), "name": r["name"], "size": r["size"],
                   "content_type": r["content_type"], "created_at": _iso(r["created_at"])} for r in page],
        "next_cursor": nxt,
    }


# ---------------------------------------------------------------- patch metadata
@router.patch("/api/v1/objects/{object_id}/metadata")
async def patch_metadata(object_id: str, request: Request):
    oid = _parse_uuid(object_id)
    raw = await request.body()
    if len(raw) > 64 * 1024:
        raise ApiError(400, "request body too large")
    if not raw.strip():
        raise ApiError(400, "JSON body required")
    patch = _parse_metadata(raw, allow_null=True)
    if len(patch) > 100:
        raise ApiError(400, "at most 100 keys per request")
    # keys with null value are removed; others added/overwritten (atomic single UPDATE)
    row = await request.app.state.pool.fetchrow(
        """UPDATE objects SET user_metadata = jsonb_strip_nulls(user_metadata || $2::jsonb), updated_at=now()
           WHERE object_id=$1 AND state='COMMITTED' RETURNING *""", oid, patch)
    if row is None:
        raise ApiError(404, "object not found")
    return _object_json(row)


# ---------------------------------------------------------------- delete
@router.delete("/api/v1/objects/{object_id}", status_code=204)
async def delete_object(object_id: str, request: Request):
    oid = _parse_uuid(object_id)
    pool = request.app.state.pool
    async with pool.acquire() as c:
        async with c.transaction():
            ok = await reaper.begin_delete(c, oid, ("COMMITTED",))
    if not ok:
        raise ApiError(404, "object not found")
    return Response(status_code=204)
