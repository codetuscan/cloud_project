"""Public client API."""
import asyncio
import json
import shutil
import logging
import uuid
import base64
import random
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from app.common.config import settings
import asyncpg
from app.common.errors import ApiError
from app.common.responses import JSONResponseNL
import httpx

from app.common import checksum as cs
from app.common import chunking, metrics
from app.common.gate import Busy
from app.controller import meta_cache, placement, reaper, replicas, upload
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


def _busy(what: str) -> ApiError:
    return ApiError(503, f"server busy: too many concurrent {what}; retry shortly", "server_busy",
                    headers={"Retry-After": "5"})


@router.post("/api/v1/objects", status_code=201)
async def upload_object(request: Request):
    """Admission control (bounded concurrent uploads) + size/space checks, then the real work."""
    state = request.app.state
    cl = request.headers.get("content-length")
    if cl and cl.isdigit():
        n = int(cl)
        if settings.max_upload_bytes and n > settings.max_upload_bytes:
            raise ApiError(400, f"upload exceeds the maximum of {settings.max_upload_bytes} bytes", "too_large")
        free = await asyncio.to_thread(lambda: shutil.disk_usage(settings.staging_dir).free)
        if n + 64 * 1024 * 1024 > free:
            raise ApiError(503, "not enough free staging space for this upload", "staging_full")
    try:
        slot = await state.upload_gate.enter(settings.admission_timeout)
    except Busy:
        metrics.inc("uploads_rejected_busy")
        raise _busy("uploads")
    metrics.inc("uploads_started")
    try:
        return await _do_upload(request)
    finally:
        slot.release()


async def _do_upload(request: Request):
    """Upload in the order "database first":

      1. insert objects (PENDING, placeholders) + uploads (STREAMING)      <- the database knows before any file exists
      2. stream the body into <id>.uploading (fsynced at the end of the part)
      3. ONE transaction: real name/size/type/checksum/chunk count/metadata + uploads.state='STAGED'
                                                                          <- the commit point of the upload
      4. rename <id>.uploading -> <id>.staged                              <- housekeeping; start-up recovery can finish it
      5. chunk + replicate -> COMMITTED -> 201

    A complete staged file therefore always has a complete row, and a STREAMING row never promises a file.
    """
    state = request.app.state
    pool = state.pool
    ctype = request.headers.get("content-type", "")
    if not ctype.lower().startswith("multipart/form-data"):
        raise ApiError(400, "use multipart/form-data with a 'file' field")

    idem = request.headers.get("idempotency-key")
    if idem:
        existing = await pool.fetchrow(
            """SELECT o.*, u.state AS upload_state FROM objects o LEFT JOIN uploads u USING (object_id)
               WHERE o.idempotency_key=$1""", idem)
        if existing is not None:
            if existing["state"] == "COMMITTED":
                return JSONResponseNL(status_code=200, content=_object_json(existing))
            if (existing["state"] == "PENDING" and existing["upload_state"] == "STREAMING"
                    and existing["object_id"] not in state.active_uploads):
                # nobody is receiving it (single controller): a dead attempt, e.g. its cleanup failed. Clear it.
                log.warning("idempotency key reused: dropping dead streaming upload %s", existing["object_id"])
                await _drop_streaming(state, existing["object_id"])
            else:
                raise ApiError(409, "an upload with this Idempotency-Key is still in progress",
                               extra={"object_id": str(existing["object_id"])})

    if await placement.healthy_count(pool) < 2:
        raise ApiError(503, "fewer than 2 healthy workers available; cannot store 2 replicas")

    object_id = uuid.uuid4()
    staging = Path(settings.staging_dir)
    part_path = staging / f"{object_id}.uploading"
    staged_path = staging / f"{object_id}.staged"
    chunk_size = settings.chunk_size

    # 1. tell the database first. Name/size/metadata are not known yet: placeholders, filled in at step 3.
    try:
        async with pool.acquire() as c:
            async with c.transaction():
                await c.execute(
                    """INSERT INTO objects (object_id, name, chunk_size, state, idempotency_key)
                       VALUES ($1, '(upload in progress)', $2, 'PENDING', $3)""", object_id, chunk_size, idem)
                await c.execute("INSERT INTO uploads (object_id, staged_path, state) VALUES ($1,$2,'STREAMING')",
                                object_id, str(staged_path))
    except asyncpg.UniqueViolationError:  # a simultaneous request with the same Idempotency-Key won the insert
        raise ApiError(409, "an upload with this Idempotency-Key is still in progress")

    state.active_uploads.add(object_id)  # from now on the reaper and the resume loop leave it alone
    try:
        su = None
        try:
            # 2. receive the bytes
            su = StreamingUpload(ctype, part_path)
            async for data in request.stream():
                await asyncio.to_thread(su.feed, data)
                if settings.max_upload_bytes and su.size > settings.max_upload_bytes:
                    raise UploadError(f"upload exceeds the maximum of {settings.max_upload_bytes} bytes")
            await asyncio.to_thread(su.finish)  # the file part was flushed + fsynced when it ended
            if su.file_parts == 0:
                raise UploadError("multipart field 'file' is required")
            if su.size == 0:
                raise UploadError("empty file; cannot store an object with no data")
            user_meta = _parse_metadata(bytes(su.meta))
            name = (su.filename or "unnamed")[:255]

            # 3. the commit point: every detail + STAGED, atomically. After this the controller owns the upload.
            async with pool.acquire() as c:
                async with c.transaction():
                    await c.execute(
                        """UPDATE objects SET name=$2, size=$3, content_type=$4, checksum=$5, chunk_count=$6,
                                              user_metadata=$7, updated_at=now()
                           WHERE object_id=$1 AND state='PENDING'""",
                        object_id, name, su.size, su.content_type, su.checksum,
                        chunking.chunk_count(su.size, chunk_size), user_meta)
                    await c.execute("UPDATE uploads SET state='STAGED', updated_at=now() WHERE object_id=$1",
                                    object_id)
        except UploadError as e:
            await _drop_streaming(state, object_id, su, part_path)
            raise ApiError(400, str(e))
        except BaseException:  # includes a client disconnect (task cancelled) and a database failure at step 3
            await _drop_streaming(state, object_id, su, part_path)
            raise

        # 4. housekeeping only: a crash here leaves STAGED + a complete .uploading, which start-up recovery renames
        await asyncio.to_thread(part_path.replace, staged_path)

        # 5. chunk + replicate
        try:
            await upload.commit_upload(state, object_id)
        except upload.UploadUnavailable as e:
            metrics.inc("uploads_pending_503")
            log.warning("upload %s not committed yet: %s", object_id, e)
            raise ApiError(503, "not enough healthy workers to store 2 replicas right now; the upload is "
                                "kept in staging and will be completed automatically when workers recover",
                           code="storage_unavailable",
                           extra={"object_id": str(object_id), "state": "PENDING"})
    finally:
        state.active_uploads.discard(object_id)

    metrics.inc("uploads_committed")
    metrics.inc("upload_bytes", su.size)
    obj = await pool.fetchrow("SELECT * FROM objects WHERE object_id=$1", object_id)
    return JSONResponseNL(status_code=201, content=_object_json(obj))


async def _drop_streaming(state, object_id, su=None, part_path: Path | None = None) -> None:
    """Forget an upload that never reached STAGED: close + delete its partial file and its rows (uploads cascades).

    Runs on errors and on client disconnects, so the database part is shielded from a second cancellation. If it
    still fails, the dead-row rule on Idempotency-Key reuse and the start-up recovery clean it up later.
    """
    if su is not None:
        su.close()
    staging = Path(settings.staging_dir)
    for p in {part_path or staging / f"{object_id}.uploading", staging / f"{object_id}.staged"}:
        p.unlink(missing_ok=True)
    try:
        await asyncio.shield(state.pool.execute(
            """DELETE FROM objects WHERE object_id=$1 AND state='PENDING'
                 AND EXISTS (SELECT 1 FROM uploads u WHERE u.object_id=$1 AND u.state='STREAMING')""", object_id))
    except (Exception, asyncio.CancelledError) as e:
        log.warning("could not remove the rows of aborted upload %s (recovery will): %r", object_id, e)


@router.get("/api/v1/objects/{object_id}/metadata")
async def get_metadata(object_id: str, request: Request):
    oid = _parse_uuid(object_id)
    pool = request.app.state.pool
    m = await meta_cache.get_manifest(pool, oid)
    if m is None:
        raise ApiError(404, "object not found")
    body = _object_json(m["obj"])
    body["chunks"] = [
        {"index": c["chunk_index"], "chunk_id": c["chunk_id"], "size": c["size"], "checksum": c["checksum"],
         "replicas": [{"worker_id": r["worker_id"], "state": r["state"], "worker_state": r["wstate"]}
                      for r in c["replicas"]]}
        for c in m["chunks"]]
    return body


# ---------------------------------------------------------------- download
async def _fetch_chunk(state, chunk, candidates: list[dict]) -> bytes:
    """Return verified bytes for one chunk, trying each healthy replica; report bad ones."""
    for attempt, rep in enumerate(candidates):
        if attempt:
            metrics.inc("replica_read_fallbacks")
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
    m = await meta_cache.get_manifest(pool, oid)
    if m is None:
        raise ApiError(404, "object not found")
    obj, chunks = m["obj"], m["chunks"]
    by_chunk = {c["chunk_id"]: [r for r in c["replicas"] if r["state"] == "HEALTHY" and r["wstate"] != "DEAD"]
                for c in chunks}
    if len(chunks) != obj["chunk_count"]:
        raise ApiError(500, "object manifest is incomplete")
    # fail before sending headers if some chunk has no replica at all
    for c in chunks:
        if not by_chunk.get(c["chunk_id"]):
            raise ApiError(503, f"no healthy replica available for chunk {c['chunk_index']}")

    try:
        slot = await state.download_gate.enter(settings.admission_timeout)
    except Busy:
        metrics.inc("downloads_rejected_busy")
        raise _busy("downloads")
    metrics.inc("downloads_started")

    async def body():
        try:
            for c in chunks:
                cands = by_chunk[c["chunk_id"]][:]
                random.shuffle(cands)
                # raising here aborts the connection: the client never gets a silently truncated object
                data = await _fetch_chunk(state, c, cands)
                metrics.inc("download_bytes", len(data))
                yield data
            metrics.inc("downloads_completed")
        except BaseException:
            metrics.inc("downloads_aborted")
            raise
        finally:
            slot.release()

    fname = obj["name"]
    ascii_name = fname.encode("ascii", "replace").decode().replace('"', "'").replace("?", "_")
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(fname)}",
        "X-Object-Checksum": obj["checksum"] or "",
        "ETag": f'"{(obj["checksum"] or "")[7:]}"',
    }
    return StreamingResponse(body(), media_type=obj["content_type"], headers=headers,
                             background=BackgroundTask(slot.release))


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
    meta_cache.invalidate_object(oid)
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
    meta_cache.invalidate_object(oid)  # after the commit: a re-fill now sees the DELETING state
    if not ok:
        raise ApiError(404, "object not found")
    return Response(status_code=204)


@router.get("/internal/cache/stats")
async def cache_stats():
    return {"metadata_cache": meta_cache.cache.stats()}


@router.post("/internal/cache/clear")
async def cache_clear():
    meta_cache.invalidate_all()
    return {"status": "cleared"}
