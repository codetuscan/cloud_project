"""Chunking, placement and commit of staged uploads (also used to resume after a restart)."""
import asyncio
import logging
import os
from pathlib import Path

import httpx

from app.common import checksum as cs
from app.common.config import settings
from app.controller import placement

log = logging.getLogger("controller.upload")

RESUME_INTERVAL = 30.0
PUT_ATTEMPTS = 3
PLACEMENT_ROUNDS = 6


class UploadUnavailable(Exception):
    """Not enough healthy workers to reach 2 replicas right now (-> HTTP 503)."""


def chunk_id_for(object_id, index: int) -> str:
    return f"{object_id}-{index:06d}"


def _read_slice(path: str, offset: int, length: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(offset)
        data = f.read(length)
    if len(data) != length:
        raise IOError(f"short read from staging file {path}")
    return data


async def _send(state, target: placement.Target, chunk_id: str, checksum: str, data: bytes) -> None:
    """PUT one chunk replica, with retries. Raises on failure."""
    last: Exception | None = None
    async with state.transfer_sem:
        for attempt in range(PUT_ATTEMPTS):
            try:
                r = await state.http.put(
                    f"{target.address}/internal/chunks/{chunk_id}", content=data,
                    headers={"X-Chunk-Checksum": checksum, "Content-Type": "application/octet-stream"})
                if r.status_code in (200, 201):
                    return
                if r.status_code in (400, 409):  # deterministic rejection: don't retry
                    raise RuntimeError(f"{target.worker_id} rejected chunk: {r.status_code} {r.text.strip()}")
                last = RuntimeError(f"{target.worker_id} returned {r.status_code}")
            except httpx.HTTPError as e:
                last = e
            await asyncio.sleep(0.2 * (2 ** attempt))
    raise RuntimeError(f"PUT {chunk_id} to {target.worker_id} failed: {last!r}")


async def _process_chunk(state, obj, index: int, path: str) -> None:
    pool = state.pool
    cs_size = obj["chunk_size"]
    offset = index * cs_size
    length = min(cs_size, obj["size"] - offset)
    data = await asyncio.to_thread(_read_slice, path, offset, length)
    checksum = cs.sha256_bytes(data)
    chunk_id = chunk_id_for(obj["object_id"], index)

    await pool.execute(
        """INSERT INTO chunks (chunk_id, object_id, chunk_index, size, checksum)
           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (chunk_id) DO NOTHING""",
        chunk_id, obj["object_id"], index, length, checksum)
    stored = await pool.fetchval("SELECT checksum FROM chunks WHERE chunk_id=$1", chunk_id)
    if stored != checksum:  # staged file changed since a previous attempt: never mix content
        raise RuntimeError(f"chunk {chunk_id} checksum differs from recorded value")

    failed: set[str] = set()
    for _ in range(PLACEMENT_ROUNDS):
        rows = await pool.fetch(
            """SELECT r.worker_id FROM replicas r JOIN workers w USING (worker_id)
               WHERE r.chunk_id=$1 AND r.state='HEALTHY' AND w.state <> 'DEAD'""", chunk_id)
        healthy = {r["worker_id"] for r in rows}
        need = 2 - len(healthy)
        if need <= 0:
            return
        targets = await placement.pick(pool, need, healthy | failed, length)
        if len(targets) < need:
            raise UploadUnavailable(f"chunk {index}: need {need} more worker(s), only {len(targets)} eligible")
        for t in targets:
            await pool.execute(
                """INSERT INTO replicas (chunk_id, worker_id, state) VALUES ($1,$2,'PENDING')
                   ON CONFLICT (chunk_id, worker_id) DO UPDATE SET state='PENDING', updated_at=now()""",
                chunk_id, t.worker_id)
        results = await asyncio.gather(
            *[_send(state, t, chunk_id, checksum, data) for t in targets], return_exceptions=True)
        for t, res in zip(targets, results):
            if isinstance(res, Exception):
                log.warning("replica placement failed: %s", res)
                failed.add(t.worker_id)
                await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING'",
                                   chunk_id, t.worker_id)
            else:
                await pool.execute(
                    """UPDATE replicas SET state='HEALTHY', last_verified=now(), updated_at=now()
                       WHERE chunk_id=$1 AND worker_id=$2""", chunk_id, t.worker_id)
    raise UploadUnavailable(f"chunk {index}: could not reach 2 healthy replicas")


async def commit_upload(state, object_id) -> bool:
    """Chunk + place the staged file; mark COMMITTED when every chunk has 2 healthy replicas.

    Idempotent: chunks that already have 2 healthy replicas are skipped, so it is safe to
    call again after a failure or a controller restart. Returns True if committed.
    """
    pool = state.pool
    obj = await pool.fetchrow("SELECT * FROM objects WHERE object_id=$1", object_id)
    if obj is None or obj["state"] != "PENDING":
        return False
    up = await pool.fetchrow("SELECT staged_path FROM uploads WHERE object_id=$1", object_id)
    if up is None or not os.path.exists(up["staged_path"]):
        raise RuntimeError(f"staged data for {object_id} is missing")
    path = up["staged_path"]

    queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(obj["chunk_count"]):
        queue.put_nowait(i)
    failure: list[BaseException] = []

    async def runner():
        while not failure:
            try:
                idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await _process_chunk(state, obj, idx, path)
            except BaseException as e:
                failure.append(e)
                return

    n_workers = max(1, min(4, settings.max_concurrent_transfers))
    await asyncio.gather(*[runner() for _ in range(n_workers)])
    if failure:
        raise failure[0]

    bad = await pool.fetchval(
        """SELECT count(*) FROM chunks c WHERE c.object_id=$1 AND
           (SELECT count(DISTINCT r.worker_id) FROM replicas r
             WHERE r.chunk_id=c.chunk_id AND r.state='HEALTHY') < 2""", object_id)
    have = await pool.fetchval("SELECT count(*) FROM chunks WHERE object_id=$1", object_id)
    if bad or have != obj["chunk_count"]:
        raise UploadUnavailable("not every chunk has 2 healthy replicas")

    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("UPDATE objects SET state='COMMITTED', updated_at=now() "
                            "WHERE object_id=$1 AND state='PENDING'", object_id)
            await c.execute("DELETE FROM uploads WHERE object_id=$1", object_id)
    Path(path).unlink(missing_ok=True)  # cleanup policy: staged source removed after commit
    log.info("object %s committed (%d chunks)", object_id, obj["chunk_count"])
    return True


def cleanup_staging(staging_dir: str) -> int:
    """At startup nothing is streaming, so partial *.uploading files are garbage."""
    n = 0
    for f in Path(staging_dir).glob("*.uploading"):
        f.unlink(missing_ok=True)
        n += 1
    return n


async def resume_loop(state) -> None:
    """Finish uploads that were staged but not committed (restart, or workers were down)."""
    while True:
        try:
            rows = await state.pool.fetch(
                """SELECT u.object_id FROM uploads u JOIN objects o USING (object_id)
                   WHERE o.state='PENDING' ORDER BY o.created_at""")
            for r in rows:
                oid = r["object_id"]
                if oid in state.active_uploads:
                    continue
                state.active_uploads.add(oid)
                try:
                    log.info("resuming staged upload %s", oid)
                    await commit_upload(state, oid)
                except UploadUnavailable as e:
                    log.warning("resume of %s postponed: %s", oid, e)
                except Exception as e:
                    log.error("resume of %s failed: %s", oid, e)
                finally:
                    state.active_uploads.discard(oid)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("resume loop error: %s", e)
        await asyncio.sleep(RESUME_INTERVAL)
