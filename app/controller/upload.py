"""Chunking, placement and commit of staged uploads (also used to resume after a restart)."""
import asyncio
import logging
import os
from pathlib import Path

import httpx

from app.common import checksum as cs
from app.common import chunking, metrics, tracing
from app.common.config import settings
from app.controller import meta_cache, placement

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


class _NotAttempted(Exception):
    """The chain never reached this worker (its predecessor failed first), so it holds nothing and is not at fault."""


async def _send_chain(state, first: placement.Target, second: placement.Target, chunk_id: str, checksum: str,
                      data: bytes) -> list:
    """Chain replication: PUT the chunk to ONE worker and name the next. That worker stores it, forwards it to
    `second`, and answers only when it knows what happened to both copies.

    Returns [outcome for first, outcome for second]; each is None (stored durably) or an Exception.
    """
    last: Exception | None = None
    async with state.transfer_sem:
        for attempt in range(PUT_ATTEMPTS):
            try:
                r = await state.http.put(
                    f"{first.address}/internal/chunks/{chunk_id}", content=data,
                    headers={"X-Chunk-Checksum": checksum, "Content-Type": "application/octet-stream",
                             "X-Forward-To": second.address, "X-Forward-Worker-Id": second.worker_id})
                if r.status_code in (200, 201):
                    metrics.inc("chunk_chain_puts")
                    fwd = next((x for x in r.json().get("replicas", []) if x.get("worker_id") == second.worker_id),
                               None)
                    if fwd is not None and fwd.get("ok"):
                        return [None, None]
                    # first copy is durable; the forward failed (the worker already retried it)
                    why = (fwd or {}).get("error", "no forward result returned")
                    return [None, RuntimeError(f"{first.worker_id} could not forward to {second.worker_id}: {why}")]
                if r.status_code in (400, 409):  # deterministic rejection: don't retry
                    err = RuntimeError(f"{first.worker_id} rejected chunk: {r.status_code} {r.text.strip()}")
                    return [err, _NotAttempted()]
                last = RuntimeError(f"{first.worker_id} returned {r.status_code}")
            except httpx.HTTPError as e:
                last = e
            await asyncio.sleep(0.2 * (2 ** attempt))
    return [RuntimeError(f"PUT {chunk_id} to {first.worker_id} failed: {last!r}"), _NotAttempted()]


async def _process_chunk(state, obj, index: int, path: str) -> None:
    with tracing.span(f"chunk {index}", chunk_index=index):
        await _process_chunk_inner(state, obj, index, path)


async def _process_chunk_inner(state, obj, index: int, path: str) -> None:
    pool = state.pool
    cs_size = obj["chunk_size"]
    offset, length = chunking.chunk_span(index, obj["size"], cs_size)
    data = await asyncio.to_thread(_read_slice, path, offset, length)
    checksum = cs.sha256_bytes(data)
    chunk_id = chunk_id_for(obj["object_id"], index)
    tracing.annotate(chunk_id=chunk_id, size=length, checksum=checksum)

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
        tracing.annotate(targets=",".join(t.worker_id for t in targets))
        if len(targets) == 2:
            # the controller uploads ONE copy; the first worker forwards it to the second
            tracing.annotate(chain=f"controller → {targets[0].worker_id} → {targets[1].worker_id}")
            results = await _send_chain(state, targets[0], targets[1], chunk_id, checksum, data)
        else:
            results = await asyncio.gather(
                *[_send(state, t, chunk_id, checksum, data) for t in targets], return_exceptions=True)
        for t, res in zip(targets, results):
            if isinstance(res, _NotAttempted):  # never received it: free the slot, don't blame the worker
                await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING'",
                                   chunk_id, t.worker_id)
            elif isinstance(res, Exception):
                log.warning("replica placement failed: %s", res)
                tracing.event("replica failed", worker=t.worker_id, error=str(res)[:200])
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

    with tracing.span("mark COMMITTED"):
        async with pool.acquire() as c:
            async with c.transaction():
                await c.execute("UPDATE objects SET state='COMMITTED', updated_at=now() "
                                "WHERE object_id=$1 AND state='PENDING'", object_id)
                await c.execute("DELETE FROM uploads WHERE object_id=$1", object_id)
    meta_cache.invalidate_object(object_id)
    Path(path).unlink(missing_ok=True)  # cleanup policy: staged source removed after commit
    log.info("object %s committed (%d chunks)", object_id, obj["chunk_count"])
    return True


async def recover_staging(pool, staging_dir: str) -> dict:
    """Run once at start-up, before the resume loop and before any request: make staging and the database agree.

    Nothing is streaming at this point, so:
      * uploads.state='STREAMING'  -> cut off before it was ever acknowledged: drop the rows (uploads cascades)
      * STAGED row + <id>.uploading -> crash between the commit point and the rename: finish the rename
      * <id>.uploading without a STAGED row, <id>.staged without any upload row -> garbage, delete it
    """
    out = {"streaming_dropped": 0, "renames_finished": 0, "files_removed": 0}
    rows = await pool.fetch("SELECT object_id, state FROM uploads")
    for r in rows:
        if r["state"] == "STREAMING":
            await pool.execute("DELETE FROM objects WHERE object_id=$1 AND state='PENDING'", r["object_id"])
            out["streaming_dropped"] += 1
    staged = {str(r["object_id"]) for r in rows if r["state"] == "STAGED"}
    for f in sorted(Path(staging_dir).iterdir()):
        if not f.is_file() or f.suffix not in (".uploading", ".staged"):
            continue
        if f.stem in staged:
            if f.suffix == ".uploading":
                f.replace(f.with_suffix(".staged"))
                out["renames_finished"] += 1
            continue
        f.unlink(missing_ok=True)
        out["files_removed"] += 1
    return out


async def resume_loop(state) -> None:
    """Finish uploads that were staged but not committed (restart, or workers were down)."""
    while True:
        try:
            rows = await state.pool.fetch(
                """SELECT u.object_id FROM uploads u JOIN objects o USING (object_id)
                   WHERE o.state='PENDING' AND u.state='STAGED' ORDER BY o.created_at""")  # never a STREAMING one
            for r in rows:
                oid = r["object_id"]
                if oid in state.active_uploads:
                    continue
                state.active_uploads.add(oid)
                try:
                    log.info("resuming staged upload %s", oid)
                    with tracing.operation("resume staged upload", object_id=str(oid)):
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
