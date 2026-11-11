"""Deletion lifecycle: DELETING objects -> remove replicas from workers -> DELETED -> purge.

Policy: an object becomes DELETED only after every replica of every chunk was confirmed
removed by its worker (204/404). Replicas on dead workers are retried until the worker
returns. Tombstones outlive the object so a stale inventory can't resurrect its chunks.
Also aborts uploads that stayed PENDING longer than UPLOAD_ABANDON_SECONDS.
"""
import asyncio
import logging
from pathlib import Path

import httpx

from app.common.config import settings

log = logging.getLogger("controller.reaper")
INTERVAL = 3.0


async def begin_delete(conn, object_id, from_states: tuple[str, ...]) -> bool:
    """Atomically hide an object and queue its replicas for removal. Caller owns the transaction."""
    row = await conn.fetchval(
        """UPDATE objects SET state='DELETING', updated_at=now(), idempotency_key=NULL
           WHERE object_id=$1 AND state = ANY($2::text[]) RETURNING 1""",
        object_id, list(from_states))
    if not row:
        return False
    await conn.execute(
        """INSERT INTO tombstones (chunk_id, worker_id)
           SELECT r.chunk_id, r.worker_id FROM replicas r JOIN chunks c USING (chunk_id)
           WHERE c.object_id=$1 ON CONFLICT DO NOTHING""", object_id)
    await conn.execute(
        """UPDATE replicas SET state='DELETING', updated_at=now()
           WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=$1)""", object_id)
    await conn.execute(
        "DELETE FROM repair_tasks WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=$1)", object_id)
    return True


async def _delete_replicas(state) -> int:
    pool = state.pool
    rows = await pool.fetch(
        """SELECT r.chunk_id, r.worker_id, w.address FROM replicas r JOIN workers w USING (worker_id)
           WHERE r.state='DELETING' AND w.state <> 'DEAD' LIMIT 100""")
    done = 0

    async def one(r):
        nonlocal done
        async with state.transfer_sem:
            try:
                resp = await state.http.delete(f"{r['address']}/internal/chunks/{r['chunk_id']}")
                if resp.status_code in (200, 204, 404):
                    await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='DELETING'",
                                       r["chunk_id"], r["worker_id"])
                    done += 1
                else:
                    log.warning("delete %s on %s -> %s", r["chunk_id"], r["worker_id"], resp.status_code)
            except httpx.HTTPError as e:
                log.warning("delete %s on %s failed: %r (will retry)", r["chunk_id"], r["worker_id"], e)

    await asyncio.gather(*[one(r) for r in rows])
    return done


async def _finalize(pool) -> None:
    rows = await pool.fetch(
        """SELECT o.object_id FROM objects o WHERE o.state='DELETING' AND NOT EXISTS
           (SELECT 1 FROM replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id=o.object_id)""")
    for r in rows:
        async with pool.acquire() as c:
            async with c.transaction():
                await c.execute("DELETE FROM chunks WHERE object_id=$1", r["object_id"])
                await c.execute("UPDATE objects SET state='DELETED', updated_at=now() WHERE object_id=$1",
                                r["object_id"])
        log.info("object %s fully deleted from all workers", r["object_id"])


async def _abort_abandoned(state) -> None:
    pool = state.pool
    rows = await pool.fetch(
        """SELECT o.object_id, u.staged_path FROM objects o LEFT JOIN uploads u USING (object_id)
           WHERE o.state='PENDING' AND o.updated_at < now() - make_interval(secs => $1)""",
        float(settings.upload_abandon_seconds))
    for r in rows:
        if r["object_id"] in state.active_uploads:
            continue
        async with pool.acquire() as c:
            async with c.transaction():
                ok = await begin_delete(c, r["object_id"], ("PENDING",))
                await c.execute("DELETE FROM uploads WHERE object_id=$1", r["object_id"])
        if ok:
            if r["staged_path"]:
                Path(r["staged_path"]).unlink(missing_ok=True)
            log.warning("aborted abandoned upload %s", r["object_id"])


async def _purge(pool) -> None:
    secs = float(settings.tombstone_retention_seconds)
    await pool.execute("DELETE FROM tombstones WHERE deleted_at < now() - make_interval(secs => $1)", secs)
    await pool.execute(
        "DELETE FROM objects WHERE state='DELETED' AND updated_at < now() - make_interval(secs => $1)", secs)


async def run(state) -> None:
    tick = 0
    while True:
        try:
            await _delete_replicas(state)
            await _finalize(state.pool)
            tick += 1
            if tick % 20 == 0:
                await _abort_abandoned(state)
                await _purge(state.pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("reaper error: %s", e)
        await asyncio.sleep(INTERVAL)
