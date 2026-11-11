"""Replica health bookkeeping shared by the read path, scrub reports and the repair manager."""
import logging
import uuid

log = logging.getLogger("controller.replicas")


async def mark_bad(pool, chunk_id: str, worker_id: str, state: str, reason: str) -> bool:
    """Mark a HEALTHY replica CORRUPT/MISSING and queue a durable repair task for the chunk.

    Returns True if the replica changed state. Safe to call repeatedly (idempotent).
    """
    assert state in ("CORRUPT", "MISSING")
    async with pool.acquire() as c:
        async with c.transaction():
            changed = await c.fetchval(
                """UPDATE replicas SET state=$3, updated_at=now()
                   WHERE chunk_id=$1 AND worker_id=$2 AND state='HEALTHY' RETURNING 1""",
                chunk_id, worker_id, state)
            if changed:
                await c.execute(
                    """INSERT INTO repair_tasks (task_id, chunk_id, target_worker_id, reason, state)
                       VALUES ($1,$2,$3,$4,'PENDING') ON CONFLICT DO NOTHING""",
                    uuid.uuid4(), chunk_id, worker_id, reason)
    if changed:
        log.warning("replica %s on %s marked %s (%s); repair queued", chunk_id, worker_id, state, reason)
    return bool(changed)
