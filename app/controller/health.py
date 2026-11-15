"""Marks workers DEAD when heartbeats stop."""
import asyncio
import logging
import time

from app.common import metrics
from app.controller import meta_cache

log = logging.getLogger("controller.health")


async def monitor(pool, timeout: float, interval: float) -> None:
    last_db_failure = float("-inf")
    while True:
        try:
            # While PostgreSQL was unreachable no heartbeat could be recorded, so every worker looks silent.
            # Give them one full timeout after the database is back before judging anyone.
            if time.monotonic() - last_db_failure < timeout:
                await pool.fetchval("SELECT 1")
                await asyncio.sleep(interval)
                continue
            async with pool.acquire() as c:
                rows = await c.fetch(
                    """UPDATE workers SET state='DEAD'
                       WHERE state <> 'DEAD'
                         AND last_heartbeat < now() - make_interval(secs => $1)
                       RETURNING worker_id""", float(timeout))
            if rows:
                meta_cache.invalidate_all()  # worker state feeds every manifest
            metrics.inc("workers_marked_dead", len(rows))
            for r in rows:
                log.warning("worker %s missed heartbeats for >%.0fs -> DEAD", r["worker_id"], timeout)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_db_failure = time.monotonic()
            log.error("health monitor error (database unreachable? grace period started): %s", e)
        await asyncio.sleep(interval)
