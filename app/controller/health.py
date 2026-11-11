"""Marks workers DEAD when heartbeats stop."""
import asyncio
import logging

log = logging.getLogger("controller.health")


async def monitor(pool, timeout: float, interval: float) -> None:
    while True:
        try:
            async with pool.acquire() as c:
                rows = await c.fetch(
                    """UPDATE workers SET state='DEAD'
                       WHERE state <> 'DEAD'
                         AND last_heartbeat < now() - make_interval(secs => $1)
                       RETURNING worker_id""", float(timeout))
            for r in rows:
                log.warning("worker %s missed heartbeats for >%.0fs -> DEAD", r["worker_id"], timeout)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("health monitor error: %s", e)
        await asyncio.sleep(interval)
