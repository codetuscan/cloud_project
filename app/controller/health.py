"""Marks workers DEAD when heartbeats stop."""
import asyncio
import logging
import time

from app.common import metrics
from app.controller import meta_cache

log = logging.getLogger("controller.health")


async def probe_workers(pool, http, timeout: float = 3.0) -> tuple[int, int]:
    """At controller start: ask every known worker whether it is alive, before anyone is judged.

    Heartbeat times live in PostgreSQL and keep ageing while the controller is down, so right after a restart
    every worker looks silent. Pinging them first refreshes the ones that answer; only workers that really
    do not respond are left for the monitor to declare DEAD. Returns (alive, unreachable).
    """
    rows = await pool.fetch("SELECT worker_id, address FROM workers WHERE state <> 'DEAD'")

    async def one(w):
        try:
            r = await http.get(f"{w['address']}/healthz", timeout=timeout)
            return w["worker_id"], r.status_code == 200
        except Exception:
            return w["worker_id"], False

    results = await asyncio.gather(*[one(w) for w in rows])
    alive = [wid for wid, ok in results if ok]
    if alive:
        await pool.execute("UPDATE workers SET last_heartbeat=now() WHERE worker_id = ANY($1::text[])", alive)
    down = [wid for wid, ok in results if not ok]
    metrics.inc("startup_probe_alive", len(alive))
    log.info("startup probe: %d worker(s) alive%s", len(alive), f", unreachable: {down}" if down else "")
    return len(alive), len(down)


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
