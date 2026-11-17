"""Repair manager, under-replication scanner and inventory reconciler.

Workers only observe and report; this module decides and coordinates:
  * scanner      - finds chunks with <2 healthy replicas on live workers (e.g. a dead worker)
                   and queues one durable repair task per chunk; trims surplus replicas.
  * runners      - claim tasks (SKIP LOCKED), copy from a healthy source to a target, verify, record.
  * reconciler   - compares each live worker's inventory with PostgreSQL: marks missing replicas,
                   removes tombstoned/orphan files after a grace period.
"""
import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

from app.common import metrics
from app.common.config import settings
from app.controller import meta_cache, placement, replicas

log = logging.getLogger("controller.repair")


# ------------------------------------------------------------------ helpers
async def _finish(pool, task_id, state: str, note: str | None = None) -> None:
    await pool.execute("UPDATE repair_tasks SET state=$2, last_error=$3, updated_at=now() WHERE task_id=$1",
                       task_id, state, note)
    metrics.inc(f"repairs_{state.lower()}")
    log.info("repair task %s -> %s%s", task_id, state, f" ({note})" if note else "")


async def _retry(pool, task, why: str) -> None:
    metrics.inc("repairs_retried")
    delay = min(2 ** task["attempts"], 60)
    await pool.execute(
        """UPDATE repair_tasks SET state='PENDING', last_error=$2, updated_at=now(),
               next_attempt_at = now() + make_interval(secs => $3) WHERE task_id=$1""",
        task["task_id"], why, float(delay))
    log.warning("repair of %s postponed (%ss): %s", task["chunk_id"], delay, why)


async def _command_copy(state, src, tgt, chunk, counter: str = "repair_direct_copies") -> str:
    """Tell worker `src` to push the chunk straight to worker `tgt`; no chunk bytes pass through the controller.

    The source verifies the chunk against its own record AND our checksum, the target verifies it again before
    acknowledging. Returns: ok | source_missing | source_corrupt | source_unreachable | target_failed.
    """
    try:
        async with state.transfer_sem:
            r = await state.http.post(
                f"{src['address']}/internal/chunks/{chunk['chunk_id']}/send",
                json={"target_address": tgt["address"], "checksum": chunk["checksum"], "size": chunk["size"]},
                timeout=45.0)
    except httpx.HTTPError as e:
        log.warning("repair source %s unreachable: %r", src["worker_id"], e)
        return "source_unreachable"
    if r.status_code != 200:
        log.warning("repair source %s refused the copy command: %s %s", src["worker_id"], r.status_code, r.text[:200])
        return "source_unreachable"
    body = r.json()
    if body.get("ok"):
        metrics.inc(counter)
        return "ok"
    reason = body.get("reason")
    if reason == "source_missing":
        await replicas.mark_bad(state.pool, chunk["chunk_id"], src["worker_id"], "MISSING", "repair source: 404")
        return reason
    if reason == "source_corrupt":
        await replicas.mark_bad(state.pool, chunk["chunk_id"], src["worker_id"], "CORRUPT",
                                f"repair source: self-check failed ({body.get('detail', '')[:120]})")
        return reason
    log.warning("repair copy %s: %s -> %s failed: %s", chunk["chunk_id"], src["worker_id"], tgt["worker_id"],
                body.get("detail", ""))
    return "target_failed"


# ------------------------------------------------------------------ task execution
async def execute(state, task) -> None:
    pool = state.pool
    chunk = await pool.fetchrow(
        """SELECT c.chunk_id, c.size, c.checksum, o.state AS ostate FROM chunks c
           JOIN objects o USING (object_id) WHERE c.chunk_id=$1""", task["chunk_id"])
    if chunk is None or chunk["ostate"] != "COMMITTED":
        return await _finish(pool, task["task_id"], "DONE", "object no longer active")

    rows = await pool.fetch(
        """SELECT r.worker_id, r.state, w.address, w.state AS wstate FROM replicas r
           JOIN workers w USING (worker_id) WHERE r.chunk_id=$1""", task["chunk_id"])
    sources = [dict(r) for r in rows if r["state"] == "HEALTHY" and r["wstate"] != "DEAD"]
    need = 2 - len(sources)
    if need <= 0:
        await pool.execute("UPDATE replicas SET state='DELETING', updated_at=now() "
                           "WHERE chunk_id=$1 AND state IN ('CORRUPT','MISSING')", task["chunk_id"])
        meta_cache.invalidate_chunk(task["chunk_id"])
        return await _finish(pool, task["task_id"], "DONE", "already 2 healthy replicas")

    if not sources:
        waiting = [r["worker_id"] for r in rows if r["state"] == "HEALTHY" and r["wstate"] == "DEAD"]
        if waiting:
            return await _retry(pool, task, f"no live source; waiting for worker(s) {waiting} to return")
        return await _finish(pool, task["task_id"], "UNRECOVERABLE",
                             "no healthy replica exists anywhere; data cannot be reconstructed")

    # targets: repair in place first (workers holding a CORRUPT/MISSING copy), then new workers
    targets: list[dict] = []
    if task["attempts"] <= 3:
        targets = [dict(r) for r in rows if r["state"] in ("CORRUPT", "MISSING") and r["wstate"] != "DEAD"][:need]
    rest = need - len(targets)
    new_ids: set[str] = set()
    if rest > 0:
        picked = await placement.pick(pool, rest, {r["worker_id"] for r in rows}, chunk["size"])
        for t in picked:
            targets.append({"worker_id": t.worker_id, "address": t.address})
            new_ids.add(t.worker_id)
            await pool.execute(
                """INSERT INTO replicas (chunk_id, worker_id, state) VALUES ($1,$2,'PENDING')
                   ON CONFLICT (chunk_id, worker_id) DO UPDATE SET state='PENDING', updated_at=now()""",
                chunk["chunk_id"], t.worker_id)
        meta_cache.invalidate_chunk(chunk["chunk_id"])
    if not targets:
        return await _retry(pool, task, "degraded: no eligible worker to hold another replica")

    # The controller only sends commands: a healthy source worker pushes the chunk directly to each target.
    done: dict[str, str] = {}  # target worker_id -> the source worker that delivered it
    remaining = list(targets)
    random.shuffle(sources)
    for src in sources:
        if not remaining:
            break
        outcomes = await asyncio.gather(*[_command_copy(state, src, t, chunk) for t in remaining])
        next_source = []
        for t, outcome in zip(remaining, outcomes):
            if outcome == "ok":
                done[t["worker_id"]] = src["worker_id"]
            elif outcome != "target_failed":  # this SOURCE was the problem: let the next source try
                next_source.append(t)
            # target_failed: the target is the problem; leave it for the retry below
        remaining = next_source

    if not done:
        for wid in new_ids:
            await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING'",
                               chunk["chunk_id"], wid)
        meta_cache.invalidate_chunk(chunk["chunk_id"])
        return await _retry(pool, task, "no source could deliver verified bytes to a target")

    await pool.execute("UPDATE repair_tasks SET source_worker_id=$2, target_worker_id=$3 WHERE task_id=$1",
                       task["task_id"], next(iter(done.values())), ",".join(t["worker_id"] for t in targets))
    for t in targets:
        if t["worker_id"] in done:
            await pool.execute(
                """UPDATE replicas SET state='HEALTHY', last_verified=now(), updated_at=now()
                   WHERE chunk_id=$1 AND worker_id=$2""", chunk["chunk_id"], t["worker_id"])
            meta_cache.invalidate_chunk(chunk["chunk_id"])
            log.info("repaired %s: copied directly from %s to %s (checksum verified, fsynced)",
                     chunk["chunk_id"], done[t["worker_id"]], t["worker_id"])
        elif t["worker_id"] in new_ids:
            await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING'",
                               chunk["chunk_id"], t["worker_id"])
            meta_cache.invalidate_chunk(chunk["chunk_id"])

    healthy = await pool.fetchval(
        """SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
           WHERE r.chunk_id=$1 AND r.state='HEALTHY' AND w.state <> 'DEAD'""", chunk["chunk_id"])
    if healthy >= 2:
        await pool.execute("UPDATE replicas SET state='DELETING', updated_at=now() "
                           "WHERE chunk_id=$1 AND state IN ('CORRUPT','MISSING')", chunk["chunk_id"])
        meta_cache.invalidate_chunk(chunk["chunk_id"])
        await _finish(pool, task["task_id"], "DONE")
        created = task.get("created_at")
        if isinstance(created, datetime):  # queued -> protected again (includes backoff waits)
            healed = (datetime.now(timezone.utc) - created).total_seconds()
            metrics.observe("repair_time_to_heal_seconds", max(0.0, healed))
    else:
        await _retry(pool, task, f"only {healthy}/2 healthy replicas after attempt")


async def _claim(pool):
    return await pool.fetchrow(
        """UPDATE repair_tasks SET state='IN_PROGRESS', attempts=attempts+1, updated_at=now()
           WHERE task_id = (SELECT task_id FROM repair_tasks
                            WHERE state='PENDING' AND next_attempt_at <= now()
                            ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
           RETURNING *""")


async def runner(state, n: int) -> None:
    while True:
        try:
            task = await _claim(state.pool)
            if task is None:
                await asyncio.sleep(1.0)
                continue
            try:
                await execute(state, task)
            except Exception as e:
                log.exception("repair task %s crashed", task["task_id"])
                await _retry(state.pool, task, f"internal error: {e!r}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("repair runner %d error: %s", n, e)
            await asyncio.sleep(2.0)


# ------------------------------------------------------------------ scanner
async def scan_once(pool) -> None:
    # tasks whose runner died mid-way (controller restart) go back to the queue
    await pool.execute(
        """UPDATE repair_tasks SET state='PENDING', updated_at=now()
           WHERE state='IN_PROGRESS' AND updated_at < now() - make_interval(secs => $1)""",
        float(settings.repair_lease_seconds))
    # copies whose repair/rebalance was interrupted (controller restart): retire them; the reaper removes the file
    stale = await pool.execute(
        """UPDATE replicas SET state='DELETING', updated_at=now()
           WHERE state='PENDING' AND updated_at < now() - make_interval(secs => $1)
             AND chunk_id IN (SELECT c.chunk_id FROM chunks c JOIN objects o USING (object_id)
                              WHERE o.state='COMMITTED')""",
        float(settings.repair_lease_seconds))
    if stale and stale != "UPDATE 0":
        log.warning("retired interrupted copies: %s", stale)
    created = await pool.fetch(
        """INSERT INTO repair_tasks (task_id, chunk_id, reason, state)
           SELECT gen_random_uuid(), c.chunk_id, 'UNDER_REPLICATED', 'PENDING'
           FROM chunks c JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED'
           WHERE (SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
                  WHERE r.chunk_id=c.chunk_id AND r.state='HEALTHY' AND w.state <> 'DEAD') < 2
           ON CONFLICT DO NOTHING RETURNING chunk_id""")
    if created:
        log.warning("%d chunk(s) below 2 healthy replicas; repair queued", len(created))
    # surplus replicas (e.g. a dead worker returned after its chunks were re-replicated)
    over = await pool.fetch(
        """SELECT r.chunk_id FROM replicas r JOIN workers w USING (worker_id)
           JOIN chunks c USING (chunk_id) JOIN objects o USING (object_id)
           WHERE o.state='COMMITTED' AND r.state='HEALTHY' AND w.state <> 'DEAD'
           GROUP BY r.chunk_id HAVING count(*) > 2""")
    for o in over:
        reps = await pool.fetch(
            """SELECT r.worker_id, (SELECT count(*) FROM replicas x WHERE x.worker_id=r.worker_id
                                    AND x.state IN ('HEALTHY','PENDING')) AS load
               FROM replicas r JOIN workers w USING (worker_id)
               WHERE r.chunk_id=$1 AND r.state='HEALTHY' AND w.state <> 'DEAD'
               ORDER BY load DESC, r.worker_id""", o["chunk_id"])
        for extra in reps[: len(reps) - 2]:
            await pool.execute(
                "UPDATE replicas SET state='DELETING', updated_at=now() "
                "WHERE chunk_id=$1 AND worker_id=$2 AND state='HEALTHY'", o["chunk_id"], extra["worker_id"])
            meta_cache.invalidate_chunk(o["chunk_id"])
            log.info("trimming surplus replica of %s on %s", o["chunk_id"], extra["worker_id"])
    await pool.execute("DELETE FROM repair_tasks WHERE state='DONE' AND updated_at < now() - interval '1 day'")
    await pool.execute("DELETE FROM scrub_results WHERE checked_at < now() - interval '7 days'")


async def scanner(state) -> None:
    while True:
        try:
            await scan_once(state.pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("repair scanner error: %s", e)
        await asyncio.sleep(settings.repair_scan_interval)


# ------------------------------------------------------------------ inventory reconciliation
async def reconcile_worker(state, w) -> None:
    pool = state.pool
    wid = w["worker_id"]
    # Read DB rows BEFORE the inventory: anything HEALTHY in the DB was durably written before
    # this point, so it must appear in the inventory fetched afterwards (no false MISSING).
    rows = await pool.fetch(
        """SELECT r.chunk_id, r.state, c.checksum FROM replicas r JOIN chunks c USING (chunk_id)
           WHERE r.worker_id=$1""", wid)
    try:
        resp = await state.http.get(f"{w['address']}/internal/workers/{wid}/chunks", timeout=15.0)
        resp.raise_for_status()
        inv = {c["chunk_id"]: c for c in resp.json()["chunks"]}
    except Exception as e:
        log.warning("inventory of %s unavailable: %r", wid, e)
        return

    known = {r["chunk_id"]: r for r in rows}
    for cid, r in known.items():
        if r["state"] != "HEALTHY":
            continue
        item = inv.get(cid)
        if item is None:
            await replicas.mark_bad(pool, cid, wid, "MISSING", "inventory: file absent on worker")
        elif item.get("checksum") and item["checksum"] != r["checksum"]:
            await replicas.mark_bad(pool, cid, wid, "CORRUPT", "inventory: recorded checksum differs")

    # unexpected files
    tomb = {r["chunk_id"] for r in await pool.fetch("SELECT chunk_id FROM tombstones WHERE worker_id=$1", wid)}
    now = time.monotonic()
    for cid in inv:
        key = (wid, cid)
        if cid in known:
            state.orphans.pop(key, None)
            continue
        if cid in tomb:
            await _delete_orphan(state, w, cid, "tombstoned (deleted object)")
            continue
        first = state.orphans.setdefault(key, now)
        if now - first >= settings.orphan_grace_seconds:
            still = await pool.fetchval("SELECT 1 FROM replicas WHERE chunk_id=$1 AND worker_id=$2", cid, wid)
            if not still:
                await _delete_orphan(state, w, cid, f"orphan unseen by metadata for >{settings.orphan_grace_seconds:.0f}s")
            state.orphans.pop(key, None)
    for key in [k for k in state.orphans if k[0] == wid and k[1] not in inv]:
        state.orphans.pop(key, None)


async def _delete_orphan(state, w, cid: str, why: str) -> None:
    try:
        r = await state.http.delete(f"{w['address']}/internal/chunks/{cid}")
        metrics.inc("orphan_chunks_deleted")
        log.warning("deleted unexpected chunk %s on %s: %s (status %s)", cid, w["worker_id"], why, r.status_code)
    except httpx.HTTPError as e:
        log.warning("could not delete orphan %s on %s: %r", cid, w["worker_id"], e)


async def reconciler(state) -> None:
    await asyncio.sleep(5)
    while True:
        try:
            workers = await state.pool.fetch("SELECT worker_id, address FROM workers WHERE state <> 'DEAD'")
            await asyncio.gather(*[reconcile_worker(state, w) for w in workers])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("reconciler error: %s", e)
        await asyncio.sleep(settings.reconcile_interval)
