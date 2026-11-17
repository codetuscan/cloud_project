"""Rebalancer: evens out chunk replicas across workers (a new worker joins) and empties draining workers.

New chunks already go to the least-loaded workers (placement.pick), but data that is already stored stays where
it is. When the replica counts of the HEALTHY workers differ by more than REBALANCE_TOLERANCE, this loop moves
replicas from the fullest worker to the emptiest one, a small batch per round.

A move never lowers the number of copies:
  1. a PENDING replica row is created on the recipient (so placement and the reconciler know about it);
  2. the donor worker pushes the chunk straight to the recipient (the same verified /send command that repair
     uses: the donor checks its bytes against the controller's checksum, the recipient verifies, fsyncs, renames);
  3. in ONE transaction the recipient's copy becomes HEALTHY and the donor's copy becomes DELETING, only if the
     chunk then has at least 3 healthy copies on live workers; the reaper removes the donor's file afterwards.
If the object is deleted meanwhile, its replica rows (including our PENDING one) were already moved to DELETING,
step 3 changes nothing and the reaper removes the copy. If the controller dies mid-move, the stale PENDING row is
cleaned up by the repair scanner.

Draining (POST /internal/workers/{id}/drain, before removing a worker): placement stops choosing the worker, and
each round first moves up to REBALANCE_BATCH of its replicas to the least-loaded other workers, the same safe way.
When it holds nothing, it can be stopped and forgotten without the cluster ever being under-replicated.

Repair always wins: a round is skipped while any chunk is under-replicated, has a surplus copy waiting to be
trimmed, or has an active repair task.
"""
import asyncio
import logging
import time

from app.common import metrics
from app.common.config import settings
from app.controller import meta_cache
from app.controller.repair import _command_copy

log = logging.getLogger("controller.rebalance")

# last view, served by /internal/status and /metrics
status: dict = {"enabled": settings.rebalance_enabled, "state": "starting", "spread": 0, "loads": {},
                "draining": {}, "moves_last_round": 0, "last_round_at": None}


def plan_moves(loads: dict[str, int], tolerance: int, limit: int,
               receivers: set[str] | None = None) -> list[tuple[str, str]]:
    """Greedy plan: repeatedly move one replica from the fullest worker to the emptiest eligible one.

    Returns (donor, recipient) pairs, at most `limit`, stopping once max - min <= tolerance.
    Pure function (unit-tested); ties are broken by worker id so the plan is deterministic.
    """
    loads = dict(loads)
    receivers = set(loads) if receivers is None else receivers & set(loads)
    moves: list[tuple[str, str]] = []
    while len(moves) < limit and receivers and len(loads) >= 2:
        donor = max(loads, key=lambda w: (loads[w], w))
        recipient = min(receivers, key=lambda w: (loads[w], w))
        if donor == recipient or loads[donor] - loads[recipient] <= max(1, tolerance):
            break
        moves.append((donor, recipient))
        loads[donor] -= 1
        loads[recipient] += 1
    return moves


async def _cluster_busy(pool) -> str | None:
    """A reason to skip this round, or None when the cluster is stable enough to move data."""
    row = await pool.fetchrow(
        """SELECT
             (SELECT count(*) FROM repair_tasks WHERE state IN ('PENDING','IN_PROGRESS')) AS repairs,
             (SELECT count(*) FROM (
                 SELECT (SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
                         WHERE r.chunk_id=c.chunk_id AND r.state='HEALTHY' AND w.state<>'DEAD') AS n
                 FROM chunks c JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED') s
              WHERE n <> 2) AS unbalanced_chunks""")
    if row["repairs"]:
        return f"{row['repairs']} repair task(s) active"
    if row["unbalanced_chunks"]:
        return f"{row['unbalanced_chunks']} chunk(s) not at exactly 2 healthy copies"
    return None


async def _loads(pool) -> tuple[dict[str, int], dict[str, str], set[str], dict[str, int]]:
    """(loads of balancing workers, addresses, workers with room to receive, loads of draining workers)."""
    rows = await pool.fetch(
        """SELECT w.worker_id, w.address, w.state, w.draining, (w.capacity_bytes - w.used_bytes) AS free,
                  count(r.chunk_id) FILTER (WHERE r.state IN ('HEALTHY','PENDING')) AS load
           FROM workers w LEFT JOIN replicas r USING (worker_id)
           WHERE w.state <> 'DEAD'
           GROUP BY w.worker_id, w.address, w.state, w.draining, w.capacity_bytes, w.used_bytes""")
    balancing = [r for r in rows if r["state"] == "HEALTHY" and not r["draining"]]
    loads = {r["worker_id"]: r["load"] for r in balancing}
    addresses = {r["worker_id"]: r["address"] for r in rows}
    margin = settings.chunk_size * max(4, settings.rebalance_batch)  # never fill a disk to the brim
    receivers = {r["worker_id"] for r in balancing if r["free"] >= margin}
    draining = {r["worker_id"]: r["load"] for r in rows if r["draining"]}
    return loads, addresses, receivers, draining


async def _drain_jobs(pool, draining: dict[str, int], loads: dict[str, int], receivers: set[str],
                      addresses: dict[str, str]) -> list:
    """Up to REBALANCE_BATCH moves off draining workers, each to the least-loaded receiver not holding that chunk."""
    loads = dict(loads)
    jobs, taken = [], []
    for donor in sorted((w for w, n in draining.items() if n), key=lambda w: -draining[w]):
        rows = await pool.fetch(
            """SELECT c.chunk_id, c.size, c.checksum, array_agg(x.worker_id) AS holders FROM replicas r
               JOIN chunks c USING (chunk_id) JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED'
               JOIN replicas x ON x.chunk_id=c.chunk_id
               WHERE r.worker_id=$1 AND r.state='HEALTHY' AND NOT (c.chunk_id = ANY($3::text[]))
                 AND NOT EXISTS (SELECT 1 FROM replicas y WHERE y.chunk_id=c.chunk_id AND y.state<>'HEALTHY')
                 AND NOT EXISTS (SELECT 1 FROM repair_tasks t WHERE t.chunk_id=c.chunk_id
                                 AND t.state IN ('PENDING','IN_PROGRESS'))
               GROUP BY c.chunk_id, c.size, c.checksum LIMIT $2""",
            donor, settings.rebalance_batch - len(jobs), taken)
        for ch in rows:
            options = [w for w in receivers if w not in ch["holders"]]
            if not options:
                continue
            target = min(options, key=lambda w: (loads[w], w))
            loads[target] += 1
            taken.append(ch["chunk_id"])
            jobs.append((ch, {"worker_id": donor, "address": addresses[donor]},
                         {"worker_id": target, "address": addresses[target]}))
        if len(jobs) >= settings.rebalance_batch:
            break
    return jobs


async def _candidates(pool, donor: str, recipient: str, n: int, taken: list[str]) -> list:
    """Chunks the donor holds that the recipient does not, with exactly 2 healthy copies and nothing in flight."""
    return await pool.fetch(
        """SELECT c.chunk_id, c.size, c.checksum FROM replicas r
           JOIN chunks c USING (chunk_id) JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED'
           WHERE r.worker_id=$1 AND r.state='HEALTHY'
             AND NOT EXISTS (SELECT 1 FROM replicas x WHERE x.chunk_id=c.chunk_id AND x.worker_id=$2)
             AND NOT EXISTS (SELECT 1 FROM replicas x WHERE x.chunk_id=c.chunk_id AND x.state<>'HEALTHY')
             AND NOT EXISTS (SELECT 1 FROM repair_tasks t WHERE t.chunk_id=c.chunk_id
                             AND t.state IN ('PENDING','IN_PROGRESS'))
             AND NOT (c.chunk_id = ANY($4::text[]))
           ORDER BY random() LIMIT $3""", donor, recipient, n, taken)


async def move(state, chunk, donor: dict, recipient: dict) -> bool:
    """Copy one replica donor -> recipient, then retire the donor's copy. True if the move completed."""
    pool, cid = state.pool, chunk["chunk_id"]
    claimed = await pool.fetchval(
        """INSERT INTO replicas (chunk_id, worker_id, state)
           SELECT $1, $2, 'PENDING' WHERE EXISTS (
               SELECT 1 FROM chunks c JOIN objects o USING (object_id) WHERE c.chunk_id=$1 AND o.state='COMMITTED')
           ON CONFLICT (chunk_id, worker_id) DO NOTHING RETURNING 1""", cid, recipient["worker_id"])
    if not claimed:
        return False
    outcome = await _command_copy(state, donor, recipient, chunk, counter="rebalance_copies")
    if outcome != "ok":
        await pool.execute("DELETE FROM replicas WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING'",
                           cid, recipient["worker_id"])
        meta_cache.invalidate_chunk(cid)
        metrics.inc("rebalance_move_failures")
        log.warning("rebalance of %s %s -> %s failed: %s", cid, donor["worker_id"], recipient["worker_id"], outcome)
        return False

    async with pool.acquire() as c:
        async with c.transaction():
            promoted = await c.fetchval(
                """UPDATE replicas SET state='HEALTHY', last_verified=now(), updated_at=now()
                   WHERE chunk_id=$1 AND worker_id=$2 AND state='PENDING' RETURNING 1""",
                cid, recipient["worker_id"])
            healthy = await c.fetchval(
                """SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
                   WHERE r.chunk_id=$1 AND r.state='HEALTHY' AND w.state <> 'DEAD'""", cid)
            retired = None
            if promoted and healthy >= 3:  # the donor's copy is now surplus; never drop below 2
                retired = await c.fetchval(
                    """UPDATE replicas SET state='DELETING', updated_at=now()
                       WHERE chunk_id=$1 AND worker_id=$2 AND state='HEALTHY' RETURNING 1""",
                    cid, donor["worker_id"])
    meta_cache.invalidate_chunk(cid)
    if not promoted:  # object deleted meanwhile: the reaper removes the copy we just made
        return False
    metrics.inc("rebalance_moves")
    log.info("rebalanced %s: %s -> %s (checksum verified, fsynced)%s", cid, donor["worker_id"],
             recipient["worker_id"], "" if retired else "; donor copy kept (another copy changed meanwhile)")
    return True


async def run_once(state) -> int:
    """One round. Returns the number of completed moves."""
    pool = state.pool
    loads, addresses, receivers, draining = await _loads(pool)
    spread = (max(loads.values()) - min(loads.values())) if len(loads) >= 2 else 0
    status.update(loads=loads, spread=spread, draining=draining, last_round_at=time.time(), moves_last_round=0)
    busy = await _cluster_busy(pool)
    if busy:
        status["state"] = f"waiting: {busy}"
        return 0

    pairs: dict[tuple[str, str], int] = {}
    jobs = await _drain_jobs(pool, draining, loads, receivers, addresses)
    if jobs:
        for ch, d, r in jobs:
            pairs[(d["worker_id"], r["worker_id"])] = pairs.get((d["worker_id"], r["worker_id"]), 0) + 1
        why = f"draining {', '.join(f'{w} ({n} left)' for w, n in sorted(draining.items()) if n)}"
    elif any(draining.values()):
        status["state"] = "draining: no chunk can move yet (no eligible receiver)"
        return 0
    else:
        plan = plan_moves(loads, settings.rebalance_tolerance, settings.rebalance_batch, receivers)
        if not plan:
            status["state"] = "balanced"
            return 0
        for p in plan:
            pairs[p] = pairs.get(p, 0) + 1
        taken: list[str] = []
        for (d, r), n in pairs.items():
            for ch in await _candidates(pool, d, r, n, taken):
                taken.append(ch["chunk_id"])
                jobs.append((ch, {"worker_id": d, "address": addresses[d]},
                             {"worker_id": r, "address": addresses[r]}))
        if not jobs:
            status["state"] = "no movable chunks"
            return 0
        why = f"replica counts {dict(sorted(loads.items()))} (spread {spread} > tolerance {settings.rebalance_tolerance})"

    status["state"] = f"moving {len(jobs)} replica(s)"
    log.info("rebalancing: %s, moving %d replica(s): %s", why, len(jobs),
             ", ".join(f"{d}->{r} x{n}" for (d, r), n in pairs.items()))
    sem = asyncio.Semaphore(max(1, settings.rebalance_concurrency))

    async def one(job):
        async with sem:
            try:
                return await move(state, *job)
            except Exception as e:
                metrics.inc("rebalance_move_failures")
                log.warning("rebalance move of %s crashed: %r", job[0]["chunk_id"], e)
                return False

    done = sum(await asyncio.gather(*[one(j) for j in jobs]))
    status["moves_last_round"] = done
    return done


async def loop(state) -> None:
    if not settings.rebalance_enabled:
        status["state"] = "disabled"
        return
    await asyncio.sleep(settings.rebalance_interval)  # let start-up probing, repairs and trims settle first
    while True:
        moved = 0
        try:
            moved = await run_once(state)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            status["state"] = f"error: {e!r}"
            log.error("rebalancer error: %s", e)
        # keep going without a pause while there is work; otherwise check again later
        await asyncio.sleep(1.0 if moved else settings.rebalance_interval)
