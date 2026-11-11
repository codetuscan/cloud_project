"""Replica placement: choose distinct, eligible workers for a chunk."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    worker_id: str
    address: str


async def healthy_count(pool) -> int:
    return await pool.fetchval("SELECT count(*) FROM workers WHERE state='HEALTHY'")


async def pick(pool, n: int, exclude: set[str], size: int) -> list[Target]:
    """Pick up to n distinct HEALTHY workers not in `exclude` with room for `size` bytes.

    Least-loaded first (fewest PENDING/HEALTHY replicas), then most free space, then random,
    so chunks spread evenly across workers.
    """
    rows = await pool.fetch(
        """SELECT w.worker_id, w.address, count(r.chunk_id) AS load
           FROM workers w
           LEFT JOIN replicas r ON r.worker_id = w.worker_id AND r.state IN ('PENDING','HEALTHY')
           WHERE w.state = 'HEALTHY'
             AND NOT (w.worker_id = ANY($1::text[]))
             AND (w.capacity_bytes - w.used_bytes) >= $2
           GROUP BY w.worker_id, w.address, w.capacity_bytes, w.used_bytes
           ORDER BY load ASC, (w.capacity_bytes - w.used_bytes) DESC, random()
           LIMIT $3""",
        list(exclude), size, n,
    )
    return [Target(r["worker_id"], r["address"]) for r in rows]
