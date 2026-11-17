"""Worker-facing endpoints on the controller: registration, heartbeat, worker listing."""
import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.common import metrics
from app.common.config import settings
from app.common.errors import ApiError
from app.controller import meta_cache, replicas

log = logging.getLogger("controller.internal")
router = APIRouter()

LOW_DISK_FRACTION = 0.05


class WorkerInfo(BaseModel):
    worker_id: str = Field(min_length=1, max_length=64)
    address: str = ""
    capacity_bytes: int = Field(ge=0, default=0)
    used_bytes: int = Field(ge=0, default=0)


def _state_for(info: WorkerInfo) -> str:
    if info.capacity_bytes and (info.capacity_bytes - info.used_bytes) / info.capacity_bytes < LOW_DISK_FRACTION:
        return "DEGRADED"
    return "HEALTHY"


@router.post("/internal/workers/register")
async def register(info: WorkerInfo, request: Request):
    if not info.address:
        raise ApiError(400, "address required")
    async with request.app.state.pool.acquire() as c:
        await c.execute(
            """INSERT INTO workers (worker_id, address, state, last_heartbeat, capacity_bytes, used_bytes)
               VALUES ($1,$2,$3,now(),$4,$5)
               ON CONFLICT (worker_id) DO UPDATE
               SET address=$2, state=$3, last_heartbeat=now(), capacity_bytes=$4, used_bytes=$5""",
            info.worker_id, info.address, _state_for(info), info.capacity_bytes, info.used_bytes,
        )
    meta_cache.invalidate_all()
    log.info("worker registered: %s at %s", info.worker_id, info.address)
    return {"status": "registered", "heartbeat_interval": settings.heartbeat_interval}


@router.post("/internal/workers/heartbeat")
async def heartbeat(info: WorkerInfo, request: Request):
    async with request.app.state.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT state FROM workers WHERE worker_id=$1 FOR UPDATE", info.worker_id)
        if row is None:
            raise ApiError(404, "unknown worker; register first", "unknown_worker")
        new_state = _state_for(info)
        await c.execute(
            """UPDATE workers SET state=$2, last_heartbeat=now(), capacity_bytes=$3, used_bytes=$4
               WHERE worker_id=$1""",
            info.worker_id, new_state, info.capacity_bytes, info.used_bytes,
        )
    if row["state"] != new_state:
        meta_cache.invalidate_all()
    if row["state"] == "DEAD":
        log.warning("worker %s is back (was DEAD) -> %s", info.worker_id, new_state)
    return {"status": "ok"}


@router.get("/internal/workers")
async def list_workers(request: Request):
    async with request.app.state.pool.acquire() as c:
        rows = await c.fetch(
            """SELECT worker_id, address, state, draining, capacity_bytes, used_bytes,
                      EXTRACT(EPOCH FROM (now() - last_heartbeat)) AS heartbeat_age_seconds
               FROM workers ORDER BY worker_id""")
    return {"workers": [
        {**dict(r), "heartbeat_age_seconds": round(float(r["heartbeat_age_seconds"]), 1),
         "free_bytes": r["capacity_bytes"] - r["used_bytes"]} for r in rows]}


# ---------------------------------------------------------------- scrubber reports
class ScrubResult(BaseModel):
    worker_id: str = Field(min_length=1, max_length=64)
    chunk_id: str = Field(min_length=1, max_length=200)
    status: Literal["HEALTHY", "CORRUPT", "MISSING", "UNREADABLE"]
    expected_checksum: str | None = None
    actual_checksum: str | None = None
    checked_at: datetime


class ScrubSummary(BaseModel):
    worker_id: str
    last_scrub_at: datetime
    chunks_checked: int = 0
    bytes_checked: int = 0
    corrupt_found: int = 0
    missing_found: int = 0
    unreadable_found: int = 0
    duration_seconds: float = 0
    passes_completed: int = 0


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@router.post("/internal/workers/scrub-results")
async def scrub_results(res: ScrubResult, request: Request):
    """A worker reports what its scrubber saw. The controller decides what it means.

    Idempotent: the same (worker, chunk, checked_at) is stored once, and replica transitions
    only ever move a HEALTHY replica to CORRUPT/MISSING, so repeats change nothing.
    """
    pool = request.app.state.pool
    if not await pool.fetchval("SELECT 1 FROM workers WHERE worker_id=$1", res.worker_id):
        raise ApiError(404, "unknown worker; register first", "unknown_worker")
    checked_at = _utc(res.checked_at)
    metrics.inc(f"scrub_reports_{res.status.lower()}")
    await pool.execute(
        """INSERT INTO scrub_results (worker_id, chunk_id, status, expected_checksum, actual_checksum, checked_at)
           VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING""",
        res.worker_id, res.chunk_id, res.status, res.expected_checksum, res.actual_checksum, checked_at)

    def done(action: str, reason: str | None = None):
        return {"status": "recorded", "action": action, **({"reason": reason} if reason else {})}

    rep = await pool.fetchrow(
        """SELECT r.state, r.updated_at, c.checksum FROM replicas r JOIN chunks c USING (chunk_id)
           WHERE r.chunk_id=$1 AND r.worker_id=$2""", res.chunk_id, res.worker_id)
    if rep is None:
        return done("ignored", "no such replica")
    if res.status == "HEALTHY":
        if rep["state"] == "HEALTHY" and checked_at >= rep["updated_at"]:
            await pool.execute("UPDATE replicas SET last_verified=$3 WHERE chunk_id=$1 AND worker_id=$2",
                               res.chunk_id, res.worker_id, checked_at)
        return done("verified")
    if rep["state"] != "HEALTHY":
        return done("ignored", f"replica already {rep['state']}")
    if checked_at < rep["updated_at"]:
        return done("ignored", "stale: replica was rewritten after this check")
    if res.status == "CORRUPT" and res.actual_checksum == rep["checksum"]:
        return done("ignored", "bytes match authoritative checksum (worker's local record was wrong)")
    new_state = "MISSING" if res.status == "MISSING" else "CORRUPT"
    detail = f"scrub: {res.status} expected={res.expected_checksum} actual={res.actual_checksum}"
    changed = await replicas.mark_bad(pool, res.chunk_id, res.worker_id, new_state, detail)
    return done("marked_" + new_state.lower() if changed else "ignored")


@router.post("/internal/workers/scrub-summary")
async def scrub_summary(s: ScrubSummary, request: Request):
    pool = request.app.state.pool
    if not await pool.fetchval("SELECT 1 FROM workers WHERE worker_id=$1", s.worker_id):
        raise ApiError(404, "unknown worker; register first", "unknown_worker")
    await pool.execute(
        """INSERT INTO worker_scrub_state (worker_id, last_scrub_at, chunks_checked, corrupt_found, missing_found,
                                           bytes_checked, unreadable_found, duration_seconds, passes_completed)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT (worker_id) DO UPDATE SET last_scrub_at=$2, chunks_checked=$3, corrupt_found=$4,
               missing_found=$5, bytes_checked=$6, unreadable_found=$7, duration_seconds=$8, passes_completed=$9""",
        s.worker_id, _utc(s.last_scrub_at), s.chunks_checked, s.corrupt_found, s.missing_found,
        s.bytes_checked, s.unreadable_found, s.duration_seconds, s.passes_completed)
    return {"status": "recorded"}


@router.get("/internal/chunks/{chunk_id}/expected")
async def expected_checksum(chunk_id: str, request: Request):
    """Authoritative checksum for a chunk (workers use it to judge their own local records)."""
    row = await request.app.state.pool.fetchrow("SELECT checksum, size FROM chunks WHERE chunk_id=$1", chunk_id)
    if row is None:
        raise ApiError(404, "unknown chunk")
    return {"chunk_id": chunk_id, "checksum": row["checksum"], "size": row["size"]}


# ------------------------------------------------------------------ scale-in: drain, then forget
@router.post("/internal/workers/{worker_id}/drain")
async def drain(worker_id: str, request: Request):
    """Stop placing data on the worker and let the rebalancer move its replicas elsewhere (reversible)."""
    pool = request.app.state.pool
    row = await pool.fetchrow("UPDATE workers SET draining=true WHERE worker_id=$1 RETURNING state", worker_id)
    if row is None:
        raise ApiError(404, "unknown worker", "not_found")
    left = await pool.fetchval(
        "SELECT count(*) FROM replicas WHERE worker_id=$1 AND state IN ('HEALTHY','PENDING')", worker_id)
    log.warning("worker %s is draining: %d replica(s) to move", worker_id, left)
    return {"worker_id": worker_id, "draining": True, "state": row["state"], "replicas_left": left}


@router.post("/internal/workers/{worker_id}/undrain")
async def undrain(worker_id: str, request: Request):
    row = await request.app.state.pool.fetchrow(
        "UPDATE workers SET draining=false WHERE worker_id=$1 RETURNING state", worker_id)
    if row is None:
        raise ApiError(404, "unknown worker", "not_found")
    log.info("worker %s is no longer draining", worker_id)
    return {"worker_id": worker_id, "draining": False, "state": row["state"]}


@router.delete("/internal/workers/{worker_id}")
async def forget(worker_id: str, request: Request):
    """Remove a DEAD worker from the cluster for good.

    Refused while the worker is alive (it would simply register again) and while it holds the only healthy copy
    of any chunk. Copies it still has on its own disk are forgotten; if it ever returns, it registers as a new,
    empty worker and those files are removed as orphans.
    """
    pool = request.app.state.pool
    async with pool.acquire() as c:
        async with c.transaction():
            w = await c.fetchrow("SELECT state FROM workers WHERE worker_id=$1 FOR UPDATE", worker_id)
            if w is None:
                raise ApiError(404, "unknown worker", "not_found")
            if w["state"] != "DEAD":
                raise ApiError(409, f"worker is {w['state']}: stop it first (drain it before, to move its data "
                                    "off without any under-replication)", "worker_alive")
            at_risk = await c.fetchval(
                """SELECT count(*) FROM replicas r WHERE r.worker_id=$1 AND r.state='HEALTHY'
                   AND (SELECT count(*) FROM replicas x JOIN workers v USING (worker_id)
                        WHERE x.chunk_id=r.chunk_id AND x.worker_id<>$1 AND x.state='HEALTHY'
                          AND v.state<>'DEAD') < 2""", worker_id)
            if at_risk:
                raise ApiError(409, f"{at_risk} chunk(s) do not yet have 2 healthy copies on other workers; "
                                    "wait for repair to finish", "would_lose_redundancy")
            dropped = await c.fetchval("SELECT count(*) FROM replicas WHERE worker_id=$1", worker_id)
            await c.execute("DELETE FROM replicas WHERE worker_id=$1", worker_id)
            await c.execute("DELETE FROM tombstones WHERE worker_id=$1", worker_id)
            await c.execute("DELETE FROM worker_scrub_state WHERE worker_id=$1", worker_id)
            await c.execute("DELETE FROM workers WHERE worker_id=$1", worker_id)
    meta_cache.invalidate_all()
    log.warning("worker %s removed from the cluster (%d replica record(s) dropped)", worker_id, dropped)
    return {"worker_id": worker_id, "removed": True, "replica_records_dropped": dropped}
