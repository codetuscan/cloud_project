"""Worker-facing endpoints on the controller: registration, heartbeat, worker listing."""
import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.common.config import settings
from app.common.errors import ApiError

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
    if row["state"] == "DEAD":
        log.warning("worker %s is back (was DEAD) -> %s", info.worker_id, new_state)
    return {"status": "ok"}


@router.get("/internal/workers")
async def list_workers(request: Request):
    async with request.app.state.pool.acquire() as c:
        rows = await c.fetch(
            """SELECT worker_id, address, state, capacity_bytes, used_bytes,
                      EXTRACT(EPOCH FROM (now() - last_heartbeat)) AS heartbeat_age_seconds
               FROM workers ORDER BY worker_id""")
    return {"workers": [
        {**dict(r), "heartbeat_age_seconds": round(float(r["heartbeat_age_seconds"]), 1),
         "free_bytes": r["capacity_bytes"] - r["used_bytes"]} for r in rows]}
