"""Registers the worker with the controller and sends periodic heartbeats."""
import asyncio
import logging

import httpx

from app.common.config import settings
from app.worker import disk

log = logging.getLogger("worker.agent")


async def _payload() -> dict:
    u = await asyncio.to_thread(disk.usage)
    return {
        "worker_id": settings.worker_id,
        "address": settings.worker_address,
        "capacity_bytes": u["capacity_bytes"],
        "used_bytes": u["used_bytes"],
    }


async def _register(client: httpx.AsyncClient) -> None:
    delay = 1.0
    while True:
        try:
            r = await client.post("/internal/workers/register", json=await _payload())
            r.raise_for_status()
            log.info("registered with controller")
            return
        except Exception as e:
            log.warning("register failed (%s); retrying in %.0fs", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)


async def run() -> None:
    async with httpx.AsyncClient(base_url=settings.controller_url, timeout=5.0) as client:
        await _register(client)
        while True:
            await asyncio.sleep(settings.heartbeat_interval)
            try:
                r = await client.post("/internal/workers/heartbeat", json=await _payload())
                if r.status_code == 404:
                    log.warning("controller does not know us; re-registering")
                    await _register(client)
                else:
                    r.raise_for_status()
            except Exception as e:
                log.warning("heartbeat failed: %s", e)
