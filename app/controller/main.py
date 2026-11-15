import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI

from app.common.config import settings
from app.common import metrics
from app.common.errors import install_error_handlers
from app.common.gate import Gate
from app.common.requestid import RequestIdMiddleware
from app.common.logging import setup_logging
from app.common.responses import JSONResponseNL
from app.controller import health, meta_cache, reaper, repair, store, upload
from app.controller.api import router as api_router
from app.controller.internal_api import router as internal_router

log = logging.getLogger("controller")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("controller")
    Path(settings.staging_dir).mkdir(parents=True, exist_ok=True)
    app.state.pool = await store.connect(settings.database_url)
    await store.migrate(app.state.pool)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    app.state.transfer_sem = Gate(settings.max_concurrent_transfers, "transfers")
    app.state.upload_gate = Gate(settings.max_concurrent_uploads, "uploads")
    app.state.download_gate = Gate(settings.max_concurrent_downloads, "downloads")
    app.state.active_uploads = set()
    app.state.orphans = {}
    junk = upload.cleanup_staging(settings.staging_dir)
    log.info("controller ready (chunk_size=%d, partial uploads removed=%d)", settings.chunk_size, junk)
    resumer = asyncio.create_task(upload.resume_loop(app.state))
    reap = asyncio.create_task(reaper.run(app.state))
    bg = [asyncio.create_task(repair.scanner(app.state)), asyncio.create_task(repair.reconciler(app.state))]
    bg += [asyncio.create_task(repair.runner(app.state, i)) for i in range(settings.repair_concurrency)]
    monitor = asyncio.create_task(
        health.monitor(app.state.pool, settings.heartbeat_timeout, max(1.0, settings.heartbeat_interval / 2)))
    yield
    monitor.cancel()
    resumer.cancel()
    reap.cancel()
    for t in bg:
        t.cancel()
    await app.state.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="Object Store Controller", lifespan=lifespan, default_response_class=JSONResponseNL)
install_error_handlers(app)
app.add_middleware(RequestIdMiddleware)
app.include_router(internal_router)
app.include_router(api_router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/internal/status")
async def status(detail: bool = False):
    pool = app.state.pool
    async with pool.acquire() as c:
        await c.fetchval("SELECT 1")
        workers = await c.fetch(
            """SELECT w.worker_id, w.state, w.capacity_bytes, w.used_bytes,
                      round(EXTRACT(EPOCH FROM (now()-w.last_heartbeat))::numeric,1) AS heartbeat_age_seconds,
                      s.last_scrub_at, s.chunks_checked, s.bytes_checked, s.corrupt_found, s.missing_found,
                      s.unreadable_found, s.duration_seconds, s.passes_completed
               FROM workers w LEFT JOIN worker_scrub_state s USING (worker_id) ORDER BY w.worker_id""")
        objs = await c.fetch("SELECT state, count(*) n FROM objects GROUP BY state")
        nchunks = await c.fetchval("SELECT count(*) FROM chunks c JOIN objects o USING(object_id) WHERE o.state='COMMITTED'")
        reps = await c.fetch("SELECT state, count(*) n FROM replicas GROUP BY state")
        tasks = await c.fetch("SELECT state, count(*) n FROM repair_tasks GROUP BY state")
        under = await c.fetchval(
            """SELECT count(*) FROM chunks c JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED'
               WHERE (SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
                      WHERE r.chunk_id=c.chunk_id AND r.state='HEALTHY' AND w.state<>'DEAD') < 2""")
    worker_detail = None
    if detail:  # ask each live worker for its own view (cache, scrub, queue) with a short timeout
        async def one(w):
            try:
                r = await app.state.http.get(f"{w['address']}/internal/status", timeout=2.0)
                return w["worker_id"], r.json()
            except Exception as e:
                return w["worker_id"], {"error": repr(e)}
        live = await pool.fetch("SELECT worker_id, address FROM workers WHERE state <> 'DEAD'")
        worker_detail = dict(await asyncio.gather(*[one(w) for w in live]))
    return {
        "controller": "ok", "database": "ok", "chunk_size": settings.chunk_size,
        "metadata_cache": meta_cache.cache.stats(),
        "gates": {g.name: g.stats() for g in (app.state.transfer_sem, app.state.upload_gate, app.state.download_gate)},
        "active_uploads": len(app.state.active_uploads),
        **({"worker_detail": worker_detail} if worker_detail is not None else {}),
        "workers": [{
            "worker_id": w["worker_id"], "state": w["state"], "capacity_bytes": w["capacity_bytes"],
            "used_bytes": w["used_bytes"], "free_bytes": w["capacity_bytes"] - w["used_bytes"],
            "heartbeat_age_seconds": float(w["heartbeat_age_seconds"]),
            "last_scrub": None if w["last_scrub_at"] is None else {
                "at": w["last_scrub_at"].isoformat(), "passes": w["passes_completed"],
                "chunks_checked": w["chunks_checked"], "bytes_checked": w["bytes_checked"],
                "corrupt_found": w["corrupt_found"], "missing_found": w["missing_found"],
                "unreadable_found": w["unreadable_found"], "duration_seconds": w["duration_seconds"]},
        } for w in workers],
        "objects": {r["state"]: r["n"] for r in objs},
        "committed_chunks": nchunks,
        "replicas": {r["state"]: r["n"] for r in reps},
        "under_replicated_chunks": under,
        "repair_tasks": {r["state"]: r["n"] for r in tasks},
    }


@app.get("/internal/metrics")
async def get_metrics():
    return {
        **metrics.snapshot(),
        "gates": {g.name: g.stats() for g in (app.state.transfer_sem, app.state.upload_gate, app.state.download_gate)},
        "metadata_cache": meta_cache.cache.stats(),
        "active_uploads": len(app.state.active_uploads),
        "repair_runners": settings.repair_concurrency,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.controller_port, log_level="info",
                limit_concurrency=settings.max_http_concurrency, timeout_keep_alive=5)
