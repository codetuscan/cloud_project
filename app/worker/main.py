import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from app.common.config import settings
from app.common.errors import install_error_handlers
from app.common.logging import setup_logging
from app.common.responses import JSONResponseNL
from app.worker import agent, disk
from app.worker.api import router

log = logging.getLogger("worker")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(settings.worker_id)
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
    removed = disk.init()
    log.info("worker %s ready (data_dir=%s, stale tmp files removed=%d, chunks on disk=%d)",
             settings.worker_id, settings.data_dir, removed, len(disk.list_chunks()))
    task = asyncio.create_task(agent.run())
    yield
    task.cancel()


app = FastAPI(title="Object Store Worker", lifespan=lifespan, default_response_class=JSONResponseNL)
install_error_handlers(app)
app.include_router(router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "worker_id": settings.worker_id}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.worker_port, log_level="info")
