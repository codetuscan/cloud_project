import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from app.common.config import settings
from app.common.errors import install_error_handlers
from app.common.logging import setup_logging
from app.common.responses import JSONResponseNL
from app.controller import store

log = logging.getLogger("controller")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("controller")
    Path(settings.staging_dir).mkdir(parents=True, exist_ok=True)
    app.state.pool = await store.connect(settings.database_url)
    await store.migrate(app.state.pool)
    log.info("controller ready (chunk_size=%d)", settings.chunk_size)
    yield
    await app.state.pool.close()


app = FastAPI(title="Object Store Controller", lifespan=lifespan, default_response_class=JSONResponseNL)
install_error_handlers(app)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/internal/status")
async def status():
    pool = app.state.pool
    async with pool.acquire() as c:
        await c.fetchval("SELECT 1")
        tables = await c.fetchval(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
        )
    return {"controller": "ok", "database": "ok", "tables": tables, "chunk_size": settings.chunk_size}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.controller_port, log_level="info")
