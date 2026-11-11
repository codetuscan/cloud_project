"""PostgreSQL access: connection pool + plain-SQL migrations."""
import asyncio
import logging
from pathlib import Path

import asyncpg

log = logging.getLogger("store")
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


async def connect(dsn: str, attempts: int = 30) -> asyncpg.Pool:
    for i in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        except (OSError, asyncpg.PostgresError) as e:
            log.warning("postgres not ready (%s/%s): %s", i, attempts, e)
            await asyncio.sleep(1)
    raise RuntimeError("could not connect to postgres")


async def migrate(pool: asyncpg.Pool) -> list[str]:
    applied: list[str] = []
    async with pool.acquire() as conn:
        # advisory lock so concurrent starts can't double-apply
        await conn.execute("SELECT pg_advisory_lock(727274)")
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
            for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
                if f.name in done:
                    continue
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute("INSERT INTO schema_migrations(name) VALUES($1)", f.name)
                log.info("applied migration %s", f.name)
                applied.append(f.name)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(727274)")
    return applied
