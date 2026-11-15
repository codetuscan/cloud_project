"""Controller metadata cache: object manifest + chunk->worker replica map, per object.

Bounded LRU with a short TTL. PostgreSQL stays authoritative; entries are invalidated on object
commit, metadata PATCH, delete, any replica health change, repair completion and any worker state
change. Only COMMITTED objects are cached (so a miss for an unknown/deleted object always hits the DB).
"""
import uuid

from app.common.config import settings
from app.common.lru import LRUCache

cache = LRUCache(max_entries=settings.meta_cache_size, ttl=settings.meta_cache_ttl,
                 enabled=settings.meta_cache_enabled)


async def get_manifest(pool, oid: uuid.UUID) -> dict | None:
    """{"obj": {...object columns...}, "chunks": [{..., "replicas": [{worker_id,state,address,wstate}]}]}"""
    key = str(oid)
    hit = cache.get(key)
    if hit is not None:
        return hit
    token = cache.token()  # take BEFORE reading the database (stale-fill protection)
    obj = await pool.fetchrow("SELECT * FROM objects WHERE object_id=$1 AND state='COMMITTED'", oid)
    if obj is None:
        return None
    chunks = await pool.fetch(
        "SELECT chunk_id, chunk_index, size, checksum FROM chunks WHERE object_id=$1 ORDER BY chunk_index", oid)
    reps = await pool.fetch(
        """SELECT r.chunk_id, r.worker_id, r.state, w.address, w.state AS wstate
           FROM replicas r JOIN workers w USING (worker_id)
           WHERE r.chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=$1) ORDER BY r.worker_id""", oid)
    by_chunk: dict[str, list[dict]] = {}
    for r in reps:
        by_chunk.setdefault(r["chunk_id"], []).append(
            {"worker_id": r["worker_id"], "state": r["state"], "address": r["address"], "wstate": r["wstate"]})
    manifest = {"obj": dict(obj),
                "chunks": [{**dict(c), "replicas": by_chunk.get(c["chunk_id"], [])} for c in chunks]}
    cache.put(key, manifest, token=token)
    return manifest


def invalidate_object(oid) -> None:
    cache.invalidate(str(oid))


def invalidate_chunk(chunk_id: str) -> None:
    """Chunk ids are '<object_id>-<index>' (see upload.chunk_id_for), so the owner is derivable."""
    cache.invalidate(chunk_id.rsplit("-", 1)[0])


def invalidate_all() -> None:
    cache.clear()
