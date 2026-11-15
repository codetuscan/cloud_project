"""Worker chunk read cache (RAM only). The chunk file on disk is always authoritative:
eviction never touches the file, and every cached entry was checksum-verified when inserted."""
from app.common.config import settings
from app.common.lru import LRUCache

chunk_cache = LRUCache(max_bytes=settings.chunk_cache_bytes, enabled=settings.chunk_cache_enabled)
disk_reads = 0  # chunk reads that had to go to disk (cache misses that were served)


def count_disk_read() -> None:
    global disk_reads
    disk_reads += 1


def stats() -> dict:
    return {**chunk_cache.stats(), "disk_reads": disk_reads}
