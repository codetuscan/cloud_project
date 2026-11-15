"""Bounded in-process LRU cache with optional TTL and a byte budget.

Used by the controller (metadata: max_entries + ttl) and by workers (chunk bytes: max_bytes).
A cache is only an optimisation; PostgreSQL / the chunk files on disk stay authoritative.

Stale-fill protection: a reader takes `token()` BEFORE loading from the source of truth and passes
it to `put()`. If the key was invalidated (or the cache cleared) in between, the put is rejected,
so a slow reader can never re-insert data that an invalidation just removed.

All operations are synchronous and never await, so with a single asyncio event loop they are
atomic and need no lock.
"""
import time
from collections import OrderedDict


class LRUCache:
    def __init__(self, max_entries: int = 0, max_bytes: int = 0, ttl: float = 0.0, enabled: bool = True):
        self.max_entries, self.max_bytes, self.ttl, self.enabled = max_entries, max_bytes, ttl, enabled
        self._d: OrderedDict[str, tuple[float, int, object]] = OrderedDict()  # key -> (expires, size, value)
        self.bytes = 0
        self._seq = 0
        self._floor = 0
        self._inval: dict[str, int] = {}
        self.hits = self.misses = self.evictions = self.expirations = 0
        self.invalidations = self.puts = self.rejected_puts = self.bytes_served = 0

    # -- reads
    def token(self) -> int:
        return self._seq

    def get(self, key: str):
        if not self.enabled:
            self.misses += 1
            return None
        item = self._d.get(key)
        if item is None:
            self.misses += 1
            return None
        expires, size, value = item
        if self.ttl and time.monotonic() >= expires:
            self._remove(key)
            self.expirations += 1
            self.misses += 1
            return None
        self._d.move_to_end(key)
        self.hits += 1
        self.bytes_served += size
        return value

    # -- writes
    def put(self, key: str, value, size: int = 1, token: int | None = None) -> bool:
        if not self.enabled:
            return False
        if token is not None and (token < self._floor or token < self._inval.get(key, 0)):
            self.rejected_puts += 1
            return False
        if self.max_bytes and size > self.max_bytes:
            self.rejected_puts += 1
            return False
        self._remove(key)
        self._d[key] = (time.monotonic() + self.ttl if self.ttl else 0.0, size, value)
        self.bytes += size
        self.puts += 1
        while (self.max_entries and len(self._d) > self.max_entries) or \
              (self.max_bytes and self.bytes > self.max_bytes):
            oldest = next(iter(self._d))
            self._remove(oldest)
            self.evictions += 1
        return True

    def invalidate(self, key: str) -> None:
        self._seq += 1
        self._inval[key] = self._seq
        if len(self._inval) > 50_000:  # bound the bookkeeping: reject all fills that started earlier
            self._inval.clear()
            self._floor = self._seq
        if self._remove(key):
            self.invalidations += 1

    def clear(self) -> None:
        self._seq += 1
        self._floor = self._seq
        self._inval.clear()
        if self._d:
            self.invalidations += len(self._d)
        self._d.clear()
        self.bytes = 0

    def _remove(self, key: str) -> bool:
        item = self._d.pop(key, None)
        if item is None:
            return False
        self.bytes -= item[1]
        return True

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "enabled": self.enabled, "entries": len(self._d), "max_entries": self.max_entries or None,
            "bytes": self.bytes, "max_bytes": self.max_bytes or None, "ttl_seconds": self.ttl or None,
            "hits": self.hits, "misses": self.misses,
            "hit_ratio": round(self.hits / total, 4) if total else None,
            "evictions": self.evictions, "expirations": self.expirations,
            "invalidations": self.invalidations, "puts": self.puts, "rejected_puts": self.rejected_puts,
            "bytes_served_from_cache": self.bytes_served,
        }
