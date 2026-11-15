from app.common import lru as lru_mod
from app.common.lru import LRUCache


def test_hit_miss_counters():
    c = LRUCache(max_entries=10)
    assert c.get("a") is None
    c.put("a", 1)
    assert c.get("a") == 1
    s = c.stats()
    assert (s["hits"], s["misses"], s["entries"]) == (1, 1, 1)


def test_entry_bound_evicts_least_recently_used():
    c = LRUCache(max_entries=2)
    c.put("a", 1); c.put("b", 2)
    c.get("a")                      # a becomes most recent
    c.put("c", 3)                   # evicts b
    assert c.get("b") is None
    assert c.get("a") == 1 and c.get("c") == 3
    assert c.stats()["evictions"] == 1


def test_byte_budget_evicts_and_tracks_bytes():
    c = LRUCache(max_bytes=100)
    c.put("a", b"x" * 60, size=60)
    c.put("b", b"y" * 60, size=60)  # 120 > 100 -> a evicted
    assert c.get("a") is None and c.get("b") is not None
    assert c.bytes == 60


def test_item_larger_than_budget_is_rejected():
    c = LRUCache(max_bytes=10)
    assert not c.put("big", b"z" * 50, size=50)
    assert c.stats()["rejected_puts"] == 1 and c.bytes == 0


def test_replacing_a_key_does_not_leak_bytes():
    c = LRUCache(max_bytes=100)
    c.put("a", b"1", size=40); c.put("a", b"2", size=30)
    assert c.bytes == 30 and c.get("a") == b"2"


def test_ttl_expiry(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(lru_mod.time, "monotonic", lambda: now[0])
    c = LRUCache(max_entries=5, ttl=30)
    c.put("a", 1)
    now[0] += 29
    assert c.get("a") == 1
    now[0] += 2
    assert c.get("a") is None
    assert c.stats()["expirations"] == 1


def test_invalidate_and_clear():
    c = LRUCache(max_entries=5)
    c.put("a", 1); c.put("b", 2)
    c.invalidate("a")
    assert c.get("a") is None and c.get("b") == 2
    c.clear()
    assert c.get("b") is None and c.stats()["entries"] == 0


def test_stale_fill_is_rejected_after_invalidation():
    """A reader that started before an invalidation must not re-insert its (stale) data."""
    c = LRUCache(max_entries=5)
    token = c.token()               # reader starts, will read the DB
    c.invalidate("k")               # ...a writer changes the data and invalidates meanwhile
    assert not c.put("k", "stale", token=token)
    assert c.get("k") is None
    assert c.put("k", "fresh", token=c.token())   # a reader that started after is fine


def test_stale_fill_is_rejected_after_clear():
    c = LRUCache(max_entries=5)
    token = c.token()
    c.clear()
    assert not c.put("k", "stale", token=token)


def test_unrelated_invalidation_does_not_block_fill():
    c = LRUCache(max_entries=5)
    token = c.token()
    c.invalidate("other")
    assert c.put("k", "v", token=token)


def test_disabled_cache_never_stores():
    c = LRUCache(max_entries=5, enabled=False)
    assert not c.put("a", 1)
    assert c.get("a") is None
    assert c.stats()["hits"] == 0
