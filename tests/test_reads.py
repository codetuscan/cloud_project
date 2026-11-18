"""Download read path: hedged chunk reads, read-ahead, Range parsing."""
import asyncio
import time
from types import SimpleNamespace

import httpx
import pytest

from app.common import checksum as cs
from app.common import metrics
from app.common.config import settings
from app.common.gate import Gate
from app.controller import reads

DATA = b"chunk-bytes" * 100
CHUNK = {"chunk_id": "obj-000000", "size": len(DATA), "checksum": cs.sha256_bytes(DATA)}
W1 = {"worker_id": "worker1", "address": "http://worker1:9000"}
W2 = {"worker_id": "worker2", "address": "http://worker2:9000"}


class FakeHttp:
    """Per-worker behaviour: (delay seconds, status, body)."""

    def __init__(self, plan):
        self.plan, self.calls, self.cancelled = plan, [], []

    async def get(self, url):
        wid = url.split("//")[1].split(":")[0]
        self.calls.append(wid)
        delay, status, body = self.plan[wid]
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            self.cancelled.append(wid)
            raise
        if isinstance(status, Exception):
            raise status
        return httpx.Response(status, content=body, request=httpx.Request("GET", url))


@pytest.fixture
def env(monkeypatch):
    marks = []

    async def mark_bad(pool, chunk_id, worker_id, state, reason):
        marks.append((worker_id, state))
        return True
    monkeypatch.setattr(reads.replicas, "mark_bad", mark_bad)
    monkeypatch.setattr(settings, "read_hedge_enabled", True)
    monkeypatch.setattr(settings, "read_hedge_delay", 0.05)       # fixed delay for deterministic tests
    monkeypatch.setattr(reads, "tracker", reads.LatencyTracker())
    for k in ("read_hedges", "read_hedge_wins", "read_hedges_skipped_busy", "replica_read_fallbacks"):
        metrics.declare(k)
    base = {k: metrics.counters().get(k, 0) for k in ("read_hedges", "read_hedge_wins",
                                                       "read_hedges_skipped_busy", "replica_read_fallbacks")}

    def make(plan, gate_limit=8):
        st = SimpleNamespace(http=FakeHttp(plan), transfer_sem=Gate(gate_limit, "t"), pool=None)
        return st

    def delta(k):
        return metrics.counters().get(k, 0) - base[k]
    return SimpleNamespace(make=make, marks=marks, delta=delta)


# ------------------------------------------------------------------ hedging
async def test_fast_first_replica_sends_no_hedge(env):
    st = env.make({"worker1": (0.0, 200, DATA), "worker2": (0.0, 200, DATA)})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert st.http.calls == ["worker1"] and env.delta("read_hedges") == 0


async def test_slow_replica_is_hedged_and_the_fast_one_wins(env):
    st = env.make({"worker1": (5.0, 200, DATA), "worker2": (0.0, 200, DATA)})
    t0 = time.monotonic()
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert time.monotonic() - t0 < 1.0                 # did not wait for the 5 s replica
    assert st.http.calls == ["worker1", "worker2"]
    assert env.delta("read_hedges") == 1 and env.delta("read_hedge_wins") == 1
    await asyncio.sleep(0)
    assert st.http.cancelled == ["worker1"]            # the slow request is cancelled, not left running
    assert env.marks == []                             # slow is not bad: nothing is marked


async def test_slow_primary_that_answers_first_still_wins(env):
    # hedge sent, but the hedge is even slower: the primary's answer is used
    st = env.make({"worker1": (0.1, 200, DATA), "worker2": (5.0, 200, DATA)})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert env.delta("read_hedges") == 1 and env.delta("read_hedge_wins") == 0


async def test_failed_replica_falls_back_immediately_without_waiting_for_the_hedge_delay(env):
    st = env.make({"worker1": (0.0, httpx.ConnectError("down"), b""), "worker2": (0.0, 200, DATA)})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert env.delta("replica_read_fallbacks") == 1 and env.delta("read_hedges") == 0


async def test_corrupt_bytes_are_never_returned_and_the_replica_is_marked(env):
    st = env.make({"worker1": (0.0, 200, b"X" + DATA[1:]), "worker2": (0.0, 200, DATA)})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert env.marks == [("worker1", "CORRUPT")]


async def test_slow_corrupt_hedge_does_not_win_over_good_data(env):
    st = env.make({"worker1": (0.15, 200, DATA), "worker2": (0.0, 200, b"bad")})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert env.marks == [("worker2", "CORRUPT")]


async def test_missing_on_both_raises(env):
    st = env.make({"worker1": (0.0, 404, b""), "worker2": (0.0, 404, b"")})
    with pytest.raises(RuntimeError):
        await reads.fetch_chunk(st, CHUNK, [W1, W2])
    assert sorted(env.marks) == [("worker1", "MISSING"), ("worker2", "MISSING")]


async def test_no_hedge_when_the_transfer_gate_is_full(env):
    st = env.make({"worker1": (0.2, 200, DATA), "worker2": (0.0, 200, DATA)}, gate_limit=1)
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert st.http.calls == ["worker1"]
    assert env.delta("read_hedges") == 0 and env.delta("read_hedges_skipped_busy") == 1


async def test_hedging_can_be_disabled(env, monkeypatch):
    monkeypatch.setattr(settings, "read_hedge_enabled", False)
    st = env.make({"worker1": (0.2, 200, DATA), "worker2": (0.0, 200, DATA)})
    assert await reads.fetch_chunk(st, CHUNK, [W1, W2]) == DATA
    assert st.http.calls == ["worker1"]


def test_adaptive_delay_is_the_p95_clamped(monkeypatch):
    monkeypatch.setattr(settings, "read_hedge_delay", 0.0)
    monkeypatch.setattr(settings, "read_hedge_min_delay", 0.02)
    monkeypatch.setattr(settings, "read_hedge_max_delay", 1.0)
    t = reads.LatencyTracker(size=100, min_samples=20)
    monkeypatch.setattr(reads, "tracker", t)
    assert reads.hedge_delay() == 0.1                   # no history yet: default
    for i in range(100):
        t.add(0.030 if i < 95 else 0.5)                 # 95 fast reads, 5 very slow ones
    t._cached = None
    assert reads.hedge_delay() == pytest.approx(0.5)    # p95 is the edge of the slow tail
    for _ in range(100):
        t.add(0.001)
    t._cached = None
    assert reads.hedge_delay() == 0.02                  # clamped to the minimum


# ------------------------------------------------------------------ read-ahead
async def test_read_ahead_keeps_order_and_overlaps_fetches():
    started, active, peak = [], 0, 0

    async def fetch(i):
        nonlocal active, peak
        started.append(i)
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05 if i % 2 == 0 else 0.01)   # out-of-order completion
        active -= 1
        return i

    t0 = time.monotonic()
    out = [x async for x in reads.stream_chunks(fetch, list(range(8)), ahead=2)]
    assert out == list(range(8))
    assert peak == 3                                        # bounded: current + 2 ahead
    assert time.monotonic() - t0 < 8 * 0.05                 # faster than one-at-a-time


async def test_read_ahead_cancels_prefetches_when_the_client_goes_away():
    finished, started = [], []

    async def fetch(i):
        started.append(i)
        await asyncio.sleep(0.01 if i == 0 else 0.2)
        finished.append(i)
        return i

    gen = reads.stream_chunks(fetch, list(range(5)), ahead=2)
    assert await gen.__anext__() == 0
    await gen.aclose()
    await asyncio.sleep(0.3)
    assert finished == [0]                       # every prefetch was cancelled, none ran to completion
    assert 4 not in started                      # and nothing beyond the read-ahead window was started


async def test_read_ahead_failure_surfaces_in_order():
    async def fetch(i):
        if i == 2:
            raise RuntimeError("no readable replica")
        return i
    got = []
    with pytest.raises(RuntimeError):
        async for x in reads.stream_chunks(fetch, list(range(5)), ahead=3):
            got.append(x)
    assert got == [0, 1]                                    # everything before the failure, nothing after


# ------------------------------------------------------------------ Range
@pytest.mark.parametrize("header,expected", [
    (None, None),
    ("bytes=0-99", (0, 99)),
    ("bytes=100-", (100, 999)),
    ("bytes=-100", (900, 999)),
    ("bytes=-5000", (0, 999)),          # suffix longer than the object = whole object
    ("bytes=990-5000", (990, 999)),     # end clamped to the object
    ("bytes=0-0", (0, 0)),
    ("bytes=5-2", None),                # invalid: ignored, whole object
    ("bytes=0-1,5-6", None),            # multi-range: ignored, whole object
    ("items=0-1", None),
    ("bytes=abc", None),
])
def test_parse_range(header, expected):
    assert reads.parse_range(header, 1000) == expected


@pytest.mark.parametrize("header", ["bytes=1000-", "bytes=5000-6000", "bytes=-0"])
def test_unsatisfiable_range(header):
    with pytest.raises(reads.RangeNotSatisfiable):
        reads.parse_range(header, 1000)


def test_chunk_span():
    MiB = 1048576
    assert reads.chunk_span(0, MiB - 1, MiB) == (0, 0)
    assert reads.chunk_span(MiB - 1, MiB, MiB) == (0, 1)
    assert reads.chunk_span(3 * MiB + 5, 3 * MiB + 9, MiB) == (3, 3)
