"""Tiny in-process counters and latency histograms (reset on restart).

Exposed as JSON by /internal/metrics and in Prometheus text format by /metrics (see app/common/prom.py).
A counter reset on restart is fine for Prometheus: rate()/increase() detect resets.
"""
import bisect
import time
from collections import Counter

_counters: Counter = Counter()
STARTED = time.time()

# seconds; wide enough for a 4 KiB object (ms) up to a multi-GiB upload or a slow repair (minutes)
BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0)


class Histogram:
    def __init__(self):
        self.counts = [0] * (len(BUCKETS) + 1)  # last slot = +Inf
        self.sum = 0.0
        self.count = 0

    def observe(self, v: float) -> None:
        self.counts[bisect.bisect_left(BUCKETS, v)] += 1
        self.sum += v
        self.count += 1

    def cumulative(self) -> list[tuple[str, int]]:
        out, run = [], 0
        for le, n in zip([*map(str, BUCKETS), "+Inf"], self.counts):
            run += n
            out.append((le, run))
        return out


_hists: dict[str, Histogram] = {}


def inc(name: str, n: int = 1) -> None:
    _counters[name] += n


def declare(*names: str) -> None:
    """Make counters exist at 0, so dashboards show 0 instead of "no data" before the first event."""
    for n in names:
        _counters[n] += 0


def observe(name: str, seconds: float) -> None:
    h = _hists.get(name)
    if h is None:
        h = _hists[name] = Histogram()
    h.observe(seconds)


def declare_histogram(*names: str) -> None:
    for n in names:
        _hists.setdefault(n, Histogram())


def counters() -> dict:
    return dict(sorted(_counters.items()))


def histograms() -> dict[str, Histogram]:
    return dict(_hists)


def snapshot() -> dict:
    return {"uptime_seconds": round(time.time() - STARTED, 1), "counters": counters(),
            "histograms": {k: {"count": h.count, "sum_seconds": round(h.sum, 3)} for k, h in sorted(_hists.items())}}
