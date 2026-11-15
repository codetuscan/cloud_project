"""Tiny in-process counters exposed by /internal/metrics (reset on restart)."""
import time
from collections import Counter

_counters: Counter = Counter()
STARTED = time.time()


def inc(name: str, n: int = 1) -> None:
    _counters[name] += n


def snapshot() -> dict:
    return {"uptime_seconds": round(time.time() - STARTED, 1), "counters": dict(sorted(_counters.items()))}
