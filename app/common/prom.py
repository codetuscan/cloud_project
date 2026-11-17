"""Prometheus text exposition format 0.0.4, written by hand (no client library needed).

    # HELP objstore_uploads_committed_total Uploads that reached COMMITTED.
    # TYPE objstore_uploads_committed_total counter
    objstore_uploads_committed_total 12
"""
import math
import time

from app.common import metrics

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
PREFIX = "objstore_"


def _esc(v) -> str:
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _num(v) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    v = float(v)
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "+Inf" if v > 0 else "-Inf"
    return str(int(v)) if v.is_integer() and abs(v) < 2 ** 53 else repr(v)


def _labels(labels: dict | None) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_esc(v)}"' for k, v in labels.items()) + "}"


class Exposition:
    """Collects samples grouped by metric family, then renders them in one go."""

    def __init__(self):
        self._fams: dict[str, list] = {}  # name -> [type, help, [sample lines]]

    def _fam(self, name: str, typ: str, help_: str) -> list:
        return self._fams.setdefault(name, [typ, help_, []])

    def gauge(self, name: str, value, help_: str = "", labels: dict | None = None) -> None:
        if value is not None:
            self._fam(PREFIX + name, "gauge", help_)[2].append(f"{PREFIX}{name}{_labels(labels)} {_num(value)}")

    def counter(self, name: str, value, help_: str = "", labels: dict | None = None) -> None:
        full = f"{PREFIX}{name}_total"
        if value is not None:
            self._fam(full, "counter", help_)[2].append(f"{full}{_labels(labels)} {_num(value)}")

    def histogram(self, name: str, h: metrics.Histogram, help_: str = "") -> None:
        full = PREFIX + name
        lines = self._fam(full, "histogram", help_)[2]
        for le, n in h.cumulative():
            lines.append(f'{full}_bucket{{le="{le}"}} {n}')
        lines.append(f"{full}_sum {_num(h.sum)}")
        lines.append(f"{full}_count {h.count}")

    def process_counters(self, helps: dict[str, str]) -> None:
        """Every in-process counter (app/common/metrics.py) becomes objstore_<name>_total, plus histograms."""
        self.gauge("uptime_seconds", time.time() - metrics.STARTED, "Seconds since this process started.")
        for name, v in metrics.counters().items():
            self.counter(name, v, helps.get(name, name.replace("_", " ")))
        for name, h in metrics.histograms().items():
            self.histogram(name, h, helps.get(name, name.replace("_", " ")))

    def gate(self, g, help_scope: str) -> None:
        """A Gate (bounded concurrency) as gauges + counters labelled gate=<name>."""
        lab = {"gate": g.name}
        s = g.stats()
        self.gauge("gate_limit", s["limit"], f"Maximum concurrent {help_scope} operations per gate.", lab)
        self.gauge("gate_inflight", s["inflight"], f"{help_scope.capitalize()} operations currently holding a slot.", lab)
        self.gauge("gate_waiting", s["waiting"], f"{help_scope.capitalize()} operations queued for a slot.", lab)
        self.gauge("gate_peak_inflight", s["peak_inflight"], "Highest in-flight count since start.", lab)
        self.counter("gate_admitted", s["admitted"], "Operations admitted through the gate.", lab)
        self.counter("gate_rejected", s["rejected"], "Operations rejected (503 server_busy) after waiting.", lab)

    def cache(self, s: dict, name: str) -> None:
        """An LRUCache.stats() dict, labelled cache=<name>."""
        lab = {"cache": name}
        self.gauge("cache_enabled", bool(s["enabled"]), "1 if the cache is enabled.", lab)
        self.gauge("cache_entries", s["entries"], "Entries currently cached.", lab)
        self.gauge("cache_bytes", s["bytes"], "Bytes currently cached.", lab)
        self.gauge("cache_max_bytes", s["max_bytes"], "Cache byte budget.", lab)
        for k, h in (("hits", "Cache hits."), ("misses", "Cache misses."), ("evictions", "Entries evicted (LRU)."),
                     ("expirations", "Entries dropped by TTL."), ("invalidations", "Entries invalidated."),
                     ("bytes_served_from_cache", "Bytes served from RAM instead of disk/database.")):
            self.counter(f"cache_{k}", s[k], h, lab)

    def render(self) -> str:
        out = []
        for name, (typ, help_, lines) in self._fams.items():
            if help_:
                out.append(f"# HELP {name} " + help_.replace("\\", "\\\\").replace("\n", "\\n"))
            out.append(f"# TYPE {name} {typ}")
            out.extend(lines)
        return "\n".join(out) + "\n"
