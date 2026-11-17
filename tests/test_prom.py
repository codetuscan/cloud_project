"""Prometheus text exposition: names, types, labels and cumulative histogram buckets."""
from app.common import metrics
from app.common.gate import Gate
from app.common.prom import Exposition


def lines(text):
    return [l for l in text.splitlines() if l and not l.startswith("#")]


def test_counter_gauge_and_label_escaping():
    e = Exposition()
    e.counter("uploads_committed", 3, "Uploads committed.")
    e.gauge("worker_alive", True, "alive", {"worker_id": 'w"1\\x'})
    e.gauge("skipped", None)  # None = no sample at all
    out = e.render()
    assert "# TYPE objstore_uploads_committed_total counter" in out
    assert "objstore_uploads_committed_total 3" in lines(out)
    assert 'objstore_worker_alive{worker_id="w\\"1\\\\x"} 1' in lines(out)
    assert "objstore_skipped" not in "".join(lines(out))


def test_one_type_line_per_family_even_with_many_label_sets():
    e = Exposition()
    for w in ("worker1", "worker2"):
        e.gauge("worker_alive", 1, "alive", {"worker_id": w})
    out = e.render()
    assert out.count("# TYPE objstore_worker_alive gauge") == 1 and len(lines(out)) == 2


def test_histogram_buckets_are_cumulative_and_end_with_inf():
    h = metrics.Histogram()
    for v in (0.001, 0.3, 0.3, 7.0, 10_000.0):
        h.observe(v)
    e = Exposition()
    e.histogram("upload_duration_seconds", h, "x")
    ls = lines(e.render())
    buckets = [(l.split('le="')[1].split('"')[0], int(l.split()[-1])) for l in ls if "_bucket" in l]
    counts = [n for _, n in buckets]
    assert counts == sorted(counts)                                  # cumulative never decreases
    assert dict(buckets)["0.01"] == 1 and dict(buckets)["0.5"] == 3 and dict(buckets)["10.0"] == 4
    assert buckets[-1] == ("+Inf", 5)
    assert "objstore_upload_duration_seconds_count 5" in ls
    assert any(l.startswith("objstore_upload_duration_seconds_sum 10007.6") for l in ls)


def test_declared_counters_appear_at_zero_and_gate_is_exported():
    metrics.declare("test_declared_counter")
    e = Exposition()
    e.process_counters({})
    e.gate(Gate(3, "uploads"), "controller")
    ls = lines(e.render())
    assert "objstore_test_declared_counter_total 0" in ls
    assert 'objstore_gate_limit{gate="uploads"} 3' in ls
    assert 'objstore_gate_rejected_total{gate="uploads"} 0' in ls
