import pytest

from app.common import checksum as cs
from app.common.config import settings
from app.worker import disk, scrub


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_dir", str(tmp_path))
    monkeypatch.setattr(settings, "scrub_rate_mb_s", 0)          # no throttling in tests
    monkeypatch.setattr(settings, "scrub_missing_min_age", 0)
    disk.init()


def put(cid, data):
    w = disk.ChunkWriter(cid); w.write(data); w.commit()


def test_healthy_chunk():
    put("chunk-s01", b"a" * 5000)
    r = scrub.check_local("chunk-s01")
    assert r["status"] == "HEALTHY" and r["bytes"] == 5000


def test_flipped_byte_is_reported_as_mismatch_with_both_checksums():
    put("chunk-s02", b"a" * 5000)
    p = disk.chunk_path("chunk-s02"); raw = bytearray(p.read_bytes()); raw[100] ^= 1; p.write_bytes(bytes(raw))
    r = scrub.check_local("chunk-s02")
    assert r["status"] == "MISMATCH"
    assert r["expected"] == cs.sha256_bytes(b"a" * 5000) and r["actual"] != r["expected"]


def test_deleted_file_with_surviving_record_is_missing():
    put("chunk-s03", b"x")
    disk.chunk_path("chunk-s03").unlink()
    assert scrub.check_local("chunk-s03")["status"] == "MISSING"


def test_young_checksum_record_without_file_is_not_flagged(monkeypatch):
    monkeypatch.setattr(settings, "scrub_missing_min_age", 3600)  # an in-flight write, not a loss
    put("chunk-s04", b"x")
    disk.chunk_path("chunk-s04").unlink()
    assert scrub.check_local("chunk-s04") is None


def test_file_without_record_cannot_be_judged_locally():
    disk.chunk_path("chunk-s05").parent.mkdir(parents=True, exist_ok=True)
    disk.chunk_path("chunk-s05").write_bytes(b"data")
    assert scrub.check_local("chunk-s05")["status"] == "NOSUM"


def test_report_queue_is_durable_and_deduplicated(tmp_path):
    scrub._queue_dir().mkdir(parents=True, exist_ok=True)
    scrub.enqueue("chunk-x__CORRUPT", "/internal/workers/scrub-results", {"n": 1})
    scrub.enqueue("chunk-x__CORRUPT", "/internal/workers/scrub-results", {"n": 2})   # same report again
    assert scrub.queue_depth() == 1
    import json
    item = json.loads(next(scrub._queue_dir().glob("*.json")).read_text())
    assert item["payload"]["n"] == 2


# ------------------------------------------------------------------ schedule (wall clock, survives restarts)
def _finished(monkeypatch, epoch):
    from datetime import datetime, timezone
    iso = datetime.fromtimestamp(epoch, timezone.utc).isoformat() if epoch is not None else None
    monkeypatch.setitem(scrub.state, "last_scrub_finished", iso)
    monkeypatch.setattr(settings, "scrub_interval", 1800)


def test_never_scrubbed_worker_starts_at_its_staggered_offset(monkeypatch):
    _finished(monkeypatch, None)
    assert scrub.first_due(10_000, offset=700) == 10_700


def test_restart_keeps_the_schedule_of_the_last_pass(monkeypatch):
    _finished(monkeypatch, 10_000 - 600)                      # last pass 10 min ago
    assert scrub.first_due(10_000, offset=700) == 10_000 - 600 + 1800   # due in 20 min, not 700 s + 30 min


def test_overdue_pass_runs_within_a_minute_after_restart_or_suspend(monkeypatch):
    _finished(monkeypatch, 10_000 - 5000)                     # host slept / worker was down for longer
    due = scrub.first_due(10_000, offset=700)
    assert 10_000 <= due < 10_060 and due == 10_000 + 700 % 60
