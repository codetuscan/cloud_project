import pytest

from app.common import checksum as cs
from app.common.config import settings
from app.worker import disk


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_dir", str(tmp_path))
    disk.init()
    return tmp_path


def put(chunk_id: str, data: bytes):
    w = disk.ChunkWriter(chunk_id)
    w.write(data)
    w.commit()
    return w.checksum


def test_write_read_roundtrip_with_checksum():
    sum_ = put("chunk-aaa", b"hello world")
    data, got = disk.read_verified("chunk-aaa")
    assert data == b"hello world" and got == sum_ == cs.sha256_bytes(b"hello world")


def test_streaming_in_pieces_gives_same_checksum():
    w = disk.ChunkWriter("chunk-bbb")
    for piece in (b"ab", b"cd", b"ef"):
        w.write(piece)
    w.commit()
    assert w.checksum == cs.sha256_bytes(b"abcdef") and w.size == 6


def test_chunk_not_visible_until_commit(tmp_path):
    w = disk.ChunkWriter("chunk-ccc")
    w.write(b"partial")
    assert disk.stored_checksum("chunk-ccc") is None
    assert not disk.chunk_path("chunk-ccc").exists()
    w.abort()
    assert list(disk.tmp_dir().iterdir()) == []          # abort leaves nothing behind


def test_init_removes_leftovers_of_interrupted_writes(tmp_path):
    w = disk.ChunkWriter("chunk-ddd")
    w.write(b"x")
    w._f.flush()                                          # simulate a crash: file left in tmp/
    assert disk.init() == 1
    assert list(disk.tmp_dir().iterdir()) == []


def test_corruption_is_detected_on_read():
    put("chunk-eee", b"original bytes")
    p = disk.chunk_path("chunk-eee")
    raw = bytearray(p.read_bytes()); raw[3] ^= 0xFF; p.write_bytes(bytes(raw))
    with pytest.raises(disk.ChunkCorrupt) as e:
        disk.read_verified("chunk-eee")
    assert e.value.expected != e.value.actual


def test_missing_chunk():
    with pytest.raises(disk.ChunkMissing):
        disk.read_verified("chunk-nope")


def test_delete_removes_file_and_checksum_record():
    put("chunk-fff", b"x")
    assert disk.delete("chunk-fff") is True
    assert not disk.chunk_path("chunk-fff").exists() and not disk.sum_path("chunk-fff").exists()
    assert disk.delete("chunk-fff") is False             # idempotent


def test_list_chunks_reports_size_and_checksum():
    put("chunk-ggg", b"12345")
    items = {c["chunk_id"]: c for c in disk.list_chunks()}
    assert items["chunk-ggg"]["size"] == 5
    assert items["chunk-ggg"]["checksum"] == cs.sha256_bytes(b"12345")


def test_scrub_candidates_include_chunk_with_only_checksum_record_left():
    put("chunk-hhh", b"x")
    disk.chunk_path("chunk-hhh").unlink()                 # file vanished, .sum remains
    assert "chunk-hhh" in disk.scrub_candidates()


@pytest.mark.parametrize("bad", ["../etc/passwd", "a/b", "ab", "", ".hidden", "x" * 200, "a b c"])
def test_chunk_id_validation_blocks_path_tricks(bad):
    assert not disk.ID_RE.match(bad)


def test_valid_chunk_ids_accepted():
    assert disk.ID_RE.match("0f8c2c1e-1111-2222-3333-444455556666-000007")
