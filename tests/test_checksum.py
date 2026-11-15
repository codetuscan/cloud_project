import hashlib

from app.common import checksum as cs


def test_sha256_bytes_format_and_value():
    out = cs.sha256_bytes(b"hello")
    assert out == "sha256:" + hashlib.sha256(b"hello").hexdigest()
    assert cs.valid(out)


def test_fmt_adds_prefix():
    assert cs.fmt("ab" * 32) == "sha256:" + "ab" * 32


def test_valid_rejects_bad_values():
    assert not cs.valid(None)
    assert not cs.valid("")
    assert not cs.valid("sha256:abc")
    assert not cs.valid("md5:" + "a" * 32)
    assert not cs.valid("sha256:" + "A" * 64)      # uppercase hex not allowed
    assert not cs.valid("sha256:" + "g" * 64)
    assert cs.valid("sha256:" + "a" * 64)


def test_different_bytes_different_checksum():
    assert cs.sha256_bytes(b"a") != cs.sha256_bytes(b"b")
