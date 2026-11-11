"""Durable chunk storage on the worker's persistent volume.

Layout under DATA_DIR:
  tmp/                      in-flight uploads (wiped on startup)
  chunks/<id[:2]>/<id>      chunk bytes (immutable)
  chunks/<id[:2]>/<id>.sum  expected checksum, written before the chunk is renamed in
"""
import hashlib
import os
import re
import shutil
import uuid
from pathlib import Path

from app.common import checksum as cs
from app.common.config import settings

ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$")


class ChunkMissing(Exception):
    pass


class ChunkCorrupt(Exception):
    def __init__(self, expected: str, actual: str):
        super().__init__(f"expected {expected} got {actual}")
        self.expected, self.actual = expected, actual


def _root() -> Path:
    return Path(settings.data_dir)


def tmp_dir() -> Path:
    return _root() / "tmp"


def chunk_path(chunk_id: str) -> Path:
    return _root() / "chunks" / chunk_id[:2] / chunk_id


def sum_path(chunk_id: str) -> Path:
    p = chunk_path(chunk_id)
    return p.with_name(p.name + ".sum")


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def init() -> int:
    """Create dirs and drop leftovers from interrupted writes. Returns #temp files removed."""
    (_root() / "chunks").mkdir(parents=True, exist_ok=True)
    t = tmp_dir()
    removed = 0
    if t.exists():
        for f in t.iterdir():
            f.unlink(missing_ok=True)
            removed += 1
    t.mkdir(parents=True, exist_ok=True)
    return removed


def stored_checksum(chunk_id: str) -> str | None:
    """Recorded checksum if the chunk file exists, else None."""
    if not chunk_path(chunk_id).exists():
        return None
    try:
        return sum_path(chunk_id).read_text().strip() or None
    except FileNotFoundError:
        return None


class ChunkWriter:
    """Streams bytes to a temp file while hashing; commit() makes it durable + visible atomically."""

    def __init__(self, chunk_id: str):
        self.chunk_id = chunk_id
        self.path = tmp_dir() / f"{chunk_id}.{uuid.uuid4().hex}.part"
        self._f = open(self.path, "wb")
        self._h = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> None:
        self._h.update(data)
        self._f.write(data)
        self.size += len(data)

    @property
    def checksum(self) -> str:
        return cs.fmt(self._h.hexdigest())

    def commit(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()
        final = chunk_path(self.chunk_id)
        final.parent.mkdir(parents=True, exist_ok=True)
        # checksum record first: a crash in between leaves no visible chunk
        sp = sum_path(self.chunk_id)
        sp_tmp = tmp_dir() / f"{self.chunk_id}.{uuid.uuid4().hex}.sum"
        with open(sp_tmp, "wb") as f:
            f.write(self.checksum.encode())
            f.flush()
            os.fsync(f.fileno())
        os.replace(sp_tmp, sp)
        os.replace(self.path, final)
        _fsync_dir(final.parent)

    def abort(self) -> None:
        try:
            self._f.close()
        finally:
            self.path.unlink(missing_ok=True)


def read_verified(chunk_id: str) -> tuple[bytes, str]:
    """Read a chunk and verify it against its recorded checksum."""
    try:
        data = chunk_path(chunk_id).read_bytes()
    except FileNotFoundError:
        raise ChunkMissing(chunk_id)
    expected = stored_checksum(chunk_id)
    actual = cs.sha256_bytes(data)
    if expected is None or expected != actual:
        raise ChunkCorrupt(expected or "<none>", actual)
    return data, actual


def delete(chunk_id: str) -> bool:
    existed = False
    for p in (chunk_path(chunk_id), sum_path(chunk_id)):
        try:
            p.unlink()
            existed = True
        except FileNotFoundError:
            pass
    return existed


def list_chunks() -> list[dict]:
    out = []
    base = _root() / "chunks"
    if not base.exists():
        return out
    for sub in sorted(base.iterdir()):
        if not sub.is_dir():
            continue
        for f in sorted(sub.iterdir()):
            if f.name.endswith(".sum"):
                continue
            try:
                size = f.stat().st_size
            except FileNotFoundError:
                continue
            try:
                checksum = (sub / (f.name + ".sum")).read_text().strip() or None
            except FileNotFoundError:
                checksum = None
            out.append({"chunk_id": f.name, "size": size, "checksum": checksum})
    return out


def usage() -> dict:
    u = shutil.disk_usage(settings.data_dir)
    return {"capacity_bytes": u.total, "used_bytes": u.used, "free_bytes": u.free}
