"""Streaming multipart/form-data parser: file bytes go straight to the staging file.

Never buffers the upload in memory. feed() is called from a worker thread so disk writes
and hashing don't block the event loop.
"""
import hashlib
import mimetypes
import os
from pathlib import Path

from multipart.multipart import MultipartParser, parse_options_header

MAX_META_BYTES = 64 * 1024


class UploadError(ValueError):
    """Client sent a malformed upload (-> HTTP 400)."""


class StreamingUpload:
    def __init__(self, content_type: str, staging_path: Path):
        ctype, opts = parse_options_header(content_type.encode())
        if ctype != b"multipart/form-data" or b"boundary" not in opts:
            raise UploadError("Content-Type must be multipart/form-data with a boundary")
        self.path = staging_path
        self.size = 0
        self._hash = hashlib.sha256()
        self.filename: str | None = None
        self.file_content_type: str | None = None
        self.file_parts = 0
        self.meta = bytearray()
        self.meta_parts = 0
        self._f = None
        self._part: str | None = None  # "file" | "metadata" | None(ignored)
        self._hname = bytearray()
        self._hval = bytearray()
        self._headers: dict[bytes, bytes] = {}
        self._parser = MultipartParser(opts[b"boundary"], {
            "on_part_begin": self._on_part_begin,
            "on_part_data": self._on_part_data,
            "on_part_end": self._on_part_end,
            "on_header_field": lambda d, s, e: self._hname.extend(d[s:e]),
            "on_header_value": lambda d, s, e: self._hval.extend(d[s:e]),
            "on_header_end": self._on_header_end,
            "on_headers_finished": self._on_headers_finished,
        })

    # -- parser callbacks
    def _on_part_begin(self):
        self._headers = {}
        self._part = None

    def _on_header_end(self):
        self._headers[bytes(self._hname).lower()] = bytes(self._hval)
        self._hname.clear()
        self._hval.clear()

    def _on_headers_finished(self):
        disp = self._headers.get(b"content-disposition", b"")
        _, params = parse_options_header(disp)
        name = params.get(b"name", b"").decode("utf-8", "replace")
        if name == "file":
            self.file_parts += 1
            if self.file_parts > 1:
                raise UploadError("only one 'file' part is allowed")
            fn = params.get(b"filename")
            self.filename = os.path.basename(fn.decode("utf-8", "replace")) if fn else None
            ct = self._headers.get(b"content-type")
            self.file_content_type = ct.decode("latin-1").strip() if ct else None
            self._f = open(self.path, "wb")
            self._part = "file"
        elif name == "metadata":
            self.meta_parts += 1
            if self.meta_parts > 1:
                raise UploadError("only one 'metadata' field is allowed")
            self._part = "metadata"

    def _on_part_data(self, data, start, end):
        chunk = data[start:end]
        if self._part == "file":
            self._hash.update(chunk)
            self._f.write(chunk)
            self.size += len(chunk)
        elif self._part == "metadata":
            self.meta.extend(chunk)
            if len(self.meta) > MAX_META_BYTES:
                raise UploadError("metadata too large")

    def _on_part_end(self):
        if self._part == "file" and self._f:
            self._f.flush()
            os.fsync(self._f.fileno())
            self._f.close()
            self._f = None

    # -- public
    def feed(self, data: bytes) -> None:
        try:
            n = self._parser.write(data)
        except UploadError:
            raise
        except Exception as e:  # MultipartParseError etc.
            raise UploadError(f"malformed multipart body: {e}")
        if n != len(data):
            raise UploadError("malformed multipart body")

    def finish(self) -> None:
        try:
            self._parser.finalize()
        except Exception as e:
            raise UploadError(f"malformed multipart body: {e}")
        self.close()

    def close(self) -> None:
        if self._f:
            self._f.close()
            self._f = None

    @property
    def checksum(self) -> str:
        return "sha256:" + self._hash.hexdigest()

    @property
    def content_type(self) -> str:
        if self.file_content_type:
            return self.file_content_type
        if self.filename:
            guess, _ = mimetypes.guess_type(self.filename)
            if guess:
                return guess
        return "application/octet-stream"
