import hashlib

import pytest

from app.controller.multipart_stream import StreamingUpload, UploadError

B = "XBOUNDARYX"
CT = f"multipart/form-data; boundary={B}"


def body(data: bytes, filename="a.bin", ctype="image/png", meta: str | None = None, second_file=False):
    parts = b""
    if meta is not None:
        parts += f'--{B}\r\nContent-Disposition: form-data; name="metadata"\r\n\r\n{meta}\r\n'.encode()
    parts += (f'--{B}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
              f'Content-Type: {ctype}\r\n\r\n').encode() + data + b"\r\n"
    if second_file:
        parts += f'--{B}\r\nContent-Disposition: form-data; name="file"; filename="b.bin"\r\n\r\nzz\r\n'.encode()
    return parts + f"--{B}--\r\n".encode()


def run(tmp_path, raw: bytes, step: int):
    su = StreamingUpload(CT, tmp_path / "u.part")
    for i in range(0, len(raw), step):
        su.feed(raw[i:i + step])
    su.finish()
    return su


@pytest.mark.parametrize("step", [1, 7, 64, 100000])
def test_parses_correctly_however_the_body_is_split(tmp_path, step):
    data = bytes(range(256)) * 40
    su = run(tmp_path, body(data, meta='{"a":"b"}'), step)
    assert su.size == len(data)
    assert su.checksum == "sha256:" + hashlib.sha256(data).hexdigest()
    assert (tmp_path / "u.part").read_bytes() == data       # streamed straight to disk
    assert su.filename == "a.bin" and su.content_type == "image/png"
    assert bytes(su.meta) == b'{"a":"b"}'


def test_data_containing_boundary_like_text_is_kept(tmp_path):
    data = b"head\r\n--NOTTHEBOUNDARY\r\nmore"
    su = run(tmp_path, body(data), 5)
    assert (tmp_path / "u.part").read_bytes() == data


def test_filename_is_reduced_to_basename(tmp_path):
    su = run(tmp_path, body(b"x", filename="../../etc/passwd"), 50)
    assert su.filename == "passwd"


def test_content_type_guessed_from_filename_when_absent(tmp_path):
    raw = (f'--{B}\r\nContent-Disposition: form-data; name="file"; filename="doc.pdf"\r\n\r\nx\r\n--{B}--\r\n').encode()
    assert run(tmp_path, raw, 50).content_type == "application/pdf"


def test_missing_boundary_rejected(tmp_path):
    with pytest.raises(UploadError):
        StreamingUpload("multipart/form-data", tmp_path / "x")


def test_non_multipart_rejected(tmp_path):
    with pytest.raises(UploadError):
        StreamingUpload("application/json", tmp_path / "x")


def test_two_file_parts_rejected(tmp_path):
    with pytest.raises(UploadError):
        run(tmp_path, body(b"x", second_file=True), 50)


def test_no_file_part(tmp_path):
    su = run(tmp_path, f'--{B}\r\nContent-Disposition: form-data; name="metadata"\r\n\r\n{{}}\r\n--{B}--\r\n'.encode(), 50)
    assert su.file_parts == 0
