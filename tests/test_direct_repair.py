"""Direct worker-to-worker copy: the worker's /send endpoint and the controller's handling of its answer."""
import json
from types import SimpleNamespace

import httpx
import pytest

from app.common import checksum as cs
from app.common.config import settings
from app.common.gate import Gate
from app.controller import repair
from app.worker import api as wapi
from app.worker import disk
from tests.test_disk import put

CID = "00000000-0000-4000-8000-000000000001-000000"
SRC = {"worker_id": "worker1", "address": "http://worker1:9000"}
TGT = {"worker_id": "worker2", "address": "http://worker2:9000"}


# ------------------------------------------------------------------ worker side
@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_dir", str(tmp_path))
    disk.init()
    return tmp_path


class FakeClient:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    async def put(self, url, content=None, headers=None):
        self.calls.append((url, content, headers))
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _resp(status, text=""):
    return httpx.Response(status, text=text, request=httpx.Request("PUT", "http://x"))


def _cmd(data: bytes, **kw):
    return wapi.SendCommand(target_address="http://worker2:9000", checksum=kw.get("checksum", cs.sha256_bytes(data)),
                            size=kw.get("size", len(data)))


def _body(resp):
    return json.loads(resp.body)


@pytest.fixture
def client(monkeypatch):
    def install(*responses):
        fake = FakeClient(*responses)
        monkeypatch.setattr(wapi, "_client", fake)

        async def no_sleep(_):
            return None
        monkeypatch.setattr(wapi.asyncio, "sleep", no_sleep)
        return fake
    return install


async def test_send_pushes_verified_bytes_to_the_target_with_repair_flag(client):
    put(CID, b"chunk bytes")
    fake = client(_resp(201))
    out = _body(await wapi.send_chunk(CID, _cmd(b"chunk bytes")))
    assert out == {"ok": True}
    url, content, headers = fake.calls[0]
    assert url == f"http://worker2:9000/internal/chunks/{CID}?repair=true"
    assert content == b"chunk bytes" and headers["X-Chunk-Checksum"] == cs.sha256_bytes(b"chunk bytes")


async def test_missing_source_is_reported_and_nothing_is_sent(client):
    fake = client()
    out = _body(await wapi.send_chunk(CID, _cmd(b"x")))
    assert out["ok"] is False and out["reason"] == "source_missing" and fake.calls == []


async def test_source_that_fails_its_own_record_is_reported(client):
    put(CID, b"good data")
    with open(disk.chunk_path(CID), "r+b") as f:
        f.write(b"BAD")
    fake = client()
    out = _body(await wapi.send_chunk(CID, _cmd(b"good data")))
    assert out["reason"] == "source_corrupt" and fake.calls == []


async def test_source_that_disagrees_with_the_controller_is_corrupt_even_if_self_consistent(client):
    put(CID, b"old data")          # its own record matches, but the controller expects different content
    fake = client()
    wrong = _cmd(b"new data")
    out = _body(await wapi.send_chunk(CID, wrong))
    assert out["reason"] == "source_corrupt" and fake.calls == []


async def test_target_failure_is_retried_then_reported_as_target_failed(client):
    put(CID, b"data")
    fake = client(_resp(500), httpx.ConnectError("down"))
    out = _body(await wapi.send_chunk(CID, _cmd(b"data")))
    assert out["ok"] is False and out["reason"] == "target_failed"
    assert len(fake.calls) == wapi.FORWARD_ATTEMPTS


async def test_target_recovers_on_second_attempt(client):
    put(CID, b"data")
    client(httpx.ConnectError("blip"), _resp(201))
    assert _body(await wapi.send_chunk(CID, _cmd(b"data"))) == {"ok": True}


# ------------------------------------------------------------------ controller side
class FakeHttp:
    def __init__(self, response):
        self.response, self.calls = response, []

    async def post(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _ctl_state(response):
    marks = []

    async def mark_bad(pool, chunk_id, worker_id, state, reason):
        marks.append((worker_id, state))
        return True
    repair.replicas.mark_bad = mark_bad
    return SimpleNamespace(http=FakeHttp(response), transfer_sem=Gate(4, "t"), pool=None), marks


def _reply(body, status=200):
    return httpx.Response(status, json=body, request=httpx.Request("POST", "http://x"))


CHUNK = {"chunk_id": CID, "size": 4, "checksum": "sha256:" + "a" * 64}


async def test_controller_sends_a_command_not_bytes():
    st, marks = _ctl_state(_reply({"ok": True}))
    assert await repair._command_copy(st, SRC, TGT, CHUNK) == "ok"
    url, body = st.http.calls[0]
    assert url == f"http://worker1:9000/internal/chunks/{CID}/send"
    assert body == {"target_address": "http://worker2:9000", "checksum": CHUNK["checksum"], "size": 4}
    assert marks == []


@pytest.mark.parametrize("reason,state", [("source_missing", "MISSING"), ("source_corrupt", "CORRUPT")])
async def test_bad_source_is_marked_and_reported(reason, state):
    st, marks = _ctl_state(_reply({"ok": False, "reason": reason, "detail": "x"}))
    assert await repair._command_copy(st, SRC, TGT, CHUNK) == reason
    assert marks == [("worker1", state)]


async def test_target_failure_does_not_blame_the_source():
    st, marks = _ctl_state(_reply({"ok": False, "reason": "target_failed", "detail": "boom"}))
    assert await repair._command_copy(st, SRC, TGT, CHUNK) == "target_failed"
    assert marks == []


async def test_unreachable_source_is_not_marked_bad():
    st, marks = _ctl_state(httpx.ConnectError("down"))
    assert await repair._command_copy(st, SRC, TGT, CHUNK) == "source_unreachable"
    assert marks == []
