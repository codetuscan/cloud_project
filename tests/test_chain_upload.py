"""Controller side of chain replication: one PUT to the first worker, outcomes read from its reply."""
from types import SimpleNamespace

import httpx

from app.common.gate import Gate
from app.controller import placement, upload

A = placement.Target("worker1", "http://worker1:9000")
B = placement.Target("worker2", "http://worker2:9000")


class FakeHttp:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    async def put(self, url, content=None, headers=None):
        self.calls.append((url, headers))
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def reply(status, replicas=None, text=""):
    body = {"replicas": replicas} if replicas is not None else {}
    return httpx.Response(status, json=body, request=httpx.Request("PUT", "http://x")) if not text else \
        httpx.Response(status, text=text, request=httpx.Request("PUT", "http://x"))


def state(*responses):
    return SimpleNamespace(http=FakeHttp(*responses), transfer_sem=Gate(4, "t"))


async def test_both_copies_ok_with_a_single_controller_upload():
    st = state(reply(201, [{"worker_id": "worker1", "ok": True}, {"worker_id": "worker2", "ok": True}]))
    out = await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data")
    assert out == [None, None]
    assert len(st.http.calls) == 1                      # the controller sent the bytes exactly once
    url, headers = st.http.calls[0]
    assert url.startswith("http://worker1:9000/") and headers["X-Forward-To"] == "http://worker2:9000"
    assert headers["X-Forward-Worker-Id"] == "worker2"


async def test_forward_failure_keeps_first_copy_and_blames_only_the_second():
    st = state(reply(201, [{"worker_id": "worker1", "ok": True},
                           {"worker_id": "worker2", "ok": False, "error": "connect error"}]))
    first, second = await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data")
    assert first is None and isinstance(second, Exception) and "connect error" in str(second)
    assert len(st.http.calls) == 1                      # no controller retry: the worker already retried


async def test_first_worker_unreachable_is_retried_and_second_is_not_blamed(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(upload.asyncio, "sleep", no_sleep)
    st = state(*[httpx.ConnectError("down") for _ in range(upload.PUT_ATTEMPTS)])
    first, second = await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data")
    assert isinstance(first, Exception) and not isinstance(first, upload._NotAttempted)
    assert isinstance(second, upload._NotAttempted)     # never received it, so it is not marked failed
    assert len(st.http.calls) == upload.PUT_ATTEMPTS


async def test_first_worker_recovers_on_retry(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(upload.asyncio, "sleep", no_sleep)
    st = state(httpx.ConnectError("blip"),
               reply(201, [{"worker_id": "worker1", "ok": True}, {"worker_id": "worker2", "ok": True}]))
    assert await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data") == [None, None]


async def test_conflict_is_not_retried():
    st = state(reply(409, text='{"error":{"code":"chunk_conflict"}}'))
    first, second = await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data")
    assert isinstance(first, Exception) and isinstance(second, upload._NotAttempted)
    assert len(st.http.calls) == 1


async def test_reply_without_forward_result_counts_as_second_failure():
    st = state(reply(201, [{"worker_id": "worker1", "ok": True}]))
    first, second = await upload._send_chain(st, A, B, "c-000000", "sha256:" + "0" * 64, b"data")
    assert first is None and isinstance(second, Exception)
