"""Tracing: sampling rules, trace-context propagation across an HTTP hop, X-Trace-Id, log correlation."""
import logging

import httpx
import pytest
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from app.common import tracing

EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module")
def provider():
    """One real SDK provider for the whole module, with the production sampler and an in-memory exporter."""
    p = TracerProvider(sampler=tracing._make_sampler())
    p.add_span_processor(SimpleSpanProcessor(EXPORTER))
    trace.set_tracer_provider(p)
    old = tracing._provider
    tracing._provider = p
    yield p
    tracing._provider = old


@pytest.fixture(autouse=True)
def clear():
    EXPORTER.clear()


# ------------------------------------------------------------------ sampling
def test_background_noise_is_not_sampled(provider):
    t = provider.get_tracer("t")
    with t.start_as_current_span("SELECT ...", kind=SpanKind.CLIENT):      # e.g. the health monitor's query
        pass
    with t.start_as_current_span("GET /internal/workers/x/chunks", kind=SpanKind.CLIENT):
        pass
    assert EXPORTER.get_finished_spans() == ()


def test_requests_and_explicit_operations_start_traces(provider):
    t = provider.get_tracer("t")
    with t.start_as_current_span("POST /api/v1/objects", kind=SpanKind.SERVER):
        pass
    with tracing.operation("repair chunk", chunk_id="c1"):
        pass
    names = sorted(s.name for s in EXPORTER.get_finished_spans())
    assert names == ["POST /api/v1/objects", "repair chunk"]


def test_children_follow_the_parent_decision(provider):
    t = provider.get_tracer("t")
    with tracing.operation("rebalance move"):
        with t.start_as_current_span("UPDATE replicas", kind=SpanKind.CLIENT):   # child of a sampled op: kept
            pass
    with t.start_as_current_span("scanner tick", kind=SpanKind.INTERNAL):      # unsampled root
        with tracing.span("child"):                                            # ... so its child is dropped
            pass
    names = sorted(s.name for s in EXPORTER.get_finished_spans())
    assert names == ["UPDATE replicas", "rebalance move"]


def test_attributes_are_namespaced_and_stringified(provider):
    with tracing.operation("op", chunk_id="c1", attempt=2, targets=["w1", "w2"], skip=None):
        tracing.annotate(served_by="worker3")
    (s,) = EXPORTER.get_finished_spans()
    assert s.attributes["objstore.chunk_id"] == "c1" and s.attributes["objstore.attempt"] == 2
    assert s.attributes["objstore.targets"] == "['w1', 'w2']" and "objstore.skip" not in s.attributes
    assert s.attributes["objstore.served_by"] == "worker3"


# ------------------------------------------------------------------ propagation across HTTP
def _worker_app(provider):
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    app = FastAPI()

    @app.put("/internal/chunks/{cid}")
    async def put(cid: str):
        with tracing.span("fsync + atomic rename"):
            pass
        return {"ok": True}

    app.add_middleware(tracing.TraceIdHeaderMiddleware)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider, excluded_urls=tracing.EXCLUDED_URLS,
                                       exclude_spans=["receive", "send"])
    return app


async def test_one_trace_crosses_the_http_hop(provider):
    worker = _worker_app(provider)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=worker), base_url="http://worker1:9000") as c:
        tracing.instrument_client(c)
        with tracing.operation("repair chunk") as op:
            r = await c.put("/internal/chunks/abc", content=b"x")
            trace_id = format(op.get_span_context().trace_id, "032x")
    assert r.status_code == 200
    assert r.headers["x-trace-id"] == trace_id                    # the worker continued the caller's trace

    spans = {s.name: s for s in EXPORTER.get_finished_spans()}
    assert {s.context.trace_id for s in spans.values()} == {op.get_span_context().trace_id}
    client = next(s for s in spans.values() if s.kind == SpanKind.CLIENT)
    # (in-process the ASGI span is INTERNAL, between real processes it is SERVER; match it by name)
    server = spans["PUT /internal/chunks/{cid}"]
    assert client.parent.span_id == op.get_span_context().span_id  # controller op -> its HTTP call
    assert server.parent.span_id == client.context.span_id         # -> the worker's request span
    assert spans["fsync + atomic rename"].parent.span_id == server.context.span_id  # -> work inside the worker


async def test_plumbing_urls_are_not_traced(provider):
    worker = _worker_app(provider)

    @worker.get("/healthz")
    async def hz():
        return {"ok": True}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=worker), base_url="http://w") as c:
        r = await c.get("/healthz")
    assert "x-trace-id" not in r.headers
    assert [s for s in EXPORTER.get_finished_spans() if s.kind == SpanKind.SERVER] == []


# ------------------------------------------------------------------ logs + disabled mode
def test_log_lines_inside_a_trace_carry_its_id(provider):
    f = tracing.TraceLogFilter()
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)
    f.filter(rec)
    assert rec.trace == ""
    with tracing.operation("op") as op:
        f.filter(rec)
    assert rec.trace == f" trace={format(op.get_span_context().trace_id, '032x')}"


def test_disabled_tracing_is_a_no_op(monkeypatch):
    monkeypatch.setattr(tracing.settings, "tracing_enabled", False)
    assert tracing.setup("controller", FastAPI()) is False
    tracing.annotate(x=1)          # helpers never fail outside a trace
    tracing.event("e")
    tracing.fail("f")
