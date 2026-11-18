"""Distributed tracing (OpenTelemetry -> Jaeger).

One upload becomes ONE trace that crosses processes: the controller's request span, its PostgreSQL queries, one span
per chunk, the PUT to the first worker, that worker's disk write and fsync, its forward to the second worker and the
second worker's write. The trace context travels in the W3C `traceparent` header that the instrumented HTTP clients
add and the instrumented servers read.

What is traced (sampler, `_RootFilter`):
  * every client-facing or worker-to-worker REQUEST (server spans, unless the URL is excluded below), and
  * background operations started explicitly with `operation(...)`: a repair, a rebalance move, a scrub report.
  Everything else a background loop does (heartbeats, health probes, the scanner's and reaper's queries every few
  seconds) would be thousands of meaningless one-span traces, so it is not sampled. Child spans always follow their
  parent's decision, so a sampled operation is complete across all processes.

Tracing is optional and fail-safe: spans are exported in a background thread with a bounded queue; if Jaeger is down,
batches are dropped and requests are not slowed down. `TRACING_ENABLED=false` turns everything off (no-op tracer).
Every traced HTTP response carries `X-Trace-Id`, and log lines inside a trace show `trace=<id>`, so a log line, a
response and the timeline in Jaeger can be matched.
"""
import logging
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from app.common.config import settings

log = logging.getLogger("tracing")
tracer = trace.get_tracer("objstore")
ROOT_ATTR = "objstore.operation"     # set on spans that may start a trace of their own
_provider = None

# requests that are pure plumbing: never worth a trace
EXCLUDED_URLS = ",".join([
    "healthz", "/metrics", "internal/metrics", "internal/status", "internal/workers/heartbeat",
    "internal/workers/register", "internal/scrub/status", "internal/cache/stats", r"internal/workers/[^/]+/chunks$",
])


# ------------------------------------------------------------------ sampling
def _make_sampler():
    from opentelemetry.sdk.trace.sampling import Decision, ParentBased, Sampler, SamplingResult

    class _RootFilter(Sampler):
        """Decides only for spans WITHOUT a parent: requests and explicit operations yes, background noise no."""

        def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None,
                          trace_state=None):
            keep = kind == SpanKind.SERVER or bool(attributes and attributes.get(ROOT_ATTR))
            return SamplingResult(Decision.RECORD_AND_SAMPLE if keep else Decision.DROP, attributes)

        def get_description(self) -> str:
            return "ObjstoreRootFilter"

    return ParentBased(root=_RootFilter())


# ------------------------------------------------------------------ setup
def setup(service: str, app=None) -> bool:
    """Install the tracer provider and instrument FastAPI + asyncpg. Returns False when tracing is disabled."""
    global _provider
    if not settings.tracing_enabled:
        return False
    if _provider is None:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        _provider = TracerProvider(
            resource=Resource.create({"service.name": service, "service.namespace": "objstore"}),
            sampler=_make_sampler())
        exporter = OTLPSpanExporter(endpoint=settings.otel_endpoint.rstrip("/") + "/v1/traces", timeout=3)
        _provider.add_span_processor(BatchSpanProcessor(exporter, max_queue_size=8192, max_export_batch_size=512,
                                                        schedule_delay_millis=1000))
        trace.set_tracer_provider(_provider)
        # an unreachable Jaeger must not flood the logs (spans are simply dropped)
        logging.getLogger("opentelemetry.exporter.otlp.proto.http.trace_exporter").setLevel(logging.CRITICAL)
        try:
            from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
            AsyncPGInstrumentor().instrument(tracer_provider=_provider)
        except Exception as e:  # e.g. asyncpg not used by this process
            log.debug("asyncpg instrumentation skipped: %r", e)
        log.info("tracing on: service=%s, exporting to %s", service, settings.otel_endpoint)
    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        app.add_middleware(TraceIdHeaderMiddleware)
        FastAPIInstrumentor.instrument_app(app, tracer_provider=_provider, excluded_urls=EXCLUDED_URLS,
                                           exclude_spans=["receive", "send"])
    return True


def instrument_client(client) -> None:
    """Trace an httpx client's requests and propagate the trace context to the server it calls."""
    if _provider is None:
        return
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    HTTPXClientInstrumentor.instrument_client(client, tracer_provider=_provider)


def shutdown() -> None:
    if _provider is not None:
        _provider.shutdown()  # flush what is still queued


# ------------------------------------------------------------------ helpers for the code
def operation(name: str, **attrs):
    """A span that may START a trace (repair, rebalance move, scrub report). Inside a request it is a child."""
    return tracer.start_as_current_span(name, attributes={ROOT_ATTR: True, **_clean(attrs)})


def span(name: str, **attrs):
    """A child span: recorded only when the surrounding operation is traced."""
    return tracer.start_as_current_span(name, attributes=_clean(attrs))


def annotate(**attrs) -> None:
    """Add attributes to the current span (no-op outside a trace)."""
    s = trace.get_current_span()
    if s.is_recording():
        s.set_attributes(_clean(attrs))


def event(name: str, **attrs) -> None:
    s = trace.get_current_span()
    if s.is_recording():
        s.add_event(name, _clean(attrs))


def fail(message: str) -> None:
    """Mark the current span as failed (shown red in Jaeger)."""
    s = trace.get_current_span()
    if s.is_recording():
        s.set_status(Status(StatusCode.ERROR, message))


def current_trace_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid and ctx.trace_flags.sampled else None


def _clean(attrs: dict) -> dict:
    return {f"objstore.{k}" if "." not in k else k: (v if isinstance(v, (str, bool, int, float)) else str(v))
            for k, v in attrs.items() if v is not None}


@contextmanager
def nothing():
    yield


# ------------------------------------------------------------------ X-Trace-Id + log correlation
class TraceIdHeaderMiddleware:
    """Adds `X-Trace-Id` to responses of traced requests (paste it into Jaeger's search box)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_with_trace(message):
            if message["type"] == "http.response.start":
                tid = current_trace_id()
                if tid:
                    message.setdefault("headers", []).append((b"x-trace-id", tid.encode()))
            await send(message)

        await self.app(scope, receive, send_with_trace)


class TraceLogFilter(logging.Filter):
    """Fills %(trace)s with ' trace=<id>' inside a traced operation, '' otherwise."""

    def filter(self, record: logging.LogRecord) -> bool:
        tid = current_trace_id()
        record.trace = f" trace={tid}" if tid else ""
        return True
