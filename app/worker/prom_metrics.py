"""GET /metrics on a worker: its own counters, disk, chunk cache, transfer gate and scrubber.

No worker_id label is emitted here: Prometheus attaches one per scrape target (monitoring/prometheus.yml),
so the same series names line up with the controller's per-worker series in Grafana.
"""
import asyncio
from datetime import datetime

from app.common import metrics
from app.common.config import settings
from app.common.prom import Exposition
from app.worker import cache as wcache
from app.worker import disk, scrub

HELP = {
    "chunk_puts": "Chunk writes stored durably (verified, fsynced, renamed).",
    "chunk_put_bytes": "Bytes of chunks stored.",
    "chunk_put_conflicts": "Writes rejected because different bytes already exist under that chunk id (409).",
    "chunk_put_checksum_mismatches": "Writes rejected because the received bytes did not match X-Chunk-Checksum.",
    "chunk_gets": "Chunk reads served (from cache or disk).",
    "chunk_get_bytes": "Bytes of chunks served.",
    "chunk_get_corrupt": "Reads refused because the file failed its checksum (reported to the controller).",
    "chunk_get_missing": "Reads of chunks that are not on this worker (404).",
    "chunk_deletes": "Chunk deletions.",
    "chunk_forwards_ok": "Chain-replication forwards to the next worker that succeeded.",
    "chunk_forwards_failed": "Chain-replication forwards that failed after retries.",
    "chunk_sends_ok": "Repair copies pushed to another worker on the controller's command.",
    "chunk_sends_failed": "Repair copies that failed (source missing/corrupt or target failed).",
    "scrub_chunks_checked": "Chunks hashed by the background scrubber (cumulative over passes).",
    "scrub_bytes_checked": "Bytes hashed by the background scrubber (cumulative over passes).",
    "scrub_findings_corrupt": "Scrubber findings queued for the controller: CORRUPT.",
    "scrub_findings_missing": "Scrubber findings queued for the controller: MISSING.",
    "scrub_findings_unreadable": "Scrubber findings queued for the controller: UNREADABLE.",
}


def declare() -> None:
    metrics.declare(*HELP)


def _ts(iso):
    try:
        return datetime.fromisoformat(iso).timestamp() if iso else None
    except ValueError:
        return None


async def render(transfers_gate) -> str:
    e = Exposition()
    e.process_counters(HELP)
    e.gate(transfers_gate, "worker")
    cs = wcache.stats()
    e.cache(cs, "chunk")
    e.counter("chunk_disk_reads", cs["disk_reads"], "Chunk reads that went to disk (cache misses served).")

    chunks = await asyncio.to_thread(lambda: len(disk.list_chunks()))
    du = await asyncio.to_thread(disk.usage)
    e.gauge("chunks_on_disk", chunks, "Chunk files on this worker's volume.")
    e.gauge("disk_capacity_bytes", du["capacity_bytes"], "Size of the filesystem holding the data directory.")
    e.gauge("disk_used_bytes", du["used_bytes"], "Used bytes on that filesystem.")
    e.gauge("disk_free_bytes", du["free_bytes"], "Free bytes on that filesystem.")

    s = scrub.state
    e.gauge("scrub_in_progress", bool(s["in_progress"]), "1 while a scrub pass is running.")
    e.gauge("scrub_interval_seconds", settings.scrub_interval,
            "Configured time between scrub passes (SCRUB_INTERVAL); the ScrubOverdue alert is relative to it.")
    e.gauge("scrub_passes_completed", s["passes_completed"], "Scrub passes completed (persisted across restarts).")
    e.gauge("scrub_last_pass_chunks_total", s["chunks_total"], "Chunks in the current/last pass.")
    e.gauge("scrub_last_pass_chunks_checked", s["chunks_checked"], "Chunks checked so far in the current/last pass.")
    e.gauge("scrub_last_pass_corrupt_found", s["corrupt_found"], "Corrupt chunks found in the current/last pass.")
    e.gauge("scrub_last_pass_missing_found", s["missing_found"], "Missing chunks found in the current/last pass.")
    e.gauge("scrub_last_pass_duration_seconds", s["duration_seconds"], "Duration of the last completed pass.")
    e.gauge("scrub_last_finished_timestamp_seconds", _ts(s["last_scrub_finished"]),
            "Unix time the last scrub pass finished.")
    e.gauge("scrub_report_queue_depth", await asyncio.to_thread(scrub.queue_depth),
            "Scrub findings waiting in the durable queue for the controller to acknowledge.")
    e.counter("scrub_reports_delivered", s["reports_delivered"], "Scrub findings acknowledged by the controller.")
    e.counter("scrub_report_delivery_failures", s["report_delivery_failures"],
              "Failed attempts to deliver a scrub finding (kept and retried).")
    return e.render()
