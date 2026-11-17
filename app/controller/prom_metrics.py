"""GET /metrics on the controller: process counters + the cluster's state as seen in PostgreSQL.

Cluster gauges are computed at scrape time (a handful of COUNT queries every scrape interval), so they are
always the truth from the database, never a cached copy. If PostgreSQL is down, objstore_database_up is 0
and the process metrics are still served.
"""
import logging

from app.common import metrics
from app.common.prom import Exposition
from app.controller import meta_cache, rebalance

log = logging.getLogger("controller.metrics")

WORKER_STATES = ("HEALTHY", "DEGRADED", "DEAD")
OBJECT_STATES = ("PENDING", "COMMITTED", "DELETING", "DELETED")
REPLICA_STATES = ("PENDING", "HEALTHY", "CORRUPT", "MISSING", "DELETING")
TASK_STATES = ("PENDING", "IN_PROGRESS", "DONE", "UNRECOVERABLE")

HELP = {
    "uploads_started": "Uploads admitted (past the upload gate).",
    "uploads_committed": "Uploads that reached COMMITTED with 2 healthy replicas per chunk (HTTP 201).",
    "uploads_pending_503": "Uploads kept in staging because 2 replicas could not be placed yet (HTTP 503).",
    "uploads_rejected_busy": "Uploads rejected with 503 server_busy (upload gate full).",
    "upload_bytes": "Bytes of committed uploads.",
    "downloads_started": "Downloads admitted (past the download gate).",
    "downloads_completed": "Downloads streamed completely.",
    "downloads_aborted": "Downloads aborted mid-stream (client gone or no readable replica).",
    "downloads_rejected_busy": "Downloads rejected with 503 server_busy (download gate full).",
    "download_bytes": "Verified chunk bytes sent to clients.",
    "replica_read_fallbacks": "Chunk reads that had to try another replica (first one unreachable/missing/corrupt).",
    "replicas_marked_corrupt": "Replicas moved HEALTHY -> CORRUPT (read check, scrub report or repair source check).",
    "replicas_marked_missing": "Replicas moved HEALTHY -> MISSING (read 404, scrub, inventory reconciliation).",
    "chunk_chain_puts": "Chunk uploads sent once to a worker that forwarded the second copy (chain replication).",
    "repair_direct_copies": "Worker-to-worker copies commanded by the controller.",
    "repairs_done": "Repair tasks finished DONE.",
    "repairs_unrecoverable": "Repair tasks that found no healthy copy anywhere.",
    "repairs_retried": "Repair attempts that failed and were rescheduled with backoff.",
    "orphan_chunks_deleted": "Unknown chunk files removed from workers after the grace period.",
    "replicas_deleted": "Replicas removed from workers by the reaper (deletion, surplus trim, rebalance).",
    "rebalance_copies": "Worker-to-worker copies commanded by the rebalancer.",
    "rebalance_moves": "Replicas moved from a fuller to an emptier worker (copy verified, old copy retired).",
    "rebalance_move_failures": "Rebalance moves that failed and were rolled back (the original copy is kept).",
    "workers_marked_dead": "Times a worker was marked DEAD (no heartbeat within the timeout).",
    "startup_probe_alive": "Workers that answered the controller's start-up probe.",
    "scrub_reports_healthy": "Scrub reports received: HEALTHY.",
    "scrub_reports_corrupt": "Scrub reports received: CORRUPT.",
    "scrub_reports_missing": "Scrub reports received: MISSING.",
    "scrub_reports_unreadable": "Scrub reports received: UNREADABLE.",
    "upload_duration_seconds": "End-to-end time of successful uploads (receive, stage, chunk, replicate, commit).",
    "download_duration_seconds": "End-to-end time of completed downloads.",
    "repair_time_to_heal_seconds": "Time from a repair task being queued to the chunk having 2 healthy replicas again.",
}
HISTOGRAMS = ("upload_duration_seconds", "download_duration_seconds", "repair_time_to_heal_seconds")


def declare() -> None:
    metrics.declare(*(k for k in HELP if k not in HISTOGRAMS))
    metrics.declare_histogram(*HISTOGRAMS)


async def _cluster(e: Exposition, pool) -> None:
    async with pool.acquire() as c:
        workers = await c.fetch(
            """SELECT worker_id, state, draining, capacity_bytes, used_bytes,
                      EXTRACT(EPOCH FROM (now()-last_heartbeat))::float8 AS age FROM workers ORDER BY worker_id""")
        objs = {r["state"]: r for r in await c.fetch(
            "SELECT state, count(*) AS n, coalesce(sum(size),0)::bigint AS bytes FROM objects GROUP BY state")}
        chunk_health = await c.fetchrow(
            """SELECT count(*) AS total, count(*) FILTER (WHERE n < 2) AS under, count(*) FILTER (WHERE n = 0) AS lost
               FROM (SELECT (SELECT count(*) FROM replicas r JOIN workers w USING (worker_id)
                             WHERE r.chunk_id=c.chunk_id AND r.state='HEALTHY' AND w.state<>'DEAD') AS n
                     FROM chunks c JOIN objects o ON o.object_id=c.object_id AND o.state='COMMITTED') s""")
        reps = await c.fetch(
            """SELECT r.worker_id, r.state, count(*) AS n, coalesce(sum(c.size),0)::bigint AS bytes
               FROM replicas r JOIN chunks c USING (chunk_id) GROUP BY r.worker_id, r.state""")
        tasks = {r["state"]: r["n"] for r in await c.fetch(
            "SELECT state, count(*) AS n FROM repair_tasks GROUP BY state")}
        oldest = await c.fetchval(
            """SELECT EXTRACT(EPOCH FROM (now()-min(created_at)))::float8 FROM repair_tasks
               WHERE state IN ('PENDING','IN_PROGRESS')""")
        staged = await c.fetchval("SELECT count(*) FROM uploads")

    e.gauge("database_up", 1, "1 if the controller could query PostgreSQL during this scrape.")
    for w in workers:
        wid = w["worker_id"]
        for s in WORKER_STATES:
            e.gauge("worker_state", w["state"] == s, "1 for the worker's current state (controller's view).",
                    {"worker_id": wid, "state": s})
        e.gauge("worker_alive", w["state"] != "DEAD", "1 unless the controller has marked the worker DEAD.",
                {"worker_id": wid})
        e.gauge("worker_draining", w["draining"], "1 while the worker is draining (no new data; replicas moved off).",
                {"worker_id": wid})
        e.gauge("worker_heartbeat_age_seconds", round(w["age"], 2), "Seconds since the worker's last heartbeat.",
                {"worker_id": wid})
        e.gauge("worker_capacity_bytes", w["capacity_bytes"], "Disk capacity reported by the worker.",
                {"worker_id": wid})
        e.gauge("worker_used_bytes", w["used_bytes"], "Disk usage reported by the worker.", {"worker_id": wid})
    for s in WORKER_STATES:
        e.gauge("workers", sum(1 for w in workers if w["state"] == s), "Registered workers by state.", {"state": s})

    for s in OBJECT_STATES:
        r = objs.get(s)
        e.gauge("objects", r["n"] if r else 0, "Objects by lifecycle state.", {"state": s})
    e.gauge("stored_bytes", objs["COMMITTED"]["bytes"] if "COMMITTED" in objs else 0,
            "Logical bytes of COMMITTED objects (before replication).")
    e.gauge("staged_uploads", staged, "Uploads with a staging file / row (streaming or awaiting commit).")

    e.gauge("committed_chunks", chunk_health["total"], "Chunks belonging to COMMITTED objects.")
    e.gauge("under_replicated_chunks", chunk_health["under"],
            "Committed chunks with fewer than 2 HEALTHY replicas on live workers.")
    e.gauge("unavailable_chunks", chunk_health["lost"],
            "Committed chunks with NO healthy replica on a live worker (reads of them fail).")

    by_worker: dict[tuple, tuple] = {(r["worker_id"], r["state"]): (r["n"], r["bytes"]) for r in reps}
    for s in REPLICA_STATES:
        e.gauge("replicas", sum(n for (w, st), (n, _) in by_worker.items() if st == s),
                "Chunk replicas by state (all workers).", {"state": s})
    for w in workers:
        for s in REPLICA_STATES:
            n, b = by_worker.get((w["worker_id"], s), (0, 0))
            e.gauge("worker_replicas", n, "Chunk replicas held by each worker, by state.",
                    {"worker_id": w["worker_id"], "state": s})
            e.gauge("worker_replica_bytes", b, "Bytes of chunk replicas held by each worker, by state.",
                    {"worker_id": w["worker_id"], "state": s})

    for s in TASK_STATES:
        e.gauge("repair_tasks", tasks.get(s, 0), "Repair tasks by state.", {"state": s})
    e.gauge("repair_oldest_active_task_age_seconds", round(oldest, 1) if oldest is not None else 0,
            "Age of the oldest PENDING/IN_PROGRESS repair task (0 if none).")


async def render(state) -> str:
    e = Exposition()
    e.process_counters(HELP)
    for g in (state.transfer_sem, state.upload_gate, state.download_gate):
        e.gate(g, "controller")
    e.cache(meta_cache.cache.stats(), "metadata")
    e.gauge("active_uploads", len(state.active_uploads), "Uploads currently being received or committed.")
    rb = rebalance.status
    e.gauge("rebalance_enabled", rb["enabled"], "1 if the rebalancer is enabled.")
    e.gauge("rebalance_moving", rb["state"].startswith("moving"), "1 while the rebalancer is moving replicas.")
    e.gauge("rebalance_replica_spread", rb["spread"],
            "Replica count of the fullest minus the emptiest HEALTHY worker (rebalancer's last round).")
    try:
        await _cluster(e, state.pool)
    except Exception as ex:  # keep serving process metrics; the dashboard shows the database as down
        log.warning("metrics: cluster query failed: %r", ex)
        e.gauge("database_up", 0, "1 if the controller could query PostgreSQL during this scrape.")
    return e.render()
