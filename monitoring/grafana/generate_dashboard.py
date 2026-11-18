"""Generates monitoring/grafana/dashboards/objstore.json (Grafana 11 schema).

Usage (from the project root):  python3 monitoring/grafana/generate_dashboard.py monitoring/grafana/dashboards/objstore.json
Grafana re-reads the file within 30 s; no restart needed.

Design rules (keep them when adding panels):
* Colour has a job. Identity: each worker always has the same colour (WORKER) in every panel, other series take
  SERIES slots in fixed order. State: the four STATUS colours are used only for good/warning/serious/critical and
  never for an ordinary series.
* Quiet by default. Status tiles show their number in the normal text colour while all is well and only turn amber
  or red when there is a problem; no coloured tile backgrounds.
* Thin, smooth lines with a faint gradient fill; one y-axis per panel (never a second axis).
* Worker 4 is drawn dashed and "waiting"/"read" series are dashed, so no series is told apart by colour alone.
"""
import json
import sys

DS = {"type": "prometheus", "uid": "prometheus"}

# Categorical slots, stepped for the dark surface (validated: blue, orange, aqua, yellow pass the adjacent-pair
# CVD and normal-vision checks on Grafana's #181b1f panel background).
SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#9085e9", "#e66767"]
WORKER = {"worker1": SERIES[0], "worker2": SERIES[1], "worker3": SERIES[2], "worker4": SERIES[3],
          "worker5": SERIES[4]}  # worker5 only exists in the scale-out demo
CONTROLLER = "#9fa7b3"                     # neutral grey for the controller's own series
GOOD, WARN, SERIOUS, CRIT = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"
TEXT = "text"                              # Grafana's theme text colour (white in dark, black in light)

panels = []
_id = 0
y = 0
_sink = panels                             # where new panels go (a collapsed row collects its own)


def nid():
    global _id
    _id += 1
    return _id


def add(p):
    _sink.append(p)


def tgt(expr, legend="", ref="A", instant=False):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend or "__auto", "refId": ref, "range": not instant}
    if instant:
        t["instant"] = True
    return t


def targets(*pairs, instant=False):
    return [tgt(e, l, chr(65 + i), instant) for i, (e, l) in enumerate(pairs)]


def row(title, collapsed=False):
    global y, _sink
    r = {"type": "row", "title": title, "id": nid(), "collapsed": collapsed,
         "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}
    panels.append(r)
    _sink = r["panels"] if collapsed else panels
    y += 1


def thresholds(steps):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}


def color_override(match, color, regexp=False, extra=()):
    props = [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}] + list(extra)
    return {"matcher": {"id": "byRegexp" if regexp else "byName", "options": match}, "properties": props}


DASHED = {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}}
DOTTED = {"id": "custom.lineStyle", "value": {"fill": "dot", "dash": [0, 6]}}


def worker_overrides():
    """Each worker keeps its colour everywhere; worker4 is dashed and worker5 dotted as a second, non-colour cue."""
    out = []
    for w, c in WORKER.items():
        extra = {"worker4": [DASHED], "worker5": [DOTTED]}.get(w, [])
        out.append(color_override(f"^{w}\\b.*", c, regexp=True, extra=extra))
    return out


# ------------------------------------------------------------------------------------------------ panel types
def stat(title, expr, x, w, h=4, unit="none", steps=((None, TEXT),), desc="", legend="", graph=False,
         decimals=None):
    d = {"type": "stat", "title": title, "id": nid(), "description": desc, "datasource": DS,
         "gridPos": {"h": h, "w": w, "x": x, "y": y},
         "targets": targets((expr, legend)),
         "fieldConfig": {"defaults": {"unit": unit, "thresholds": thresholds(steps), "mappings": [],
                                      "color": {"mode": "thresholds"}}, "overrides": []},
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "colorMode": "value", "graphMode": "area" if graph else "none", "textMode": "value",
                     "justifyMode": "auto", "orientation": "auto", "wideLayout": True,
                     "showPercentChange": False, "text": {"valueSize": 34}}}
    if decimals is not None:
        d["fieldConfig"]["defaults"]["decimals"] = decimals
    add(d)


def ts(title, pairs, x, w, h=8, unit="short", desc="", stack=False, min0=True, threshold_line=None,
       fill=12, colors=None, overrides=None):
    """colors: one colour per (expr, legend) pair for static legends; per-worker legends get WORKER colours."""
    custom = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": fill, "gradientMode": "opacity",
              "showPoints": "never", "pointSize": 4, "spanNulls": False, "lineInterpolation": "smooth",
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "axisBorderShow": False, "axisPlacement": "auto", "axisLabel": "",
              "axisSoftMin": 0 if min0 else None, "barAlignment": 0, "insertNulls": False,
              "hideFrom": {"legend": False, "tooltip": False, "viz": False}}
    steps = [(None, "transparent")]
    if threshold_line is not None:
        steps.append((threshold_line, CRIT))
        custom["thresholdsStyle"] = {"mode": "dashed"}
    ov = []
    if any("worker_id" in l for _, l in pairs):
        ov += worker_overrides()
    for (_, legend), c in zip(pairs, colors or []):
        if c:
            ov.append(color_override(legend, c))
    ov += overrides or []
    add({
        "type": "timeseries", "title": title, "id": nid(), "description": desc, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": targets(*pairs),
        "fieldConfig": {"defaults": {"unit": unit, "custom": custom,
                                     "color": {"mode": "fixed", "fixedColor": SERIES[0]},
                                     "thresholds": thresholds(steps)},
                        "overrides": ov},
        # compact one-line legend (a table is clipped at this height); values are in the shared tooltip
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True, "calcs": []},
                    "tooltip": {"mode": "multi", "sort": "desc"}}})


def bargauge(title, pairs, x, w, h=8, unit="short", desc="", steps=None, maxv=None):
    """Without steps the bars take the worker colours (identity); with steps they show state."""
    d = {"type": "bargauge", "title": title, "id": nid(), "description": desc, "datasource": DS,
         "gridPos": {"h": h, "w": w, "x": x, "y": y},
         "targets": targets(*pairs, instant=True),
         "fieldConfig": {"defaults": {"unit": unit, "min": 0,
                                      "thresholds": thresholds(steps or ((None, SERIES[0]),)),
                                      "color": {"mode": "thresholds"}},
                         "overrides": [] if steps else worker_overrides()},
         "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                     "valueMode": "text", "namePlacement": "left", "sizing": "manual", "minVizHeight": 14,
                     "maxVizHeight": 18, "minVizWidth": 8,
                     "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}
    if maxv is not None:
        d["fieldConfig"]["defaults"]["max"] = maxv
    add(d)


def timeline(title, pairs, x, w, h, mappings, steps, desc=""):
    add({
        "type": "state-timeline", "title": title, "id": nid(), "description": desc, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": targets(*pairs),
        "fieldConfig": {"defaults": {"mappings": mappings, "color": {"mode": "fixed", "fixedColor": "text"},
                                     "thresholds": thresholds(steps),
                                     "custom": {"fillOpacity": 90, "lineWidth": 0}}, "overrides": []},
        "options": {"showValue": "auto", "rowHeight": 0.72, "mergeValues": True, "alignValue": "left",
                    "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "single"}}})


def vmap(pairs):
    """Range mappings (v-0.5 .. v+0.5) -> text + colour; thresholds below repeat the colours as a fallback."""
    return [{"type": "range", "options": {"from": v - 0.5, "to": v + 0.5,
                                          "result": {"text": t, "color": c, "index": i}}}
            for i, (v, t, c) in enumerate(pairs)]


CALM = "#2c5e45"                           # muted green for "healthy" bands, so only problems stand out
UPDOWN = vmap([(1, "Up", CALM), (0, "Down", CRIT)])
UPDOWN_STEPS = [(None, CRIT), (0.5, CALM)]
DRAIN = "#4a6a99"                          # muted blue: a planned state, not a problem
WSTATE = vmap([(4, "Draining", DRAIN), (3, "Draining · low disk", WARN), (2, "Healthy", CALM),
               (1, "Degraded", WARN), (0, "Dead", CRIT)])
WSTATE_STEPS = [(None, CRIT), (0.5, WARN), (1.5, CALM), (2.5, WARN), (3.5, DRAIN)]
# 2 healthy / 1 degraded / 0 dead, +2 while a live worker is draining
WORKER_STATE_EXPR = ('2 * sum by (worker_id) (objstore_worker_state{state="HEALTHY"})'
                     ' + sum by (worker_id) (objstore_worker_state{state="DEGRADED"})'
                     ' + 2 * sum by (worker_id) (objstore_worker_draining * objstore_worker_alive)')
ZERO_OK_WARN = ((None, TEXT), (1, WARN))   # 0 is normal; any value is worth a look
ZERO_OK_CRIT = ((None, TEXT), (1, CRIT))   # 0 is normal; any value is an outage


# ------------------------------------------------------------------------------------------------ overview
row("Cluster overview")
stat("Workers alive", "sum(objstore_worker_alive)", 0, 3,
     steps=((None, CRIT), (2, WARN), (4, TEXT)),
     desc="Workers not marked DEAD by the controller (heartbeat within the timeout). 2 are needed to accept uploads.")
stat("Under-replicated", "objstore_under_replicated_chunks", 3, 3, steps=ZERO_OK_WARN,
     desc="Committed chunks with fewer than 2 HEALTHY replicas on live workers. Should return to 0 after repair.")
stat("Unavailable", "objstore_unavailable_chunks", 6, 3, steps=ZERO_OK_CRIT,
     desc="Committed chunks with no readable replica on a live worker. Downloads touching them fail with 503.")
stat("Active repairs", 'sum(objstore_repair_tasks{state=~"PENDING|IN_PROGRESS"})', 9, 3, steps=ZERO_OK_WARN,
     desc="Repair tasks queued or running.")
stat("Objects", 'objstore_objects{state="COMMITTED"}', 12, 3, desc="COMMITTED objects.")
stat("Stored data", "objstore_stored_bytes", 15, 3, unit="bytes",
     desc="Logical size of committed objects (each byte is stored twice).")
stat("Chunks", "objstore_committed_chunks", 18, 3,
     desc="Chunks of committed objects (1 MiB each, last one smaller).")
stat("Healthy replicas", 'objstore_replicas{state="HEALTHY"}', 21, 3,
     desc="HEALTHY replica rows; 2 per chunk when fully replicated (briefly more while surplus copies are trimmed).")
y += 4
timeline("Reachability",
         [('up{job="controller"}', "controller"), ("objstore_database_up", "postgres"),
          ('up{job="workers"}', "{{worker_id}}")], 0, 12, 7, UPDOWN, UPDOWN_STEPS,
         desc="Whether Prometheus can scrape each process (postgres: whether the controller can query it). "
              "A stopped container turns red within one scrape (5 s).")
timeline("Worker state (controller's view)", [(WORKER_STATE_EXPR, "{{worker_id}}")], 12, 12, 7, WSTATE, WSTATE_STEPS,
         desc="HEALTHY / DEGRADED / DEAD as stored in PostgreSQL (DEAD only after the heartbeat timeout, 15 s). "
              "Draining: being emptied by the rebalancer before removal.")
y += 7

# ------------------------------------------------------------------------------------------------ workers
row("Workers")
ts("Heartbeat age", [("objstore_worker_heartbeat_age_seconds", "{{worker_id}}")], 0, 8, unit="s",
   threshold_line=15, fill=0, desc="Seconds since each worker's last heartbeat. Dashed red line = DEAD threshold (15 s).")
ts("Healthy replicas per worker", [('objstore_worker_replicas{state="HEALTHY"}', "{{worker_id}}")], 8, 8, fill=0,
   desc="Chunk copies each worker holds (controller's records). Watch them move when a worker dies and is repaired, "
        "and converge when the rebalancer fills a new worker.")
bargauge("Replica data per worker", [('objstore_worker_replica_bytes{state="HEALTHY"}', "{{worker_id}}")], 16, 8,
         unit="bytes", desc="Bytes of HEALTHY chunk replicas per worker.")
y += 8
ts("Chunk files on disk", [("objstore_chunks_on_disk", "{{worker_id}}")], 0, 8, fill=0,
   desc="Files actually present on each worker volume (worker's own view; includes copies awaiting trim/delete).")
ts("Chunk I/O  ·  write ↑  read ↓",
   [('sum by (worker_id) (rate(objstore_chunk_put_bytes_total[$__rate_interval]))', "{{worker_id}} write"),
    ('-sum by (worker_id) (rate(objstore_chunk_get_bytes_total[$__rate_interval]))', "{{worker_id}} read")],
   8, 8, unit="Bps", min0=False, fill=8, overrides=[color_override(".* read$", "", regexp=True)],
   desc="Bytes/s written (above zero) and read (below zero, dashed) per worker.")
ts("Rebalancing  ·  per min", [("increase(objstore_rebalance_moves_total[1m])", "replicas moved"),
                              ("increase(objstore_rebalance_move_failures_total[1m])", "moves rolled back")],
   16, 8, fill=8, colors=[SERIES[5], CRIT],
   desc="Replicas moved from the fullest to the emptiest worker (e.g. after worker5 joins). Each move copies and "
        "verifies first, then retires the old copy, so a chunk never has fewer than 2 copies. Paused while repairs run.")
y += 8

# ------------------------------------------------------------------------------------------------ replication
row("Replication and repair")
ts("Replication health", [("objstore_under_replicated_chunks", "under-replicated (< 2 copies)"),
                          ("objstore_unavailable_chunks", "unavailable (0 copies)")], 0, 8,
   colors=[WARN, CRIT], desc="Rises when a worker dies or a copy is found bad; falls back to 0 as repairs complete.")
ts("Repair queue", [('objstore_repair_tasks{state="PENDING"}', "pending"),
                    ('objstore_repair_tasks{state="IN_PROGRESS"}', "in progress"),
                    ('objstore_repair_tasks{state="UNRECOVERABLE"}', "unrecoverable")], 8, 8, fill=0,
   colors=[SERIES[0], SERIES[2], CRIT], desc="Durable repair tasks in PostgreSQL by state.")
ts("Repair activity  ·  per min",
   [("increase(objstore_repairs_done_total[1m])", "repairs done"),
    ("increase(objstore_repairs_retried_total[1m])", "attempts retried"),
    ("sum(increase(objstore_chunk_sends_ok_total[1m]))", "worker-to-worker copies"),
    ("increase(objstore_orphan_chunks_deleted_total[1m])", "orphans removed")], 16, 8, fill=8,
   colors=SERIES[:4], desc="Repairs completed; copies are pushed directly between workers on the controller's command.")
y += 8
ts("Replicas not healthy", [('objstore_replicas{state="PENDING"}', "pending (being written)"),
                            ('objstore_replicas{state="CORRUPT"}', "corrupt"),
                            ('objstore_replicas{state="MISSING"}', "missing"),
                            ('objstore_replicas{state="DELETING"}', "deleting")], 0, 8, fill=0,
   colors=SERIES[:4], desc="Replica rows by state, excluding HEALTHY.")
ts("Time to heal", [('histogram_quantile(0.5, sum by (le) (rate(objstore_repair_time_to_heal_seconds_bucket[5m])))', "p50"),
                    ('histogram_quantile(0.95, sum by (le) (rate(objstore_repair_time_to_heal_seconds_bucket[5m])))', "p95")],
   8, 8, unit="s", fill=0, colors=[SERIES[0], SERIES[1]],
   desc="From repair task queued to the chunk having 2 healthy replicas again (5 min window).")
ts("Oldest active repair", [("objstore_repair_oldest_active_task_age_seconds", "age")], 16, 8, unit="s",
   desc="How long the oldest unfinished repair has waited. Keeps growing if no healthy source exists.")
y += 8

# ------------------------------------------------------------------------------------------------ integrity
row("Data integrity")
stat("Corrupt copies detected", "objstore_replicas_marked_corrupt_total", 0, 4, h=5, steps=ZERO_OK_WARN,
     desc="Replicas moved to CORRUPT since the controller started (read-time check, scrubber or repair source check).")
stat("Missing copies detected", "objstore_replicas_marked_missing_total", 4, 4, h=5, steps=ZERO_OK_WARN,
     desc="Replicas moved to MISSING since the controller started (404 on read, scrubber, inventory reconciliation).")
stat("Read fallbacks", "objstore_replica_read_fallbacks_total", 8, 4, h=5,
     desc="Chunk reads that switched to the other replica; the client still got verified bytes.")
bargauge("Since last scrub pass", [("time() - objstore_scrub_last_finished_timestamp_seconds", "{{worker_id}}")],
         12, 6, h=5, unit="s", steps=((None, GOOD), (3600, WARN), (7200, CRIT)), maxv=7200,
         desc="Time since each worker's last complete scrub pass (SCRUB_INTERVAL is 30 min).")
bargauge("Scrub report queue", [("objstore_scrub_report_queue_depth", "{{worker_id}}")], 18, 6, h=5,
         steps=((None, GOOD), (1, WARN)),
         desc="Findings stored durably on the worker, waiting for the controller to acknowledge them.")
y += 5
ts("Corruption and loss events  ·  per min",
   [("increase(objstore_replicas_marked_corrupt_total[1m])", "marked CORRUPT"),
    ("increase(objstore_replicas_marked_missing_total[1m])", "marked MISSING"),
    ("sum(increase(objstore_chunk_get_corrupt_total[1m]))", "worker refused corrupt read"),
    ("sum(increase(objstore_scrub_findings_corrupt_total[1m]))", "scrubber found corrupt"),
    ("sum(increase(objstore_scrub_findings_missing_total[1m]))", "scrubber found missing")], 0, 12, fill=8,
   colors=SERIES[:5], desc="Every path that detects bad data. Each one leads to a repair task.")
ts("Scrubber throughput", [('rate(objstore_scrub_bytes_checked_total[$__rate_interval])', "{{worker_id}}")], 12, 12,
   unit="Bps", stack=True, fill=25,
   desc="Bytes/s re-hashed by the background scrubbers, stacked (rate limited by SCRUB_RATE_MB_S).")
y += 8

# ------------------------------------------------------------------------------------------------ traffic
row("Client traffic")
ts("Throughput", [("rate(objstore_upload_bytes_total[$__rate_interval])", "upload (committed)"),
                  ("rate(objstore_download_bytes_total[$__rate_interval])", "download")], 0, 8, unit="Bps",
   colors=[SERIES[0], SERIES[2]],
   desc="Bytes/s through the controller's client API. Upload bytes are counted when an object commits.")
ts("Requests  ·  per min",
   [("increase(objstore_uploads_committed_total[1m])", "uploads committed"),
    ("increase(objstore_downloads_completed_total[1m])", "downloads completed"),
    ("increase(objstore_uploads_pending_503_total[1m])", "uploads kept pending (503)"),
    ("increase(objstore_downloads_aborted_total[1m])", "downloads aborted"),
    ('sum(increase(objstore_gate_rejected_total{job="controller"}[1m]))', "rejected busy (503)"),
    ("increase(objstore_range_requests_total[1m])", "range requests (206)")], 8, 8, fill=8,
   colors=[SERIES[0], SERIES[2], SERIES[3], SERIES[4], SERIES[1], SERIES[5]])
ts("Latency", [('histogram_quantile(0.5, sum by (le) (rate(objstore_upload_duration_seconds_bucket[2m])))', "upload p50"),
               ('histogram_quantile(0.95, sum by (le) (rate(objstore_upload_duration_seconds_bucket[2m])))', "upload p95"),
               ('histogram_quantile(0.5, sum by (le) (rate(objstore_download_duration_seconds_bucket[2m])))', "download p50"),
               ('histogram_quantile(0.95, sum by (le) (rate(objstore_download_duration_seconds_bucket[2m])))', "download p95")],
   16, 8, unit="s", fill=0, colors=[SERIES[0], SERIES[0], SERIES[2], SERIES[2]],
   overrides=[color_override(".* p95$", "", regexp=True)],
   desc="End-to-end request time (2 min window); p95 dashed. Upload includes replication of every chunk.")
y += 8
ts("Chain replication", [("rate(objstore_chunk_chain_puts_total[$__rate_interval])", "controller sends (one per chunk)"),
                         ("sum(rate(objstore_chunk_forwards_ok_total[$__rate_interval]))", "worker forwards ok"),
                         ("sum(rate(objstore_chunk_forwards_failed_total[$__rate_interval]))", "worker forwards failed")],
   0, 8, unit="ops", fill=0, colors=[SERIES[0], SERIES[2], CRIT],
   desc="The controller uploads each chunk once; the first worker forwards the second copy. "
        "'sends' = 'forwards ok' means no chunk crossed the controller twice.")
ts("Concurrency (controller gates)", [('objstore_gate_inflight{job="controller"}', "{{gate}} in flight"),
                                      ('objstore_gate_waiting{job="controller"}', "{{gate}} waiting"),
                                      ("objstore_active_uploads", "active uploads")], 8, 8, fill=0,
   overrides=[color_override("^uploads .*", SERIES[0], regexp=True),
              color_override("^downloads .*", SERIES[2], regexp=True),
              color_override("^transfers .*", SERIES[1], regexp=True),
              color_override("active uploads", CONTROLLER),
              color_override(".* waiting$", "", regexp=True)],
   desc="Bounded concurrency: in-flight work and requests waiting for a slot (dashed).")
ts("Worker transfer slots", [('objstore_gate_inflight{job="workers"}', "{{worker_id}} in flight"),
                             ('objstore_gate_waiting{job="workers"}', "{{worker_id}} waiting")], 16, 8, fill=0,
   overrides=[color_override(".* waiting$", "", regexp=True)],
   desc="Per-worker bounded concurrency for chunk reads/writes (backpressure). Waiting = dashed.")
y += 8
ts("Hedged reads  ·  per min",
   [("increase(objstore_read_hedges_total[1m])", "hedges sent (first replica slow)"),
    ("increase(objstore_read_hedge_wins_total[1m])", "won by the second replica"),
    ("increase(objstore_replica_read_fallbacks_total[1m])", "fallbacks (first replica failed)"),
    ("increase(objstore_read_hedges_skipped_busy_total[1m])", "hedges skipped (controller busy)")], 0, 12, fill=8,
   colors=[SERIES[0], SERIES[2], SERIES[1], SERIES[3]],
   desc="A chunk read slower than the hedge delay also asks the other replica; the first verified answer wins and "
        "the other request is cancelled. Wins ≈ sent means a replica is slow or hung.")
ts("Hedge delay (adaptive)", [("objstore_read_hedge_delay_seconds", "delay")], 12, 12, unit="s", fill=8,
   desc="p95 of recent chunk reads, clamped to READ_HEDGE_MIN_DELAY..READ_HEDGE_MAX_DELAY: only the slowest ~5% of "
        "reads trigger a second request.")
y += 8

# ------------------------------------------------------------------------------------------------ caches
row("Caches", collapsed=True)
ts("Cache hit ratio",
   [('sum(rate(objstore_cache_hits_total{cache="metadata"}[1m])) / (sum(rate(objstore_cache_hits_total{cache="metadata"}[1m])) + sum(rate(objstore_cache_misses_total{cache="metadata"}[1m])))', "controller metadata"),
    ('sum by (worker_id) (rate(objstore_cache_hits_total{cache="chunk"}[1m])) / (sum by (worker_id) (rate(objstore_cache_hits_total{cache="chunk"}[1m])) + sum by (worker_id) (rate(objstore_cache_misses_total{cache="chunk"}[1m])))', "{{worker_id}} chunks")],
   0, 12, unit="percentunit", fill=0, overrides=[color_override("controller metadata", CONTROLLER)],
   desc="Hits / (hits + misses) over 1 min. Empty when there are no reads.")
ts("Chunk cache memory", [('objstore_cache_bytes{cache="chunk"}', "{{worker_id}}")], 12, 8, unit="bytes", fill=0,
   desc="Bytes held by each worker's RAM chunk cache (budget CHUNK_CACHE_BYTES).")
stat("Metadata cache entries", 'objstore_cache_entries{cache="metadata"}', 20, 4, h=8,
     desc="Objects cached by the controller's metadata cache (META_CACHE_SIZE).")
y += 8


# The DASHED overrides above with an empty colour only set the line style; drop their empty colour property.
def _strip_empty(p):
    for o in p.get("fieldConfig", {}).get("overrides", []):
        props = []
        for prop in o["properties"]:
            if prop["id"] == "color" and not prop["value"]["fixedColor"]:
                props.append(DASHED)
            else:
                props.append(prop)
        o["properties"] = props
    for child in p.get("panels", []):
        _strip_empty(child)


for p in panels:
    _strip_empty(p)

dash = {
    "uid": "objstore", "title": "Object Store", "tags": ["objstore"], "timezone": "browser",
    "schemaVersion": 39, "version": 2, "editable": False, "graphTooltip": 1, "liveNow": False,
    "refresh": "5s", "time": {"from": "now-30m", "to": "now"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
    "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                              "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
                              "name": "Annotations & Alerts", "type": "dashboard"}]},
    "templating": {"list": []}, "panels": panels,
    "links": [{"title": "Traces (Jaeger)", "type": "link", "url": "http://localhost:16686/search?service=controller",
               "icon": "external link", "targetBlank": True,
               "tooltip": "One timeline per upload/download/repair across controller and workers"},
              {"title": "Prometheus", "type": "link", "url": "http://localhost:9090/targets",
               "icon": "external link", "targetBlank": True, "tooltip": "Scrape targets"}],
    "description": "Mini distributed object store: 1 controller, 4 workers, 2 replicas per 1 MiB chunk.",
}
with open(sys.argv[1], "w") as f:
    json.dump(dash, f, indent=1)
print(f"{sum(1 + len(p.get('panels', [])) for p in panels)} panels")
