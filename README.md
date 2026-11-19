# Mini Distributed Cloud Object Store

A small distributed object store in the spirit of HDFS / GFS / S3, written in Python and run with
Docker Compose. It stores arbitrary binary objects, cuts them into **1 MiB chunks**, keeps **two copies of
every chunk on two different workers**, and detects and repairs lost or corrupted copies.

| Component | Count | Role |
|---|---|---|
| Controller | 1 | Public REST API, metadata authority, placement, repair coordination |
| Storage workers | 4 | Store chunk files on their own persistent volume, scrub them, report problems |
| PostgreSQL | 1 | Durable metadata (objects, chunks, replicas, repair tasks) |
| Clients | 1 or more | Anything that speaks HTTP (`curl`, scripts, …) |

Detailed documents: [API.md](API.md) (every endpoint) · [DESIGN.md](DESIGN.md) (architecture and decisions) ·
[DEMO.md](DEMO.md) (the demonstrations required by the course).

```
                      clients (curl, …)
                            │  REST /api/v1/objects…            host port 8090
                            ▼
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ controller   API · placement · repair manager · reconciler · reaper · cache │──► PostgreSQL
 │              staging volume (uploads wait here until committed)             │     (metadata)
 └──────┬───────────────┬───────────────┬───────────────┬─────────────────────┘
        │ /internal/*   │               │               │      private Docker network
   ┌────▼───┐      ┌────▼───┐      ┌────▼───┐      ┌────▼───┐
   │worker1 │      │worker2 │      │worker3 │      │worker4 │   each: own volume, chunk files,
   │ volume │      │ volume │      │ volume │      │ volume │   scrubber, RAM chunk cache
   └────────┘      └────────┘      └────────┘      └────────┘
```

## 1. Requirements

* Docker with the Compose plugin (v2), ~2 GB free disk
* `bash`, `curl`, and `python3` on the host (only the helper/test scripts use `python3`)

## 2. Build and start

```bash
git clone <this repo> && cd cloud_project
docker compose up -d --build        # builds one image, starts postgres, controller, worker1..worker4,
                                    # and the monitoring stack (prometheus, grafana, jaeger)
docker compose ps                   # wait until the six object-store containers are "healthy"
curl -s localhost:8090/internal/status | python3 -m json.tool | head -30
```

The controller is published on **host port 8090** (change it with `CONTROLLER_PORT`). Workers and PostgreSQL are
only reachable inside the private Docker network. To talk to a worker use
`docker exec worker1 curl -s localhost:9000/...`.

Fresh start (⚠ deletes every stored object): `./scripts/clean_start.sh`. Stop: `docker compose down`
(data stays in volumes); `docker compose down -v` also deletes the volumes.

## 3. Using it

```bash
API=http://localhost:8090

# upload (multipart: required field "file", optional field "metadata" with a JSON object)
curl -s -F file=@photo.jpg -F 'metadata={"owner":"alice","project":"cloud"}' $API/api/v1/objects
#   -> 201 {"object_id":"3f0c…","name":"photo.jpg","size":…,"chunk_count":…,"checksum":"sha256:…",…}

OID=<object_id from the response>
curl -s $API/api/v1/objects/$OID -o photo_copy.jpg                       # download
curl -s -r 0-1023 $API/api/v1/objects/$OID -o first_kib.bin               # partial download (Range -> 206)
curl -s $API/api/v1/objects/$OID/metadata | python3 -m json.tool          # system + custom metadata + chunk placement
curl -s -X PATCH -d '{"owner":"bob","tag":"v2","project":null}' \
     $API/api/v1/objects/$OID/metadata                                     # add/update keys; null removes a key
curl -s "$API/api/v1/objects?limit=20"                                     # list (pagination: next_cursor → &cursor=…)
curl -s -X DELETE $API/api/v1/objects/$OID                                 # 204
```

Errors are JSON: `{"error":{"code":"not_found","message":"object not found"}}` with status 400 / 404 / 409 /
500 / 503. See [API.md](API.md).

### Look inside the cluster

```bash
curl -s localhost:8090/internal/status | python3 -m json.tool        # workers, replicas, repairs, caches, scrub results
curl -s "localhost:8090/internal/status?detail=true"                  # + each worker's own view
curl -s localhost:8090/internal/metrics                               # counters, in-flight limits
docker exec worker1 curl -s localhost:9000/internal/workers/worker1/chunks   # chunk files on worker1
docker exec worker1 curl -s -X POST localhost:9000/internal/scrub/trigger    # scrub worker1 now
docker compose logs -f controller                                     # repair / DEAD / scrub activity
```

### Monitoring dashboard (Prometheus + Grafana)

| What | URL |
|---|---|
| Grafana dashboard "Object Store" (opens without login, read-only; admin / `GRAFANA_ADMIN_PASSWORD` to edit) | http://localhost:3000 |
| Prometheus (raw queries, target health under *Status → Targets*) | http://localhost:9090 |
| Prometheus alerts (rules in `monitoring/alerts.yml`) | http://localhost:9090/alerts |
| Jaeger: request traces, one timeline per upload/download/repair across controller and workers | http://localhost:16686 |
| Raw metrics, Prometheus text format | `curl -s localhost:8090/metrics` · `docker exec worker1 curl -s localhost:9000/metrics` |

Prometheus scrapes the controller and the four workers every 5 s. The dashboard (refreshes every 5 s) shows, in rows:
alerts (firing count and a table of active alerts with what each one means) · cluster overview (workers alive, under-replicated/unavailable chunks, objects, reachability and worker-state
timelines) · workers (heartbeat age, replicas and bytes per worker, chunk I/O) · replication and repair (repair queue,
repairs per minute, time to heal) · data integrity (corrupt/missing copies detected, scrubber activity, report queue) ·
client traffic (throughput, request rates, p50/p95 latency, chain replication) · caches (hit ratio, memory;
collapsed by default). Each worker keeps the same colour in every panel; amber and red appear only when something
needs attention.
Keep it open while running `./scripts/demo/05_worker_failure.sh` or `06_data_integrity.sh` to watch a failure being
detected and repaired. The object store does not depend on the monitoring containers; they can be stopped at any time.

### Alerts, traffic generator and chaos

**Alerts.** Prometheus evaluates 17 rules from `monitoring/alerts.yml` every 5 s. A firing alert is drawn across every
graph of the dashboard: a red band for critical, amber for warning, a short blue marker for info, each labelled with
the alert and worker (e.g. `WorkerDown worker2`). The "Active alerts" table at the top lists what is firing now and
since when. No Alertmanager is configured: nothing is emailed or paged.

| Severity | Alerts |
|---|---|
| critical | `ControllerDown`, `DatabaseDown`, `WorkerDown`, `ChunksUnavailable` (a chunk has no readable copy), `DataLost` |
| warning | `ChunksUnderReplicated` (for > 1 min), `RepairStuck` (> 5 min), `ScrubOverdue` (no pass in 2 × `SCRUB_INTERVAL`), `ScrubReportsUndelivered`, `WorkerNotResponding`, `WorkerLowDisk`, `UploadsCannotReplicate`, `ServerBusy` |
| info | `CorruptReplicaDetected`, `MissingReplicaDetected`, `ReplicaSlow` (reads served by the hedge), `WorkerDrainedAndStopped` |

The rules have unit tests (no cluster needed):
`docker run --rm --entrypoint promtool -v $PWD/monitoring:/m:ro -w /m prom/prometheus:v2.53.0 test rules alerts_test.yml`

**Traffic generator.** `python3 scripts/load.py` keeps a few clients uploading, downloading, reading ranges and
deleting, so the graphs move. Every download is checked byte for byte against what was uploaded, and it prints a line
every 5 s (ops/s, failures and why, p50/p95). Ctrl-C prints a summary and deletes its objects (`load-*`).
Options: `--clients 6`, `--duration 300`, `--max-size 8` (MiB), `--objects 25`, `--keep`, `--cleanup`.

**Chaos.** `./scripts/chaos.sh` injects one fault at a time, waits for the system to detect and heal it, prints the
times and moves on (default: a random fault every 60 s until Ctrl-C).

| Fault | What it does | What you should see |
|---|---|---|
| `stop` | `docker stop` a random worker for 45 s | `WorkerDown` after ~15 s, chunks re-replicated with it still down, extra copies trimmed after it returns |
| `pause` | freeze a worker for 8 s | reads answered by the other copy (`ReplicaSlow`), worker not marked DEAD |
| `corrupt` | overwrite 5 bytes in one chunk file | `CorruptReplicaDetected`, copy rebuilt from the healthy replica in seconds |
| `delete` | remove one chunk file | `MissingReplicaDetected`, file restored |
| `controller` | `docker restart controller` | API back in ~2 s, no data lost |

`corrupt` and `delete` only touch a test object the script uploads itself and verifies after every fault, never your
own objects. Each fault is a purple band on the dashboard from injection to healed. Ctrl-C restarts or unpauses anything it
touched and prints a summary table. Options: `--faults corrupt,delete`, `--rounds 5`, `--interval 30`,
`--downtime 60`, `--list`. A good demo: run both side by side with the dashboard open.

```bash
python3 scripts/load.py &          # traffic (or in a second terminal)
./scripts/chaos.sh --rounds 5      # five faults, each detected and healed
```

### Request tracing (OpenTelemetry + Jaeger)

Open **http://localhost:16686** and pick service `controller`.
* **What a trace is:** every upload, download, repair, rebalance move and scrub report is one trace, shown as a timeline
  across processes. For an upload you see: receive → staging file, then for each chunk the PUT to the first worker →
  its write + fsync → its forward to the second worker → that worker's write + fsync, then `mark COMMITTED`.
  PostgreSQL queries appear as child spans too.
* **Finding a request:** every traced response carries `X-Trace-Id`. Paste it into Jaeger's search box.
  Controller log lines written during a trace show the same `trace=<id>`.

```bash
curl -s -D - -o /dev/null -F file=@photo.jpg localhost:8090/api/v1/objects | grep -i x-trace-id
```
Heartbeats, health probes and other background polling are not traced, so Jaeger only lists real operations.
Turn tracing off with `TRACING_ENABLED=false`. The object store does not depend on Jaeger: if Jaeger is down, spans are
dropped.

### Add or remove a worker (rebalancing)

```bash
docker compose up -d worker5                                   # a 5th, empty worker joins (compose profile "scaleout")
curl -s localhost:8090/internal/status | python3 -c "import json,sys; print(json.load(sys.stdin)['rebalance'])"
#   -> after ~30 s of stable membership, replicas are moved onto worker5 until all workers hold about the same number
curl -s -X POST localhost:8090/internal/workers/worker5/drain    # move everything off it again, while it is still up
docker compose rm -sf worker5                                    # once it holds 0 replicas: stop it (nothing to repair)
curl -s -X DELETE localhost:8090/internal/workers/worker5        # after ~15 s (DEAD): remove it from the cluster
```
`./scripts/demo/07_scale_out_and_in.sh` runs and checks this whole cycle.

### Try the failure scenarios by hand

```bash
docker stop worker2          # controller marks it DEAD after ~15 s, re-replicates its chunks, downloads keep working
docker start worker2         # it returns with its persistent chunks; surplus copies are trimmed
# corrupt one stored copy and let the scrubber find it:
docker exec worker1 sh -c 'printf BAD | dd of=/data/chunks/<xx>/<chunk_id> bs=1 seek=100 conv=notrunc'
docker exec worker1 curl -s -X POST localhost:9000/internal/scrub/trigger
```

## 4. Configuration (environment variables, set in `.env` or the shell)

Copy `.env.example` to `.env` to change defaults; `docker-compose.yml` passes them on.

| Variable | Default | Meaning |
|---|---|---|
| `CONTROLLER_PORT` | 8090 | Host port of the REST API |
| `CHUNK_SIZE` | 1048576 | Chunk size in bytes (**1 MiB**; use `1000000` if "1 MB" must mean decimal) |
| `HEARTBEAT_INTERVAL` / `HEARTBEAT_TIMEOUT` | 5 / 15 s | Worker heartbeat period / time until a worker is DEAD |
| `SCRUB_INTERVAL` | 1800 s | Time between scrub passes (start staggered per worker) |
| `SCRUB_RATE_MB_S` | 25 | Scrub read-rate limit per worker |
| `RECONCILE_INTERVAL` | 30 s | Inventory reconciliation period |
| `ORPHAN_GRACE_SECONDS` | 120 | How long an unknown chunk file is tolerated before removal |
| `REPAIR_CONCURRENCY` / `REPAIR_SCAN_INTERVAL` | 2 / 10 s | Parallel repairs / under-replication scan period |
| `META_CACHE_ENABLED` / `_SIZE` / `_TTL` | true / 1000 / 30 s | Controller metadata cache |
| `CHUNK_CACHE_ENABLED` / `_BYTES` | true / 134217728 | Worker chunk cache (128 MiB) |
| `MAX_CONCURRENT_TRANSFERS` | 8 | Chunk transfers in flight (per controller, per worker) |
| `MAX_CONCURRENT_UPLOADS` / `_DOWNLOADS` | 4 / 16 | Client requests served at once |
| `ADMISSION_TIMEOUT` | 30 s | How long a request waits for a slot before `503 server_busy` |
| `MAX_UPLOAD_BYTES` | 21474836480 | Largest accepted upload (400 `too_large` above it) |
| `UPLOAD_ABANDON_SECONDS` / `TOMBSTONE_RETENTION_SECONDS` | 3600 / 86400 | Cleanup policies |
| `READ_HEDGE_ENABLED` / `READ_HEDGE_DELAY` | true / 0 | Ask the 2nd replica when the 1st is slow; delay in s, 0 = adaptive (p95 of recent reads, 20 ms–1 s) |
| `READ_AHEAD_CHUNKS` | 2 | Chunks fetched ahead of the one being sent during a download |
| `REBALANCE_ENABLED` / `_INTERVAL` / `_BATCH` | true / 15 s / 16 | Rebalancer: on/off, round period, moves per round |
| `REBALANCE_TOLERANCE` / `_TOLERANCE_PCT` / `_SETTLE_SECONDS` | 4 / 10 / 30 s | Allowed replica-count difference between workers: the larger of 4 replicas or 10 % of the average. Data only moves after worker membership has been stable for 30 s |
| `PROMETHEUS_PORT` / `GRAFANA_PORT` / `GRAFANA_ADMIN_PASSWORD` | 9090 / 3000 / admin | Monitoring stack |
| `TRACING_ENABLED` / `JAEGER_PORT` | true / 16686 | OpenTelemetry tracing to Jaeger; Jaeger UI port |

## 5. Tests and demonstrations

```bash
./scripts/run_unit_tests.sh                 # 150 pytest unit tests, no cluster needed
./scripts/demo/run_all.sh                   # the six demonstrations required by the course (narrated)
./scripts/demo/07_scale_out_and_in.sh       # extra: add worker5, rebalance, drain, remove
./scripts/demo/08_slow_worker_and_ranges.sh # extra: hung worker -> hedged reads; partial downloads
./scripts/phase4_test.sh                    # end-to-end checks per phase (phase2_3 … phase10_persistence)
./scripts/run_all_tests.sh                  # everything (~40 min)
./scripts/cache_benchmark.sh                # cold vs warm vs caches-off read latency
```

The demos map one-to-one to the required demonstrations in the assignment; see [DEMO.md](DEMO.md).

## 6. Repository layout

```
docker-compose.yml  Dockerfile  .env.example  requirements.txt  pytest.ini
migrations/         001 schema · 002 idempotency key · 003 scrub statistics · 004 draining flag
app/common/         config, errors, checksum, chunking maths, LRU cache, gate (backpressure), metrics, prom (exposition),
                    tracing (OpenTelemetry setup, sampler, span helpers)
app/controller/     api.py (public API) · upload.py · placement.py · repair.py · reaper.py · health.py
                    meta_cache.py · replicas.py · internal_api.py · multipart_stream.py · store.py · main.py
                    reads.py (hedged reads, read-ahead, Range) · rebalance.py (moves replicas onto new workers,
                    drains leaving ones) · prom_metrics.py (/metrics)
app/worker/         api.py (chunk API) · disk.py (atomic storage) · scrub.py · cache.py · agent.py · main.py
                    prom_metrics.py (/metrics)
monitoring/         prometheus.yml · alerts.yml (alert rules) + alerts_test.yml · grafana/provisioning
                    grafana/dashboards/objstore.json · grafana/generate_dashboard.py (regenerates the JSON)
scripts/            phase*_test.sh · demo/ · load.py (traffic) · chaos.sh (fault injection) · run_all_tests.sh ·
                    clean_start.sh · cache_benchmark.sh
tests/              pytest unit tests
```

## 7. Scope and limitations (by design)

* **One controller**, no Raft/consensus. If the controller process restarts, metadata (PostgreSQL) and staged uploads
  (persistent staging volume) survive; loss of the *host or its disks* is not covered.
* Two replicas tolerate the loss or corruption of **one** copy per chunk, not of both at once.
* No authentication, versioning, erasure coding, multi-region or S3 protocol compatibility.
* All four workers share one physical disk here, so reported free space is identical for all of them.
