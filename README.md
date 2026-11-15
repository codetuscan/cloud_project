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
docker compose up -d --build        # builds one image, starts postgres, controller and worker1..worker4
docker compose ps                   # wait until all six containers are "healthy"
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

## 5. Tests and demonstrations

```bash
./scripts/run_unit_tests.sh                 # 60+ pytest unit tests, no cluster needed
./scripts/demo/run_all.sh                   # the six demonstrations required by the course (narrated)
./scripts/phase4_test.sh                    # end-to-end checks per phase (phase2_3 … phase10_persistence)
./scripts/run_all_tests.sh                  # everything (~40 min)
./scripts/cache_benchmark.sh                # cold vs warm vs caches-off read latency
```

The demos map one-to-one to the required demonstrations in the assignment; see [DEMO.md](DEMO.md).

## 6. Repository layout

```
docker-compose.yml  Dockerfile  .env.example  requirements.txt  pytest.ini
migrations/         001 schema · 002 idempotency key · 003 scrub statistics
app/common/         config, errors, checksum, chunking maths, LRU cache, gate (backpressure), metrics
app/controller/     api.py (public API) · upload.py · placement.py · repair.py · reaper.py · health.py
                    meta_cache.py · replicas.py · internal_api.py · multipart_stream.py · store.py · main.py
app/worker/         api.py (chunk API) · disk.py (atomic storage) · scrub.py · cache.py · agent.py · main.py
scripts/            phase*_test.sh · demo/ · run_all_tests.sh · clean_start.sh · cache_benchmark.sh
tests/              pytest unit tests
```

## 7. Scope and limitations (by design)

* **One controller**, no Raft/consensus. If the controller process restarts, metadata (PostgreSQL) and staged uploads
  (persistent staging volume) survive; loss of the *host or its disks* is not covered.
* Two replicas tolerate the loss or corruption of **one** copy per chunk, not of both at once.
* No authentication, versioning, erasure coding, multi-region or S3 protocol compatibility.
* All four workers share one physical disk here, so reported free space is identical for all of them.
