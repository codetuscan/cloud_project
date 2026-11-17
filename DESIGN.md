# Design

## 1. Principles

1. **One authority.** PostgreSQL (written only by the controller) is the single source of truth for objects,
   chunks, worker membership, replica locations/health and repair state. Caches never decide anything.
2. **Workers observe, the controller decides.** A worker stores chunks, scrubs them and *reports* what it sees.
   It never changes global replica state or decides which copy is right.
3. **Checksums come from the data's owner.** Each chunk's SHA-256 is computed once, at upload, from the staged bytes
   and stored in PostgreSQL. It is never recomputed from a replica, so a damaged copy can never vouch for itself.
4. **Acknowledge only what is durable.** A worker answers a write only after the bytes are verified, `fsync`ed and
   atomically renamed into place. The controller marks a replica healthy only after that acknowledgement.
5. **Bounded memory and concurrency.** Data is streamed in pieces; at most one chunk-sized buffer exists per
   in-flight transfer, and every kind of concurrent work has a limit.

## 2. Topology

Docker Compose project `objstore`: `postgres`, `controller`, `worker1..worker4`, one private bridge network, and
volumes `pgdata`, `staging` (controller) and `worker1_data..worker4_data` (one per worker — workers never share a
directory). Only the controller's port is published (host 8090). One Docker image runs every role.

## 3. Data model (`migrations/`)

| Table | Purpose |
|---|---|
| `objects` | One row per object: name, size, content type, whole-object checksum, chunk count and size, `user_metadata` (jsonb), timestamps, `idempotency_key`; **state** `PENDING → COMMITTED → DELETING → DELETED` |
| `chunks` | Manifest: `chunk_id` (`<object_id>-<6-digit index>`), `chunk_index` (order), size, **authoritative checksum** |
| `workers` | Address, state `HEALTHY/DEGRADED/DEAD`, last heartbeat, capacity |
| `replicas` | Key `(chunk_id, worker_id)` — so a worker can hold at most one copy of a chunk. State `PENDING/HEALTHY/CORRUPT/MISSING/DELETING` |
| `repair_tasks` | Durable repair queue. A **partial unique index** allows only one active (`PENDING`/`IN_PROGRESS`) task per chunk, so any component may queue a repair without coordination. States also `DONE`, `UNRECOVERABLE`; attempts, backoff, last error, source/target worker |
| `uploads` | Staged files awaiting commit (lets the controller resume after a crash) |
| `tombstones` | `(chunk_id, worker_id)` of deleted replicas, to stop stale inventories resurrecting them |
| `scrub_results`, `worker_scrub_state` | Scrubber findings (unique per worker/chunk/time → idempotent) and per-worker last-pass statistics |

A chunk is **protected** when it has two `HEALTHY` replicas on two different *live* workers. That is computed, not stored.

## 4. Upload (`app/controller/api.py`, `upload.py`, `multipart_stream.py`)

1. Admission control (bounded concurrent uploads, size cap, staging-space check).
2. **The database is told first.** Before any byte is stored, one transaction inserts the `objects` row (`PENDING`,
   placeholder name/size) and the `uploads` row (`STREAMING`), with the `Idempotency-Key`.
3. The multipart body is parsed incrementally and the `file` part is written straight to a **persistent staging
   file** `<id>.uploading` (never into RAM, never into an ephemeral temp dir) while the whole-object SHA-256 is
   computed; the file is fsynced when the part ends. Then **one transaction** writes the real name, size, content type,
   checksum, chunk count and custom metadata *and* sets `uploads.state='STAGED'`. That is the commit point of the
   upload: before it the attempt can be discarded, after it the controller promises to finish it. Only then is the file
   renamed `*.uploading → *.staged` (housekeeping). A client disconnect or error before the commit point removes the
   row and the partial file.
4. `commit_upload`: a small pool (≤ 4) of tasks reads the staged file one chunk at a time (`pread` at
   `index × chunk_size`), hashes it, records the chunk and picks **two distinct HEALTHY workers** (least loaded first).
   It uploads the chunk **once**, to the first worker, naming the second in `X-Forward-To`. That worker verifies,
   fsyncs and renames its copy, **forwards the same bytes to the second worker** (chain replication), and acknowledges
   only when it knows the outcome of both copies (`replicas: [{worker_id, ok}, …]`). The controller marks each replica
   `HEALTHY` from that reply. If only one copy failed, the next round asks for just the missing one; if the first
   worker is unreachable, the second is not blamed and the pair is re-picked.
5. When every chunk has two `HEALTHY` replicas on different workers, one transaction sets `COMMITTED`; the staged
   file is deleted (cleanup policy: remove after commit, abort after `UPLOAD_ABANDON_SECONDS`).
6. If it cannot reach two replicas the client gets `503` but the object stays `PENDING` in staging; a resume loop
   (also run at startup) finishes it later. Work is idempotent (chunk ids are deterministic, a chunk that already has
   two healthy replicas is skipped, `PUT` of identical bytes is a no-op), so retries never duplicate chunks.
   `Idempotency-Key` prevents duplicate *objects* from client retries.

Crash safety of staging: a complete staged file always has a complete row, and a `STREAMING` row never promises a
file. At start-up, before the resume loop, `recover_staging` drops `STREAMING` rows and their partial files, finishes a
rename interrupted after the commit point, and deletes staging files that have no row. A retry that finds a dead
`STREAMING` row holding its `Idempotency-Key` (not in `active_uploads`; valid because there is one controller) clears
it instead of answering 409; two simultaneous uploads with one key are serialised by the key's unique index (409).
Tested by `scripts/phase11_upload_crash_test.sh`.

Durability limits (as planned): a controller *process/container* restart loses nothing; loss of the controller
*host disk* loses data that is only staged.

## 5. Download

Manifest and replica map come from the metadata cache or PostgreSQL. For each chunk, in order: pick a healthy replica
on a live worker, fetch it, **verify size and SHA-256 against the authoritative checksum**, send it. A replica that is
unreachable is skipped; a replica that is missing/corrupt is skipped *and reported* (`mark_bad` → state `CORRUPT` /
`MISSING` + a repair task). If no replica of a chunk is readable the response is `503` before streaming starts when
that is known in advance, otherwise the connection is aborted (headers already sent) — a client never receives a
silently truncated, reordered or corrupted object.

## 6. Failure handling and repair (`repair.py`, `health.py`)

* **Heartbeats** (worker → controller every 5 s). No heartbeat for 15 s ⇒ `DEAD`; a heartbeat revives it.
  Heartbeat times are stored in PostgreSQL, so they keep ageing while the *controller* is down. To avoid declaring
  every worker dead on restart, the controller first **probes each known worker (`GET /healthz`) at start-up** and
  refreshes the ones that answer; only workers that really do not respond are left for the monitor to mark `DEAD`.
  After a PostgreSQL outage the monitor additionally waits one full timeout before judging anyone.
* **Under-replication scanner** (every 10 s): chunks of committed objects with fewer than two healthy replicas on live
  workers get one repair task each. Surplus replicas (e.g. a revived worker after its chunks were re-replicated) are
  trimmed, removing the copy on the most loaded worker.
* **Repair runners** claim tasks with `SELECT … FOR UPDATE SKIP LOCKED` and **command a healthy source worker to push
  the chunk straight to the target** (`POST /internal/chunks/{id}/send` on the source; no chunk bytes pass through the
  controller). The source verifies the chunk against its own record *and* the controller's checksum, then
  `PUT … ?repair=true` to the target (repair in place on the worker with the bad copy first, otherwise a new worker
  that holds no copy). The target verifies the checksum, fsyncs and renames; only after that acknowledgement does the
  controller mark the replica `HEALTHY`. The source reports *why* a copy failed (`source_missing`, `source_corrupt`,
  `target_failed`), so a bad source is marked and another is tried, while a bad target is just retried. Failures back
  off exponentially; with no live source but a copy on a *dead* worker the task waits for that worker; if no healthy
  copy exists anywhere the task becomes `UNRECOVERABLE` (reported, never fabricated). Tasks left `IN_PROGRESS` by a
  crashed controller return to the queue.
* **Inventory reconciliation** (every 30 s, per live worker): compares the worker's chunk list with PostgreSQL.
  Expected-but-absent ⇒ `MISSING` + repair. Files of deleted objects (tombstones) are removed at once. Unknown files are
  *orphans*, removed only after a grace period and only if the metadata still has no replica row for them. The DB rows
  are read *before* the inventory is fetched so an in-flight write can never be mistaken for a loss.
* With fewer than two eligible workers the system reports degraded protection (`under_replicated_chunks`, pending
  tasks with `last_error`) and retries when capacity returns.
* Copies whose repair or rebalance move was cut off by a controller restart (a `PENDING` replica row of a committed
  object older than the repair lease) are retired by the scanner; the reaper deletes the file.

## 6a. Rebalancing and scaling (`rebalance.py`)

Placement fills the least-loaded worker with *new* chunks, but stored data would stay where it is. When a new or empty
worker joins, it would hold nothing old for a long time. The **rebalancer** (every 15 s) closes that gap:

* **When:** the replica counts of the HEALTHY, non-draining workers differ by more than `REBALANCE_TOLERANCE` (2).
  It waits while repair has work: any active repair task, or any chunk that does not have exactly two healthy copies on
  live workers (under-replicated, or a surplus copy waiting to be trimmed). Repair always comes first.
* **What:** a greedy plan moves one replica at a time from the fullest worker to the emptiest one with free space, up to
  `REBALANCE_BATCH` (16) moves per round, 2 at a time. It only picks chunks that are completely stable: two healthy
  copies, no other copy in flight, and no repair task. The receiving worker must not already hold the chunk.
* **How (never fewer than two copies):**
  1. A `PENDING` replica row is created on the receiving worker.
  2. The donor worker pushes the chunk directly to the receiver with the same verified `/send` command that repair uses.
  3. In **one transaction** the new copy becomes `HEALTHY` and the donor's copy becomes `DELETING`, but only if the
     chunk then has at least three healthy copies. The reaper then deletes the donor's file.

  A failed copy is rolled back, and the donor keeps its copy.
* **Races:**
  - If the object is deleted during a move, deletion has already turned every replica row, including the new
    `PENDING` one, into `DELETING`. Step 3 then changes nothing and the reaper removes the copy.
  - If the other copy goes bad during the move, the healthy count is only 2, so the donor's copy is kept.
* **Scale in:** `POST /internal/workers/{id}/drain` stops placement on the worker. The rebalancer moves its replicas
  away first, each one to the least-loaded worker that does not already hold that chunk, with the same copy → verify →
  retire steps. Once it holds nothing, the container can be stopped. Its `DEAD` state then triggers no repair at all.
  `DELETE /internal/workers/{id}` then removes it from the registry. Removal is refused while the worker is alive, and
  while removing it would leave any chunk with fewer than two healthy copies elsewhere. A draining flag survives
  restarts (column `workers.draining`, migration 004).
* **Measured** (demo 7): with 4 workers × 53 replicas, a new worker5 received 41 replicas in about 12 s. Every chunk
  stayed at exactly two healthy copies, the donors' files were deleted, and all objects downloaded byte-identical.
  Draining it moved all of its replicas back in about 16 s, and stopping it then caused no repair.

## 7. Data integrity (`scrub.py`)

Three independent layers, because each catches something the others cannot:

| Layer | Catches |
|---|---|
| Write verification (worker) | Corruption in transit / during the write |
| **Read-time verification** (worker + controller) | Corruption met during normal reads; the bad replica is skipped and reported |
| **Background scrubber** (each worker) | Latent corruption in chunks nobody reads |
| Inventory reconciliation (controller) | Missing files, lost workers, orphans, stale inventories |

The scrubber hashes each chunk file with 64 KiB reads, one chunk at a time, throttled to `SCRUB_RATE_MB_S`, started at
a per-worker offset so the four workers don't scan simultaneously. It compares against the worker's local checksum
record; on disagreement it asks the controller for the authoritative value (so a damaged *record* isn't mistaken for
damaged *data*). A file deleted while its record survives is `MISSING` (only if the record is older than 30 s, to avoid
racing a write). Findings are written to a **durable queue** (fsynced file per report in `/data/report_queue`) and
removed only when the controller acknowledges them, so a controller outage or worker restart loses nothing.

The controller handles `POST /internal/workers/scrub-results` idempotently: it validates the worker and the replica,
ignores stale reports (older than the replica's last write), ignores `CORRUPT` reports whose bytes actually match the
authoritative checksum, and otherwise marks the replica and queues a repair.

## 8. Caching (`lru.py`, `meta_cache.py`, `worker/cache.py`)

Caches are optimisations only. One bounded LRU implementation (entry limit, byte budget, optional TTL, statistics) with
**stale-fill protection**: a reader takes a token before consulting the source of truth and its insert is rejected if the
key was invalidated meanwhile.

* **Controller metadata cache** (1000 entries, 30 s TTL): manifest + replica map per committed object. Invalidated on
  commit, PATCH, delete, every replica state change (reads, scrub reports, repairs, trimming, reaping) and every
  worker state change.
* **Worker chunk cache** (128 MiB): holds checksum-verified immutable chunk bytes. Filled on cache-miss reads and
  written through *after* a durable write; evicted on delete and when a chunk is found corrupt/missing. Eviction never
  touches the file; a worker restart clears it. Writes are never acknowledged from RAM.
* Trade-off: a cache hit skips the disk, so silent disk corruption of a *cached* chunk is only found by the scrubber
  (which evicts it). The bytes served are the previously verified good ones.

## 9. Deletion (`reaper.py`)

`DELETE` atomically sets `DELETING` (the object vanishes from every API at once), inserts tombstones, marks replicas
`DELETING` and cancels repairs. The reaper removes each replica from its worker (retrying while a worker is down),
and marks the object `DELETED` only when *every* replica is confirmed gone. Tombstones and the tombstoned object row are
kept 24 h so a returning worker's stale inventory cannot bring data back. The reaper also aborts uploads stuck
`PENDING` for over an hour.

## 10. Backpressure and concurrency

`Gate` (a counting semaphore that records in-flight/peak/waiting/rejected) bounds: chunk transfers (controller-wide and
per worker), concurrent uploads, concurrent downloads. Requests wait up to `ADMISSION_TIMEOUT` and then get
`503 server_busy` with `Retry-After`. Repair runners (2) and the scrubber (1 chunk at a time, rate-limited) are bounded
background work, so foreground reads/writes keep priority while repairs still make progress. Memory therefore depends on
concurrency limits, not object size — measured in `scripts/phase9_test.sh` (controller peak RSS +12 MiB for a 384 MiB
upload; a worker is bounded by its cache budget).

## 10a. Monitoring (`prom_metrics.py`, `monitoring/`)

Prometheus (`prometheus` container, host port 9090) scrapes `GET /metrics` on the controller and on each worker every
5 s; Grafana (`grafana`, host port 3000) has the data source and the "Object Store" dashboard provisioned from files, so
`docker compose up` needs no manual setup. The exposition format is written by hand (`app/common/prom.py`, no new
dependency) from the same in-process counters that `/internal/metrics` returns as JSON.

* **Two views of worker health.** `up{job="workers"}` is Prometheus's own reachability (a stopped container is DOWN within one
  scrape); `objstore_worker_state` is the controller's decision from heartbeats (DEAD after 15 s). Seeing both on one
  timeline shows the detection delay.
* **Cluster gauges come from PostgreSQL at scrape time**, never from a cache: under-replicated and unavailable chunks,
  replicas per worker and state, repair queue, objects. The scrape is a handful of `COUNT` queries.
* **Counters reset on restart**; Prometheus's `rate()`/`increase()` handle resets. Known counters are declared at 0 at
  start-up so panels show 0, not "no data".
* **Histograms**: end-to-end upload and download time, and *time to heal* (repair task queued → chunk has 2 healthy
  replicas again).
* `worker_id` labels: the controller emits them for per-worker gauges; for worker metrics Prometheus attaches them per
  scrape target, so both line up in Grafana.
* **Dashboard design.** The JSON is generated by `monitoring/grafana/generate_dashboard.py`. Each worker has one fixed
  colour in every panel; other series use a fixed colour order. These colours were checked for colour-blind contrast
  on the dark panel background. Green, amber and red are used only for health state. Status tiles stay neutral while
  everything is fine. Each panel has one y-axis. Dashed lines (worker4, read, waiting, p95) give a second cue so no
  series depends on colour alone.
* Monitoring is optional: nothing in the object store depends on Prometheus or Grafana.

## 11. Decisions on open points from the plan

| Question | Decision |
|---|---|
| "1 MB" | 1 MiB = 1,048,576 bytes, one setting (`CHUNK_SIZE`) |
| Language | Python 3.12 (FastAPI, asyncpg, httpx) |
| Upload replication | Chain: the controller sends each chunk to one worker, which forwards it to the second (halves the controller's upload bandwidth). The forwarding worker releases its own transfer slot first, so two workers forwarding to each other cannot deadlock |
| Repair transfer | Controller-commanded, worker-to-worker: the source pushes straight to the target and the target verifies against the controller's checksum, so the controller never carries chunk data and a bad copy can never become `HEALTHY` |
| Chunk id | `<object_id>-<index>`: unique, ordered, makes resume and cache-invalidation trivial |
| Replica placement | Least-loaded first among HEALTHY, non-draining workers with free space, always two distinct workers; the rebalancer evens out existing data |
| Adding / removing workers | Join: register, then rebalance (copy → verify → retire). Leave: drain, stop, forget. A move never lowers a chunk below two healthy copies |
| Orphan policy | Never deleted immediately; tombstoned chunks at once, unknown ones after a grace period |
| Dead worker with the only copy | Repair waits for it to return rather than declaring loss |

## 12. Known limitations

Single controller (a coordination SPOF by requirement); staged data is lost with the controller's disk; two replicas
survive one loss per chunk; no authentication; all four workers in this deployment share one physical disk, so free
space readings are identical; workers accept `X-Forward-To` / `/send` commands from anyone on the internal network (no auth); a cache hit hides silent disk
corruption of that chunk until the next scrub.
