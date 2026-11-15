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
2. The multipart body is parsed incrementally and the `file` part is written straight to a **persistent staging
   file** (never into RAM, never into an ephemeral temp dir) while the whole-object SHA-256 is computed.
3. In one transaction: `objects` row `PENDING` + `uploads` row; the file is renamed `*.uploading → *.staged`.
4. `commit_upload`: a small pool (≤ 4) of tasks reads the staged file one chunk at a time (`pread` at
   `index × chunk_size`), hashes it, records the chunk, picks **two distinct HEALTHY workers** (least loaded first),
   `PUT`s the chunk to both in parallel with retries, and on failure fails over to another worker.
5. When every chunk has two `HEALTHY` replicas on different workers, one transaction sets `COMMITTED`; the staged
   file is deleted (cleanup policy: remove after commit, abort after `UPLOAD_ABANDON_SECONDS`).
6. If it cannot reach two replicas the client gets `503` but the object stays `PENDING` in staging; a resume loop
   (also run at startup) finishes it later. Work is idempotent (chunk ids are deterministic, a chunk that already has
   two healthy replicas is skipped, `PUT` of identical bytes is a no-op), so retries never duplicate chunks.
   `Idempotency-Key` prevents duplicate *objects* from client retries.

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
* **Under-replication scanner** (every 10 s): chunks of committed objects with fewer than two healthy replicas on live
  workers get one repair task each. Surplus replicas (e.g. a revived worker after its chunks were re-replicated) are
  trimmed, removing the copy on the most loaded worker.
* **Repair runners** claim tasks with `SELECT … FOR UPDATE SKIP LOCKED`, copy a **verified** chunk from a healthy
  source and `PUT … ?repair=true` to the target (repair in place on the worker with the bad copy first, otherwise a new
  worker that holds no copy). The target verifies the checksum, fsyncs and renames; only then is the replica `HEALTHY`.
  Failures back off exponentially and retry; with no live source but a copy on a *dead* worker the task waits for that
  worker; if no healthy copy exists anywhere the task becomes `UNRECOVERABLE` (reported, never fabricated).
  Tasks left `IN_PROGRESS` by a crashed controller return to the queue.
* **Inventory reconciliation** (every 30 s, per live worker): compares the worker's chunk list with PostgreSQL.
  Expected-but-absent ⇒ `MISSING` + repair. Files of deleted objects (tombstones) are removed at once. Unknown files are
  *orphans*, removed only after a grace period and only if the metadata still has no replica row for them. The DB rows
  are read *before* the inventory is fetched so an in-flight write can never be mistaken for a loss.
* With fewer than two eligible workers the system reports degraded protection (`under_replicated_chunks`, pending
  tasks with `last_error`) and retries when capacity returns.

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

## 11. Decisions on open points from the plan

| Question | Decision |
|---|---|
| "1 MB" | 1 MiB = 1,048,576 bytes, one setting (`CHUNK_SIZE`) |
| Language | Python 3.12 (FastAPI, asyncpg, httpx) |
| Repair transfer | Controller-mediated streaming with bounded buffers (simpler; permitted by the plan) |
| Chunk id | `<object_id>-<index>`: unique, ordered, makes resume and cache-invalidation trivial |
| Replica placement | Least-loaded first among HEALTHY workers with free space, always two distinct workers |
| Orphan policy | Never deleted immediately; tombstoned chunks at once, unknown ones after a grace period |
| Dead worker with the only copy | Repair waits for it to return rather than declaring loss |

## 12. Known limitations

Single controller (a coordination SPOF by requirement); staged data is lost with the controller's disk; two replicas
survive one loss per chunk; no authentication; all four workers in this deployment share one physical disk, so free
space readings are identical; repair data passes through the controller (streamed, bounded); a cache hit hides silent disk
corruption of that chunk until the next scrub.
