# Demonstrations

Everything the course asks to be demonstrated (assignment §10) has a narrated script in
[scripts/demo/](scripts/demo/). Each script prints what it does, shows the evidence, and ends with `DEMO OK`
(or exits non-zero if any check fails).

```bash
docker compose up -d --build             # or ./scripts/clean_start.sh for a fresh deployment
./scripts/demo/run_all.sh                # all six, ~3 minutes
./scripts/demo/05_worker_failure.sh      # or any single one
```

| # | Assignment requirement | Script | What it proves |
|---|---|---|---|
| 1 | Object storage: upload and retrieve; verify it matches the original | `01_store_and_retrieve.sh` | Upload → 201 with object info; download is byte-identical (SHA-256 compared); `Content-Type`/`Content-Length`/`Content-Disposition`; 404/400 behaviour and JSON error body; list; delete (204, then 404) |
| 2 | Chunking and distribution: object larger than 1 MB | `02_chunking_and_distribution.sh` | 5.5 MiB → 6 chunks (5 × 1,048,576 B + 524,288 B), ordered; chunk files physically on the workers' own volumes (12 files = 6 × 2); spread over all four workers; reassembling the chunk files from the workers reproduces the original |
| 3 | Replication: each chunk has two copies; show where | `03_replication.sh` | Every chunk has exactly 2 `HEALTHY` replicas on different workers, checked in PostgreSQL *and* on the workers' disks (size and SHA-256 of the file) |
| 4 | Metadata: upload custom metadata; read and modify without re-uploading | `04_metadata.sh` | System + custom metadata; `PATCH` adds/updates/removes (`null`) keys; `last_modified` advances; chunk ids and data checksum unchanged; 404/400 cases |
| 5 | Worker failure: `docker stop worker2` / `docker start worker2` — before, during, after; is replication maintained? | `05_worker_failure.sh` | worker2 declared DEAD after ~15 s; downloads keep working; the controller re-replicates the affected chunks onto live workers (2 healthy copies again, with worker2 *still down*); worker2 returns with its persistent chunks, surplus copies are trimmed; replication factor 2 maintained; final download identical |
| 6 | Data integrity: intentionally corrupt stored data; show detection and handling | `06_data_integrity.sh` | Follows the 10-step corruption-recovery demonstration of the design plan: corrupt one replica on disk → the worker's scrubber reports expected vs actual checksum → controller marks it `CORRUPT`, queues a repair and picks the other replica as source → target verifies, fsyncs, renames → `HEALTHY`, 2 replicas on different workers → full download matches. Also shows a read that meets a bad replica (skipped, data still correct, copy repaired) and a deleted chunk file (found and restored) |

**Extra: scaling (beyond the required six).** `./scripts/demo/07_scale_out_and_in.sh` starts an empty fifth
worker and shows the rebalancer moving existing replicas onto it. Each chunk is copied, checked and only then
removed from its old worker, so it always keeps two healthy copies. It then **drains** worker5, stops it (no repair
is triggered, because it holds nothing) and removes it from the cluster. On the dashboard, watch *Healthy replicas per
worker* level out, *Rebalancing · per min* spike, and worker5 turn blue (Draining) on *Worker state*.

**Extra: slow worker and partial downloads.** `./scripts/demo/08_slow_worker_and_ranges.sh` freezes worker2 with
`docker pause`. The worker still accepts connections but never answers, which is the hardest kind of failure for a
reader. Every download still finishes in a fraction of a second, byte-identical, because each chunk read that waits
longer than the adaptive hedge delay also asks the other copy (a *hedged read*). Without hedging, the same download
takes 30 s. worker2 is not marked bad, because it is only slow. The script then checks Range requests: the first
100 bytes, a range across a chunk boundary, the last 1000 bytes, and a 416 for a range past the end. On the dashboard,
*Hedged reads · per min* shows the hedges and their wins.

**Live dashboard.** Open http://localhost:3000 (Grafana, no login) next to the terminal while running demos 5 and 6:

* Demo 5: on *Reachability*, worker2 turns DOWN within ~5 s. On *Worker state*, it turns DEAD at the 15 s heartbeat
  timeout. *Under-replicated chunks* jumps and returns to 0 while worker2 is still down. *Repair activity* shows the
  worker-to-worker copies. After `docker start worker2`, the surplus copies are trimmed (*Healthy replicas per worker*).
* Demo 6: *Corrupt copies detected* / *Missing copies detected* go up, the *Corruption and loss events* panel shows
  which path found each problem (read check, worker self-check, scrubber), and *Time to heal* shows how quickly the
  copy was restored.

## Sample output (from a clean deployment)

### 5 — Worker failure

```
▶ BEFORE the failure
    workers: worker1=HEALTHY, worker2=HEALTHY, worker3=HEALTHY, worker4=HEALTHY
    chunk 0   1048576 bytes  sha256:0ec94dbdd867…  ->  worker1(HEALTHY), worker4(HEALTHY)
    chunk 1   1048576 bytes  sha256:a809c656a197…  ->  worker1(HEALTHY), worker2(HEALTHY)
    chunk 2   1048576 bytes  sha256:2740eea92bb2…  ->  worker1(HEALTHY), worker4(HEALTHY)
    chunk 3   1048576 bytes  sha256:b74959992792…  ->  worker2(HEALTHY), worker3(HEALTHY)
    chunk 4      4242 bytes  sha256:963cb7b8e53d…  ->  worker3(HEALTHY), worker4(HEALTHY)

▶ DURING: docker stop worker2
  ✔ worker2 declared DEAD (after ~16s)
  ✔ download still works while worker2 is down (other replicas serve it)
  ✔ every chunk has 2 healthy replicas on LIVE workers again (after ~2s)
    controller | WARNING controller.health: worker worker2 missed heartbeats for >15s -> DEAD
    controller | WARNING controller.repair: 2 chunk(s) below 2 healthy replicas; repair queued
    controller | INFO    controller.repair: repaired …-000001: copied directly from worker1 to worker3 (checksum verified, fsynced)
    controller | INFO    controller.repair: repaired …-000003: copied directly from worker3 to worker1 (checksum verified, fsynced)
    chunk 1 -> worker1, worker2(DEAD), worker3        <- worker2's copy is no longer counted
    chunk 3 -> worker1, worker2(DEAD), worker3

▶ AFTER: docker start worker2
  ✔ worker2 HEALTHY again (after ~2s)
  ✔ exactly 2 replicas per chunk again (surplus trimmed) (after ~8s)
  ✔ replication factor 2 maintained for every chunk
  ✔ final download identical to the original
```

### 6 — Data integrity

```
▶ 1) chunk 95f0b721-…-000002   expected sha256:fe886510b785…   replicas worker2 and worker3
▶ 2) corrupt the copy on worker2:  printf CORRUPTED | dd of=/data/chunks/95/95f0b721-…-000002 bs=1 seek=100 conv=notrunc
     sha256 now on worker2: sha256:412eae9a09eb…  (differs from expected)
▶ 3) scrubber on worker2 reports to the controller
     scrub result: status=CORRUPT
       expected=sha256:fe886510b785…
       actual  =sha256:412eae9a09eb…
▶ 4) repair task: reason=scrub: CORRUPT…  source=worker3  target=worker2  state=DONE
     controller | repaired …-000002: copied directly from worker3 to worker2 (checksum verified, fsynced)
▶ 5) worker2's file matches the expected checksum again; two healthy replicas on different workers
▶ 8) sha256 of the downloaded object equals the original
```

## Showing it live (manual commands)

```bash
# placement of an object's chunks
curl -s localhost:8090/api/v1/objects/$OID/metadata | python3 -m json.tool
# who holds what, replica health, repair queue, last scrub per worker, caches
curl -s localhost:8090/internal/status | python3 -m json.tool
# the chunk files themselves
docker exec worker1 ls -R /data/chunks | head
# watch the controller work
docker compose logs -f controller
```

## Other things the test suite covers

`scripts/run_all_tests.sh` runs the pytest unit tests and one end-to-end script per phase (about 40 minutes):
upload/chunk boundaries, 503-and-resume when workers are down, replica fallback on reads, metadata PATCH, deletion
with workers down, dead-worker re-replication, corrupt/missing/orphan/tombstoned chunks, unrecoverable chunks,
scrub-report durability across controller outages, cache correctness (patch/delete/corruption/repair never serve
stale data), cache on/off and cold/warm comparison (`scripts/cache_benchmark.sh`), bounded concurrency/backpressure,
flat memory use for a 384 MiB object, and controller / worker / PostgreSQL / full-stack restarts.
