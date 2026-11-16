# API Reference

Base URL of the controller: `http://localhost:8090`. Structured data is JSON; object data is binary.
Timestamps are ISO-8601 (UTC). Every response carries an `X-Request-ID` header.

## Error format and status codes

```json
{"error": {"code": "not_found", "message": "object not found"}}
```

| Status | Meaning here |
|---|---|
| 200 | Success (download, metadata, patch, list; idempotent re-upload) |
| 201 | Object created |
| 204 | Deleted |
| 400 | Invalid request: wrong content type, missing `file`, empty file, bad metadata JSON, invalid `limit`/`cursor`, `too_large` |
| 404 | Unknown, malformed or deleted object id |
| 409 | Conflict: upload with the same `Idempotency-Key` still in progress (`error.object_id` is returned) |
| 500 | Internal error (details only in the controller log) |
| 503 | Storage unavailable: fewer than 2 healthy workers, no readable replica, or `server_busy` (has `Retry-After`) |

Extra `error.code` values: `storage_unavailable` (upload kept in staging, see below), `server_busy`,
`staging_full`, `too_large`, `invalid_request`.

---

## Client API

### `POST /api/v1/objects` — upload → 201

`multipart/form-data`

| Field | Required | Notes |
|---|---|---|
| `file` | yes | The binary object. Its filename becomes `name`, its part `Content-Type` becomes `content_type` (guessed from the filename if absent, else `application/octet-stream`). Empty files are rejected. |
| `metadata` | no | JSON **object** of custom key/values (string, number or boolean values; ≤ 128-char keys, ≤ 4096-char strings, ≤ 64 KiB total). |

Optional header `Idempotency-Key: <any string>`: a retry with the same key returns the already committed object
(`200`) instead of creating a duplicate; while the first upload is unfinished it returns `409`.

```bash
curl -s -F file=@photo.jpg -F 'metadata={"owner":"alice"}' http://localhost:8090/api/v1/objects
```
```json
{
  "object_id": "06799f9c-588e-44a9-b369-b0e491b384bd",
  "name": "photo.jpg", "size": 5767168, "content_type": "image/jpeg",
  "checksum": "sha256:66d9f4f1…", "chunk_count": 6, "chunk_size": 1048576,
  "state": "COMMITTED",
  "created_at": "2026-11-11T07:10:09.557620+00:00", "last_modified": "2026-11-11T07:10:09.701576+00:00",
  "user_metadata": {"owner": "alice"}
}
```

The upload is streamed to a persistent staging file, cut into chunks, and each chunk is stored on two different
workers and acknowledged before the object becomes `COMMITTED`. If fewer than two workers can take the data, the
response is **503 `storage_unavailable`** with `error.object_id`; the data stays safely in staging and is completed
automatically when workers return (the object is invisible until then).

### `GET /api/v1/objects/{object_id}` — download → 200

Binary body with `Content-Type`, `Content-Length`, `Content-Disposition: attachment; filename=…`, `X-Object-Checksum`
and `ETag`. Chunks are streamed in order; each chunk is verified against its authoritative checksum, bad or
unavailable replicas are skipped, and the connection is aborted rather than returning truncated or wrong data.
`404` for an unknown/deleted object; `503` if some chunk has no readable replica.

### `GET /api/v1/objects/{object_id}/metadata` — 200

System metadata, custom metadata, and the chunk manifest with replica locations:

```json
{
  "object_id": "…", "name": "…", "size": 5767168, "content_type": "…", "checksum": "sha256:…",
  "chunk_count": 6, "chunk_size": 1048576, "state": "COMMITTED",
  "created_at": "…", "last_modified": "…", "user_metadata": {"owner": "alice"},
  "chunks": [
    {"index": 0, "chunk_id": "<object_id>-000000", "size": 1048576, "checksum": "sha256:…",
     "replicas": [{"worker_id": "worker1", "state": "HEALTHY", "worker_state": "HEALTHY"},
                  {"worker_id": "worker3", "state": "HEALTHY", "worker_state": "HEALTHY"}]}
  ]
}
```
Replica `state`: `HEALTHY`, `CORRUPT`, `MISSING`, `PENDING`, `DELETING` — what the last check found about the stored
copy. `worker_state` (`HEALTHY`, `DEGRADED`, `DEAD`) is whether that worker is reachable now. A copy is usable
only when `state` is `HEALTHY` **and** `worker_state` is not `DEAD`; a copy on a dead worker keeps its `HEALTHY`
state so repair can wait for the worker instead of declaring data lost.

### `PATCH /api/v1/objects/{object_id}/metadata` — 200

JSON object merged into the custom metadata: new keys are added, existing keys overwritten, keys set to `null` are
removed. Returns the updated object (same shape as the upload response). `last_modified` advances; the data is
untouched. `400` for invalid JSON, a non-object body, nested values or more than 100 keys; `404` if unknown.

```bash
curl -s -X PATCH -d '{"owner":"bob","tag":"v2","project":null}' http://localhost:8090/api/v1/objects/$OID/metadata
```

### `DELETE /api/v1/objects/{object_id}` — 204

The object disappears immediately (`GET` → 404). Replicas are then removed from the workers in the background
(retried while a worker is down); tombstones prevent a returning worker from resurrecting the chunks. `404` if
unknown or already deleted.

### `GET /api/v1/objects?limit=100&cursor=…` — 200

```json
{"items": [{"object_id": "…", "name": "…", "size": 123, "content_type": "…", "created_at": "…"}],
 "next_cursor": "MjAyNi0x…"}
```
Oldest first. `limit` is 1–1000 (default 100). `next_cursor` is `null` on the last page; pass it back as `cursor`.

---

## Operational endpoints (controller)

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness |
| `GET /internal/status[?detail=true]` | Cluster view: workers (state, heartbeat age, free/used, last scrub), object/chunk counts, replica counts by state, under-replicated chunks, repair tasks by state, cache and concurrency stats; `detail=true` adds each live worker's own `/internal/status` |
| `GET /internal/metrics` | Counters (uploads, downloads, repairs, scrub reports, fallbacks, …), in-flight/peak/rejected per concurrency gate, metadata-cache statistics |
| `GET /internal/cache/stats`, `POST /internal/cache/clear` | Metadata cache hit/miss/eviction/expiration/invalidation statistics; clear it (demo/testing) |

## Worker-facing endpoints (controller, internal network)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/internal/workers/register` | `{worker_id, address, capacity_bytes, used_bytes}` → worker becomes HEALTHY |
| `POST` | `/internal/workers/heartbeat` | Same body; `404 unknown_worker` makes the worker re-register |
| `GET` | `/internal/workers` | Workers with heartbeat age and free space |
| `POST` | `/internal/workers/scrub-results` | One scrubber finding: `{worker_id, chunk_id, status: HEALTHY\|CORRUPT\|MISSING\|UNREADABLE, expected_checksum, actual_checksum, checked_at}`. Idempotent; response `{"status":"recorded","action":"marked_corrupt"\|"marked_missing"\|"ignored"\|"verified", "reason"?}`. Stale reports, reports for unknown replicas, and CORRUPT reports whose bytes match the authoritative checksum are ignored. |
| `POST` | `/internal/workers/scrub-summary` | Totals of a finished scrub pass (shown in status) |
| `GET` | `/internal/chunks/{chunk_id}/expected` | Authoritative checksum of a chunk |

## Worker endpoints (each worker, port 9000, internal network only)

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/internal/chunks/{chunk_id}` | Store a chunk. Header `X-Chunk-Checksum: sha256:<hex>` is required. The body is written to a temp file, hashed, `fsync`ed and atomically renamed; acknowledged only after that. `201` created, `200` identical chunk already stored (idempotent), `409 chunk_conflict` same id with different content, `400 checksum_mismatch`. `?repair=true` (controller only) replaces a damaged copy after verifying against the controller's checksum. **Chain replication:** with headers `X-Forward-To: http://<next worker>:9000` and `X-Forward-Worker-Id`, the worker stores the chunk, forwards it to that worker, and replies with `replicas: [{"worker_id", "ok", "error"?}, …]` (one entry per copy). The local copy is acknowledged even if the forward failed (`ok: false`). Not allowed together with `repair=true` (400). |
| `POST` | `/internal/chunks/{chunk_id}/send` | Controller command: push this worker's copy straight to another worker. Body `{target_address, checksum, size}`. The chunk is read from disk, verified against the worker's own record **and** the controller's checksum, then sent with `PUT …?repair=true`. Always `200` with `{"ok": true}` or `{"ok": false, "reason": "source_missing" \| "source_corrupt" \| "target_failed", "detail"}`. |
| `GET` | `/internal/chunks/{chunk_id}` | Read a chunk (verified against its stored checksum; `500 chunk_corrupt` instead of bad bytes; `404` if missing). `X-Cache: HIT\|MISS` header. |
| `DELETE` | `/internal/chunks/{chunk_id}` | Delete a chunk (204, also if already gone) |
| `GET` | `/internal/workers/{worker_id}/chunks` | Inventory: `{chunks: [{chunk_id, size, checksum}]}` |
| `POST` | `/internal/scrub/trigger` | Start a scrub pass now (202) |
| `GET` | `/internal/scrub/status` | Progress and counters of the scrubber, durable report-queue depth |
| `GET` | `/internal/status`, `/internal/metrics` | Worker health, disk, cache, scrub, counters |
| `GET` | `/internal/cache/stats`, `POST /internal/cache/clear` | Chunk-cache statistics (hits, misses, evictions, bytes served from cache, disk reads); clear RAM cache |
