CREATE TABLE objects (
    object_id      uuid PRIMARY KEY,
    name           text NOT NULL,
    size           bigint NOT NULL DEFAULT 0,
    content_type   text NOT NULL DEFAULT 'application/octet-stream',
    checksum       text,
    chunk_count    int NOT NULL DEFAULT 0,
    chunk_size     int NOT NULL,
    state          text NOT NULL CHECK (state IN ('PENDING','COMMITTED','DELETING','DELETED')),
    user_metadata  jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX objects_list_idx ON objects (created_at, object_id) WHERE state = 'COMMITTED';

CREATE TABLE chunks (
    chunk_id     text PRIMARY KEY,
    object_id    uuid NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index  int NOT NULL,
    size         int NOT NULL,
    checksum     text NOT NULL,
    UNIQUE (object_id, chunk_index)
);

CREATE TABLE workers (
    worker_id       text PRIMARY KEY,
    address         text NOT NULL,
    state           text NOT NULL DEFAULT 'HEALTHY' CHECK (state IN ('HEALTHY','DEGRADED','DEAD')),
    last_heartbeat  timestamptz NOT NULL DEFAULT now(),
    capacity_bytes  bigint NOT NULL DEFAULT 0,
    used_bytes      bigint NOT NULL DEFAULT 0,
    registered_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE replicas (
    chunk_id       text NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id      text NOT NULL REFERENCES workers(worker_id),
    state          text NOT NULL CHECK (state IN ('PENDING','HEALTHY','CORRUPT','MISSING','DELETING')),
    last_verified  timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX replicas_worker_idx ON replicas (worker_id, state);

CREATE TABLE repair_tasks (
    task_id           uuid PRIMARY KEY,
    chunk_id          text NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    target_worker_id  text,
    source_worker_id  text,
    reason            text NOT NULL,
    state             text NOT NULL CHECK (state IN ('PENDING','IN_PROGRESS','DONE','FAILED','UNRECOVERABLE')),
    attempts          int NOT NULL DEFAULT 0,
    last_error        text,
    next_attempt_at   timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
-- at most one active repair per chunk
CREATE UNIQUE INDEX repair_tasks_active_idx ON repair_tasks (chunk_id)
    WHERE state IN ('PENDING','IN_PROGRESS');

CREATE TABLE scrub_results (
    id                bigserial PRIMARY KEY,
    worker_id         text NOT NULL,
    chunk_id          text NOT NULL,
    status            text NOT NULL CHECK (status IN ('HEALTHY','CORRUPT','MISSING','UNREADABLE')),
    expected_checksum text,
    actual_checksum   text,
    checked_at        timestamptz NOT NULL,
    UNIQUE (worker_id, chunk_id, checked_at)
);

CREATE TABLE worker_scrub_state (
    worker_id       text PRIMARY KEY,
    last_scrub_at   timestamptz,
    chunks_checked  bigint NOT NULL DEFAULT 0,
    corrupt_found   bigint NOT NULL DEFAULT 0,
    missing_found   bigint NOT NULL DEFAULT 0
);

CREATE TABLE uploads (
    object_id    uuid PRIMARY KEY REFERENCES objects(object_id) ON DELETE CASCADE,
    staged_path  text NOT NULL,
    state        text NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tombstones (
    chunk_id    text NOT NULL,
    worker_id   text NOT NULL,
    deleted_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_id, worker_id)
);
