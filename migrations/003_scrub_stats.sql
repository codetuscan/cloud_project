ALTER TABLE worker_scrub_state
    ADD COLUMN bytes_checked     bigint NOT NULL DEFAULT 0,
    ADD COLUMN unreadable_found  bigint NOT NULL DEFAULT 0,
    ADD COLUMN duration_seconds  real   NOT NULL DEFAULT 0,
    ADD COLUMN passes_completed  bigint NOT NULL DEFAULT 0;
