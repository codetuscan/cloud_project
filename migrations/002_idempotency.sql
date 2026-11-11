ALTER TABLE objects ADD COLUMN idempotency_key text;
CREATE UNIQUE INDEX objects_idem_idx ON objects (idempotency_key) WHERE idempotency_key IS NOT NULL;
