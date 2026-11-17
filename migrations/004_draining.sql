-- A draining worker receives no new replicas; the rebalancer moves its replicas to other workers.
ALTER TABLE workers ADD COLUMN draining boolean NOT NULL DEFAULT false;
