-- A queue message is a delivery attempt, not the job itself. Keeping a
-- durable generation and delivery ID lets stale Stream entries be safely
-- acknowledged after a retry or lease recovery creates a newer attempt.
ALTER TABLE processed_jobs
    ADD COLUMN delivery_generation INT NOT NULL DEFAULT 0;

ALTER TABLE outbox
    ADD COLUMN delivery_id UUID,
    ADD COLUMN generation INT NOT NULL DEFAULT 0,
    ADD COLUMN not_before TIMESTAMP WITH TIME ZONE,
    ADD COLUMN relay_lease_token UUID,
    ADD COLUMN relay_lease_expires_at TIMESTAMP WITH TIME ZONE;

UPDATE outbox
SET delivery_id = uuid_generate_v4(),
    generation = 0,
    not_before = created_at
WHERE delivery_id IS NULL OR not_before IS NULL;

ALTER TABLE outbox
    ALTER COLUMN delivery_id SET NOT NULL,
    ALTER COLUMN not_before SET NOT NULL;

DROP INDEX IF EXISTS outbox_task_id_unique;

CREATE UNIQUE INDEX outbox_task_generation_unique
    ON outbox(task_id, generation);

CREATE INDEX idx_outbox_due
    ON outbox(not_before, created_at)
    WHERE delivered = FALSE;

CREATE INDEX idx_jobs_delivery_recovery
    ON processed_jobs(lease_expires_at)
    WHERE status = 'RUNNING';
