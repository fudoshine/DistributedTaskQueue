DROP INDEX IF EXISTS idx_jobs_delivery_recovery;
DROP INDEX IF EXISTS idx_outbox_due;
DROP INDEX IF EXISTS outbox_task_generation_unique;

CREATE UNIQUE INDEX outbox_task_id_unique ON outbox(task_id);

ALTER TABLE outbox
    DROP COLUMN IF EXISTS relay_lease_expires_at,
    DROP COLUMN IF EXISTS relay_lease_token,
    DROP COLUMN IF EXISTS not_before,
    DROP COLUMN IF EXISTS generation,
    DROP COLUMN IF EXISTS delivery_id;

ALTER TABLE processed_jobs
    DROP COLUMN IF EXISTS delivery_generation;
