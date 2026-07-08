-- Transactional Outbox Table
-- Written atomically with processed_jobs in CreateJob. The outbox relay reads
-- undelivered rows and enqueues them to Redis, then marks them delivered.
-- Guarantees no task is lost between DB write and Redis enqueue, even across crashes.
CREATE TABLE outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id      UUID NOT NULL REFERENCES processed_jobs(id) ON DELETE CASCADE,
    queue_name   VARCHAR(255) NOT NULL,
    delivered    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    delivered_at TIMESTAMP WITH TIME ZONE
);

-- Partial index: only undelivered rows — keeps the relay query fast
CREATE INDEX idx_outbox_undelivered ON outbox(delivered, created_at)
    WHERE delivered = FALSE;
