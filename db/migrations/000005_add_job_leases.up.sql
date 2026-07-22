-- A running job is owned by a single, time-bounded execution attempt. The
-- opaque token is a fencing token: stale workers cannot complete or fail a
-- job after a newer attempt has taken ownership.
ALTER TABLE processed_jobs
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN attempt_number INT NOT NULL DEFAULT 0;

CREATE INDEX idx_jobs_expired_leases
    ON processed_jobs (lease_expires_at)
    WHERE status = 'RUNNING';

-- Existing RUNNING rows predate leases. Treat them as immediately recoverable
-- after deployment rather than leaving them owned forever.
UPDATE processed_jobs
SET lease_expires_at = NOW()
WHERE status = 'RUNNING'
  AND lease_expires_at IS NULL;
