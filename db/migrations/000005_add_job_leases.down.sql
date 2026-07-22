DROP INDEX IF EXISTS idx_jobs_expired_leases;

ALTER TABLE processed_jobs
    DROP COLUMN IF EXISTS attempt_number,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS lease_token;
