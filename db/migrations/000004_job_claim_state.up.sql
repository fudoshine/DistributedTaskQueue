-- available_at makes retry timing durable. Claiming checks it even if a stale
-- Redis message arrives before its delayed retry is due.
ALTER TABLE processed_jobs
    ADD COLUMN available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();

-- Older root jobs were stored as PENDING despite already having an outbox
-- entry. Roots are explicitly QUEUED in the claim state machine.
UPDATE processed_jobs AS job
SET status = 'QUEUED'
WHERE job.status = 'PENDING'
  AND EXISTS (SELECT 1 FROM outbox WHERE outbox.task_id = job.id)
  AND NOT EXISTS (
      SELECT 1 FROM job_dependencies edge WHERE edge.child_job_id = job.id
  );
