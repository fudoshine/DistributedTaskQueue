UPDATE processed_jobs
SET status = 'PENDING'
WHERE status = 'QUEUED'
  AND EXISTS (SELECT 1 FROM outbox WHERE outbox.task_id = processed_jobs.id)
  AND NOT EXISTS (
      SELECT 1 FROM job_dependencies edge WHERE edge.child_job_id = processed_jobs.id
  );

ALTER TABLE processed_jobs DROP COLUMN IF EXISTS available_at;
