-- Enables UUID generation if not already active
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Workflow Management Hierarchy Table
CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, COMPLETED, FAILED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Processed Jobs Execution Node Table
CREATE TABLE processed_jobs (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    current_retries INT NOT NULL DEFAULT 0,
    task_details JSONB NOT NULL,
    error_log TEXT,
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Job Dependencies Junction Table (DAG edges)
CREATE TABLE job_dependencies (
    child_job_id UUID NOT NULL REFERENCES processed_jobs(id) ON DELETE CASCADE,
    parent_job_id UUID NOT NULL REFERENCES processed_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (child_job_id, parent_job_id),
    CONSTRAINT no_self_reference CHECK (child_job_id != parent_job_id)
);

-- Performance Optimization Indexes
CREATE INDEX idx_jobs_workflow_id ON processed_jobs(workflow_id);
CREATE INDEX idx_jobs_status ON processed_jobs(status);
CREATE INDEX idx_deps_child ON job_dependencies(child_job_id);
CREATE INDEX idx_deps_parent ON job_dependencies(parent_job_id);

-- 4. Transactional Outbox Table
-- Written atomically with processed_jobs in CreateJob. The outbox relay reads
-- undelivered rows and enqueues them to Redis, then marks them delivered.
-- This guarantees no task is ever lost between DB write and Redis enqueue,
-- even across server crashes.
CREATE TABLE outbox (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID NOT NULL REFERENCES processed_jobs(id) ON DELETE CASCADE,
    queue_name  VARCHAR(255) NOT NULL,
    delivered   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    delivered_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_outbox_undelivered ON outbox(delivered, created_at)
    WHERE delivered = FALSE;
