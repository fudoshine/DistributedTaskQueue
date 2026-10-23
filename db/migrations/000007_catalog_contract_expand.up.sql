
ALTER TABLE processed_jobs
    ADD COLUMN tenant_id UUID, 
    ADD COLUMN execution_contract JSONB,
    ADD COLUMN effect_key UUID; 

ALTER TABLE workflows
    ADD COLUMN tenant_id UUID,
    ADD COLUMN sealed_at TIMESTAMPTZ,
    ADD COLUMN draft_expires_at TIMESTAMPTZ;





-- This migration will to change the DB behavior for Job contract
-- Currently, routing is stored in outbox.queue_name, while the 
-- job itself lacks its accepted contract. That’s why child scheduling 
-- and retries can accidentally inherit another queue