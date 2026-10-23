
ALTER TABLE processed_jobs
    DROP COLUMN IF EXISTS tenant_id,  
    DROP COLUMN IF EXISTS execution_contract,
    DROP COLUMN IF EXISTS effect_key;

ALTER TABLE workflows
    DROP COLUMN IF EXISTS tenant_id, 
    DROP COLUMN IF EXISTS sealed_at, 
    DROP COLUMN IF EXISTS draft_expires_at; 