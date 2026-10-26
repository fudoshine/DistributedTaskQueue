package repository_test

import (
	"context"
	"distributed-queue/internal/execution"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestTaskInsertion(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	jobID := uuid.New()
	query := `INSERT INTO processed_jobs (id, workflow_id, task_type,task_details)
			  VALUES ($1, $2, $3, $4);`
	_, err = sharedPool.Exec(ctx, query, jobID, workflowID, "BENCH_TASK", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}

	job, err := sharedRepo.GetTask(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.WorkflowID != workflowID {
		t.Fatalf("expected workflow_id %v, got %v", workflowID, job.WorkflowID)
	}
	if job.TaskType != "BENCH_TASK" {
		t.Fatalf("expected task_type BENCH_TASK, got %v", job.TaskType)
	}
	if job.TenantID != nil || job.ExecutionContract != nil || job.EffectKey != nil {
		t.Fatalf("expected tenant_id, execution_contract, and effect_key to be nil, got %v, %v, %v", job.TenantID, job.ExecutionContract, job.EffectKey)
	}
}

func TestTaskInsertionWithTenant(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	tenantID := uuid.New()
	jobID := uuid.New()
	query := `INSERT INTO processed_jobs (id, workflow_id, task_type, task_details, tenant_id)
			  VALUES ($1, $2, $3, $4, $5);`
	_, err = sharedPool.Exec(ctx, query, jobID, workflowID, "BENCH_TASK", json.RawMessage(`{"tenant_id": "`+tenantID.String()+`"}`), tenantID)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	job, err := sharedRepo.GetTask(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.WorkflowID != workflowID {
		t.Fatalf("expected workflow_id %v, got %v", workflowID, job.WorkflowID)
	}
	if job.TaskType != "BENCH_TASK" {
		t.Fatalf("expected task_type BENCH_TASK, got %v", job.TaskType)
	}
	if job.TenantID == nil || *job.TenantID != tenantID {
		t.Fatalf("expected tenant_id %v, got %v", tenantID, job.TenantID)
	}
	if job.ExecutionContract != nil {
		t.Fatalf("expected execution_contract to be nil, got %v", job.ExecutionContract)
	}
	if job.EffectKey != nil {
		t.Fatalf("expected effect_key to be nil, got %v", job.EffectKey)
	}
}

func TestTaskInsertionWithExecutionContract(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	tenantID := uuid.New()
	catalog, err := execution.NewCatalog()
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("get catalog entry: %v", err)
	}
	contractJSON, err := json.Marshal(entry)
	jobID := uuid.New()
	if err != nil {
		t.Fatalf("marshal execution contract: %v", err)
	}
	query := `INSERT INTO processed_jobs (id, workflow_id, task_type, task_details, tenant_id, execution_contract)
			  VALUES ($1, $2, $3, $4, $5, $6);`
	_, err = sharedPool.Exec(ctx, query, jobID, workflowID, entry.TaskType, json.RawMessage(`{"message":"hello"}`), tenantID, contractJSON)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	job, err := sharedRepo.GetTask(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.WorkflowID != workflowID {
		t.Fatalf("expected workflow_id %v, got %v", workflowID, job.WorkflowID)
	}
	if job.TaskType != entry.TaskType {
		t.Fatalf("expected task_type %v, got %v", entry.TaskType, job.TaskType)
	}
	if job.TenantID == nil || *job.TenantID != tenantID {
		t.Fatalf("expected tenant_id %v, got %v", tenantID, job.TenantID)
	}
	if job.ExecutionContract == nil || *job.ExecutionContract != entry {
		t.Fatalf("expected execution_contract %v, got %v", entry, job.ExecutionContract)
	}
	if job.EffectKey != nil {
		t.Fatalf("expected effect_key to be nil, got %v", job.EffectKey)
	}
}

func TestTaskInsertionWithEffectKey(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	tenantID := uuid.New()
	effectKey := uuid.New()
	jobID := uuid.New()
	query := `INSERT INTO processed_jobs (id, workflow_id, task_type, task_details, tenant_id, effect_key)
			  VALUES ($1, $2, $3, $4, $5, $6);`
	_, err = sharedPool.Exec(ctx, query, jobID, workflowID, "BENCH_TASK", json.RawMessage(`{}`), tenantID, effectKey)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	job, err := sharedRepo.GetTask(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.WorkflowID != workflowID {
		t.Fatalf("expected workflow_id %v, got %v", workflowID, job.WorkflowID)
	}
	if job.TaskType != "BENCH_TASK" {
		t.Fatalf("expected task_type BENCH_TASK, got %v", job.TaskType)
	}
	if job.TenantID == nil || *job.TenantID != tenantID {
		t.Fatalf("expected tenant_id %v, got %v", tenantID, job.TenantID)
	}
	if job.ExecutionContract != nil {
		t.Fatalf("expected execution_contract to be nil, got %v", job.ExecutionContract)
	}
	if job.EffectKey == nil || *job.EffectKey != effectKey {
		t.Fatalf("expected effect_key %v, got %v", effectKey, job.EffectKey)
	}
}
