package repository_test

import (
	"context"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCreateJobReturnsEmptyRowOnRollback(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "create rollback")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := sharedRepo.CreateJob(t.Context(), "queue:test", repository.Task{
		ExecutionContract: contract,
		EffectKey:         uuid.New(),
		ID:                uuid.New(), WorkflowID: workflow.ID, TaskType: "reference.echo",
		Payload: json.RawMessage(`{"message":"parent"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		queue   string
		parents []uuid.UUID
	}{
		{"dependency failure after inserting an edge", "queue:test", []uuid.UUID{parent.ID, uuid.New()}},
		{"outbox failure", strings.Repeat("q", 256), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			result, err := sharedRepo.CreateJob(t.Context(), tc.queue, repository.Task{
				ExecutionContract: contract,
				EffectKey:         uuid.New(),
				ID:                uuid.New(), WorkflowID: workflow.ID, ParentIDs: tc.parents,
				TaskType: "reference.echo", Payload: json.RawMessage(`{"message":"hello"}`),
			})
			if err == nil {
				t.Fatal("expected job creation to fail")
			}
			if !reflect.DeepEqual(result, repository.JobRow{}) {
				t.Fatalf("failed transaction returned a job: %+v", result)
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs || afterOutbox != beforeOutbox {
				t.Fatalf("failed creation changed job/outbox counts: %d/%d -> %d/%d",
					beforeJobs, beforeOutbox, afterJobs, afterOutbox)
			}
		})
	}
}

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
