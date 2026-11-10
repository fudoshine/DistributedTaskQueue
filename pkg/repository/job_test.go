package repository_test

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestCreateJobValidatesParents(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	contract := referenceContract(t)
	var workflowID, foreignWorkflowID uuid.UUID
	for _, id := range []*uuid.UUID{&workflowID, &foreignWorkflowID} {
		if err := sharedPool.QueryRow(ctx, `
			INSERT INTO workflows (name, status, draft_expires_at)
			VALUES ('parent validation', 'DRAFT', clock_timestamp() + INTERVAL '24 hours')
			RETURNING id
		`).Scan(id); err != nil {
			t.Fatal(err)
		}
	}
	parents := make([]uuid.UUID, 3)
	for i, id := range []uuid.UUID{workflowID, workflowID, foreignWorkflowID} {
		parent, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
			ID: uuid.New(), WorkflowID: id, TaskType: contract.TaskType,
			ExecutionContract: contract, EffectKey: uuid.New(),
		})
		if err != nil {
			t.Fatal(err)
		}
		parents[i] = parent.ID
	}
	taskID := uuid.New()
	for _, tc := range []struct {
		name    string
		parents []uuid.UUID
	}{
		{"missing", []uuid.UUID{parents[0], uuid.New()}},
		{"duplicate", []uuid.UUID{parents[0], parents[0]}},
		{"self", []uuid.UUID{taskID}},
		{"foreign workflow", []uuid.UUID{parents[0], parents[2]}},
		{"valid", parents[:2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			job, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
				ID: taskID, WorkflowID: workflowID, ParentIDs: tc.parents,
				TaskType: contract.TaskType, ExecutionContract: contract, EffectKey: uuid.New(),
			})
			if tc.name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(job.ParentIDs, tc.parents) {
					t.Fatalf("expected parents %v, got %v", tc.parents, job.ParentIDs)
				}
				return
			}
			if !errors.Is(err, repository.ErrInvalidParents) {
				t.Fatalf("expected ErrInvalidParents, got %v", err)
			}
			if !reflect.DeepEqual(job, repository.JobRow{}) {
				t.Fatalf("invalid parents returned a job: %+v", job)
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs || afterOutbox != beforeOutbox {
				t.Fatal("invalid parents changed job/outbox counts")
			}
		})
	}
}

func TestCreateJobWithNullWorkflowExpiry(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	for _, status := range []domain.WorkflowStatus{
		domain.WorkflowStatusDraft,
		domain.WorkflowStatusCompleted,
		domain.WorkflowStatusFailed,
		domain.WorkflowStatusCancelled,
		domain.WorkflowStatusPending,
	} {
		t.Run(string(status), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("CreateJob panicked with NULL workflow expiry: %v", recovered)
				}
			}()
			ctx := t.Context()
			var workflowID uuid.UUID
			if err := sharedPool.QueryRow(ctx, `
				INSERT INTO workflows (name, status, sealed_at, draft_expires_at)
				VALUES ('NULL expiry regression', $1, NULL, NULL)
				RETURNING id
			`, status).Scan(&workflowID); err != nil {
				t.Fatal(err)
			}
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			taskID := uuid.New()
			job, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
				ID: taskID, WorkflowID: workflowID, TaskType: contract.TaskType,
				ExecutionContract: contract, EffectKey: uuid.New(),
			})
			wantErr := repository.ErrWorkflowNotDraft
			if status == domain.WorkflowStatusDraft {
				wantErr = repository.ErrDraftExpired
			}
			if !errors.Is(err, wantErr) {
				t.Errorf("expected %s workflow with NULL expiry to return %v, got %v", status, wantErr, err)
			}
			if !reflect.DeepEqual(job, repository.JobRow{}) {
				t.Errorf("rejected workflow returned a job: %+v", job)
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs || afterOutbox != beforeOutbox {
				t.Errorf("job/outbox counts = %d/%d, want %d/%d",
					afterJobs, afterOutbox, beforeJobs, beforeOutbox)
			}
			workflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
			if err != nil {
				t.Fatal(err)
			}
			if workflow.Status != status || workflow.SealedAt != nil || workflow.DraftExpiresAt != nil {
				t.Errorf("job creation changed workflow state: %+v", workflow)
			}
		})
	}
}

func TestCreateJobReturnsEmptyRowOnRollback(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "create rollback")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := sharedRepo.CreateJob(t.Context(), contract.CapabilityStream, repository.Task{
		ExecutionContract: contract,
		EffectKey:         uuid.New(),
		ID:                uuid.New(), WorkflowID: workflow.ID, TaskType: "reference.echo",
		Payload: json.RawMessage(`{"message":"parent"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
	result, err := sharedRepo.CreateJob(t.Context(), "queue:test", repository.Task{
		ExecutionContract: contract,
		EffectKey:         uuid.New(),
		ID:                uuid.New(), WorkflowID: workflow.ID, ParentIDs: []uuid.UUID{parent.ID, uuid.New()},
		TaskType: "reference.echo", Payload: json.RawMessage(`{"message":"hello"}`),
	})
	if !errors.Is(err, repository.ErrInvalidParents) {
		t.Fatalf("expected missing parent rejection, got %v", err)
	}
	if !reflect.DeepEqual(result, repository.JobRow{}) {
		t.Fatalf("failed transaction returned a job: %+v", result)
	}
	afterJobs, afterOutbox := taskSubmissionRowCounts(t)
	if afterJobs != beforeJobs || afterOutbox != beforeOutbox {
		t.Fatalf("failed creation changed job/outbox counts: %d/%d -> %d/%d",
			beforeJobs, beforeOutbox, afterJobs, afterOutbox)
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
