package repository_test

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkflowInsertion(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	tenantID := uuid.New()
	workflowID := uuid.Nil
	err := sharedPool.QueryRow(ctx, "INSERT INTO workflows (name,tenant_id) VALUES ($1,$2) RETURNING id", "test workflow", tenantID).Scan(&workflowID)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if workflowID == uuid.Nil {
		t.Fatal("Passed the correct input but got empty workflowId")
	}

	workflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatalf("Getting some error in getting workflow: %v", err)
	}

	if workflow.TenantID == nil || *workflow.TenantID != tenantID {
		t.Fatalf("tenant mismatch: got %v", workflow.TenantID)
	}
}

func TestWorkflowInsertionWithNILTenantTimeStamps(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	workflowID := uuid.Nil
	err := sharedPool.QueryRow(ctx, "INSERT INTO workflows (name) VALUES ($1) RETURNING id", "test workflow").Scan(&workflowID)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if workflowID == uuid.Nil {
		t.Fatal("Passed the correct input but got empty workflowId")
	}

	workflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatalf("Getting some error in getting workflow: %v", err)
	}

	if workflow.TenantID != nil ||
		workflow.SealedAt != nil ||
		workflow.DraftExpiresAt != nil {
		t.Fatalf("expected NULL metadata, got %+v", workflow)
	}
}
func TestWorkflowInsertionWithTenantTimestamps(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	tenantID := uuid.New()
	sealedAt := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	draftExpiresAt := sealedAt.Add(24 * time.Hour)
	workflowID := uuid.Nil
	err := sharedPool.QueryRow(ctx, "INSERT INTO workflows (name,tenant_id,sealed_at,draft_expires_at) VALUES ($1,$2,$3,$4) RETURNING id", "test workflow", tenantID, sealedAt, draftExpiresAt).Scan(&workflowID)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if workflowID == uuid.Nil {
		t.Fatal("Passed the correct input but got empty workflowId")
	}

	workflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatalf("Getting some error in getting workflow: %v", err)
	}

	if workflow.TenantID == nil || *workflow.TenantID != tenantID || workflow.SealedAt == nil || !workflow.SealedAt.Equal(sealedAt) || workflow.DraftExpiresAt == nil || !workflow.DraftExpiresAt.Equal(draftExpiresAt) {
		t.Fatalf("tenant mismatch: got %v", workflow.TenantID)
	}
}

func TestWorkflowIntegration(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	wname := "Hello world"
	workflow, err := sharedRepo.CreateWorkflow(ctx, wname)
	if err != nil {
		t.Fatalf("Getting error in creating workflow %v", err)
	}
	if workflow.Name != wname {
		t.Fatalf("Name is not matching with passed name in input passed: %v, getting %v", wname, workflow.Name)
	}
	if workflow.Status != domain.WorkflowStatusDraft {
		t.Fatalf("Expected status is Draft but getting different %v", workflow.Status)
	}
	if workflow.SealedAt != nil {
		t.Fatal("Expected 'SealedAt' as null")
	}
	if workflow.DraftExpiresAt == nil || !workflow.DraftExpiresAt.Equal(workflow.CreatedAt.Add(24*time.Hour)) {
		t.Fatal("Wrong Draft Expires Time or its null")
	}
	curWorkflow, err := sharedRepo.GetWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("Getting error in fetching workflow detail %v", err)
	}
	if curWorkflow.DraftExpiresAt == nil {
		t.Fatal("stored draft expiry is NULL")
	}
	if curWorkflow.SealedAt != nil {
		t.Fatal("draft workflow must not be sealed")
	}
	if workflow.ID != curWorkflow.ID ||
		workflow.Status != curWorkflow.Status ||
		workflow.TenantID != curWorkflow.TenantID ||
		workflow.Name != curWorkflow.Name {
		t.Fatalf("Same instance returning different  value %v : %v", workflow, curWorkflow)
	}
	if !workflow.CreatedAt.Equal(curWorkflow.CreatedAt) ||
		!workflow.UpdatedAt.Equal(curWorkflow.UpdatedAt) ||
		!workflow.DraftExpiresAt.Equal(*curWorkflow.DraftExpiresAt) {
		t.Fatal("stored timestamps do not match")
	}
}
func TestSealUnreadyRouteLeavesDraftUnchanged(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)

	var workflowID uuid.UUID
	err := sharedPool.QueryRow(ctx, `
		INSERT INTO workflows
			(name, status, sealed_at, draft_expires_at)
		VALUES
			($1, 'DRAFT', NULL, NOW() + INTERVAL '24 hours')
		RETURNING id
	`, "seal readiness test").Scan(&workflowID)
	if err != nil {
		t.Fatal(err)
	}

	parent, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflowID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"parent"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := sharedRepo.CreateJob(ctx, revision2.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflowID, TaskType: revision2.TaskType,
		ParentIDs:         []uuid.UUID{parent.ID},
		ExecutionContract: revision2, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"child"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	notReady := errors.New("worker unavailable")

	checkReady := func(_ context.Context, entry execution.CatalogEntry) error {
		if entry.CapabilityStream == revision2.CapabilityStream {
			return notReady
		}
		return nil
	}

	if _, err := sharedRepo.SealWorkflow(ctx, workflowID, nil); err == nil {
		t.Fatal("expected an error when readiness checker is missing")
	}

	_, err = sharedRepo.SealWorkflow(ctx, workflowID, checkReady)
	if !errors.Is(err, notReady) {
		t.Fatalf("seal error = %v, want %v", err, notReady)
	}
	gotWorkflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal("Got error during Worflow getter")
	}
	if gotWorkflow.Status != domain.WorkflowStatusDraft || gotWorkflow.SealedAt != nil {
		t.Fatal("Getting wrong status or SealtAt is not nill")
	}
	gotParentJob, err := sharedRepo.GetTask(ctx, parent.ID)
	if err != nil {
		t.Fatal("Got error during getting parent job by id")
	}
	gotChildJob, err := sharedRepo.GetTask(ctx, child.ID)
	if err != nil {
		t.Fatal("Got error during getting parent job by id")
	}
	if gotParentJob.Status != domain.TaskStatusPending || gotChildJob.Status != domain.TaskStatusPending {
		t.Fatal("Tasks are not in pending status")
	}

	var count int
	err = sharedPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox
		WHERE task_id IN ($1, $2)
	`, parent.ID, child.ID).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("outbox entries = %d, want 0", count)
	}

}

func TestSealEmptyWorkflowLeavesDraftUnchanged(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)

	var workflowID uuid.UUID
	err := sharedPool.QueryRow(ctx, `
		INSERT INTO workflows
			(name, status, sealed_at, draft_expires_at)
		VALUES
			($1, 'DRAFT', NULL, NOW() + INTERVAL '24 hours')
		RETURNING id
	`, "seal readiness test").Scan(&workflowID)
	if err != nil {
		t.Fatal(err)
	}
	checkReady := func(context.Context, execution.CatalogEntry) error {
		t.Fatal("readiness must not be checked for an empty workflow")
		return nil
	}
	_, err = sharedRepo.SealWorkflow(ctx, workflowID, checkReady)
	if !errors.Is(err, repository.ErrEmptyWorkflow) {
		t.Fatalf("seal error = %v, want ErrEmptyWorkflow", err)
	}
	got, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal("Got error in fetching workflow data")
	}
	if got.Status != domain.WorkflowStatusDraft || got.SealedAt != nil {
		t.Fatal("Either status is not in draft or SealAt is not nil")
	}
}
