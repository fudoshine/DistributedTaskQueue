package repository_test

import (
	"context"
	"distributed-queue/internal/domain"
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
	if workflow.Status != domain.WorkflowStatusPending {
		t.Fatalf("Expected status is pending but getting different %v", workflow.Status)
	}
	curWorkflow, err := sharedRepo.GetWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("Getting error in fetching workflow detail %v", err)
	}
	if workflow.ID != curWorkflow.ID ||
		workflow.Status != curWorkflow.Status ||
		workflow.TenantID != curWorkflow.TenantID ||
		workflow.SealedAt != curWorkflow.SealedAt ||
		workflow.DraftExpiresAt != curWorkflow.DraftExpiresAt ||
		workflow.Name != curWorkflow.Name {
		t.Fatalf("Same instance returning different  value %v : %v", workflow, curWorkflow)
	}
	if !workflow.CreatedAt.Equal(curWorkflow.CreatedAt) ||
		!workflow.UpdatedAt.Equal(curWorkflow.UpdatedAt) {
		t.Fatal("stored timestamps do not match")
	}
}
