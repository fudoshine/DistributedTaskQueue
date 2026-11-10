package repository_test

import (
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/repository"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestTryStartRejectsDraftWorkflow(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	contract := referenceContract(t)
	workflow, err := sharedRepo.CreateWorkflow(ctx, "injected draft delivery")
	if err != nil {
		t.Fatal(err)
	}
	job, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: contract.TaskType,
		ExecutionContract: contract, EffectKey: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an incorrect activation so task status alone cannot protect the draft.
	if _, err := sharedPool.Exec(ctx, `UPDATE processed_jobs SET status = $2 WHERE id = $1`, job.ID, domain.TaskStatusQueued); err != nil {
		t.Fatal(err)
	}
	deliveryID := uuid.New()
	if _, err := sharedPool.Exec(ctx, `
		INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before)
		VALUES ($1, $2, $3, 0, NOW())
	`, job.ID, contract.CapabilityStream, deliveryID); err != nil {
		t.Fatal(err)
	}
	claim, err := sharedRepo.TryStartJob(ctx, contract.CapabilityStream, job.ID.String(), deliveryID.String(), 0, contract.LeaseTTL)
	if !errors.Is(err, repository.ErrJobNotReady) || claim != (repository.JobClaim{}) {
		t.Fatalf("draft claim = %+v, error = %v; want empty claim and ErrJobNotReady", claim, err)
	}
	var status domain.TaskStatus
	var workflowStatus domain.WorkflowStatus
	var attempts int
	var tokenIsNull, expiryIsNull bool
	if err := sharedPool.QueryRow(ctx, `
		SELECT job.status, job.attempt_number, job.lease_token IS NULL,
		       job.lease_expires_at IS NULL, workflow.status
		FROM processed_jobs job JOIN workflows workflow ON workflow.id = job.workflow_id
		WHERE job.id = $1
	`, job.ID).Scan(&status, &attempts, &tokenIsNull, &expiryIsNull, &workflowStatus); err != nil {
		t.Fatal(err)
	}
	if status != domain.TaskStatusQueued || attempts != 0 || !tokenIsNull || !expiryIsNull || workflowStatus != domain.WorkflowStatusDraft {
		t.Fatalf("draft delivery changed execution state: status=%s attempts=%d lease_null=%v expiry_null=%v workflow=%s",
			status, attempts, tokenIsNull, expiryIsNull, workflowStatus)
	}
}

func TestLegacyUnsealedWorkflowCanExecute(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	contract := referenceContract(t)
	workflow, err := sharedRepo.CreateWorkflow(ctx, "legacy workflow draining")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: contract.TaskType,
		ExecutionContract: contract, EffectKey: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: contract.TaskType,
		ParentIDs: []uuid.UUID{parent.ID}, ExecutionContract: contract, EffectKey: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sharedPool.Exec(ctx, `UPDATE workflows SET status = $2, sealed_at = NULL WHERE id = $1`, workflow.ID, domain.WorkflowStatusPending); err != nil {
		t.Fatal(err)
	}
	if _, err := sharedPool.Exec(ctx, `UPDATE processed_jobs SET status = $2 WHERE id = $1`, parent.ID, domain.TaskStatusQueued); err != nil {
		t.Fatal(err)
	}
	deliveryID := uuid.New()
	if _, err := sharedPool.Exec(ctx, `
		INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before)
		VALUES ($1, $2, $3, 0, NOW())
	`, parent.ID, contract.CapabilityStream, deliveryID); err != nil {
		t.Fatal(err)
	}
	claim, err := sharedRepo.TryStartJob(ctx, contract.CapabilityStream, parent.ID.String(), deliveryID.String(), 0, contract.LeaseTTL)
	if err != nil || claim.LeaseToken == uuid.Nil || claim.Attempt != 1 {
		t.Fatalf("legacy parent claim = %+v, error = %v", claim, err)
	}
	children, err := sharedRepo.CompleteJobAndScheduleChildren(ctx, contract.CapabilityStream, parent.ID.String(), claim.LeaseToken)
	if err != nil || len(children) != 1 || children[0] != child.ID.String() {
		t.Fatalf("legacy completion children = %v, error = %v; want [%s]", children, err, child.ID)
	}
	childDeliveryID, err := getDeliveryID(ctx, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	childClaim, err := sharedRepo.TryStartJob(ctx, contract.CapabilityStream, child.ID.String(), childDeliveryID.String(), 0, contract.LeaseTTL)
	if err != nil || childClaim.LeaseToken == uuid.Nil || childClaim.Attempt != 1 {
		t.Fatalf("legacy child claim = %+v, error = %v", childClaim, err)
	}
	stored, err := sharedRepo.GetWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != domain.WorkflowStatusPending || stored.SealedAt != nil {
		t.Fatalf("legacy execution changed workflow state: %+v", stored)
	}
}
