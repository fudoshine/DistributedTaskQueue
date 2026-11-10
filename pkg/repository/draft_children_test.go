package repository_test

import (
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/repository"
	"testing"

	"github.com/google/uuid"
)

func TestCompletionRejectsDraftOrForeignWorkflowChildren(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	for _, tc := range []struct {
		name    string
		foreign bool
		draft   bool
	}{
		{"foreign draft workflow", true, true},
		{"foreign non-draft workflow", true, false},
		{"same draft workflow", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			workflow, err := sharedRepo.CreateWorkflow(ctx, tc.name)
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
			childWorkflow := workflow
			parents := []uuid.UUID{parent.ID}
			if tc.foreign {
				childWorkflow, err = sharedRepo.CreateWorkflow(ctx, "foreign child")
				if err != nil {
					t.Fatal(err)
				}
				parents = nil
			}
			child, err := sharedRepo.CreateJob(ctx, contract.CapabilityStream, repository.Task{
				ID: uuid.New(), WorkflowID: childWorkflow.ID, ParentIDs: parents, TaskType: contract.TaskType,
				ExecutionContract: contract, EffectKey: uuid.New(),
			})
			if err != nil {
				t.Fatal(err)
			}
			sealWorkflow(t, workflow.ID)
			deliveryID, err := getDeliveryID(ctx, parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := sharedRepo.TryStartJob(ctx, contract.CapabilityStream, parent.ID.String(), deliveryID.String(), 0, contract.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			if tc.foreign {
				if _, err := sharedPool.Exec(ctx, `INSERT INTO job_dependencies (child_job_id, parent_job_id) VALUES ($1, $2)`, child.ID, parent.ID); err != nil {
					t.Fatal(err)
				}
			}
			wantWorkflowStatus := domain.WorkflowStatusDraft
			if !tc.draft {
				wantWorkflowStatus = domain.WorkflowStatusPending
			}
			if _, err := sharedPool.Exec(ctx, `UPDATE workflows SET status = $2 WHERE id = $1`, childWorkflow.ID, wantWorkflowStatus); err != nil {
				t.Fatal(err)
			}

			children, err := sharedRepo.CompleteJobAndScheduleChildren(ctx, contract.CapabilityStream, parent.ID.String(), claim.LeaseToken)
			if err != nil {
				t.Fatal(err)
			}
			if len(children) != 0 {
				t.Errorf("completion scheduled invalid child: %v", children)
			}
			var status domain.TaskStatus
			var attempts, outboxRows int
			var leaseTokenIsNull, leaseExpiresAtIsNull bool
			if err := sharedPool.QueryRow(ctx, `
				SELECT status, attempt_number, lease_token IS NULL, lease_expires_at IS NULL,
				       (SELECT COUNT(*) FROM outbox WHERE task_id = job.id)
				FROM processed_jobs job WHERE id = $1
			`, child.ID).Scan(&status, &attempts, &leaseTokenIsNull, &leaseExpiresAtIsNull, &outboxRows); err != nil {
				t.Fatal(err)
			}
			if status != domain.TaskStatusPending || attempts != 0 || !leaseTokenIsNull || !leaseExpiresAtIsNull || outboxRows != 0 {
				t.Errorf("invalid child changed: status=%s attempts=%d null lease=%t/%t outbox=%d",
					status, attempts, leaseTokenIsNull, leaseExpiresAtIsNull, outboxRows)
			}
			storedParent, err := sharedRepo.GetTask(ctx, parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			if storedParent.Status != domain.TaskStatusCompleted {
				t.Errorf("parent status = %s, want COMPLETED", storedParent.Status)
			}
			storedWorkflow, err := sharedRepo.GetWorkflow(ctx, childWorkflow.ID)
			if err != nil {
				t.Fatal(err)
			}
			if storedWorkflow.Status != wantWorkflowStatus {
				t.Errorf("child workflow status = %s, want %s", storedWorkflow.Status, wantWorkflowStatus)
			}
		})
	}
}
