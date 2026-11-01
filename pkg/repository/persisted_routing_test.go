package repository_test

import (
	"context"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// checks that a child uses its own saved route when its parent finishes.
func TestPersistedJobRouting(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)
	workflow, err := sharedRepo.CreateWorkflow(ctx, "persisted child routing")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"parent"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := sharedRepo.CreateJob(ctx, revision2.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision2.TaskType,
		ParentIDs:         []uuid.UUID{parent.ID},
		ExecutionContract: revision2, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"child"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	deliveryID, err := getDeliveryID(ctx, parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := sharedRepo.TryStartJob(ctx, revision1.CapabilityStream, parent.ID.String(), deliveryID.String(), 0, revision1.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sharedRepo.CompleteJobAndScheduleChildren(ctx, revision1.CapabilityStream, parent.ID.String(), claim.LeaseToken); err != nil {
		t.Fatal(err)
	}

	var queueName string
	if err := sharedPool.QueryRow(ctx, `SELECT queue_name FROM outbox WHERE task_id = $1 AND generation = 0`, child.ID).Scan(&queueName); err != nil {
		t.Fatal(err)
	}
	if queueName != revision2.CapabilityStream {
		t.Fatalf("child initial outbox queue = %q, want revision-2 stream %q", queueName, revision2.CapabilityStream)
	}
}

// A job created with r1 must retry on r1, even if the caller supplies r2 later.
func TestRetryUsesPersistedRoute(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)

	workflow, err := sharedRepo.CreateWorkflow(ctx, "persisted child routing")
	if err != nil {
		t.Fatal(err)
	}
	job, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"job"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := getDeliveryID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := sharedRepo.TryStartJob(ctx, revision1.CapabilityStream, job.ID.String(), deliveryID.String(), 0, revision1.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = sharedRepo.RecordFailure(
		ctx,
		revision2.CapabilityStream,
		job.ID.String(),
		claim.LeaseToken,
		"temporary failure",
	)
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}
	var queueName string
	if err := sharedPool.QueryRow(ctx, `SELECT queue_name FROM outbox WHERE task_id = $1 AND generation = 1`, job.ID).Scan(&queueName); err != nil {
		t.Fatal(err)
	}
	if queueName != revision1.CapabilityStream {
		t.Fatalf("retry outbox queue = %q, want saved stream %q",
			queueName, revision1.CapabilityStream)
	}
}

func TestExpiredLeaseRecoveryUsesPersistedRoutes(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)

	workflow, err := sharedRepo.CreateWorkflow(ctx, "persisted child routing")
	if err != nil {
		t.Fatal(err)
	}
	job1, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"job"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	job2, err := sharedRepo.CreateJob(ctx, revision2.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision2.TaskType,
		ExecutionContract: revision2, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"job"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID1, err := getDeliveryID(ctx, job1.ID)
	if err != nil {
		t.Fatal(err)
	}
	deliveryID2, err := getDeliveryID(ctx, job2.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sharedRepo.TryStartJob(ctx, revision1.CapabilityStream, job1.ID.String(), deliveryID1.String(), 0, revision1.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sharedRepo.TryStartJob(ctx, revision2.CapabilityStream, job2.ID.String(), deliveryID2.String(), 0, revision2.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sharedPool.Exec(ctx, `
		UPDATE processed_jobs
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE id IN ($1, $2)
	`, job1.ID, job2.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sharedRepo.RecoverExpiredLeases(
		ctx, 10,
	)
	if err != nil {
		t.Fatalf("recover expired leases: %v", err)
	}
	var queueName1, queueName2 string
	if err := sharedPool.QueryRow(ctx, `SELECT queue_name FROM outbox WHERE task_id = $1 AND generation = 1`, job1.ID).Scan(&queueName1); err != nil {
		t.Fatal(err)
	}
	if err := sharedPool.QueryRow(ctx, `SELECT queue_name FROM outbox WHERE task_id = $1 AND generation = 1`, job2.ID).Scan(&queueName2); err != nil {
		t.Fatal(err)
	}

	if queueName1 != revision1.CapabilityStream {
		t.Fatalf("recovered job1 outbox queue = %q, want saved stream %q",
			queueName1, revision1.CapabilityStream)
	}
	if queueName2 != revision2.CapabilityStream {
		t.Fatalf("recovered job2 outbox queue = %q, want saved stream %q",
			queueName2, revision2.CapabilityStream)
	}

}

func TestTryStartRejectsWrongRoute(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
	revision2 := revision1
	revision2.RouteRevision = 2
	revision2.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revision2.TaskType, revision2.HandlerVersion, revision2.RouteRevision)

	workflow, err := sharedRepo.CreateWorkflow(ctx, "persisted child routing")
	if err != nil {
		t.Fatal(err)
	}
	job, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflow.ID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"job"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := getDeliveryID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := sharedRepo.TryStartJob(ctx, revision2.CapabilityStream, job.ID.String(), deliveryID.String(), 0, revision2.LeaseTTL)
	if !errors.Is(err, repository.ErrRouteMismatch) {
		t.Fatalf("error = %v, want ErrRouteMismatch", err)
	}
	if task.LeaseToken != uuid.Nil {
		t.Fatalf("expected empty task on rejected TryStartJob, got %+v", task)
	}
	var status string
	var attemptNumber int
	var leaseTokenIsNull, leaseExpiresAtIsNull bool
	err = sharedPool.QueryRow(ctx, `SELECT status,attempt_number,lease_token IS NULL, lease_expires_at IS NULL FROM processed_jobs WHERE id = $1`, job.ID).Scan(&status, &attemptNumber, &leaseTokenIsNull, &leaseExpiresAtIsNull)
	if err != nil {
		t.Fatal(err)
	}
	if status != "QUEUED" {
		t.Fatalf("expected job status to be 'QUEUED', got %q", status)
	}
	if attemptNumber != 0 {
		t.Fatalf("expected job attempt number to be 0, got %d", attemptNumber)
	}
	if !leaseTokenIsNull {
		t.Fatalf("expected job lease token to be NULL, got %v", leaseTokenIsNull)
	}
	if !leaseExpiresAtIsNull {
		t.Fatalf("expected job lease expires at to be NULL, got %v", leaseExpiresAtIsNull)
	}
	task, err = sharedRepo.TryStartJob(ctx, revision1.CapabilityStream, job.ID.String(), deliveryID.String(), 0, revision2.LeaseTTL)
	if err != nil {
		t.Fatal("expected TryStartJob to succeed with correct route")
	}
	if task.LeaseToken == uuid.Nil || task.Attempt != 1 {
		t.Fatalf("correct-route claim = %+v, want token and attempt 1", task)
	}
	err = sharedPool.QueryRow(ctx, `SELECT status,attempt_number,lease_token IS NULL, lease_expires_at IS NULL FROM processed_jobs WHERE id = $1`, job.ID).Scan(&status, &attemptNumber, &leaseTokenIsNull, &leaseExpiresAtIsNull)
	if err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("expected job status to be 'RUNNING', got %q", status)
	}
	if attemptNumber != 1 {
		t.Fatalf("expected job attempt number to be 1, got %d", attemptNumber)
	}
	if leaseTokenIsNull {
		t.Fatalf("expected job lease token to be not NULL, got %v", leaseTokenIsNull)
	}
	if leaseExpiresAtIsNull {
		t.Fatalf("expected job lease expires at to be not NULL, got %v", leaseExpiresAtIsNull)
	}

}

func TestJobNotFound(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()
	revision1 := referenceContract(t)
	task, err := sharedRepo.TryStartJob(ctx, revision1.CapabilityStream, uuid.New().String(), uuid.New().String(), 0, revision1.LeaseTTL)
	if !errors.Is(err, repository.ErrJobNotFound) {
		t.Fatalf("error = %v, want ErrJobNotFound", err)
	}
	if task.LeaseToken != uuid.Nil {
		t.Fatalf("expected empty task on not-found TryStartJob, got %+v", task)
	}
}
