package repository_test

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
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
func TestWorkflowRoutes(t *testing.T) {
	RequireRepositoryIntegration(t)
	type routeKey struct {
		TaskType         string
		HandlerVersion   string
		RouteRevision    int
		CapabilityStream string
		ExecutionPool    string
	}
	keyFor := func(entry execution.CatalogEntry) routeKey {
		return routeKey{
			TaskType:         entry.TaskType,
			HandlerVersion:   entry.HandlerVersion,
			RouteRevision:    entry.RouteRevision,
			CapabilityStream: entry.CapabilityStream,
			ExecutionPool:    entry.ExecutionPool,
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*execution.CatalogEntry)
	}{
		{"identical", nil},
		{"task_type", func(entry *execution.CatalogEntry) { entry.TaskType += "-other" }},
		{"handler_version", func(entry *execution.CatalogEntry) { entry.HandlerVersion += "-other" }},
		{"route_revision", func(entry *execution.CatalogEntry) { entry.RouteRevision++ }},
		{"capability_stream", func(entry *execution.CatalogEntry) { entry.CapabilityStream += "-other" }},
		{"execution_pool", func(entry *execution.CatalogEntry) { entry.ExecutionPool += "-other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			first := referenceContract(t)
			second := first
			if tc.mutate != nil {
				tc.mutate(&second)
			}
			workflowID, jobs := seedWorkflowSealDraft(t, first, second)
			before := workflowSealSnapshot(t, workflowID)

			unavailable := errors.New("worker unavailable")
			checkedRoutes := make(map[routeKey]int)

			checkReady := func(_ context.Context, entry execution.CatalogEntry) error {
				checkedRoutes[keyFor(entry)]++
				if len(checkedRoutes) == 2 {
					return unavailable
				}
				return nil
			}
			workflow, err := sharedRepo.SealWorkflow(ctx, workflowID, checkReady)
			if tc.mutate == nil {
				if err != nil {
					t.Fatalf("seal identical routes: %v", err)
				}
				if workflow.ID != workflowID || workflow.Status != domain.WorkflowStatusPending || workflow.SealedAt == nil {
					t.Fatalf("sealed workflow = %+v", workflow)
				}
				assertWorkflowSealPublications(t, workflowID, jobs...)
			} else {
				if !errors.Is(err, unavailable) {
					t.Fatalf("seal error = %v, want %v", err, unavailable)
				}
				if !reflect.DeepEqual(workflow, repository.Workflow{}) {
					t.Fatalf("seal = %+v, want empty workflow", workflow)
				}
				if after := workflowSealSnapshot(t, workflowID); after != before {
					t.Fatalf("failed seal changed draft: before %s, after %s", before, after)
				}
			}
			wantChecks := map[routeKey]int{keyFor(first): 1, keyFor(second): 1}
			if !reflect.DeepEqual(checkedRoutes, wantChecks) {
				t.Fatalf("readiness checks = %v, want %v", checkedRoutes, wantChecks)
			}
		})
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
func TestSealInvalidContractLeavesDraftUnchanged(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := context.Background()

	revision1 := referenceContract(t)
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

	job, err := sharedRepo.CreateJob(ctx, revision1.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflowID, TaskType: revision1.TaskType,
		ExecutionContract: revision1, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"parent"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sharedPool.Exec(ctx, `
		UPDATE processed_jobs
		SET execution_contract = '{}'::jsonb
		WHERE id = $1
	`, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	checkReady := func(context.Context, execution.CatalogEntry) error {
		t.Fatal("invalid contract must be rejected before readiness")
		return nil
	}
	_, err = sharedRepo.SealWorkflow(ctx, workflowID, checkReady)
	if err == nil || !strings.Contains(err.Error(), "invalid saved execution contract") {
		t.Fatalf("expected invalid saved contract error, got %v", err)
	}
	gotWorkflow, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal("Got error during fetching workflow detail")
	}
	gotTask, err := sharedRepo.GetTask(ctx, job.ID)
	if err != nil {
		t.Fatal("Got error during fetchign task detail")
	}
	if gotWorkflow.SealedAt != nil || gotWorkflow.Status != domain.WorkflowStatusDraft {
		t.Fatal("Workflow status is not in draft or it's sealedAt is not null")
	}
	if gotTask.Status != domain.TaskStatusPending {
		t.Fatal("Task is not in pending status")
	}
	var count int
	err = sharedPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox
		WHERE task_id=$1
	`, job.ID).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("outbox entries = %d, want 0", count)
	}
}

func TestSealWorkflowActivatesOnlyRoots(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	rootContract := referenceContract(t)
	workflowID, roots := seedWorkflowSealDraft(t, rootContract, rootContract)
	tenantID := uuid.New()
	if _, err := sharedPool.Exec(ctx, `UPDATE workflows SET tenant_id = $2 WHERE id = $1`, workflowID, tenantID); err != nil {
		t.Fatal(err)
	}
	childContract := rootContract
	childContract.RouteRevision++
	childContract.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", childContract.TaskType, childContract.HandlerVersion, childContract.RouteRevision)
	childContract.MaxJobAge *= 2
	child, err := sharedRepo.CreateJob(ctx, childContract.CapabilityStream, repository.Task{
		ID: uuid.New(), WorkflowID: workflowID, TaskType: childContract.TaskType,
		ParentIDs:         []uuid.UUID{roots[0].ID, roots[1].ID},
		ExecutionContract: childContract, EffectKey: uuid.New(),
		Payload: json.RawMessage(`{"message":"child"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs := append(roots, child)
	assertWorkflowSealPublications(t, workflowID)
	for _, job := range jobs {
		var status domain.TaskStatus
		var deadline *time.Time
		if err := sharedPool.QueryRow(ctx, `SELECT status, job_deadline_at FROM processed_jobs WHERE id = $1`, job.ID).Scan(&status, &deadline); err != nil {
			t.Fatal(err)
		}
		if status != domain.TaskStatusPending || deadline != nil {
			t.Fatalf("draft job %s: status = %s, deadline = %v", job.ID, status, deadline)
		}
	}

	checkedRoutes := make(map[string]int)
	sealed, err := sharedRepo.SealWorkflow(ctx, workflowID, func(_ context.Context, entry execution.CatalogEntry) error {
		checkedRoutes[entry.CapabilityStream]++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sealed.ID != workflowID || sealed.Status != domain.WorkflowStatusPending || sealed.SealedAt == nil {
		t.Fatalf("sealed workflow = %+v", sealed)
	}
	stored, err := sharedRepo.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, sealed) {
		t.Fatalf("stored workflow %+v does not match seal response %+v", stored, sealed)
	}
	if len(checkedRoutes) != 2 || checkedRoutes[rootContract.CapabilityStream] != 1 || checkedRoutes[childContract.CapabilityStream] != 1 {
		t.Fatalf("readiness checks = %v, want one per saved route", checkedRoutes)
	}
	for i, job := range jobs {
		var status domain.TaskStatus
		var deadline *time.Time
		if err := sharedPool.QueryRow(ctx, `SELECT status, job_deadline_at FROM processed_jobs WHERE id = $1`, job.ID).Scan(&status, &deadline); err != nil {
			t.Fatal(err)
		}
		wantStatus := domain.TaskStatusQueued
		if i == len(roots) {
			wantStatus = domain.TaskStatusPending
		}
		wantDeadline := sealed.SealedAt.Add(job.ExecutionContract.MaxJobAge)
		if status != wantStatus || deadline == nil || !deadline.Equal(wantDeadline) {
			t.Fatalf("job %s: status = %s, deadline = %v; want %s, %v", job.ID, status, deadline, wantStatus, wantDeadline)
		}
	}
	assertWorkflowSealPublications(t, workflowID, roots...)
	for _, root := range roots {
		var notBefore time.Time
		if err := sharedPool.QueryRow(ctx, `SELECT not_before FROM outbox WHERE task_id = $1 AND generation = 0`, root.ID).Scan(&notBefore); err != nil {
			t.Fatal(err)
		}
		if !notBefore.Equal(*sealed.SealedAt) {
			t.Fatalf("root %s publication time = %v, want seal time %v", root.ID, notBefore, *sealed.SealedAt)
		}
	}
}

func TestSealWorkflowRejectsExpiredDraftWithoutChanges(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	workflowID, _ := seedWorkflowSealDraft(t, referenceContract(t))
	if _, err := sharedPool.Exec(ctx, `UPDATE workflows SET draft_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, workflowID); err != nil {
		t.Fatal(err)
	}
	before := workflowSealSnapshot(t, workflowID)
	sealed, err := sharedRepo.SealWorkflow(ctx, workflowID, func(context.Context, execution.CatalogEntry) error {
		t.Fatal("expired draft must be rejected before readiness")
		return nil
	})
	if !errors.Is(err, repository.ErrDraftExpired) || sealed.ID != uuid.Nil {
		t.Fatalf("seal = %+v, error = %v; want ErrDraftExpired", sealed, err)
	}
	if after := workflowSealSnapshot(t, workflowID); after != before {
		t.Fatalf("expired seal changed draft: before %s, after %s", before, after)
	}
}

func TestSealWorkflowRejectsResealWithoutPublications(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	contract := referenceContract(t)
	workflowID, roots := seedWorkflowSealDraft(t, contract, contract)
	if _, err := sharedRepo.SealWorkflow(ctx, workflowID, func(context.Context, execution.CatalogEntry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	before := workflowSealSnapshot(t, workflowID)
	sealed, err := sharedRepo.SealWorkflow(ctx, workflowID, func(context.Context, execution.CatalogEntry) error {
		t.Fatal("reseal must be rejected before readiness")
		return nil
	})
	if !errors.Is(err, repository.ErrWorkflowSealed) || sealed.ID != uuid.Nil {
		t.Fatalf("reseal = %+v, error = %v; want ErrWorkflowSealed", sealed, err)
	}
	if after := workflowSealSnapshot(t, workflowID); after != before {
		t.Fatalf("reseal changed workflow: before %s, after %s", before, after)
	}
	assertWorkflowSealPublications(t, workflowID, roots...)
}

func TestSealWorkflowRejectsLateAppend(t *testing.T) {
	RequireRepositoryIntegration(t)
	ctx := t.Context()
	contract := referenceContract(t)
	workflowID, roots := seedWorkflowSealDraft(t, contract)
	if _, err := sharedRepo.SealWorkflow(ctx, workflowID, func(context.Context, execution.CatalogEntry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	before := workflowSealSnapshot(t, workflowID)
	for _, tc := range []struct {
		name    string
		parents []uuid.UUID
	}{
		{"root", nil},
		{"child", []uuid.UUID{roots[0].ID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job, err := sharedRepo.CreateJob(t.Context(), contract.CapabilityStream, repository.Task{
				ID: uuid.New(), WorkflowID: workflowID, TaskType: contract.TaskType,
				ParentIDs: tc.parents, ExecutionContract: contract, EffectKey: uuid.New(),
			})
			if !errors.Is(err, repository.ErrWorkflowSealed) || job.ID != uuid.Nil {
				t.Fatalf("late append = %+v, error = %v; want ErrWorkflowSealed", job, err)
			}
			if after := workflowSealSnapshot(t, workflowID); after != before {
				t.Fatalf("late append changed workflow: before %s, after %s", before, after)
			}
		})
	}
}

func TestSealWorkflowRollsBackOutboxFailure(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	longRoute := contract
	longRoute.CapabilityStream = strings.Repeat("q", 256)
	workflowID, _ := seedWorkflowSealDraft(t, contract, longRoute)
	before := workflowSealSnapshot(t, workflowID)
	sealed, err := sharedRepo.SealWorkflow(t.Context(), workflowID, func(context.Context, execution.CatalogEntry) error { return nil })
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22001" || sealed.ID != uuid.Nil {
		t.Fatalf("seal = %+v, error = %v; want outbox string-length violation (22001)", sealed, err)
	}
	if after := workflowSealSnapshot(t, workflowID); after != before {
		t.Fatalf("failed seal changed draft: before %s, after %s", before, after)
	}
}

func TestSealWorkflowConcurrentSealsPublishOnce(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	workflowID, roots := seedWorkflowSealDraft(t, contract, contract)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := sharedRepo.SealWorkflow(ctx, workflowID, func(context.Context, execution.CatalogEntry) error { return nil })
			results <- err
		}()
	}
	close(start)
	var successes, alreadySealed int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				successes++
			case errors.Is(err, repository.ErrWorkflowSealed):
				alreadySealed++
			default:
				t.Errorf("concurrent seal: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if successes != 1 || alreadySealed != 1 {
		t.Fatalf("successful seals = %d, already-sealed errors = %d; want one each", successes, alreadySealed)
	}
	assertWorkflowSealPublications(t, workflowID, roots...)
}

func seedWorkflowSealDraft(t *testing.T, contracts ...execution.CatalogEntry) (uuid.UUID, []repository.JobRow) {
	t.Helper()
	var workflowID uuid.UUID
	if err := sharedPool.QueryRow(t.Context(), `
		INSERT INTO workflows (name, status, sealed_at, draft_expires_at)
		VALUES ('workflow seal test', 'DRAFT', NULL, NOW() + INTERVAL '24 hours')
		RETURNING id
	`).Scan(&workflowID); err != nil {
		t.Fatal(err)
	}
	jobs := make([]repository.JobRow, 0, len(contracts))
	for _, contract := range contracts {
		job, err := sharedRepo.CreateJob(t.Context(), contract.CapabilityStream, repository.Task{
			ID: uuid.New(), WorkflowID: workflowID, TaskType: contract.TaskType,
			ExecutionContract: contract, EffectKey: uuid.New(),
			Payload: json.RawMessage(`{"message":"root"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	return workflowID, jobs
}

func workflowSealSnapshot(t *testing.T, workflowID uuid.UUID) string {
	t.Helper()
	var snapshot string
	if err := sharedPool.QueryRow(t.Context(), `
		SELECT jsonb_build_object(
			'workflow', (SELECT to_jsonb(w) FROM workflows w WHERE w.id = $1),
			'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY j.id)
				FROM processed_jobs j WHERE j.workflow_id = $1),
			'dependencies', (SELECT jsonb_agg(to_jsonb(e) ORDER BY e.child_job_id, e.parent_job_id)
				FROM job_dependencies e JOIN processed_jobs j ON j.id = e.child_job_id
				WHERE j.workflow_id = $1),
			'outbox', (SELECT jsonb_agg(to_jsonb(o) ORDER BY o.id)
				FROM outbox o JOIN processed_jobs j ON j.id = o.task_id
				WHERE j.workflow_id = $1)
		)::text
	`, workflowID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertWorkflowSealPublications(t *testing.T, workflowID uuid.UUID, roots ...repository.JobRow) {
	t.Helper()
	var total int
	if err := sharedPool.QueryRow(t.Context(), `
		SELECT COUNT(*) FROM outbox o
		JOIN processed_jobs j ON j.id = o.task_id
		WHERE j.workflow_id = $1
	`, workflowID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != len(roots) {
		t.Fatalf("outbox entries = %d, want %d", total, len(roots))
	}
	for _, root := range roots {
		var count, valid int
		if err := sharedPool.QueryRow(t.Context(), `
			SELECT COUNT(*), COUNT(*) FILTER (
				WHERE queue_name = $2 AND generation = 0 AND delivery_id IS NOT NULL AND NOT delivered
			)
			FROM outbox WHERE task_id = $1
		`, root.ID, root.ExecutionContract.CapabilityStream).Scan(&count, &valid); err != nil {
			t.Fatal(err)
		}
		if count != 1 || valid != 1 {
			t.Fatalf("root %s: outbox entries = %d, valid initial publications = %d; want one each", root.ID, count, valid)
		}
	}
}
