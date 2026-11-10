package repository

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/redis/metrics"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CreateWorkflow inserts a new workflow row and returns its generated ID.
// Call this once per logical batch of related tasks before creating the
// tasks themselves, since processed_jobs.workflow_id is NOT NULL with a
// foreign key into workflows.
func (r *JobRepository) CreateWorkflow(ctx context.Context, name string) (Workflow, error) {
	var workflow Workflow
	query := `
		INSERT INTO workflows (name, status,draft_expires_at)
		VALUES ($1, $2, NOW() + INTERVAL '24 hours')
		RETURNING id, name, sealed_at, status, tenant_id,
          created_at, updated_at, draft_expires_at;
	`
	if err := r.pool.QueryRow(ctx, query, name, domain.WorkflowStatusDraft).Scan(&workflow.ID, &workflow.Name, &workflow.SealedAt, &workflow.Status, &workflow.TenantID, &workflow.CreatedAt, &workflow.UpdatedAt, &workflow.DraftExpiresAt); err != nil {
		metrics.JobRepoErrors.WithLabelValues("_workflow", "CreateWorkflow").Inc()
		return Workflow{}, fmt.Errorf("failed to create workflow %q: %w", name, err)
	}
	metrics.WorkflowsCreated.Inc()
	return workflow, nil
}

func (r *JobRepository) SealWorkflow(ctx context.Context, workflowID uuid.UUID, checkReady func(context.Context, execution.CatalogEntry) error,
) (Workflow, error) {
	if checkReady == nil {
		return Workflow{}, errors.New("readiness checker is required")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Workflow{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // Rollback is a no-op after commit.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0));`,
		workflowID,
	); err != nil {
		return Workflow{}, fmt.Errorf("failed to lock workflow: %w", err)
	}
	var status domain.WorkflowStatus
	var draftExpiresAt *time.Time
	var sealedAt *time.Time
	var dbNow time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, draft_expires_at, sealed_at, clock_timestamp()
		FROM workflows
		WHERE id = $1
	`, workflowID).Scan(&status, &draftExpiresAt, &sealedAt, &dbNow)
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, fmt.Errorf("workflow %s not found: %w", workflowID, err)
	}
	if err != nil {
		return Workflow{}, fmt.Errorf("failed to query workflow: %w", err)
	}
	if sealedAt != nil {
		return Workflow{}, ErrWorkflowSealed
	}
	if status != domain.WorkflowStatusDraft {
		return Workflow{}, ErrWorkflowNotDraft
	}
	if draftExpiresAt == nil || !draftExpiresAt.After(dbNow) {
		return Workflow{}, ErrDraftExpired
	}

	rows, err := tx.Query(ctx, `SELECT execution_contract,ID,task_type,effect_key,status FROM processed_jobs WHERE workflow_id=$1`, workflowID)
	if err != nil {
		return Workflow{}, fmt.Errorf("failed to query jobs for workflow %s: %w", workflowID, err)
	}
	defer rows.Close()
	jobs := []JobRow{}
	for rows.Next() {
		var contractJSON []byte
		var id uuid.UUID
		var taskType string
		var effectKey uuid.UUID
		var status domain.TaskStatus
		if err := rows.Scan(&contractJSON, &id, &taskType, &effectKey, &status); err != nil {
			return Workflow{}, fmt.Errorf("failed to scan contract: %w", err)
		}

		var entry execution.CatalogEntry
		if err := json.Unmarshal(contractJSON, &entry); err != nil {
			return Workflow{}, fmt.Errorf("failed to decode contract: %w", err)
		}
		if err := entry.Validate(); err != nil {
			return Workflow{}, fmt.Errorf("%w for job %s: %w", ErrInvalidExecutionContract, id, err)
		}

		if status != domain.TaskStatusPending {
			return Workflow{}, fmt.Errorf("job %s is not pending", id)
		}
		if entry.TaskType != taskType {
			return Workflow{}, fmt.Errorf("job %s task type mismatch: expected %q, got %q", id, entry.TaskType, taskType)
		}
		if effectKey == uuid.Nil {
			return Workflow{}, fmt.Errorf("job %s has no effect key", id)
		}
		jobs = append(jobs, JobRow{
			ID:                id,
			TaskType:          taskType,
			EffectKey:         &effectKey,
			ExecutionContract: &entry,
			Status:            status,
		})
	}

	if err := rows.Err(); err != nil {
		return Workflow{}, fmt.Errorf("failed to iterate contracts: %w", err)
	}

	if len(jobs) == 0 {
		return Workflow{}, ErrEmptyWorkflow
	}
	type routeKey struct {
		TaskType         string
		HandlerVersion   string
		RouteRevision    int
		CapabilityStream string
		ExecutionPool    string
	}
	checkedRoutes := make(map[routeKey]struct{}, len(jobs))
	for _, job := range jobs {
		entry := *job.ExecutionContract
		key := routeKey{
			TaskType:         entry.TaskType,
			HandlerVersion:   entry.HandlerVersion,
			RouteRevision:    entry.RouteRevision,
			CapabilityStream: entry.CapabilityStream,
			ExecutionPool:    entry.ExecutionPool,
		}
		if _, checked := checkedRoutes[key]; checked {
			continue
		}
		if err := checkReady(ctx, entry); err != nil {
			return Workflow{}, fmt.Errorf("route %q is not ready: %w", entry.CapabilityStream, err)
		}
		checkedRoutes[key] = struct{}{}
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return Workflow{}, fmt.Errorf("failed to query current database time: %w", err)
	}
	if !draftExpiresAt.After(dbNow) {
		return Workflow{}, ErrDraftExpired
	}

	//Atomic activation
	sealTime := dbNow
	ids := make([]uuid.UUID, len(jobs))
	deadlines := make([]time.Time, len(jobs))

	for i, job := range jobs {
		ids[i] = job.ID
		deadlines[i] = sealTime.Add(job.ExecutionContract.MaxJobAge)
	}
	_, err = tx.Exec(ctx, `
		UPDATE processed_jobs AS job
		SET job_deadline_at = deadline.deadline_at
		FROM unnest($1::uuid[], $2::timestamptz[]) AS deadline(id, deadline_at)
		WHERE job.id = deadline.id AND job.workflow_id = $3
	`, ids, deadlines, workflowID)
	if err != nil {
		return Workflow{}, fmt.Errorf("failed to set job deadlines for workflow %s: %w", workflowID, err)
	}

	// 1. Flip the workflow first; the guard makes sealing idempotent and locks the row.
	var wf Workflow
	err = tx.QueryRow(ctx, `
		UPDATE workflows
		SET status = $2, updated_at = $3, sealed_at = $3
		WHERE id = $1 AND sealed_at IS NULL
		RETURNING id, name, status, tenant_id, sealed_at,
		          draft_expires_at, created_at, updated_at
	`, workflowID, domain.WorkflowStatusPending, sealTime).Scan(
		&wf.ID, &wf.Name, &wf.Status, &wf.TenantID, &wf.SealedAt,
		&wf.DraftExpiresAt, &wf.CreatedAt, &wf.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, ErrWorkflowNotFoundOrAlreadySealed
	}
	if err != nil {
		return Workflow{}, fmt.Errorf("scan workflow %s: %w", workflowID, err)
	}

	// 2. Queue root jobs and write their outbox rows in one statement.
	if _, err := tx.Exec(ctx, `
		WITH queued AS (
			UPDATE processed_jobs AS job
			SET status = $2
			WHERE job.workflow_id = $1
			  AND job.status = $3
			  AND NOT EXISTS (
				SELECT 1 FROM job_dependencies edge
				WHERE edge.child_job_id = job.id
			  )
			RETURNING job.id, job.execution_contract->>'capability_stream' AS queue_name
		)
		INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before, delivered)
		SELECT id, queue_name, gen_random_uuid(), 0, $4, FALSE
		FROM queued
		ON CONFLICT (task_id, generation) DO NOTHING
	`, workflowID, domain.TaskStatusQueued, domain.TaskStatusPending, sealTime); err != nil {
		return Workflow{}, fmt.Errorf("queue root tasks for workflow %s: %w", workflowID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Workflow{}, fmt.Errorf("commit seal for workflow %s: %w", workflowID, err)
	}
	return wf, nil
}

func (r *JobRepository) GetWorkflow(ctx context.Context, workflowID uuid.UUID) (Workflow, error) {
	if workflowID == uuid.Nil {
		return Workflow{}, fmt.Errorf("workflowID is nil")
	}

	var workflow Workflow
	query := `SELECT id, name, status, tenant_id, sealed_at, draft_expires_at,
				created_at, updated_at
				FROM workflows
				WHERE id = $1;`
	err := r.pool.QueryRow(ctx, query, workflowID).Scan(&workflow.ID, &workflow.Name, &workflow.Status, &workflow.TenantID, &workflow.SealedAt, &workflow.DraftExpiresAt, &workflow.CreatedAt, &workflow.UpdatedAt)
	if err != nil {
		return Workflow{}, fmt.Errorf("failed to get workflow for ID %s: %w", workflowID, err)
	}
	return workflow, nil
}

// UpdateWorkflowStatus checks all jobs in a workflow and updates the workflow
// status based on aggregate job states:
// - RUNNING: If any job is RUNNING
// - FAILED: If all jobs are terminal and at least one FAILED
// - COMPLETED: If all jobs are COMPLETED
// - PENDING: Otherwise
func (r *JobRepository) UpdateWorkflowStatus(ctx context.Context, workflowID uuid.UUID) error {
	query := `
		UPDATE workflows w
		SET status = CASE
			-- If any job is RUNNING, workflow is RUNNING
			WHEN EXISTS (
				SELECT 1 FROM processed_jobs 
				WHERE workflow_id = $1 AND status = $2
			) THEN $2
			
			-- If all jobs are terminal (COMPLETED/FAILED/CANCELLED)
			WHEN NOT EXISTS (
				SELECT 1 FROM processed_jobs 
				WHERE workflow_id = $1 
				  AND status NOT IN ($3, $4, $5)
			) THEN 
				-- If any job FAILED, workflow FAILED
				CASE 
					WHEN EXISTS (
						SELECT 1 FROM processed_jobs 
						WHERE workflow_id = $1 AND status = $4
					) THEN $4
					-- Otherwise all COMPLETED/CANCELLED, workflow COMPLETED
					ELSE $3
				END
			
			-- Otherwise keep current status (PENDING or RUNNING)
			ELSE w.status
		END,
		updated_at = NOW()
		WHERE id = $1
		  AND status != CASE
			WHEN EXISTS (
				SELECT 1 FROM processed_jobs 
				WHERE workflow_id = $1 AND status = $2
			) THEN $2
			WHEN NOT EXISTS (
				SELECT 1 FROM processed_jobs 
				WHERE workflow_id = $1 
				  AND status NOT IN ($3, $4, $5)
			) THEN 
				CASE 
					WHEN EXISTS (
						SELECT 1 FROM processed_jobs 
						WHERE workflow_id = $1 AND status = $4
					) THEN $4
					ELSE $3
				END
			ELSE w.status
		END;  -- Only update if status actually changed
	`

	tag, err := r.pool.Exec(ctx, query, workflowID, domain.TaskStatusRunning, domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusCancelled)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues("_workflow", "UpdateWorkflowStatus").Inc()
		return fmt.Errorf("failed to update workflow status: %w", err)
	}

	if tag.RowsAffected() > 0 {
		// Check what the new status is to increment appropriate metric
		var newStatus domain.TaskStatus
		checkQuery := `SELECT status FROM workflows WHERE id = $1;`
		if err := r.pool.QueryRow(ctx, checkQuery, workflowID).Scan(&newStatus); err == nil {
			switch newStatus {
			case domain.TaskStatusCompleted:
				metrics.WorkflowsCompleted.Inc()
				slog.Info("[WORKFLOW] completed", "workflowId", workflowID)
			case domain.TaskStatusFailed:
				metrics.WorkflowsFailed.Inc()
				slog.Warn("[WORKFLOW] failed", "workflowId", workflowID)
			default:
				slog.Info("[WORKFLOW] status updated", "workflowId", workflowID, "status", newStatus)
			}
		}
	}
	return nil
}
