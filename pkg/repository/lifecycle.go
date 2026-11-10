package repository

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/redis/metrics"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TryStartJob atomically claims an eligible job. QUEUED is the only claimable
// state. Workflow and dependency predicates are checked at claim time as a final guard
// against stale or externally injected Redis messages.
func (r *JobRepository) TryStartJob(ctx context.Context, queueName, taskID, deliveryID string, generation int, leaseTTL time.Duration) (JobClaim, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
		return JobClaim{}, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, err)
	}
	parsedDeliveryID, err := uuid.Parse(deliveryID)
	if err != nil {
		return JobClaim{}, fmt.Errorf("deliveryID %q is not a valid UUID: %w", deliveryID, err)
	}
	if leaseTTL <= 0 {
		return JobClaim{}, fmt.Errorf("lease TTL must be positive")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
		return JobClaim{}, fmt.Errorf("failed to begin idempotency transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // Rollback is a no-op after commit.

	claim := JobClaim{LeaseToken: uuid.New()}
	query := `
		UPDATE processed_jobs AS job
		SET status = $2,
		    lease_token = $3,
		    lease_expires_at = NOW() + $4::interval,
		    attempt_number = attempt_number + 1
		WHERE job.id = $1
		  AND job.status = $5
		  AND job.delivery_generation = $7
		  AND EXISTS (
			  SELECT 1 FROM outbox delivery
			  WHERE delivery.task_id = job.id
			    AND delivery.generation = $7
			    AND delivery.delivery_id = $8
		  )
		  AND job.available_at <= NOW()
		  AND job.execution_contract->>'capability_stream' = $9
		  AND EXISTS (
			  SELECT 1 FROM workflows workflow
			  WHERE workflow.id = job.workflow_id
			    AND workflow.status <> $10
		  )
		  AND NOT EXISTS (
			  SELECT 1
			  FROM job_dependencies edge
			  JOIN processed_jobs parent ON parent.id = edge.parent_job_id
			  WHERE edge.child_job_id = job.id
			    AND parent.status <> $6
		  )
		RETURNING lease_token, attempt_number;
		`
	err = tx.QueryRow(ctx, query, id, domain.TaskStatusRunning, claim.LeaseToken, leaseTTL.String(), domain.TaskStatusQueued, domain.TaskStatusCompleted, generation, parsedDeliveryID, queueName, domain.WorkflowStatusDraft).Scan(&claim.LeaseToken, &claim.Attempt)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
			return JobClaim{}, fmt.Errorf("failed to claim job %s: %w", taskID, err)
		}

		var status domain.TaskStatus
		var currentGeneration int
		var savedStream *string

		err = tx.QueryRow(ctx, `
			SELECT status, delivery_generation,
				execution_contract->>'capability_stream'
			FROM processed_jobs
			WHERE id = $1;
		`, id).Scan(&status, &currentGeneration, &savedStream)
		if errors.Is(err, pgx.ErrNoRows) {
			metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
			return JobClaim{}, ErrJobNotFound
		}
		if err != nil {
			metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
			return JobClaim{}, fmt.Errorf("failed to inspect unclaimable job %s: %w", taskID, err)
		}
		if savedStream == nil || *savedStream != queueName {
			return JobClaim{}, ErrRouteMismatch
		}
		if currentGeneration != generation {
			return JobClaim{}, ErrStaleDelivery
		}
		switch status {
		case domain.TaskStatusCompleted:
			metrics.JobsDuplicateSkipped.WithLabelValues(queueName).Inc()
			return JobClaim{}, ErrDuplicateJob
		case domain.TaskStatusFailed, domain.TaskStatusCancelled:
			return JobClaim{}, ErrTerminalJob
		case domain.TaskStatusRunning:
			return JobClaim{}, ErrJobAlreadyOwned
		default:
			return JobClaim{}, ErrJobNotReady
		}
	}

	slog.Info("[IDEMPOTENCY] job claimed", "taskId", taskID, "attempt", claim.Attempt, "leaseToken", claim.LeaseToken)

	if err := tx.Commit(ctx); err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "TryStartJob").Inc()
		return JobClaim{}, fmt.Errorf("failed to commit idempotency transaction: %w", err)
	}

	return claim, nil
}

// RecordFailure increments the retry counter and stores the latest error,
// transitioning status to FAILED once max_retries is reached. Returns the
// updated (currentRetries, maxRetries) so the caller can decide retry vs DLQ.
func (r *JobRepository) RecordFailure(ctx context.Context, queueName, taskID string, leaseToken uuid.UUID, errLog string) (currentRetries int, maxRetries int, err error) {
	id, parseErr := uuid.Parse(taskID)
	if parseErr != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RecordFailure").Inc()
		return 0, 0, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, parseErr)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin failure transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // Rollback is a no-op after commit.

	query := `
		UPDATE processed_jobs
		SET current_retries = current_retries + 1,
		    error_log = $2,
	    status = CASE
	        WHEN current_retries + 1 >= max_retries THEN $3
	        ELSE $4
	    END,
		    available_at = CASE
				WHEN current_retries + 1 >= max_retries THEN NOW()
				ELSE NOW() + make_interval(secs => ((1::bigint << (current_retries + 1))::double precision))
			END,
			lease_token = NULL,
			lease_expires_at = NULL,
			delivery_generation = CASE
				WHEN current_retries + 1 >= max_retries THEN delivery_generation
				ELSE delivery_generation + 1
			END
		WHERE id = $1
		  AND status = $5
		  AND lease_token = $6
		  AND lease_expires_at > NOW()
		RETURNING current_retries, max_retries, status, delivery_generation, available_at,
				execution_contract->>'capability_stream';
	`
	var status domain.TaskStatus
	var generation int
	var availableAt time.Time
	var capabilityStream string
	err = tx.QueryRow(ctx, query, id, errLog, domain.TaskStatusFailed, domain.TaskStatusQueued, domain.TaskStatusRunning, leaseToken).Scan(&currentRetries, &maxRetries, &status, &generation, &availableAt, &capabilityStream)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, ErrLeaseLost
		}
		metrics.JobRepoErrors.WithLabelValues(queueName, "RecordFailure").Inc()
		return 0, 0, fmt.Errorf("failed to record failure for task %s: %w", taskID, err)
	}
	if status == domain.TaskStatusQueued {
		if _, err := insertDelivery(ctx, tx, id, capabilityStream, generation, availableAt); err != nil {
			return 0, 0, fmt.Errorf("schedule retry delivery: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit failure transaction: %w", err)
	}
	if currentRetries >= maxRetries {
		metrics.JobsFailedTerminal.WithLabelValues(queueName).Inc()
	}
	return currentRetries, maxRetries, nil
}

// RenewLease extends an active attempt using the database clock. It returns
// false when another attempt reclaimed the job or the lease already expired.
func (r *JobRepository) RenewLease(ctx context.Context, queueName, taskID string, leaseToken uuid.UUID, leaseTTL time.Duration) (bool, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RenewLease").Inc()
		return false, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, err)
	}
	if leaseTTL <= 0 {
		return false, fmt.Errorf("lease TTL must be positive")
	}

	tag, err := r.pool.Exec(ctx, `
		UPDATE processed_jobs
		SET lease_expires_at = NOW() + $3::interval
		WHERE id = $1
		  AND status = $4
		  AND lease_token = $2
		  AND lease_expires_at > NOW();
	`, id, leaseToken, leaseTTL.String(), domain.TaskStatusRunning)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RenewLease").Inc()
		return false, fmt.Errorf("failed to renew lease for job %s: %w", taskID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// CompleteJobAndScheduleChildren durably completes a job and promotes every
// newly-ready child to QUEUED in the same PostgreSQL transaction. Each
// promoted child receives an outbox entry, so Redis publication is retried by
// the outbox relay instead of being coupled to a worker's process lifetime.
//
// Completion for a workflow is serialized with a transaction advisory lock.
// This matters for a multi-parent child: two parents may finish concurrently;
// without serialization each transaction could observe the other as RUNNING
// and neither would promote the child.
func (r *JobRepository) CompleteJobAndScheduleChildren(ctx context.Context, queueName, taskID string, leaseToken uuid.UUID) ([]string, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("failed to begin completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // Rollback is a no-op after commit.

	var workflowID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE processed_jobs
		SET status = $2,
		    processed_at = NOW(),
		    lease_token = NULL,
		    lease_expires_at = NULL
		WHERE id = $1
		  AND status = $3
		  AND lease_token = $4
		  AND lease_expires_at > NOW()
		RETURNING workflow_id;
	`, id, domain.TaskStatusCompleted, domain.TaskStatusRunning, leaseToken).Scan(&workflowID)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrLeaseLost
		}
		return nil, fmt.Errorf("failed to complete job %s: %w", taskID, err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0));`, workflowID); err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("failed to lock workflow %s for completion: %w", workflowID, err)
	}

	rows, err := tx.Query(ctx, `
		WITH ready_children AS (
			SELECT DISTINCT child.id
			FROM job_dependencies edge
			JOIN processed_jobs child ON child.id = edge.child_job_id
			JOIN workflows workflow ON workflow.id = child.workflow_id
			WHERE edge.parent_job_id = $1
			  AND child.status = $2
			  AND child.workflow_id = $5
			  AND workflow.status <> $6
			  AND NOT EXISTS (
				SELECT 1
				FROM job_dependencies parent_edge
				JOIN processed_jobs parent ON parent.id = parent_edge.parent_job_id
				WHERE parent_edge.child_job_id = child.id
				  AND parent.status <> $3
			  )
		), promoted_children AS (
			UPDATE processed_jobs child
			SET status = $4,
			    available_at = NOW()
			FROM ready_children ready
			WHERE child.id = ready.id AND child.status = $2
			RETURNING child.id,child.execution_contract->>'capability_stream' AS capability_stream
		)
		INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before, delivered)
		SELECT id, capability_stream, uuid_generate_v4(), 0, NOW(), FALSE FROM promoted_children
		ON CONFLICT (task_id, generation) DO NOTHING
		RETURNING task_id;
	`, id, domain.TaskStatusPending, domain.TaskStatusCompleted, domain.TaskStatusQueued, workflowID, domain.WorkflowStatusDraft)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("failed to schedule children of job %s: %w", taskID, err)
	}
	defer rows.Close()

	children := make([]string, 0)
	for rows.Next() {
		var childID uuid.UUID
		if err := rows.Scan(&childID); err != nil {
			metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
			return nil, fmt.Errorf("failed to scan scheduled child of job %s: %w", taskID, err)
		}
		children = append(children, childID.String())
	}
	if err := rows.Err(); err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("failed to schedule children of job %s: %w", taskID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "CompleteJobAndScheduleChildren").Inc()
		return nil, fmt.Errorf("failed to commit completion of job %s: %w", taskID, err)
	}

	metrics.JobsCompleted.WithLabelValues(queueName).Inc()
	if len(children) > 0 {
		metrics.ChildrenEnqueued.WithLabelValues(queueName).Add(float64(len(children)))
	}
	return children, nil
}

// MarkAsCancelledCascading recursively cancels all descendant tasks when a
// parent fails. This enforces strict topological ordering: if any required
// parent fails, all downstream tasks are blocked and should be cancelled.
//
// For DAGs with multiple parents (AND semantics): if ANY parent fails, children
// cannot run because they need outputs from ALL parents.
func (r *JobRepository) MarkAsCancelledCascading(ctx context.Context, queueName, failedTaskID string) error {
	id, err := uuid.Parse(failedTaskID)
	if err != nil {
		return fmt.Errorf("taskID %q is not a valid UUID: %w", failedTaskID, err)
	}

	// Recursively find and cancel all descendants of the failed task.
	// This uses an explicit path array to avoid cycles and stays compatible with
	// PostgreSQL 14's recursive CTE support.
	query := `
		WITH RECURSIVE descendants AS (
			-- Base case: direct children of failed task
			SELECT child_job_id, ARRAY[child_job_id] AS path
			FROM job_dependencies
			WHERE parent_job_id = $1
			
			UNION ALL
			
			-- Recursive case: children of children
			SELECT jd.child_job_id, d.path || jd.child_job_id
			FROM job_dependencies jd
			INNER JOIN descendants d ON jd.parent_job_id = d.child_job_id
			WHERE NOT jd.child_job_id = ANY(d.path)
		)
		UPDATE processed_jobs
		SET status = $2,
		    lease_token = NULL,
		    lease_expires_at = NULL
		FROM descendants
		WHERE processed_jobs.id = descendants.child_job_id
		  AND processed_jobs.status IN ($3, $4, $5);  -- Only cancel PENDING/QUEUED/RUNNING
	`

	tag, err := r.pool.Exec(ctx, query, id, domain.TaskStatusCancelled, domain.TaskStatusPending, domain.TaskStatusQueued, domain.TaskStatusRunning)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "MarkAsCancelledCascading").Inc()
		return fmt.Errorf("failed to mark descendants as CANCELLED: %w", err)
	}

	cancelled := tag.RowsAffected()
	if cancelled > 0 {
		metrics.TasksCancelledCascade.WithLabelValues(queueName).Add(float64(cancelled))
		slog.Warn("[CASCADE] cancelled all descendants of failed parent", "parentId", failedTaskID, "cancelledCount", cancelled)
	}
	return nil
}
