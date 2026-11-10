package repository

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/redis/metrics"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecoverExpiredJob tells the janitor whether a Redis inflight entry may be
// moved. The Redis visibility timestamp is only a prompt: recovery happens
// solely after the durable Postgres lease has expired.
func (r *JobRepository) RecoverExpiredJob(ctx context.Context, queueName, taskID string) (RecoveryAction, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RecoverExpiredJob").Inc()
		return RecoveryKeepInflight, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, err)
	}

	tag, err := r.pool.Exec(ctx, `
		UPDATE processed_jobs
		SET status = $2,
		    available_at = NOW(),
		    lease_token = NULL,
		    lease_expires_at = NULL
		WHERE id = $1
		  AND status = $3
		  AND lease_expires_at <= NOW();
	`, id, domain.TaskStatusQueued, domain.TaskStatusRunning)
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RecoverExpiredJob").Inc()
		return RecoveryKeepInflight, fmt.Errorf("failed to recover expired job %s: %w", taskID, err)
	}
	if tag.RowsAffected() == 1 {
		return RecoveryRequeue, nil
	}

	var status domain.TaskStatus
	err = r.pool.QueryRow(ctx, `SELECT status FROM processed_jobs WHERE id = $1;`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecoveryAcknowledge, nil
	}
	if err != nil {
		metrics.JobRepoErrors.WithLabelValues(queueName, "RecoverExpiredJob").Inc()
		return RecoveryKeepInflight, fmt.Errorf("failed to inspect abandoned job %s: %w", taskID, err)
	}
	if status == domain.TaskStatusQueued {
		// The process may have crashed after changing Postgres to QUEUED but
		// before it republished the Redis entry. Repeating the move is safe.
		return RecoveryRequeue, nil
	}
	if status == domain.TaskStatusRunning {
		return RecoveryKeepInflight, nil
	}
	return RecoveryAcknowledge, nil
}

// RecoverExpiredLeases durably schedules a new delivery generation for every
// expired running attempt. It does not depend on Redis pending state, so a
// broker outage or a lost old PEL entry cannot strand a RUNNING job.
func (r *JobRepository) RecoverExpiredLeases(ctx context.Context, limit int) ([]Delivery, error) {
	if limit < 1 {
		return nil, fmt.Errorf("recovery limit must be positive")
	}
	rows, err := r.pool.Query(ctx, `
		WITH expired AS (
			SELECT id, delivery_generation + 1 AS next_generation
			FROM processed_jobs
			WHERE status = $1 AND lease_expires_at <= NOW()
			ORDER BY lease_expires_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		), recovered AS (
			UPDATE processed_jobs job
			SET status = $3, available_at = NOW(), lease_token = NULL, lease_expires_at = NULL,
			    delivery_generation = expired.next_generation
			FROM expired WHERE job.id = expired.id
			RETURNING job.id, job.delivery_generation, job.execution_contract->>'capability_stream' AS capability_stream
		), published AS (
			INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before, delivered)
			SELECT id, capability_stream, uuid_generate_v4(), delivery_generation, NOW(), FALSE FROM recovered
			RETURNING id, task_id, delivery_id, generation, queue_name
		)
		SELECT id, task_id, delivery_id, generation, queue_name FROM published;
	`, domain.TaskStatusRunning, limit, domain.TaskStatusQueued)
	if err != nil {
		return nil, fmt.Errorf("recover expired leases: %w", err)
	}
	defer rows.Close()
	entries := make([]Delivery, 0)
	for rows.Next() {
		var entry Delivery
		if err := rows.Scan(&entry.OutboxID, &entry.TaskID, &entry.DeliveryID, &entry.Generation, &entry.QueueName); err != nil {
			return nil, fmt.Errorf("scan recovered delivery: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// RecoverIdleDelivery handles a PEL entry whose consumer stopped before it
// claimed the job. Running jobs with a live DB lease remain untouched; all
// other current queued deliveries get a new durable generation before the old
// Stream entry is acknowledged.
func (r *JobRepository) RecoverIdleDelivery(ctx context.Context, queueName, taskID string, generation int) (RecoveryAction, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		return RecoveryAcknowledge, fmt.Errorf("taskID %q is not a valid UUID: %w", taskID, err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return RecoveryKeepInflight, fmt.Errorf("begin idle-delivery recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // Rollback is a no-op after commit.
	var status domain.TaskStatus
	var currentGeneration int
	var availableAt time.Time
	var capabilityStream string
	err = tx.QueryRow(ctx, `
		SELECT status, delivery_generation, available_at, execution_contract->>'capability_stream'
		FROM processed_jobs WHERE id = $1 FOR UPDATE;
	`, id).Scan(&status, &currentGeneration, &availableAt, &capabilityStream)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecoveryAcknowledge, nil
	}
	if err != nil {
		return RecoveryKeepInflight, fmt.Errorf("inspect idle delivery: %w", err)
	}
	if currentGeneration != generation || status == domain.TaskStatusCompleted || status == domain.TaskStatusFailed || status == domain.TaskStatusCancelled || status == domain.TaskStatusPending {
		return RecoveryAcknowledge, nil
	}
	if status != domain.TaskStatusQueued && status != domain.TaskStatusRunning {
		return RecoveryAcknowledge, nil
	}
	nextGeneration := currentGeneration + 1
	result, err := tx.Exec(ctx, `
		UPDATE processed_jobs
		SET status = $2, lease_token = NULL, lease_expires_at = NULL, delivery_generation = $3
		WHERE id = $1
		  AND (status <> $4 OR lease_expires_at IS NULL OR lease_expires_at <= NOW());
	`, id, domain.TaskStatusQueued, nextGeneration, domain.TaskStatusRunning)
	if err != nil {
		return RecoveryKeepInflight, fmt.Errorf("reset idle delivery: %w", err)
	}
	if result.RowsAffected() == 0 {
		// PostgreSQL is the clock and the fence. A live lease must remain
		// owned by its worker even when the janitor host clock is ahead.
		return RecoveryKeepInflight, nil
	}
	if _, err := insertDelivery(ctx, tx, id, capabilityStream, nextGeneration, availableAt); err != nil {
		return RecoveryKeepInflight, fmt.Errorf("schedule recovered delivery: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RecoveryKeepInflight, fmt.Errorf("commit idle-delivery recovery: %w", err)
	}
	return RecoveryRequeue, nil
}

// GetStuckPendingJobs returns task IDs that have been sitting in PENDING status
// for longer than the given threshold. These are candidates for the orphan
// reconciler — they exist in the DB but were never enqueued to Redis (e.g.
// because EnqueueTask failed after CreateJob succeeded).
func (r *JobRepository) GetStuckPendingJobs(ctx context.Context, olderThan time.Duration) ([]string, error) {
	query := `
		SELECT id FROM processed_jobs
		WHERE status = $1
		  AND created_at < NOW() - ($2 || ' seconds')::INTERVAL;
	`
	rows, err := r.pool.Query(ctx, query, domain.TaskStatusPending, int(olderThan.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to query stuck pending jobs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan pending job id: %w", err)
		}
		ids = append(ids, id.String())
	}
	return ids, rows.Err()
}
