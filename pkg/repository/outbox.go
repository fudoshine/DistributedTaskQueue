package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func insertDelivery(ctx context.Context, tx pgx.Tx, taskID uuid.UUID, queueName string, generation int, notBefore time.Time) (Delivery, error) {
	delivery := Delivery{DeliveryID: uuid.New(), TaskID: taskID, QueueName: queueName, Generation: generation}
	err := tx.QueryRow(ctx, `
		INSERT INTO outbox (task_id, queue_name, delivery_id, generation, not_before, delivered)
		VALUES ($1, $2, $3, $4, $5, FALSE)
		RETURNING id;
	`, taskID, queueName, delivery.DeliveryID, generation, notBefore).Scan(&delivery.OutboxID)
	return delivery, err
}

// ClaimPendingOutbox leases due rows so multiple relay replicas never publish
// the same row concurrently. A relay crash merely lets the short lease expire;
// a repeated publication is safe because the delivery ID is immutable.
func (r *JobRepository) ClaimPendingOutbox(ctx context.Context, limit int, leaseTTL time.Duration) ([]Delivery, uuid.UUID, error) {
	if limit < 1 || leaseTTL <= 0 {
		return nil, uuid.Nil, fmt.Errorf("invalid outbox claim limit or lease TTL")
	}
	token := uuid.New()
	rows, err := r.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM outbox
			WHERE delivered = FALSE
			  AND not_before <= NOW()
			  AND (relay_lease_expires_at IS NULL OR relay_lease_expires_at <= NOW())
			ORDER BY not_before, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox entry
		SET relay_lease_token = $2,
		    relay_lease_expires_at = NOW() + $3::interval
		FROM due
		WHERE entry.id = due.id
		RETURNING entry.id, entry.task_id, entry.delivery_id, entry.generation, entry.queue_name;
	`, limit, token, leaseTTL.String())
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("claim due outbox rows: %w", err)
	}
	defer rows.Close()
	entries := make([]Delivery, 0)
	for rows.Next() {
		var entry Delivery
		if err := rows.Scan(&entry.OutboxID, &entry.TaskID, &entry.DeliveryID, &entry.Generation, &entry.QueueName); err != nil {
			return nil, uuid.Nil, fmt.Errorf("scan claimed outbox row: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, token, rows.Err()
}

func (r *JobRepository) MarkOutboxDelivered(ctx context.Context, outboxID, relayToken uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox
		SET delivered = TRUE, delivered_at = NOW(), relay_lease_token = NULL, relay_lease_expires_at = NULL
		WHERE id = $1 AND delivered = FALSE AND relay_lease_token = $2;
	`, outboxID, relayToken)
	if err != nil {
		return fmt.Errorf("mark outbox row %s delivered: %w", outboxID, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrRelayLeaseLost
	}
	return nil
}

func (r *JobRepository) ReleaseOutboxLease(ctx context.Context, outboxID, relayToken uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox SET relay_lease_token = NULL, relay_lease_expires_at = NULL
		WHERE id = $1 AND delivered = FALSE AND relay_lease_token = $2;
	`, outboxID, relayToken)
	return err
}
