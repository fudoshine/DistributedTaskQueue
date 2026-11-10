package repository

import (
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDuplicateJob                    = errors.New("job execution skipped: already completed")
	ErrJobAlreadyOwned                 = errors.New("job execution skipped: another worker owns this job")
	ErrJobNotReady                     = errors.New("job execution skipped: not queued or dependencies are incomplete")
	ErrTerminalJob                     = errors.New("job execution skipped: job is terminal")
	ErrLeaseLost                       = errors.New("job execution skipped: lease is no longer owned by this attempt")
	ErrStaleDelivery                   = errors.New("job execution skipped: delivery is no longer current")
	ErrRelayLeaseLost                  = errors.New("outbox relay no longer owns this delivery")
	ErrRouteMismatch                   = errors.New("job route does not match delivery stream")
	ErrEmptyWorkflow                   = errors.New("workflow has no tasks")
	ErrWorkflowSealed                  = errors.New("workflow is already sealed")
	ErrWorkflowNotDraft                = errors.New("workflow is not a draft")
	ErrDraftExpired                    = errors.New("workflow draft has expired")
	ErrInvalidExecutionContract        = errors.New("invalid saved execution contract")
	ErrInvalidParents                  = errors.New("invalid task parents")
	ErrWorkflowNotFoundOrAlreadySealed = errors.New("workflow not found or already sealed")
	// ErrJobNotFound means TryStartJob was called for a taskID with no
	// corresponding processed_jobs row. Under this system's design that
	// should never happen — every task is created in Postgres via CreateJob
	// BEFORE its ID is ever pushed onto the Redis queue — so seeing this
	// error means something enqueued an ID directly into Redis without
	// going through the proper creation path, or the row was deleted out
	// from under a live task.
	ErrJobNotFound = errors.New("job execution failed: no processed_jobs row found for taskID")
)

// Task represents a single unit of work to be created in Postgres before
// its ID is enqueued onto Redis. WorkflowID and ParentIDs model the DAG
// structure: every task belongs to exactly one workflow, and may optionally
// have parent tasks it depends on (must complete before this runs).
type Task struct {
	ID                uuid.UUID
	WorkflowID        uuid.UUID
	ParentIDs         []uuid.UUID // empty slice for root tasks in a workflow
	TaskType          string
	ExecutionContract execution.CatalogEntry
	EffectKey         uuid.UUID
	Payload           json.RawMessage
}
type Workflow struct {
	ID             uuid.UUID
	Name           string
	Status         domain.WorkflowStatus
	TenantID       *uuid.UUID
	SealedAt       *time.Time
	DraftExpiresAt *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
type JobRow struct {
	ID                uuid.UUID
	WorkflowID        uuid.UUID
	ParentIDs         []uuid.UUID
	TaskType          string
	TenantID          *uuid.UUID
	ExecutionContract *execution.CatalogEntry
	EffectKey         *uuid.UUID
	Status            domain.TaskStatus
	Payload           json.RawMessage
	CurrentRetries    int
	MaxRetries        int
	ErrorLog          *string
	CreatedAt         time.Time
	ProcessedAt       *time.Time
	AvailableAt       time.Time
}
type JobRepository struct {
	pool *pgxpool.Pool
}

// JobClaim identifies one bounded execution attempt. The lease token is a
// fencing token: every heartbeat, completion, and failure update must present
// this exact value, so a stale worker cannot mutate a reclaimed job.
type JobClaim struct {
	LeaseToken uuid.UUID
	Attempt    int
}

// Delivery is an immutable publication request held in Postgres until the
// relay has appended it to the broker. Generation distinguishes retries and
// recovery from earlier messages for the same logical job.
type Delivery struct {
	OutboxID   uuid.UUID
	TaskID     uuid.UUID
	DeliveryID uuid.UUID
	Generation int
	QueueName  string
}

type RecoveryAction int

const (
	// RecoveryKeepInflight means the lease is still active; Redis must not move
	// the message even if its original visibility timestamp is old.
	RecoveryKeepInflight RecoveryAction = iota
	// RecoveryRequeue means the durable lease expired and Redis may republish.
	RecoveryRequeue
	// RecoveryAcknowledge means the job is terminal or missing; discard the
	// stale Redis inflight entry.
	RecoveryAcknowledge
)

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}
