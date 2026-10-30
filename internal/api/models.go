package api

import (
	"distributed-queue/internal/domain"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type CreateWorkflowRequest struct {
	Name string `json:"name" binding:"required"`
}
type ErrorResponse struct {
	Error string `json:"error" binding:"required"`
}
type CreateTaskRequest struct {
	ParentIDs []uuid.UUID     `json:"parent_ids"`
	TaskType  string          `json:"task_type" binding:"required"`
	Payload   json.RawMessage `json:"payload"`
}

type WorkflowResponse struct {
	ID             uuid.UUID             `json:"id"`
	Name           string                `json:"name"`
	Status         domain.WorkflowStatus `json:"status"`
	SealedAt       *time.Time            `json:"sealed_at"`
	DraftExpiresAt *time.Time            `json:"draft_expires_at"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

type TaskResponse struct {
	ID             uuid.UUID         `json:"id"`
	WorkflowID     uuid.UUID         `json:"workflow_id"`
	ParentIDs      []uuid.UUID       `json:"parent_ids"`
	TaskType       string            `json:"task_type"`
	Status         domain.TaskStatus `json:"status"`
	CurrentRetries int               `json:"current_retries"`
	MaxRetries     int               `json:"max_retries"`
	ErrorLog       *string           `json:"error_log,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	ProcessedAt    *time.Time        `json:"processed_at,omitempty"`
	AvailableAt    time.Time         `json:"available_at"`
}
