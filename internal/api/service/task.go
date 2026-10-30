package service

import (
	"context"
	"distributed-queue/internal/api"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var ErrInvalidTaskInput = errors.New("invalid task input")

type TaskService struct {
	repo    *repository.JobRepository
	catalog *execution.Catalog
}

func NewTaskService(repo *repository.JobRepository, catalog *execution.Catalog) *TaskService {
	return &TaskService{
		repo:    repo,
		catalog: catalog,
	}
}

func (s *TaskService) CreateTask(ctx context.Context, queueName string, workflowID uuid.UUID, parentIDs []uuid.UUID, taskType string, payload json.RawMessage) (api.TaskResponse, error) {
	entry, err := s.catalog.Lookup(taskType)
	if err != nil {
		return api.TaskResponse{}, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
	}
	if err := s.catalog.ValidatePayload(entry, payload); err != nil {
		return api.TaskResponse{}, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
	}

	taskID := uuid.New()
	effectKey := uuid.New()
	task := repository.Task{
		ID:                taskID,
		WorkflowID:        workflowID,
		ParentIDs:         parentIDs,
		TaskType:          taskType,
		Payload:           payload,
		ExecutionContract: entry,
		EffectKey:         effectKey,
	}
	stored, err := s.repo.CreateJob(ctx, queueName, task)
	if err != nil {
		return api.TaskResponse{}, err
	}
	return api.TaskResponse{
		ID:         stored.ID,
		WorkflowID: stored.WorkflowID,
		ParentIDs: func() []uuid.UUID {
			if stored.ParentIDs == nil {
				return []uuid.UUID{}
			}
			return stored.ParentIDs
		}(),
		TaskType:       stored.TaskType,
		Status:         stored.Status,
		CurrentRetries: stored.CurrentRetries,
		MaxRetries:     stored.MaxRetries,
		ErrorLog:       stored.ErrorLog,
		CreatedAt:      stored.CreatedAt,
		ProcessedAt:    stored.ProcessedAt,
		AvailableAt:    stored.AvailableAt,
	}, nil
}

func (s *TaskService) GetTask(ctx context.Context, taskID uuid.UUID) (*api.TaskResponse, error) {
	res, err := s.repo.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	var errorLog *string
	if res.ErrorLog != nil && *res.ErrorLog != "" {
		errorLog = res.ErrorLog
	}
	return &api.TaskResponse{
		ID:             res.ID,
		WorkflowID:     res.WorkflowID,
		ParentIDs:      res.ParentIDs,
		TaskType:       res.TaskType,
		Status:         res.Status,
		CurrentRetries: res.CurrentRetries,
		MaxRetries:     res.MaxRetries,
		ErrorLog:       errorLog,
		CreatedAt:      res.CreatedAt,
		AvailableAt:    res.AvailableAt,
		ProcessedAt:    res.ProcessedAt,
	}, nil
}
