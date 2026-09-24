package service

import (
	"context"
	"distributed-queue/internal/api"
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskService struct {
	repo *repository.JobRepository
}

func NewTaskService(repo *repository.JobRepository) *TaskService {
	return &TaskService{
		repo: repo,
	}
}

func (s *TaskService) CreateTask(ctx context.Context, queueName string, workflowID uuid.UUID, parentIDs []uuid.UUID, taskType string, payload json.RawMessage, maxRetries int) (api.TaskResponse, error) {
	taskID := uuid.New()
	task := repository.Task{
		ID:         taskID,
		WorkflowID: workflowID,
		ParentIDs:  parentIDs,
		TaskType:   taskType,
		Payload:    payload,
		MaxRetries: maxRetries,
	}
	err := s.repo.CreateJob(ctx, queueName, task)
	if err != nil {
		return api.TaskResponse{}, err
	}
	return api.TaskResponse{
		ID:         taskID,
		WorkflowID: workflowID,
		ParentIDs: func() []uuid.UUID {
			if parentIDs == nil {
				return []uuid.UUID{}
			}
			return parentIDs
		}(),
		TaskType:       taskType,
		Status:         domain.TaskStatusPending,
		CurrentRetries: 0,
		MaxRetries:     maxRetries,
		CreatedAt:      time.Now(),
		AvailableAt:    time.Now(),
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
