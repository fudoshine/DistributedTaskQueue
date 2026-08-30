package service

import (
	"context"
	"distributed-queue/internal/api"
	"distributed-queue/pkg/repository"

	"github.com/google/uuid"
)

type WorkflowService struct {
	repo *repository.JobRepository
}

func NewWorkflowService(repo *repository.JobRepository) *WorkflowService {
	return &WorkflowService{
		repo: repo,
	}
}

func (s *WorkflowService) CreateWorkflow(ctx context.Context, name string) (uuid.UUID, error) {
	return s.repo.CreateWorkflow(ctx, name)
}

func (s *WorkflowService) CreateTask(ctx context.Context, queueName string, req api.CreateTaskRequest) (uuid.UUID, error) {
	taskID := uuid.New()

	task := repository.Task{
		ID:         taskID,
		WorkflowID: req.WorkflowID,
		ParentIDs:  req.ParentIDs,
		TaskType:   req.TaskType,
		Payload:    req.Payload,
		MaxRetries: req.MaxRetries,
	}
	return taskID, s.repo.CreateJob(ctx, queueName, task)
}
