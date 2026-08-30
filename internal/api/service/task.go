package service

import (
	"context"
	"distributed-queue/internal/api"
	"distributed-queue/pkg/repository"
)

type TaskService struct {
	repo *repository.JobRepository
}

func NewTaskService(repo *repository.JobRepository) *TaskService {
	return &TaskService{
		repo: repo,
	}
}

func (s *TaskService) GetWorkflow(ctx context.Context, workflowID string) (*api.WorkflowResponse, error) {
	res, err := s.repo.GetWorkflow(ctx, workflowID)
	return &api.WorkflowResponse{
		ID:        res.ID,
		Name:      res.Name,
		CreatedAt: res.CreatedAt,
		UpdatedAt: res.UpdatedAt,
	}, err
}

func (s *TaskService) GetTask(ctx context.Context, taskID string) (*api.TaskResponse, error) {
	res, err := s.repo.GetTask(ctx, taskID)
	return &api.TaskResponse{
		ID:             res.ID,
		WorkflowID:     res.WorkflowID,
		ParentIDs:      res.ParentIDs,
		TaskType:       res.TaskType,
		Status:         res.Status,
		CurrentRetries: res.CurrentRetries,
		MaxRetries:     res.MaxRetries,
		ErrorLog:       res.ErrorLog,
		CreatedAt:      res.CreatedAt,
		AvailableAt:    res.AvailableAt,
		ProcessedAt:    res.ProcessedAt,
	}, err
}
