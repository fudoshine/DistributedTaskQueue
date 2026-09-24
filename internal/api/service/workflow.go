package service

import (
	"context"
	"distributed-queue/internal/api"
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/repository"
	"time"

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

func (s *WorkflowService) CreateWorkflow(ctx context.Context, workflowName string) (*api.WorkflowResponse, error) {
	id, err := s.repo.CreateWorkflow(ctx, workflowName)
	if err != nil {
		return nil, err
	}
	return &api.WorkflowResponse{
		ID:        id,
		Name:      workflowName,
		Status:    domain.WorkflowStatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func (s *WorkflowService) GetWorkflow(ctx context.Context, workflowID uuid.UUID) (*api.WorkflowResponse, error) {
	res, err := s.repo.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return &api.WorkflowResponse{
		ID:        res.ID,
		Name:      res.Name,
		CreatedAt: res.CreatedAt,
		UpdatedAt: res.UpdatedAt,
		Status:    res.Status,
	}, nil
}
