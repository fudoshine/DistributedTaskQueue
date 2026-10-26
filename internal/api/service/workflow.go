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

func (s *WorkflowService) CreateWorkflow(ctx context.Context, workflowName string) (*api.WorkflowResponse, error) {
	workflow, err := s.repo.CreateWorkflow(ctx, workflowName)
	if err != nil {
		return nil, err
	}
	return &api.WorkflowResponse{
		ID:             workflow.ID,
		Name:           workflow.Name,
		Status:         workflow.Status,
		SealedAt:       workflow.SealedAt,
		DraftExpiresAt: workflow.DraftExpiresAt,
		CreatedAt:      workflow.CreatedAt,
		UpdatedAt:      workflow.UpdatedAt,
	}, nil
}

func (s *WorkflowService) GetWorkflow(ctx context.Context, workflowID uuid.UUID) (*api.WorkflowResponse, error) {
	res, err := s.repo.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return &api.WorkflowResponse{
		ID:             res.ID,
		Name:           res.Name,
		SealedAt:       res.SealedAt,
		DraftExpiresAt: res.DraftExpiresAt,
		CreatedAt:      res.CreatedAt,
		UpdatedAt:      res.UpdatedAt,
		Status:         res.Status,
	}, nil
}
