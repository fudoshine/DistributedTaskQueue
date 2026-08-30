package v1

import (
	"distributed-queue/internal/api"
	"distributed-queue/internal/api/service"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type WorkflowHandler struct {
	workflowSvc *service.WorkflowService
}

func NewWorkflowHandler(workflowSvc *service.WorkflowService) *WorkflowHandler {
	return &WorkflowHandler{
		workflowSvc: workflowSvc,
	}
}

func (h *WorkflowHandler) CreateWorkflow(c *gin.Context) {
	var req api.CreateWorkflowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	workflow, err := h.workflowSvc.CreateWorkflow(c.Request.Context(), req.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, workflow)
}
func (h *WorkflowHandler) GetWorkflow(c *gin.Context) {
	if c.Param("id") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}
	ID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id format"})
		return
	}

	workflow, err := h.workflowSvc.GetWorkflow(c.Request.Context(), ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, workflow)
}
