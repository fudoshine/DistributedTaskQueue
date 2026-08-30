package v1

import (
	"distributed-queue/internal/api"
	"distributed-queue/internal/api/service"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TaskHandler struct {
	taskSvc   *service.TaskService
	queueName string
}

func NewTaskHandler(taskSvc *service.TaskService, queueName string) *TaskHandler {
	return &TaskHandler{
		taskSvc:   taskSvc,
		queueName: queueName,
	}
}

func (h *TaskHandler) CreateTask(c *gin.Context) {
	var req api.CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	task, err := h.taskSvc.CreateTask(c.Request.Context(), h.queueName, req.WorkflowID, req.ParentIDs, req.TaskType, req.Payload, req.MaxRetries)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, task)
}

func (h *TaskHandler) GetTask(c *gin.Context) {
	if c.Param("id") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}
	ID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id format"})
		return
	}
	task, err := h.taskSvc.GetTask(c.Request.Context(), ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, task)
}
