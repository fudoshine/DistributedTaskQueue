package v1

import (
	"distributed-queue/internal/api"
	"distributed-queue/internal/api/service"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type TaskHandler struct {
	taskSvc           *service.TaskService
	defaultQueueNames string
}

func NewTaskHandler(taskSvc *service.TaskService, defaultQueueName string) *TaskHandler {
	return &TaskHandler{
		taskSvc:           taskSvc,
		defaultQueueNames: defaultQueueName,
	}
}

func (h *TaskHandler) CreateTask(c *gin.Context) {
	workflowID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workflow_id"})
		return
	}
	var req api.CreateTaskRequest
	if err := bindStrictJSON(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	queueName := h.defaultQueueNames
	task, err := h.taskSvc.CreateTask(c.Request.Context(), queueName, workflowID, req.ParentIDs, req.TaskType, req.Payload)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, service.ErrInvalidTaskInput) {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
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
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, task)
}
