package repository_test

import (
	"distributed-queue/internal/api"
	v1 "distributed-queue/internal/api/handlers/v1"
	"distributed-queue/internal/api/service"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTaskServiceCreateTask(t *testing.T) {
	RequireRepositoryIntegration(t)
	catalog, err := execution.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewTaskService(sharedRepo, catalog)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "task service validation")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	oversized, err := json.Marshal(map[string]string{
		"message": strings.Repeat("x", int(entry.PayloadSchema.MaxBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		taskType string
		payload  json.RawMessage
		invalid  bool
	}{
		{"unknown type", "email.send", json.RawMessage(`{"message":"hello"}`), true},
		{"malformed JSON", "reference.echo", json.RawMessage(`{`), true},
		{"invalid schema", "reference.echo", json.RawMessage(`{"message":42}`), true},
		{"oversized payload", "reference.echo", oversized, true},
		{"valid echo", "reference.echo", json.RawMessage(`{"message":"hello"}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			result, err := svc.CreateTask(t.Context(), "queue:service-test", workflow.ID, nil, tc.taskType, tc.payload, 3)
			var added int64
			if tc.invalid {
				if !errors.Is(err, service.ErrInvalidTaskInput) {
					t.Fatalf("expected invalid task input, got %v", err)
				}
				if tc.taskType == "email.send" && !errors.Is(err, execution.ErrUnknownTaskType) {
					t.Fatalf("unknown task cause was lost: %v", err)
				}
				if result.ID != uuid.Nil {
					t.Fatalf("rejected input returned task ID %s", result.ID)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				stored, err := sharedRepo.GetTask(t.Context(), result.ID)
				if err != nil {
					t.Fatal(err)
				}
				if stored.WorkflowID != workflow.ID || stored.TaskType != tc.taskType {
					t.Fatalf("unexpected stored task: %+v", stored)
				}
				added = 1
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs+added || afterOutbox != beforeOutbox+added {
				t.Fatalf("job/outbox counts changed from %d/%d to %d/%d; expected delta %d/%d",
					beforeJobs, beforeOutbox, afterJobs, afterOutbox, added, added)
			}
		})
	}
}

func TestTaskHandlerCreateTask(t *testing.T) {
	RequireRepositoryIntegration(t)
	catalog, err := execution.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewTaskService(sharedRepo, catalog)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "task HTTP validation")
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	handler := v1.NewTaskHandler(svc, "queue:service-test")
	router.POST("/api/v1/workflows/:id/tasks", handler.CreateTask)
	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
		closedPool bool
	}{
		{"unknown type", `{"task_type":"email.send","payload":{"message":"hello"}}`, http.StatusBadRequest, false},
		{"invalid payload", `{"task_type":"reference.echo","payload":{"message":42}}`, http.StatusBadRequest, false},
		{"database failure", `{"task_type":"reference.echo","payload":{"message":"hello"}}`, http.StatusInternalServerError, true},
		{"valid echo", `{"task_type":"reference.echo","payload":{"message":"hello"},"max_retries":3}`, http.StatusCreated, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testRouter := router
			if tc.closedPool {
				pool, err := pgxpool.NewWithConfig(t.Context(), sharedPool.Config().Copy())
				if err != nil {
					t.Fatal(err)
				}
				pool.Close()
				failedService := service.NewTaskService(repository.NewJobRepository(pool), catalog)
				testRouter = gin.New()
				testRouter.POST("/api/v1/workflows/:id/tasks", v1.NewTaskHandler(failedService, "queue:service-test").CreateTask)
			}
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/"+workflow.ID.String()+"/tasks", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			testRouter.ServeHTTP(response, request)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			var added int64
			if tc.wantStatus == http.StatusCreated {
				var result api.TaskResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				stored, err := sharedRepo.GetTask(t.Context(), result.ID)
				if err != nil {
					t.Fatal(err)
				}
				if stored.WorkflowID != workflow.ID || stored.TaskType != "reference.echo" {
					t.Fatalf("unexpected stored task: %+v", stored)
				}
				added = 1
			} else {
				var result api.ErrorResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Error == "" {
					t.Fatalf("expected JSON error response, got %s", response.Body.String())
				}
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs+added || afterOutbox != beforeOutbox+added {
				t.Fatalf("job/outbox counts changed from %d/%d to %d/%d; expected delta %d/%d",
					beforeJobs, beforeOutbox, afterJobs, afterOutbox, added, added)
			}
		})
	}
}

func taskSubmissionRowCounts(t *testing.T) (jobs, outbox int64) {
	t.Helper()
	if err := sharedPool.QueryRow(t.Context(), `SELECT
		(SELECT COUNT(*) FROM processed_jobs),
		(SELECT COUNT(*) FROM outbox)`).Scan(&jobs, &outbox); err != nil {
		t.Fatal(err)
	}
	return jobs, outbox
}
