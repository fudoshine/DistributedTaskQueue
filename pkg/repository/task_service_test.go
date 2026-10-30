package repository_test

import (
	"distributed-queue/internal/api"
	v1 "distributed-queue/internal/api/handlers/v1"
	"distributed-queue/internal/api/service"
	"distributed-queue/internal/domain"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
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
			result, err := svc.CreateTask(t.Context(), "queue:service-test", workflow.ID, nil, tc.taskType, tc.payload)
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
				assertTaskResponseMatchesStored(t, result, stored)
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
	contract, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewTaskService(sharedRepo, catalog)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "task HTTP validation")
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	const queueName = "queue:service-test"
	handler := v1.NewTaskHandler(svc, queueName)
	router.POST("/api/v1/workflows/:id/tasks", handler.CreateTask)
	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
		closedPool bool
	}{
		{"unknown type", `{"task_type":"email.send","payload":{"message":"hello"}}`, http.StatusBadRequest, false},
		{"invalid payload", `{"task_type":"reference.echo","payload":{"message":42}}`, http.StatusBadRequest, false},
		{"queue override", `{"task_type":"reference.echo","payload":{"message":"hello"},"queue_name":"other"}`, http.StatusBadRequest, false},
		{"empty queue override", `{"task_type":"reference.echo","payload":{"message":"hello"},"queue_name":""}`, http.StatusBadRequest, false},
		{"null queue override", `{"task_type":"reference.echo","payload":{"message":"hello"},"queue_name":null}`, http.StatusBadRequest, false},
		{"retry override", `{"task_type":"reference.echo","payload":{"message":"hello"},"max_retries":7}`, http.StatusBadRequest, false},
		{"zero retry override", `{"task_type":"reference.echo","payload":{"message":"hello"},"max_retries":0}`, http.StatusBadRequest, false},
		{"null retry override", `{"task_type":"reference.echo","payload":{"message":"hello"},"max_retries":null}`, http.StatusBadRequest, false},
		{"tenant override", `{"task_type":"reference.echo","payload":{"message":"hello"},"tenant_id":"00000000-0000-0000-0000-000000000001"}`, http.StatusBadRequest, false},
		{"empty tenant override", `{"task_type":"reference.echo","payload":{"message":"hello"},"tenant_id":""}`, http.StatusBadRequest, false},
		{"null tenant override", `{"task_type":"reference.echo","payload":{"message":"hello"},"tenant_id":null}`, http.StatusBadRequest, false},
		{"TTL override", `{"task_type":"reference.echo","payload":{"message":"hello"},"ttl_seconds":3600}`, http.StatusBadRequest, false},
		{"zero TTL override", `{"task_type":"reference.echo","payload":{"message":"hello"},"ttl_seconds":0}`, http.StatusBadRequest, false},
		{"null TTL override", `{"task_type":"reference.echo","payload":{"message":"hello"},"ttl_seconds":null}`, http.StatusBadRequest, false},
		{"handler override", `{"task_type":"reference.echo","payload":{"message":"hello"},"handler_version":"v2"}`, http.StatusBadRequest, false},
		{"contract override", `{"task_type":"reference.echo","payload":{"message":"hello"},"execution_contract":{}}`, http.StatusBadRequest, false},
		{"effect key override", `{"task_type":"reference.echo","payload":{"message":"hello"},"effect_key":null}`, http.StatusBadRequest, false},
		{"unknown field", `{"task_type":"reference.echo","payload":{"message":"hello"},"unknown":true}`, http.StatusBadRequest, false},
		{"trailing JSON", `{"task_type":"reference.echo","payload":{"message":"hello"}} {}`, http.StatusBadRequest, false},
		{"trailing malformed data", `{"task_type":"reference.echo","payload":{"message":"hello"}} invalid`, http.StatusBadRequest, false},
		{"missing task type", `{"payload":{"message":"hello"}}`, http.StatusBadRequest, false},
		{"empty task type", `{"task_type":"","payload":{"message":"hello"}}`, http.StatusBadRequest, false},
		{"missing payload", `{"task_type":"reference.echo"}`, http.StatusBadRequest, false},
		{"null payload", `{"task_type":"reference.echo","payload":null}`, http.StatusBadRequest, false},
		{"empty object", `{}`, http.StatusBadRequest, false},
		{"null body", `null`, http.StatusBadRequest, false},
		{"empty body", ``, http.StatusBadRequest, false},
		{"malformed JSON", `{"task_type":`, http.StatusBadRequest, false},
		{"database failure", `{"task_type":"reference.echo","payload":{"message":"hello"}}`, http.StatusInternalServerError, true},
		{"valid echo", `{"task_type":"reference.echo","payload":{"message":"hello"}}`, http.StatusCreated, false},
		{"valid echo with whitespace", "{\"task_type\":\"reference.echo\",\"parent_ids\":[],\"payload\":{\"message\":\"hello\"}} \n\t", http.StatusCreated, false},
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
				testRouter.POST("/api/v1/workflows/:id/tasks", v1.NewTaskHandler(failedService, queueName).CreateTask)
			}
			beforeWorkflows, beforeJobs, beforeOutbox := submissionRowCounts(t)
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
				assertTaskResponseMatchesStored(t, result, stored)
				if result.MaxRetries != contract.MaxAttempts {
					t.Fatalf("retry budget = %d, want catalog budget %d", result.MaxRetries, contract.MaxAttempts)
				}
				var storedQueue string
				if err := sharedPool.QueryRow(t.Context(),
					`SELECT queue_name FROM outbox WHERE task_id = $1 AND generation = 0`, result.ID).Scan(&storedQueue); err != nil {
					t.Fatal(err)
				}
				if storedQueue != queueName {
					t.Fatalf("outbox queue = %q, want configured queue %q", storedQueue, queueName)
				}
				added = 1
			} else {
				var result api.ErrorResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Error == "" {
					t.Fatalf("expected JSON error response, got %s", response.Body.String())
				}
			}
			afterWorkflows, afterJobs, afterOutbox := submissionRowCounts(t)
			if afterWorkflows != beforeWorkflows || afterJobs != beforeJobs+added || afterOutbox != beforeOutbox+added {
				t.Fatalf("workflow/job/outbox counts changed from %d/%d/%d to %d/%d/%d; expected delta 0/%d/%d",
					beforeWorkflows, beforeJobs, beforeOutbox, afterWorkflows, afterJobs, afterOutbox, added, added)
			}
		})
	}
}

func TestTaskCreationReturnsPersistedState(t *testing.T) {
	RequireRepositoryIntegration(t)
	catalog, err := execution.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewTaskService(sharedRepo, catalog)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "persisted task responses")
	if err != nil {
		t.Fatal(err)
	}
	var parentID uuid.UUID
	for _, tc := range []struct {
		name       string
		dependent  bool
		wantStatus domain.TaskStatus
	}{
		{"root uses catalog budget", false, domain.TaskStatusQueued},
		{"child uses catalog budget", true, domain.TaskStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var parents []uuid.UUID
			if tc.dependent {
				parents = []uuid.UUID{parentID}
			}
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			result, err := svc.CreateTask(t.Context(), "queue:service-test", workflow.ID, parents,
				"reference.echo", json.RawMessage(`{"message":"hello"}`))
			if err != nil {
				t.Fatal(err)
			}
			stored, err := sharedRepo.GetTask(t.Context(), result.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertTaskResponseMatchesStored(t, result, stored)
			if result.MaxRetries != contract.MaxAttempts || result.Status != tc.wantStatus || !slices.Equal(result.ParentIDs, parents) {
				t.Fatalf("unexpected catalog budget, status or parents: %+v", result)
			}
			if !tc.dependent {
				parentID = result.ID
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			wantOutbox := beforeOutbox
			if !tc.dependent {
				wantOutbox++
			}
			if afterJobs != beforeJobs+1 || afterOutbox != wantOutbox {
				t.Fatalf("unexpected job/outbox counts: %d/%d", afterJobs, afterOutbox)
			}
		})
	}
}

func assertTaskResponseMatchesStored(t *testing.T, result api.TaskResponse, stored repository.JobRow) {
	t.Helper()
	sameError := result.ErrorLog == nil && stored.ErrorLog == nil ||
		result.ErrorLog != nil && stored.ErrorLog != nil && *result.ErrorLog == *stored.ErrorLog
	sameProcessed := result.ProcessedAt == nil && stored.ProcessedAt == nil ||
		result.ProcessedAt != nil && stored.ProcessedAt != nil && result.ProcessedAt.Equal(*stored.ProcessedAt)
	if result.ID != stored.ID || result.WorkflowID != stored.WorkflowID || result.TaskType != stored.TaskType ||
		result.Status != stored.Status || result.CurrentRetries != stored.CurrentRetries || result.MaxRetries != stored.MaxRetries ||
		!slices.Equal(result.ParentIDs, stored.ParentIDs) || !sameError || !sameProcessed ||
		!result.CreatedAt.Equal(stored.CreatedAt) || !result.AvailableAt.Equal(stored.AvailableAt) {
		t.Fatalf("response differs from persisted task: response=%+v stored=%+v", result, stored)
	}
}

func taskSubmissionRowCounts(t *testing.T) (jobs, outbox int64) {
	_, jobs, outbox = submissionRowCounts(t)
	return jobs, outbox
}

func submissionRowCounts(t *testing.T) (workflows, jobs, outbox int64) {
	t.Helper()
	if err := sharedPool.QueryRow(t.Context(), `SELECT
		(SELECT COUNT(*) FROM workflows),
		(SELECT COUNT(*) FROM processed_jobs),
		(SELECT COUNT(*) FROM outbox)`).Scan(&workflows, &jobs, &outbox); err != nil {
		t.Fatal(err)
	}
	return workflows, jobs, outbox
}
