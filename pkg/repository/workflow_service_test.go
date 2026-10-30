package repository_test

import (
	"distributed-queue/internal/api"
	v1 "distributed-queue/internal/api/handlers/v1"
	"distributed-queue/internal/api/service"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestWorkflowHandlerCreateWorkflow(t *testing.T) {
	RequireRepositoryIntegration(t)
	router := gin.New()
	handler := v1.NewWorkflowHandler(service.NewWorkflowService(sharedRepo))
	router.POST("/api/v1/workflows", handler.CreateWorkflow)
	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{"tenant override", `{"name":"workflow HTTP validation","tenant_id":"3029669a-a0bd-415c-b1b0-b6d43e3b36cb"}`, http.StatusBadRequest},
		{"empty tenant override", `{"name":"workflow HTTP validation","tenant_id":""}`, http.StatusBadRequest},
		{"null tenant override", `{"name":"workflow HTTP validation","tenant_id":null}`, http.StatusBadRequest},
		{"null queue override", `{"name":"workflow HTTP validation","queue_name":null}`, http.StatusBadRequest},
		{"zero retry override", `{"name":"workflow HTTP validation","max_retries":0}`, http.StatusBadRequest},
		{"null retry override", `{"name":"workflow HTTP validation","max_retries":null}`, http.StatusBadRequest},
		{"zero TTL override", `{"name":"workflow HTTP validation","ttl_seconds":0}`, http.StatusBadRequest},
		{"null TTL override", `{"name":"workflow HTTP validation","ttl_seconds":null}`, http.StatusBadRequest},
		{"unknown field", `{"name":"workflow HTTP validation","unknown":true}`, http.StatusBadRequest},
		{"trailing JSON", `{"name":"workflow HTTP validation"} {"name":"second workflow"}`, http.StatusBadRequest},
		{"trailing invalid text", `{"name":"workflow HTTP validation"} invalid`, http.StatusBadRequest},
		{"missing name", `{}`, http.StatusBadRequest},
		{"empty name", `{"name":""}`, http.StatusBadRequest},
		{"null name", `{"name":null}`, http.StatusBadRequest},
		{"null body", `null`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
		{"malformed JSON", `{"name":`, http.StatusBadRequest},
		{"valid workflow", `{"name":"workflow HTTP validation"}`, http.StatusCreated},
		{"valid trailing whitespace", "{\"name\":\"workflow HTTP validation\"}\n\t ", http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeWorkflows, beforeJobs, beforeOutbox := submissionRowCounts(t)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			var added int64
			if tc.wantStatus == http.StatusCreated {
				var result api.WorkflowResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.ID == uuid.Nil || result.Name != "workflow HTTP validation" {
					t.Fatalf("unexpected workflow response: %+v", result)
				}
				stored, err := sharedRepo.GetWorkflow(t.Context(), result.ID)
				if err != nil {
					t.Fatal(err)
				}
				if result.ID != stored.ID || result.Name != stored.Name || result.Status != stored.Status ||
					result.SealedAt != nil || stored.SealedAt != nil ||
					result.DraftExpiresAt != nil || stored.DraftExpiresAt != nil ||
					!result.CreatedAt.Equal(stored.CreatedAt) || !result.UpdatedAt.Equal(stored.UpdatedAt) {
					t.Fatalf("response differs from persisted workflow: response=%+v stored=%+v", result, stored)
				}
				added = 1
			} else {
				var result api.ErrorResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Error == "" {
					t.Fatalf("expected JSON error response, got %s", response.Body.String())
				}
			}
			afterWorkflows, afterJobs, afterOutbox := submissionRowCounts(t)
			if afterWorkflows != beforeWorkflows+added || afterJobs != beforeJobs || afterOutbox != beforeOutbox {
				t.Fatalf("workflow/job/outbox counts changed from %d/%d/%d to %d/%d/%d; expected delta %d/0/0",
					beforeWorkflows, beforeJobs, beforeOutbox, afterWorkflows, afterJobs, afterOutbox, added)
			}
		})
	}
}
