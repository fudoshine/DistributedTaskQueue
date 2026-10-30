package v1

import (
	"distributed-queue/internal/api"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindStrictJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name    string
		body    string
		invalid bool
	}{
		{"valid request", `{"task_type":"reference.echo","payload":{"message":"hello"}}`, false},
		{"trailing whitespace", "{\"task_type\":\"reference.echo\",\"payload\":{\"message\":\"hello\"}} \n\t", false},
		{"unknown field", `{"task_type":"reference.echo","payload":{"message":"hello"},"queue_name":null}`, true},
		{"trailing JSON", `{"task_type":"reference.echo","payload":{"message":"hello"}} {}`, true},
		{"trailing malformed data", `{"task_type":"reference.echo","payload":{"message":"hello"}} invalid`, true},
		{"missing required task type", `{"payload":{"message":"hello"}}`, true},
		{"empty required task type", `{"task_type":"","payload":{"message":"hello"}}`, true},
		{"empty object", `{}`, true},
		{"null body", `null`, true},
		{"empty body", ``, true},
		{"malformed JSON", `{"task_type":`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/id/tasks", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = request

			var payload api.CreateTaskRequest
			err := bindStrictJSON(ctx, &payload)
			if (err != nil) != tc.invalid {
				t.Fatalf("bindStrictJSON error = %v, want invalid = %t", err, tc.invalid)
			}
			if !tc.invalid && (payload.TaskType != "reference.echo" || string(payload.Payload) != `{"message":"hello"}`) {
				t.Fatalf("unexpected decoded request: %+v", payload)
			}
		})
	}
}
