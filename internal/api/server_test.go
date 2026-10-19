package api

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type noopWorkflowHandler struct{}

func (noopWorkflowHandler) CreateWorkflow(*gin.Context) {}
func (noopWorkflowHandler) GetWorkflow(*gin.Context)    {}

type noopTaskHandler struct{}

func (noopTaskHandler) CreateTask(*gin.Context) {}
func (noopTaskHandler) GetTask(*gin.Context)    {}

func TestNewServerSetsConnectionTimeouts(t *testing.T) {
	server := NewServer(":0", noopWorkflowHandler{}, noopTaskHandler{})
	if server.srv.ReadHeaderTimeout != 5*time.Second ||
		server.srv.ReadTimeout != 30*time.Second ||
		server.srv.WriteTimeout != 30*time.Second ||
		server.srv.IdleTimeout != 60*time.Second {
		t.Fatalf("unexpected server timeouts: %#v", server.srv)
	}
}
