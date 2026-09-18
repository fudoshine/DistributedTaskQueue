package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine *gin.Engine
	addr   string
	srv    *http.Server
}

type WorkflowHandler interface {
	CreateWorkflow(*gin.Context)
	GetWorkflow(*gin.Context)
}

type TaskHandler interface {
	CreateTask(*gin.Context)
	GetTask(*gin.Context)
}

func NewServer(addr string, workflowHandler WorkflowHandler, taskHandler TaskHandler) *Server {
	router := gin.Default()

	//routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/workflows", workflowHandler.CreateWorkflow)
		v1.GET("/workflows/:id", workflowHandler.GetWorkflow)
		v1.POST("/workflows/:id/tasks", taskHandler.CreateTask)
		v1.GET("/tasks/:id", taskHandler.GetTask)
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: router.Handler(),
	}
	return &Server{
		engine: router,
		addr:   addr,
		srv:    srv,
	}
}

func (s *Server) Run() error {
	slog.Info("Server is running", "addr", s.addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("failed to start server", "error", err)
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
