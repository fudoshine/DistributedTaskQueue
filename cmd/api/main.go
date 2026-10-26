package main

import (
	"context"
	apiserver "distributed-queue/internal/api"
	v1 "distributed-queue/internal/api/handlers/v1"
	"distributed-queue/internal/api/service"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/repository"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		return
	}

	pool, err := repository.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		return
	}
	defer pool.Close()

	jobRepo := repository.NewJobRepository(pool)
	catalog, err := execution.NewCatalog()
	if err != nil {
		slog.Error("failed to initialize catalog", "error", err)
		return
	}
	workflowSvc := service.NewWorkflowService(jobRepo)
	taskSvc := service.NewTaskService(jobRepo, catalog)

	workflowHandler := v1.NewWorkflowHandler(workflowSvc)
	taskHandler := v1.NewTaskHandler(taskSvc, cfg.Queue.Name)

	server := apiserver.NewServer(cfg.ServerAddr, workflowHandler, taskHandler)

	go func() {
		if err := server.Run(); err != nil {
			slog.Error("server exited with error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down API server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	slog.Info("API server stopped")
}
