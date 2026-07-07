package main

import (
	"context"
	"distributed-queue/pkg/circuitbreaker"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/redis"
	"distributed-queue/pkg/redis/metrics"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// simulateProcessing stands in for real task work. Replace with real
// processing logic; keep the ctx-cancellation check if real work is itself
// cancellable.
func simulateProcessing(ctx context.Context, taskId string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if rand.Intn(5) == 0 {
		return errors.New("simulated processing failure")
	}
	return nil
}

// runWorker continuously consumes tasks from Redis, checks idempotency in Postgres,
// processes them, and updates Postgres and Redis accordingly. It handles retries
// and dead-lettering based on the Postgres job row's retry counter.
func runWorker(ctx context.Context, queueName string, store *redis.RedisStore, jobRepo *repository.JobRepository, dbBreaker *circuitbreaker.CircuitBreaker, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("worker thread started, waiting for tasks...")

	for {
		if ctx.Err() != nil {
			slog.Info("worker thread received shutdown signal, exiting...")
			return
		}

		taskId, err := store.ConsumeTask(ctx, queueName)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("worker thread received shutdown signal, exiting...")
				return
			}
			slog.Error("failed to consume task", "error", err)
			// Exponential backoff with jitter to avoid hammering Redis
			backoff := time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
			time.Sleep(backoff)
			continue
		}
		if taskId == "" {
			continue
		}

		slog.Info("task consumed, running idempotency check", "taskId", taskId)

		var status string
		var tryErr error
		
		// Use circuit breaker for DB call
		cbErr1 := dbBreaker.Call(func() error {
			status, tryErr = jobRepo.TryStartJob(ctx, queueName, taskId)
			return tryErr
		})
		
		if cbErr1 != nil {
			if errors.Is(cbErr1, circuitbreaker.ErrCircuitOpen) {
				slog.Warn("circuit breaker open, skipping DB call", "taskId", taskId)
				// Leave task in inflight, janitor will recover
				time.Sleep(time.Second)
				continue
			}
			tryErr = cbErr1
		}
		
		if tryErr != nil {
			if errors.Is(tryErr, repository.ErrDuplicateJob) {
				slog.Warn("[IDEMPOTENCY] task already completed, dropping from Redis", "taskId", taskId)
				metrics.TasksAcknowledged.WithLabelValues(queueName).Inc()
				if err := store.AcknowledgeTask(ctx, queueName, taskId); err != nil {
					slog.Error("failed to clean up duplicate task from Redis", "taskId", taskId, "error", err)
				}
				continue
			}
			if errors.Is(tryErr, repository.ErrJobNotFound) {
				slog.Error("[IDEMPOTENCY] task in Redis but no DB row exists - data inconsistency, moving to DLQ", "taskId", taskId)
				metrics.TasksOrphaned.WithLabelValues(queueName).Inc()
				if err := store.MoveToDeadLetterQueue(ctx, queueName, taskId); err != nil {
					slog.Error("failed to move orphaned task to DLQ", "taskId", taskId, "error", err)
				}
				continue
			}
			// Any other error (DB down, timeout, etc): task stays claimed in
			// Redis inflight. We deliberately do NOT retry/DLQ here — we
			// leave it for the janitor to sweep back to the main queue after
			// holdTTL, since we don't know if the DB write actually landed.
			slog.Error("idempotency check failed, leaving task for janitor to recover", "taskId", taskId, "error", tryErr)
			continue
		}
		slog.Info("idempotency check passed", "taskId", taskId, "status", status)

		processingStart := time.Now()
		procErr := simulateProcessing(ctx, taskId)
		metrics.ProcessingDuration.WithLabelValues(queueName).Observe(time.Since(processingStart).Seconds())

		if procErr == nil {
			slog.Info("task processing completed, preparing to complete", "taskId", taskId)

			// STEP 1: Check for ready children FIRST (before marking parent complete)
			children, err := jobRepo.CheckAndEnqueueChildren(ctx, queueName, taskId)
			if err != nil {
				slog.Error("failed to check for ready children", "taskId", taskId, "error", err)
				// Don't complete parent if we can't check children - leave for janitor
				continue
			}

			// STEP 2: Enqueue children to Redis BEFORE marking parent complete
			if len(children) > 0 {
				slog.Info("enqueuing ready children before marking parent complete", "parentTask", taskId, "childCount", len(children))
				allEnqueued := true
				for _, childID := range children {
					if err := store.EnqueueTask(ctx, queueName, childID); err != nil {
						slog.Error("failed to enqueue child task", "childId", childID, "error", err)
						metrics.ChildEnqueueFailures.WithLabelValues(queueName).Inc()
						allEnqueued = false
					} else {
						metrics.ChildrenEnqueued.WithLabelValues(queueName).Inc()
					}
				}
				// If ANY child enqueue failed, don't complete parent - let janitor retry
				if !allEnqueued {
					slog.Error("failed to enqueue all children, leaving parent for janitor to retry", "taskId", taskId)
					continue
				}
			}

			// STEP 3: NOW mark parent as complete (after children are in Redis)
			cbErr3 := dbBreaker.Call(func() error {
				return jobRepo.CompleteJob(ctx, queueName, taskId)
			})
			
			if cbErr3 != nil {
				if errors.Is(cbErr3, circuitbreaker.ErrCircuitOpen) {
					slog.Warn("circuit breaker open for DB, cannot complete job", "taskId", taskId)
					time.Sleep(time.Second)
					continue
				}
				slog.Error("failed to write job completion to postgres, leaving for janitor to recover", "taskId", taskId, "error", cbErr3)
				continue
			}

			// STEP 4: Update workflow status
			workflowID, err := jobRepo.GetWorkflowID(ctx, taskId)
			if err != nil {
				slog.Error("failed to get workflow_id for status update", "taskId", taskId, "error", err)
			} else {
				if err := jobRepo.UpdateWorkflowStatus(ctx, workflowID); err != nil {
					slog.Error("failed to update workflow status", "workflowId", workflowID, "error", err)
				}
			}

			if err := store.AcknowledgeTask(ctx, queueName, taskId); err != nil {
				slog.Error("failed to acknowledge task, will be recovered by janitor", "taskId", taskId, "error", err)
				// Exponential backoff with jitter to avoid hammering Redis
				backoff := 500*time.Millisecond + time.Duration(rand.Intn(500))*time.Millisecond
				time.Sleep(backoff)
			} else {
				slog.Info("task acknowledged and synchronized everywhere", "taskId", taskId)
			}
			continue
		}

		// Processing failed. Decide retry vs dead-letter using the DB's
		// per-job retry counter as the source of truth.
		metrics.ProcessingFailures.WithLabelValues(queueName).Inc()
		slog.Warn("task processing failed, recording failure in postgres", "taskId", taskId, "error", procErr)

		var currentRetry, maxRetry int
		var dbErr error
		
		// Use circuit breaker for DB call
		cbErr2 := dbBreaker.Call(func() error {
			currentRetry, maxRetry, dbErr = jobRepo.RecordFailure(ctx, queueName, taskId, procErr.Error())
			return dbErr
		})
		
		if cbErr2 != nil {
			if errors.Is(cbErr2, circuitbreaker.ErrCircuitOpen) {
				slog.Warn("circuit breaker open for DB, skipping failure record", "taskId", taskId)
				// Leave task in inflight, janitor will recover
				time.Sleep(time.Second)
				continue
			}
			dbErr = cbErr2
		}
		
		if dbErr != nil {
			slog.Error("failed to record failure in postgres, leaving for janitor to recover", "taskId", taskId, "error", dbErr)
			// Exponential backoff to avoid hammering DB when it's down
			backoff := time.Duration(rand.Intn(1000)) * time.Millisecond
			time.Sleep(backoff)
			continue
		}

		if currentRetry >= maxRetry {
			slog.Error("task exceeded max retries, moving to DLQ", "taskId", taskId, "retries", currentRetry, "maxRetries", maxRetry)

			// Cancel all dependent children since this parent will never complete
			if err := jobRepo.MarkAsCancelledCascading(ctx, queueName, taskId); err != nil {
				slog.Error("failed to cascade cancellation to children", "taskId", taskId, "error", err)
			}

			if err := store.MoveToDeadLetterQueue(ctx, queueName, taskId); err != nil {
				slog.Error("failed to move task to DLQ, leaving for janitor to recover", "taskId", taskId, "error", err)
			}

			// Update workflow status (task failed, workflow might be failed too)
			workflowID, err := jobRepo.GetWorkflowID(ctx, taskId)
			if err != nil {
				slog.Error("failed to get workflow_id for status update", "taskId", taskId, "error", err)
			} else {
				if err := jobRepo.UpdateWorkflowStatus(ctx, workflowID); err != nil {
					slog.Error("failed to update workflow status", "workflowId", workflowID, "error", err)
				}
			}
			continue
		}

		slog.Warn("scheduling backoff retry", "taskId", taskId, "attempt", currentRetry, "max", maxRetry)
		if err := store.RetryTaskWithBackoff(ctx, queueName, taskId, currentRetry); err != nil {
			slog.Error("failed to schedule retry, leaving for janitor to recover", "taskId", taskId, "error", err)
		}
	}
}

// retryWithBackoff retries fn up to maxAttempts times with exponential backoff
// (1s, 2s, 4s, ...). Respects ctx cancellation between attempts.
func retryWithBackoff(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			break
		}
		backoff := time.Duration(1<<uint(attempt)) * time.Second
		slog.Warn("retrying after backoff", "attempt", attempt+1, "maxAttempts", maxAttempts, "backoff", backoff, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}

// runProducer generates tasks at a fixed interval, creating a workflow and jobs in Postgres, then enqueuing them to Redis.
// It simulates a task producer for demonstration purposes.
func runProducer(ctx context.Context, queueName string, store *redis.RedisStore, jobRepo *repository.JobRepository, dbBreaker *circuitbreaker.CircuitBreaker, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("producer thread started, generating tasks...")

	// Create one workflow for all demo tasks — retry with backoff if DB is down
	var workflowID uuid.UUID
	err := retryWithBackoff(ctx, 5, func() error {
		return dbBreaker.Call(func() error {
			var createErr error
			workflowID, createErr = jobRepo.CreateWorkflow(ctx, "demo-workflow")
			return createErr
		})
	})
	if err != nil {
		slog.Error("failed to create workflow after retries, producer cannot start", "error", err)
		return
	}

	taskCounter := 1
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("producer thread received shutdown signal, exiting...")
			return
		case <-ticker.C:
			taskID := uuid.New()
			task := repository.Task{
				ID:         taskID,
				WorkflowID: workflowID,
				ParentIDs:  []uuid.UUID{},
				TaskType:   "VIDEO_PROCESS",
				Payload:    json.RawMessage(`{}`),
			}

			// WAL: Write DB row + outbox row atomically — relay will enqueue to Redis
			err := retryWithBackoff(ctx, 3, func() error {
				return dbBreaker.Call(func() error {
					return jobRepo.CreateJob(ctx, queueName, task)
				})
			})
			if err != nil {
				slog.Error("failed to create job in DB after retries, task dropped", "taskId", taskID, "error", err)
				continue
			}

			slog.Info("task written to DB and outbox, relay will enqueue to Redis", "taskId", taskID, "counter", taskCounter)
			taskCounter++
		}
	}
}

// runOutboxRelay polls the outbox table for undelivered rows and enqueues
// each task to Redis, then marks the row delivered. Because the outbox row
// is written in the same DB transaction as the processed_jobs row, this
// guarantees at-least-once delivery to Redis even across server crashes.
func runOutboxRelay(ctx context.Context, interval time.Duration, store *redis.RedisStore, jobRepo *repository.JobRepository, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("outbox relay started")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox relay received shutdown signal, exiting...")
			return
		case <-ticker.C:
			entries, err := jobRepo.GetPendingOutbox(ctx)
			if err != nil {
				slog.Error("outbox relay failed to read pending entries", "error", err)
				continue
			}
			if len(entries) == 0 {
				continue
			}

			for _, entry := range entries {
				if err := store.EnqueueTask(ctx, entry.QueueName, entry.TaskID.String()); err != nil {
					slog.Error("outbox relay failed to enqueue task", "taskId", entry.TaskID, "error", err)
					continue // leave outbox row undelivered, retry next tick
				}
				if err := jobRepo.MarkOutboxDelivered(ctx, entry.ID); err != nil {
					slog.Error("outbox relay failed to mark row delivered, task may be double-enqueued on next tick", "outboxId", entry.ID, "taskId", entry.TaskID, "error", err)
					continue
				}
				metrics.OutboxRelayed.WithLabelValues(entry.QueueName).Inc()
				slog.Info("outbox relay delivered task to Redis", "taskId", entry.TaskID)
			}
		}
	}
}

// runJanitor periodically checks for tasks that have been inflight longer than holdTTL and moves them back to the main queue for retry.
func runJanitor(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("janitor thread started, monitoring for abandoned tasks...")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("janitor thread received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.SweepAbondonedTasks(ctx, queueName); err != nil {
				slog.Error("failed to sweep abandoned tasks", "error", err)
			} else {
				slog.Info("janitor sweep completed")
			}
		}
	}
}

// runDelayedPoller periodically checks the delayed queue for tasks whose delay has expired and moves them to the main queue.
func runDelayedPoller(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("delayed-task poller started")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("delayed-task poller received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.PolledDelayedTasks(ctx, queueName); err != nil {
				slog.Error("failed to poll delayed tasks", "error", err)
			}
		}
	}
}

// runDepthCollector periodically snapshots queue/inflight/delayed/DLQ sizes
// into Prometheus gauges.
func runDepthCollector(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("depth collector started")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("depth collector received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.CollectDepths(ctx, queueName); err != nil {
				slog.Error("failed to collect queue depths", "error", err)
			}
		}
	}
}

// runMetricsServer serves Prometheus metrics on metricsAddr until ctx is
// cancelled, then shuts the HTTP server down gracefully.
func runMetricsServer(ctx context.Context, metricsAddr string, wg *sync.WaitGroup) {
	defer wg.Done()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	
	// Health check endpoint for load balancers
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"distributed-queue"}`))
	})
	
	srv := &http.Server{Addr: metricsAddr, Handler: mux}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("metrics server starting", "addr", metricsAddr)
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed to start", "error", err, "addr", metricsAddr)
		}
		serverErr <- err
	}()

	select {
	case <-ctx.Done():
		slog.Info("metrics server received shutdown signal, shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown error", "error", err)
		}
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}

	// Retry Redis connection with exponential backoff
	var store *redis.RedisStore
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		store, err = redis.NewRedisStore(ctx, cfg.Redis, cfg.Queue.HoldTTL)
		if err == nil {
			break
		}
		
		if i < maxRetries-1 {
			backoff := time.Duration(1<<uint(i)) * time.Second
			slog.Warn("failed to connect to Redis, retrying...", "attempt", i+1, "backoff", backoff, "error", err)
			time.Sleep(backoff)
		} else {
			slog.Error("failed to initialize Redis store after retries", "error", err)
			return
		}
	}

	pgCfg := repository.PostgresConfig{ConnString: cfg.Postgres.ConnString}
	pgPool, err := repository.NewPostgresPool(ctx, pgCfg)
	if err != nil {
		slog.Error("failed to initialize PostgreSQL connection pool", "error", err)
		store.Close()
		return
	}

	jobRepo := repository.NewJobRepository(pgPool)

	// Create circuit breaker for DB operations
	// Opens after 5 failures, resets after 30 seconds
	dbBreaker := circuitbreaker.New(5, 30*time.Second)

	defer func() {
		slog.Info("shutting down, closing database connections...")
		pgPool.Close()
		store.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(7)
	go runWorker(ctx, cfg.Queue.Name, store, jobRepo, dbBreaker, &wg)
	go runProducer(ctx, cfg.Queue.Name, store, jobRepo, dbBreaker, &wg)
	go runOutboxRelay(ctx, 2*time.Second, store, jobRepo, &wg)
	go runJanitor(ctx, cfg.Queue.Name, cfg.Queue.JanitorInterval, store, &wg)
	go runDelayedPoller(ctx, cfg.Queue.Name, cfg.Queue.DelayedPollEvery, store, &wg)
	go runDepthCollector(ctx, cfg.Queue.Name, cfg.Queue.DepthPollEvery, store, &wg)
	go runMetricsServer(ctx, cfg.Metrics.Addr, &wg)

	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for all threads to exit...")
	
	// Graceful shutdown with timeout
	done := make(chan struct{})
	
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		slog.Info("all threads exited gracefully")
	case <-time.After(cfg.Worker.ShutdownTimeout):
		slog.Warn("shutdown timeout reached, forcing exit", "timeout", cfg.Worker.ShutdownTimeout)
		// Note: In-flight tasks will be recovered by janitor on next startup
	}

	slog.Info("shutdown complete")
}
