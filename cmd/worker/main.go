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
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	outboxBatchSize   = 100
	recoveryBatchSize = 100
)

func simulateProcessing(ctx context.Context, taskID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	if rand.Intn(5) == 0 {
		return errors.New("simulated processing failure")
	}
	return nil
}

// startLeaseHeartbeat renews the durable fence and resets Stream PEL idle time.
// Postgres remains the authority: a Redis heartbeat failure never permits a
// stale completion, and a DB lease loss cancels the handler.
func startLeaseHeartbeat(parent context.Context, queueName, consumer string, delivery redis.Delivery, claim repository.JobClaim, leaseTTL time.Duration, store *redis.RedisStore, jobs *repository.JobRepository) (context.Context, func() bool) {
	processingCtx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	done := make(chan struct{})
	var lost atomic.Bool
	interval := leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-processingCtx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				renewed, err := jobs.RenewLease(processingCtx, queueName, delivery.TaskID, claim.LeaseToken, leaseTTL)
				if err != nil {
					slog.Warn("lease heartbeat failed", "taskId", delivery.TaskID, "error", err)
					continue
				}
				if !renewed {
					lost.Store(true)
					cancel()
					return
				}
				if err := store.TouchDelivery(processingCtx, queueName, consumer, delivery.StreamID); err != nil {
					slog.Warn("stream heartbeat failed; DB lease remains authoritative", "streamId", delivery.StreamID, "error", err)
				}
			}
		}
	}()
	return processingCtx, func() bool { close(stop); cancel(); <-done; return lost.Load() }
}

func acknowledge(ctx context.Context, store *redis.RedisStore, queueName string, delivery redis.Delivery) {
	if err := store.AcknowledgeDelivery(ctx, queueName, delivery.StreamID); err != nil {
		slog.Error("failed to acknowledge stream delivery", "streamId", delivery.StreamID, "taskId", delivery.TaskID, "error", err)
	}
}

func processDelivery(ctx context.Context, queueName, consumer string, leaseTTL time.Duration, store *redis.RedisStore, jobs *repository.JobRepository, dbBreaker *circuitbreaker.CircuitBreaker, delivery redis.Delivery) {
	var claim repository.JobClaim
	var claimErr error
	cbErr := dbBreaker.Call(func() error {
		claim, claimErr = jobs.TryStartJob(ctx, queueName, delivery.TaskID, delivery.DeliveryID, delivery.Generation, leaseTTL)
		return claimErr
	})
	if cbErr != nil {
		claimErr = cbErr
	}
	if claimErr != nil {
		switch {
		case errors.Is(claimErr, repository.ErrJobAlreadyOwned),
			errors.Is(claimErr, repository.ErrStaleDelivery),
			errors.Is(claimErr, repository.ErrDuplicateJob),
			errors.Is(claimErr, repository.ErrTerminalJob),
			errors.Is(claimErr, repository.ErrJobNotReady):
			// This Stream entry cannot become the current execution any more.
			acknowledge(ctx, store, queueName, delivery)
			return
		case errors.Is(claimErr, repository.ErrJobNotFound):
			slog.Error("stream delivery has no job row; moving to DLQ", "taskId", delivery.TaskID)
			if err := store.MoveToDeadLetterQueue(ctx, queueName, delivery); err == nil {
				acknowledge(ctx, store, queueName, delivery)
			}
			return
		default:
			// Leave PEL entry unacked. XAUTOCLAIM and the durable DB recovery
			// sweep will retry once the backend is healthy.
			slog.Warn("claim failed; retaining PEL delivery for recovery", "taskId", delivery.TaskID, "error", claimErr)
			return
		}
	}

	processingCtx, stopHeartbeat := startLeaseHeartbeat(ctx, queueName, consumer, delivery, claim, leaseTTL, store, jobs)
	started := time.Now()
	processingErr := simulateProcessing(processingCtx, delivery.TaskID)
	metrics.ProcessingDuration.WithLabelValues(queueName).Observe(time.Since(started).Seconds())
	if stopHeartbeat() {
		return
	}

	if processingErr == nil {
		var children []string
		var completeErr error
		cbErr := dbBreaker.Call(func() error {
			children, completeErr = jobs.CompleteJobAndScheduleChildren(ctx, queueName, delivery.TaskID, claim.LeaseToken)
			return completeErr
		})
		if cbErr != nil {
			completeErr = cbErr
		}
		if completeErr != nil {
			if errors.Is(completeErr, repository.ErrLeaseLost) {
				return
			}
			slog.Warn("completion failed; retaining PEL delivery for recovery", "taskId", delivery.TaskID, "error", completeErr)
			return
		}
		if len(children) > 0 {
			slog.Info("scheduled ready child deliveries", "taskId", delivery.TaskID, "count", len(children))
		}
		if workflowID, err := jobs.GetWorkflowID(ctx, delivery.TaskID); err == nil {
			if err := jobs.UpdateWorkflowStatus(ctx, workflowID); err != nil {
				slog.Warn("update workflow status", "error", err)
			}
		}
		acknowledge(ctx, store, queueName, delivery)
		return
	}

	metrics.ProcessingFailures.WithLabelValues(queueName).Inc()
	var currentRetry, maxRetry int
	var failureErr error
	cbErr = dbBreaker.Call(func() error {
		currentRetry, maxRetry, failureErr = jobs.RecordFailure(ctx, queueName, delivery.TaskID, claim.LeaseToken, processingErr.Error())
		return failureErr
	})
	if cbErr != nil {
		failureErr = cbErr
	}
	if failureErr != nil {
		if !errors.Is(failureErr, repository.ErrLeaseLost) {
			slog.Warn("record failure failed; retaining PEL delivery", "taskId", delivery.TaskID, "error", failureErr)
		}
		return
	}
	if currentRetry >= maxRetry {
		if err := jobs.MarkAsCancelledCascading(ctx, queueName, delivery.TaskID); err != nil {
			slog.Warn("cascade cancellation failed", "error", err)
		}
		if err := store.MoveToDeadLetterQueue(ctx, queueName, delivery); err != nil {
			slog.Error("move terminal job to DLQ", "error", err)
			return
		}
		if workflowID, err := jobs.GetWorkflowID(ctx, delivery.TaskID); err == nil {
			_ = jobs.UpdateWorkflowStatus(ctx, workflowID)
		}
	}
	// For a non-terminal failure RecordFailure inserted the next durable outbox
	// generation in the same transaction. The old PEL entry is now stale.
	acknowledge(ctx, store, queueName, delivery)
}

func runWorker(ctx context.Context, queueName, consumer string, leaseTTL time.Duration, store *redis.RedisStore, jobs *repository.JobRepository, dbBreaker *circuitbreaker.CircuitBreaker, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("stream worker started", "consumer", consumer)
	for ctx.Err() == nil {
		delivery, err := store.ConsumeDelivery(ctx, queueName, consumer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("consume stream delivery", "consumer", consumer, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if delivery.StreamID == "" {
			continue
		}
		processDelivery(ctx, queueName, consumer, leaseTTL, store, jobs, dbBreaker, delivery)
	}
}

func runOutboxRelay(ctx context.Context, interval, leaseTTL time.Duration, store *redis.RedisStore, jobs *repository.JobRepository, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, token, err := jobs.ClaimPendingOutbox(ctx, outboxBatchSize, leaseTTL)
			if err != nil {
				slog.Error("claim outbox", "error", err)
				continue
			}
			for _, entry := range entries {
				if err := store.PublishDelivery(ctx, entry.QueueName, entry.TaskID.String(), entry.DeliveryID.String(), entry.Generation); err != nil {
					slog.Warn("publish outbox delivery", "outboxId", entry.OutboxID, "error", err)
					_ = jobs.ReleaseOutboxLease(ctx, entry.OutboxID, token)
					continue
				}
				if err := jobs.MarkOutboxDelivered(ctx, entry.OutboxID, token); err != nil {
					slog.Warn("mark outbox delivery", "outboxId", entry.OutboxID, "error", err)
					continue
				}
				metrics.OutboxRelayed.WithLabelValues(entry.QueueName).Inc()
			}
		}
	}
}

func runJanitor(ctx context.Context, queueName string, interval, leaseTTL time.Duration, store *redis.RedisStore, jobs *repository.JobRepository, wg *sync.WaitGroup) {
	defer wg.Done()
	sweep := func() {
		if recovered, err := jobs.RecoverExpiredLeases(ctx, queueName, recoveryBatchSize); err != nil {
			slog.Error("recover expired DB leases", "error", err)
		} else if len(recovered) > 0 {
			slog.Warn("scheduled expired job recoveries", "count", len(recovered))
		}
		idle, err := store.ClaimIdleDeliveries(ctx, queueName, leaseTTL, recoveryBatchSize)
		if err != nil {
			slog.Error("claim idle PEL deliveries", "error", err)
			return
		}
		for _, delivery := range idle {
			action, err := jobs.RecoverIdleDelivery(ctx, queueName, delivery.TaskID, delivery.Generation)
			if err != nil {
				slog.Warn("recover idle delivery", "streamId", delivery.StreamID, "error", err)
				continue
			}
			if action != repository.RecoveryKeepInflight {
				acknowledge(ctx, store, queueName, delivery)
			}
		}
	}
	// A startup sweep reduces downtime recovery latency without recovering a
	// live attempt: every decision still uses the DB lease condition.
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

func retryWithBackoff(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<uint(attempt)) * time.Second):
		}
	}
	return err
}

func runProducer(ctx context.Context, queueName string, interval time.Duration, jobs *repository.JobRepository, dbBreaker *circuitbreaker.CircuitBreaker, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var workflowID uuid.UUID
			err := retryWithBackoff(ctx, 3, func() error {
				return dbBreaker.Call(func() error {
					workflow, e := jobs.CreateWorkflow(ctx, fmt.Sprintf("video-pipeline-%d", n))
					workflowID=workflow.ID
					return e
				})
			})
			if err != nil {
				slog.Error("create workflow", "error", err)
				continue
			}
			upload, transcode, notify := uuid.New(), uuid.New(), uuid.New()
			tasks := []repository.Task{
				{ID: upload, WorkflowID: workflowID, TaskType: "VIDEO_UPLOAD", Payload: json.RawMessage(`{"file":"video.mp4"}`)},
				{ID: transcode, WorkflowID: workflowID, ParentIDs: []uuid.UUID{upload}, TaskType: "VIDEO_TRANSCODE", Payload: json.RawMessage(`{"resolution":"1080p"}`)},
				{ID: notify, WorkflowID: workflowID, ParentIDs: []uuid.UUID{transcode}, TaskType: "VIDEO_NOTIFY", Payload: json.RawMessage(`{"channel":"email"}`)},
			}
			for _, task := range tasks {
				if err := jobs.CreateJob(ctx, queueName, task); err != nil {
					slog.Error("create task", "taskId", task.ID, "error", err)
					break
				}
			}
		}
	}
}

func runDepthCollector(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := store.CollectDepths(ctx, queueName); err != nil {
				slog.Warn("collect queue depths", "error", err)
			}
		}
	}
}

func runMetricsServer(ctx context.Context, address string, wg *sync.WaitGroup) {
	defer wg.Done()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy","service":"distributed-queue"}`))
	})
	server := &http.Server{Addr: address, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server", "error", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		return
	}
	store, err := redis.NewRedisStore(ctx, cfg.Redis, cfg.Queue.HoldTTL)
	if err != nil {
		slog.Error("connect Redis", "error", err)
		return
	}
	defer func() { _ = store.Close() }() // Best-effort cleanup on process exit.
	if err := repository.RunMigrations(cfg.Postgres.ConnString, cfg.Postgres.MigrationPath); err != nil {
		slog.Error("run migrations", "error", err)
		return
	}
	pool, err := repository.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("connect Postgres", "error", err)
		return
	}
	defer pool.Close()
	if err := store.EnsureConsumerGroup(ctx, cfg.Queue.Name); err != nil {
		slog.Error("initialize stream group", "error", err)
		return
	}
	jobs := repository.NewJobRepository(pool)
	breaker := circuitbreaker.New(5, 30*time.Second)
	var wg sync.WaitGroup
	workerCount := cfg.Worker.Count
	wg.Add(workerCount + 5)
	for i := 0; i < workerCount; i++ {
		go runWorker(ctx, cfg.Queue.Name, fmt.Sprintf("worker-%s-%d", uuid.NewString(), i), cfg.Queue.HoldTTL, store, jobs, breaker, &wg)
	}
	go runProducer(ctx, cfg.Queue.Name, cfg.Queue.TaskProducerInterval, jobs, breaker, &wg)
	go runOutboxRelay(ctx, time.Second, cfg.Queue.HoldTTL, store, jobs, &wg)
	go runJanitor(ctx, cfg.Queue.Name, cfg.Queue.JanitorInterval, cfg.Queue.HoldTTL, store, jobs, &wg)
	go runDepthCollector(ctx, cfg.Queue.Name, cfg.Queue.DepthPollEvery, store, &wg)
	go runMetricsServer(ctx, cfg.Metrics.Addr, &wg)
	<-ctx.Done()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(cfg.Worker.ShutdownTimeout):
		slog.Warn("shutdown timeout reached")
	}
}
