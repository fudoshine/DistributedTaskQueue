// Benchmarks for the repository layer.
//
// Environment: Apple M3 Pro (11-core), 18GB RAM, local Docker Postgres 14
// synchronous_commit = on (verified — numbers are not inflated by async commits)
// pgx pool of 20 connections
//
// Run all benchmarks:
//
//	RUN_REPOSITORY_INTEGRATION=1 TEST_POSTGRES_CONN_STRING='postgres://...' go test ./pkg/repository/ -run '^$' -bench=. -benchtime=20000x -count=1
//
// Run only latency test:
//
//	RUN_REPOSITORY_INTEGRATION=1 RUN_LATENCY_BENCHMARK=1 TEST_POSTGRES_CONN_STRING='postgres://...' go test ./pkg/repository/ -run TestEndToEndLatency -v -count=1
package repository_test

import (
	"context"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	sharedRepo *repository.JobRepository
	sharedPool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	if os.Getenv("RUN_REPOSITORY_INTEGRATION") != "1" {
		os.Exit(m.Run())
	}
	connString := os.Getenv("TEST_POSTGRES_CONN_STRING")
	if connString == "" {
		panic("TEST_POSTGRES_CONN_STRING is required when RUN_REPOSITORY_INTEGRATION=1")
	}
	cfg := config.PostgresConfig{
		ConnString:            connString,
		MaxConns:              20,
		MinConns:              2,
		MaxConnIdle:           5 * time.Minute,
		MaxConnLifetime:       10 * time.Minute,
		MaxConnLifetimeJitter: time.Minute,
	}
	if err := repository.RunMigrations(cfg.ConnString, "file://../../db/migrations"); err != nil {
		panic("run migrations: " + err.Error())
	}
	pool, err := repository.NewPostgresPool(context.Background(), cfg)
	if err != nil {
		panic("connect postgres: " + err.Error())
	}
	sharedPool = pool
	sharedRepo = repository.NewJobRepository(pool)

	// silence structured and standard loggers so benchmark output is clean
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	log.SetOutput(io.Discard)

	// truncate benchmark tables so rows don't accumulate across runs
	_, _ = pool.Exec(context.Background(),
		`TRUNCATE processed_jobs, workflows, job_dependencies, outbox CASCADE`)

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func RequireRepositoryIntegration(tb testing.TB) {
	tb.Helper()
	if sharedRepo == nil {
		tb.Skip("set RUN_REPOSITORY_INTEGRATION=1 and TEST_POSTGRES_CONN_STRING to run repository integration tests")
	}
}

func referenceContract(tb testing.TB) execution.CatalogEntry {
	tb.Helper()
	catalog, err := execution.NewCatalog()
	if err != nil {
		tb.Fatal(err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		tb.Fatal(err)
	}
	return entry
}

// getDeliveryID reads back the outbox delivery_id written by CreateJob.
// In production this flows: outbox → relay → Redis message → worker.
// In tests we read it directly to simulate what the worker receives.
func getDeliveryID(ctx context.Context, taskID uuid.UUID) (uuid.UUID, error) {
	var deliveryID uuid.UUID
	err := sharedPool.QueryRow(ctx,
		`SELECT delivery_id FROM outbox WHERE task_id = $1 AND generation = 0`,
		taskID,
	).Scan(&deliveryID)
	return deliveryID, err
}

// ── Serial benchmarks ────────────────────────────────────────────────────────

// BenchmarkCreateWorkflow measures workflow creation throughput (serial).
func BenchmarkCreateWorkflow(b *testing.B) {
	RequireRepositoryIntegration(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("bench-workflow-%d", i))
		if err != nil {
			b.Fatalf("create workflow: %v", err)
		}
	}
}

// BenchmarkCreateJob measures task creation throughput (serial).
// Includes the full transactional outbox write — this is the honest API cost.
func BenchmarkCreateJob(b *testing.B) {
	RequireRepositoryIntegration(b)
	contract := referenceContract(b)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		b.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                uuid.New(),
			WorkflowID:        workflowID,
			TaskType:          contract.TaskType,
			Payload:           json.RawMessage(`{"message":"benchmark"}`),
		}); err != nil {
			b.Fatalf("create job: %v", err)
		}
	}
}

// BenchmarkTryStartJob measures job claim throughput (serial).
// Reads back the outbox delivery_id to simulate what a real worker receives
// from Redis after the relay publishes the outbox row.
func BenchmarkTryStartJob(b *testing.B) {
	RequireRepositoryIntegration(b)
	contract := referenceContract(b)
	ctx := context.Background()
	workflow, _ := sharedRepo.CreateWorkflow(ctx, "bench-trystartjob")
	workflowID := workflow.ID
	type entry struct {
		taskID     uuid.UUID
		deliveryID uuid.UUID
	}
	entries := make([]entry, b.N)
	for i := 0; i < b.N; i++ {
		taskID := uuid.New()
		if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                taskID, WorkflowID: workflowID,
			TaskType: contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
		}); err != nil {
			b.Fatalf("create job: %v", err)
		}
		deliveryID, err := getDeliveryID(ctx, taskID)
		if err != nil {
			b.Fatalf("get delivery id: %v", err)
		}
		entries[i] = entry{taskID, deliveryID}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		claim, err := sharedRepo.TryStartJob(ctx, "queue:bench",
			entries[i].taskID.String(), entries[i].deliveryID.String(), 0, 30*time.Second)
		if err != nil {
			b.Fatalf("claim job: %v", err)
		}
		if claim.LeaseToken == uuid.Nil {
			b.Fatalf("expected valid lease token, got nil")
		}
	}
}

// BenchmarkCascadingCancellation measures MarkAsCancelledCascading
// with an 11-task chain (root + 10 children) per run.
func BenchmarkCascadingCancellation(b *testing.B) {
	RequireRepositoryIntegration(b)
	contract := referenceContract(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workflow, _ := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("cancel-bench-%d", i))
		workflowID := workflow.ID
		rootID := uuid.New()
		if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                rootID, WorkflowID: workflowID,
			TaskType: contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
		}); err != nil {
			b.Fatalf("create root: %v", err)
		}
		prevID := rootID
		for j := 0; j < 10; j++ {
			childID := uuid.New()
			if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
				ExecutionContract: contract,
				EffectKey:         uuid.New(),
				ID:                childID, WorkflowID: workflowID,
				ParentIDs: []uuid.UUID{prevID},
				TaskType:  contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
			}); err != nil {
				b.Fatalf("create child: %v", err)
			}
			prevID = childID
		}
		b.StartTimer()
		if err := sharedRepo.MarkAsCancelledCascading(ctx, "queue:bench", rootID.String()); err != nil {
			b.Fatalf("cascade cancel: %v", err)
		}
	}
}

// ── Parallel benchmarks ──────────────────────────────────────────────────────

// BenchmarkCreateJobParallel measures task creation under concurrent load.
// Uses b.RunParallel with GOMAXPROCS goroutines sharing the connection pool.
func BenchmarkCreateJobParallel(b *testing.B) {
	RequireRepositoryIntegration(b)
	contract := referenceContract(b)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob-parallel")
	if err != nil {
		b.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
				ExecutionContract: contract,
				EffectKey:         uuid.New(),
				ID:                uuid.New(),
				WorkflowID:        workflowID,
				TaskType:          contract.TaskType,
				Payload:           json.RawMessage(`{"message":"benchmark"}`),
			}); err != nil {
				b.Errorf("create job: %v", err)
				return
			}
		}
	})
}

// BenchmarkTryStartJobParallel measures job claim throughput under concurrent load.
// Pre-creates all tasks before the benchmark timer starts.
func BenchmarkTryStartJobParallel(b *testing.B) {
	RequireRepositoryIntegration(b)
	contract := referenceContract(b)
	ctx := context.Background()
	workflow, _ := sharedRepo.CreateWorkflow(ctx, "bench-trystartjob-parallel")
	workflowID := workflow.ID
	type entry struct {
		taskID     uuid.UUID
		deliveryID uuid.UUID
	}
	entries := make([]entry, b.N)
	for i := 0; i < b.N; i++ {
		taskID := uuid.New()
		if _, err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                taskID, WorkflowID: workflowID,
			TaskType: contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
		}); err != nil {
			b.Fatalf("create job: %v", err)
		}
		deliveryID, err := getDeliveryID(ctx, taskID)
		if err != nil {
			b.Fatalf("get delivery id: %v", err)
		}
		entries[i] = entry{taskID, deliveryID}
	}

	var idx atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1)) - 1
			if i >= len(entries) {
				return
			}
			claim, err := sharedRepo.TryStartJob(ctx, "queue:bench",
				entries[i].taskID.String(), entries[i].deliveryID.String(), 0, 30*time.Second)
			if err != nil {
				b.Errorf("claim job: %v", err)
				return
			}
			if claim.LeaseToken == uuid.Nil {
				b.Errorf("expected valid lease token, got nil")
				return
			}
		}
	})
}

// ── Latency test ─────────────────────────────────────────────────────────────

// TestEndToEndLatency measures p50/p95/p99 latency for the full creation path:
// CreateWorkflow + CreateJob (includes transactional outbox write).
// Includes a warmup phase to avoid cold-start skew.
func TestEndToEndLatency(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	if os.Getenv("RUN_LATENCY_BENCHMARK") != "1" {
		t.Skip("set RUN_LATENCY_BENCHMARK=1 to run the latency benchmark")
	}
	ctx := context.Background()
	const warmup = 20
	const runs = 5000

	// warmup — not measured
	for i := 0; i < warmup; i++ {
		workflow, _ := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("warmup-%d", i))
		wid := workflow.ID
		_, _ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                uuid.New(), WorkflowID: wid,
			TaskType: contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
		})
	}

	durations := make([]time.Duration, runs)
	for i := 0; i < runs; i++ {
		start := time.Now()
		workflow, err := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("latency-test-%d", i))
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		if _, err = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ExecutionContract: contract,
			EffectKey:         uuid.New(),
			ID:                uuid.New(),
			WorkflowID:        workflow.ID,
			TaskType:          contract.TaskType,
			Payload:           json.RawMessage(`{"message":"benchmark"}`),
		}); err != nil {
			t.Fatalf("create job: %v", err)
		}
		durations[i] = time.Since(start)
	}

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	avg := total / runs

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	t.Logf("Environment: Apple M3 Pro, local Docker Postgres 14, synchronous_commit=on, pool=20")
	t.Logf("CreateWorkflow+CreateJob latency over %d runs (after %d warmup):", runs, warmup)
	t.Logf("  avg: %v", avg)
	t.Logf("  p50: %v", durations[runs*50/100])
	t.Logf("  p95: %v", durations[runs*95/100])
	t.Logf("  p99: %v", durations[runs*99/100])
	t.Logf("  max: %v", durations[runs-1])
}

func TestRecoverIdleDeliveryRespectsDatabaseLease(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	ctx := context.Background()
	workflow, err := sharedRepo.CreateWorkflow(ctx, "recover-idle-delivery")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	workflowID := workflow.ID
	taskID := uuid.New()
	effectKey := uuid.New()
	if _, err := sharedRepo.CreateJob(ctx, "queue:recover", repository.Task{
		ExecutionContract: contract,
		EffectKey:         effectKey,
		ID:                taskID, WorkflowID: workflowID, TaskType: contract.TaskType, Payload: json.RawMessage(`{"message":"benchmark"}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	deliveryID, err := getDeliveryID(ctx, taskID)
	if err != nil {
		t.Fatalf("get delivery ID: %v", err)
	}
	if _, err := sharedRepo.TryStartJob(ctx, "queue:recover", taskID.String(), deliveryID.String(), 0, time.Minute); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	action, err := sharedRepo.RecoverIdleDelivery(ctx, "queue:recover", taskID.String(), 0)
	if err != nil {
		t.Fatalf("recover live lease: %v", err)
	}
	if action != repository.RecoveryKeepInflight {
		t.Fatalf("live lease action = %v, want keep inflight", action)
	}

	if _, err := sharedPool.Exec(ctx, `UPDATE processed_jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, taskID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	action, err = sharedRepo.RecoverIdleDelivery(ctx, "queue:recover", taskID.String(), 0)
	if err != nil {
		t.Fatalf("recover expired lease: %v", err)
	}
	if action != repository.RecoveryRequeue {
		t.Fatalf("expired lease action = %v, want requeue", action)
	}

	var recoveryDeliveries int
	if err := sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE task_id = $1 AND generation = 1`, taskID).Scan(&recoveryDeliveries); err != nil {
		t.Fatalf("count recovery deliveries: %v", err)
	}
	if recoveryDeliveries != 1 {
		t.Fatalf("recovery deliveries = %d, want 1", recoveryDeliveries)
	}

	stored, err := sharedRepo.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.EffectKey == nil || *stored.EffectKey != effectKey ||
		stored.ExecutionContract == nil || *stored.ExecutionContract != contract {
		t.Fatalf("recovery changed accepted metadata: %+v", stored)
	}
}
