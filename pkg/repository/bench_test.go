// Benchmarks for the repository layer.
//
// Environment: Apple M3 Pro, 18GB RAM, local Docker Postgres 14
// synchronous_commit = on (verified — numbers are not inflated by async commits)
//
// Run all benchmarks:
//
//	go test ./pkg/repository/ -bench=. -benchtime=5s -benchmem
//
// Run only latency test:
//
//	go test ./pkg/repository/ -run TestEndToEndLatency -v
package repository_test

import (
	"context"
	"distributed-queue/internal/domain"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
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
	cfg := config.PostgresConfig{
		ConnString:            "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable",
		MaxConns:              20, // higher for parallel benchmarks
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
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	log.SetOutput(io.Discard)
	code := m.Run()
	pool.Close()
	os.Exit(code)
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
	ctx := context.Background()
	workflowID, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob")
	if err != nil {
		b.Fatalf("create workflow: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID:         uuid.New(),
			WorkflowID: workflowID,
			TaskType:   "BENCH_TASK",
			Payload:    json.RawMessage(`{"file":"video.mp4"}`),
			MaxRetries: 3,
		}); err != nil {
			b.Fatalf("create job: %v", err)
		}
	}
}

// BenchmarkTryStartJob measures job claim throughput (serial).
// Reads back the outbox delivery_id to simulate what a real worker receives
// from Redis after the relay publishes the outbox row.
func BenchmarkTryStartJob(b *testing.B) {
	ctx := context.Background()
	workflowID, _ := sharedRepo.CreateWorkflow(ctx, "bench-trystartjob")

	// pre-create all tasks and collect their real delivery IDs
	type entry struct {
		taskID     uuid.UUID
		deliveryID uuid.UUID
	}
	entries := make([]entry, b.N)
	for i := 0; i < b.N; i++ {
		taskID := uuid.New()
		_ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID: taskID, WorkflowID: workflowID,
			TaskType: "BENCH", Payload: json.RawMessage(`{}`),
		})
		deliveryID, err := getDeliveryID(ctx, taskID)
		if err != nil {
			b.Fatalf("get delivery id: %v", err)
		}
		entries[i] = entry{taskID, deliveryID}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := sharedRepo.TryStartJob(ctx, "queue:bench",
			entries[i].taskID.String(), entries[i].deliveryID.String(), 0, 30*time.Second)
		if err != nil {
			b.Fatalf("claim job: %v", err)
		}
	}
}

// BenchmarkCascadingCancellation measures MarkAsCancelledCascading
// with a 10-node linear chain per run.
func BenchmarkCascadingCancellation(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workflowID, _ := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("cancel-bench-%d", i))
		rootID := uuid.New()
		_ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID: rootID, WorkflowID: workflowID,
			TaskType: "ROOT", Payload: json.RawMessage(`{}`),
		})
		prevID := rootID
		for j := 0; j < 10; j++ {
			childID := uuid.New()
			_ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
				ID: childID, WorkflowID: workflowID,
				ParentIDs: []uuid.UUID{prevID},
				TaskType:  "CHILD", Payload: json.RawMessage(`{}`),
			})
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
	ctx := context.Background()
	workflowID, err := sharedRepo.CreateWorkflow(ctx, "bench-createjob-parallel")
	if err != nil {
		b.Fatalf("create workflow: %v", err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
				ID:         uuid.New(),
				WorkflowID: workflowID,
				TaskType:   "BENCH_TASK",
				Payload:    json.RawMessage(`{"file":"video.mp4"}`),
				MaxRetries: 3,
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
	ctx := context.Background()
	workflowID, _ := sharedRepo.CreateWorkflow(ctx, "bench-trystartjob-parallel")

	type entry struct {
		taskID     uuid.UUID
		deliveryID uuid.UUID
	}
	entries := make([]entry, b.N)
	for i := 0; i < b.N; i++ {
		taskID := uuid.New()
		_ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID: taskID, WorkflowID: workflowID,
			TaskType: "BENCH", Payload: json.RawMessage(`{}`),
		})
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
			_, err := sharedRepo.TryStartJob(ctx, "queue:bench",
				entries[i].taskID.String(), entries[i].deliveryID.String(), 0, 30*time.Second)
			if err != nil {
				b.Errorf("claim job: %v", err)
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
	ctx := context.Background()
	const warmup = 20
	const runs = 5000

	// warmup — not measured
	for i := 0; i < warmup; i++ {
		wid, _ := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("warmup-%d", i))
		_ = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID: uuid.New(), WorkflowID: wid,
			TaskType: "WARMUP", Payload: json.RawMessage(`{}`),
		})
	}

	durations := make([]time.Duration, runs)
	for i := 0; i < runs; i++ {
		start := time.Now()
		workflowID, err := sharedRepo.CreateWorkflow(ctx, fmt.Sprintf("latency-test-%d", i))
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		if err = sharedRepo.CreateJob(ctx, "queue:bench", repository.Task{
			ID:         uuid.New(),
			WorkflowID: workflowID,
			TaskType:   "LATENCY_TEST",
			Payload:    json.RawMessage(`{}`),
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

	// sort for percentiles
	for i := 0; i < len(durations); i++ {
		for j := i + 1; j < len(durations); j++ {
			if durations[j] < durations[i] {
				durations[i], durations[j] = durations[j], durations[i]
			}
		}
	}

	t.Logf("Environment: local Docker Postgres 14, synchronous_commit=on")
	t.Logf("CreateWorkflow+CreateJob latency over %d runs (after %d warmup):", runs, warmup)
	t.Logf("  avg: %v", avg)
	t.Logf("  p50: %v", durations[runs*50/100])
	t.Logf("  p95: %v", durations[runs*95/100])
	t.Logf("  p99: %v", durations[runs*99/100])
	t.Logf("  max: %v", durations[runs-1])

	_ = domain.TaskStatusPending
}
