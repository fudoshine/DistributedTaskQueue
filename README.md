# Distributed Task Queue

A production-ready task queue in Go with DAG workflow orchestration — tasks declare parent dependencies and only execute once all parents complete. Failures cascade: if a parent fails, all descendants are automatically cancelled. Built on Redis for queueing and PostgreSQL for durable state.

---

## How It Works

```
                      ┌─────────────────────────────┐
                      │        Your Workflow         │
                      │                              │
                      │   [Upload] ──▶ [Transcode]   │
                      │                    │         │
                      │                    ▼         │
                      │              [Thumbnail]     │
                      │                    │         │
                      │                    ▼         │
                      │               [Notify]       │
                      └─────────────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              ▼                      ▼                      ▼
       ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
       │   Producer  │       │    Redis    │       │   Worker    │
       │             │──────▶│    Queue    │──────▶│   (x N)     │
       │ Creates DAG │       │             │       │             │
       └─────────────┘       └─────────────┘       └──────┬──────┘
                                                          │
                                                          ▼
                                                  ┌─────────────┐
                                                  │  PostgreSQL │
                                                  │  Job State  │
                                                  │  + DAG Deps │
                                                  └─────────────┘
```

Tasks with unmet dependencies stay in Postgres as `PENDING`. A worker completing a parent automatically enqueues its children — you never manage execution order manually.

---

## Quick Start

```bash
# Start dependencies
docker-compose up -d

# Apply schema
psql "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable" < db/schema.sql

# Copy and configure env
cp .env.example .env

# Build and run
go build -o distributed-queue .
./distributed-queue
```

Check it's running:
```bash
curl http://localhost:9090/health
# {"status":"healthy","service":"distributed-queue"}
```

Expected output when running:
```
{"level":"INFO","msg":"worker thread started, waiting for tasks..."}
{"level":"INFO","msg":"producer thread started, generating tasks..."}
{"level":"INFO","msg":"metrics server starting","addr":":9090"}
{"level":"INFO","msg":"task enqueued","taskId":"a1b2c3...","counter":1}
{"level":"INFO","msg":"task consumed, running idempotency check","taskId":"a1b2c3..."}
{"level":"INFO","msg":"task acknowledged and synchronized everywhere","taskId":"a1b2c3..."}
```

---

## Usage

```go
// 1. Create a workflow
workflowID, _ := jobRepo.CreateWorkflow(ctx, "video-pipeline")

// 2. Define tasks — root tasks have no parents
upload := repository.Task{
    ID:         uuid.New(),
    WorkflowID: workflowID,
    ParentIDs:  []uuid.UUID{},           // root: runs immediately
    TaskType:   "VIDEO_UPLOAD",
    Payload:    json.RawMessage(`{"file":"video.mp4"}`),
}

// 3. Dependent tasks declare their parents
transcode := repository.Task{
    ID:         uuid.New(),
    WorkflowID: workflowID,
    ParentIDs:  []uuid.UUID{upload.ID},  // waits for upload
    TaskType:   "VIDEO_TRANSCODE",
    Payload:    json.RawMessage(`{"resolution":"1080p"}`),
}

// 4. Write to DB first (write-ahead log), then enqueue root to Redis
jobRepo.CreateJob(ctx, queueName, upload)
jobRepo.CreateJob(ctx, queueName, transcode)
store.EnqueueTask(ctx, queueName, upload.ID.String())

// transcode will be auto-enqueued once upload completes ✅
```

---

## Key Features

- **DAG Workflows** — multi-parent task dependencies via junction table
- **Idempotency** — CAS pattern prevents duplicate processing
- **Circuit Breaker** — fast-fail when database is down
- **Cascading Cancellation** — parent failure cancels all descendants
- **Graceful Shutdown** — waits for in-flight tasks before exit
- **Retry + DLQ** — exponential backoff, dead letter queue for permanent failures
- **Prometheus Metrics** — all critical paths instrumented

---

## Why Not Bull / Asynq / Temporal?

This was built from scratch to deeply understand distributed systems internals — idempotency, WAL patterns, CAS, cascading cancellations in recursive SQL, circuit breakers. Libraries abstract all of that away. See [docs/Tradeoff.md](docs/Tradeoff.md) for every design decision and what was traded off.

---

## Tech Stack

- **Go 1.25.6** — application runtime
- **Redis 7** — task queue and inflight tracking
- **PostgreSQL 14+** — durable job state and DAG dependencies
- **Prometheus** — metrics and observability

---

## Documentation

### Guides
| File | What it covers |
|------|----------------|
| [docs/quick-start.md](docs/quick-start.md) | Running the system, first workflow example |
| [docs/architecture.md](docs/architecture.md) | Components, data flow, design patterns |
| [docs/system-flows.md](docs/system-flows.md) | Step-by-step flow diagrams for every operation |
| [docs/configuration.md](docs/configuration.md) | All environment variables with tuning guidance |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Common issues and debugging strategies |

### Reference
| File | What it covers |
|------|----------------|
| [docs/Tradeoff.md](docs/Tradeoff.md) | Design tradeoffs and why decisions were made |
| [docs/METRICS_COMPLETE.md](docs/METRICS_COMPLETE.md) | All metrics with Grafana query examples |
| [docs/ATOMIC_DAG_ORCHESTRATION.md](docs/ATOMIC_DAG_ORCHESTRATION.md) | Write-ahead log and atomic child enqueue |
| [docs/GRACEFUL_SHUTDOWN.md](docs/GRACEFUL_SHUTDOWN.md) | Shutdown sequence and timeout behavior |

---

## Project Structure

```
.
├── main.go                        # Entry point, goroutine setup, graceful shutdown
├── db/
│   └── schema.sql                 # PostgreSQL schema
├── pkg/
│   ├── circuitbreaker/            # Circuit breaker implementation
│   ├── config/                    # Environment config loading and validation
│   ├── models/                    # Shared types
│   ├── redis/                     # Redis store and metrics
│   └── repository/                # PostgreSQL job and workflow queries
└── docs/                          # Documentation
```

---

## License

MIT
