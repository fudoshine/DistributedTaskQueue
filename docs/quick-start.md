# Quick Start

Get the distributed queue running in 5 minutes!

---

## Prerequisites

- Go 1.21 or higher
- Docker & Docker Compose
- Git

---

## Step 1: Clone and Setup (1 minute)

```bash
# Clone repository
git clone https://github.com/yourusername/distributed-queue.git
cd distributed-queue

# Copy environment file
cp .env.example .env
```

---

## Step 2: Start Dependencies (2 minutes)

```bash
# Start Redis and PostgreSQL
docker-compose up -d

# Wait for services to be ready
sleep 5

# Apply database schema
psql "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable" < db/schema.sql
```

---

## Step 3: Build and Run (1 minute)

```bash
# Build the application
go build -o distributed-queue .

# Run the queue
./distributed-queue
```

You should see:
```json
{"level":"info","msg":"connected to Redis","addr":"localhost:6379"}
{"level":"info","msg":"successfully connected to postgres database"}
{"level":"info","msg":"worker thread started, waiting for tasks..."}
{"level":"info","msg":"producer thread started, generating tasks..."}
{"level":"info","msg":"metrics server starting","addr":":9090"}
```

---

## Step 4: Verify It's Working (1 minute)

### **Check Metrics**
```bash
curl http://localhost:9090/metrics | grep queue_tasks
```

Expected output:
```
queue_tasks_enqueued_total{queue="queue:video_processing"} 10
queue_tasks_consumed_total{queue="queue:video_processing"} 10
queue_tasks_acknowledged_total{queue="queue:video_processing"} 8
```

### **Check Health**
```bash
curl http://localhost:9090/health
```

Expected output:
```json
{"status":"healthy","service":"distributed-queue"}
```

### **Check Logs**
```bash
tail -f logs.txt | grep "task enqueued"
```

---

## Step 5: Create Your First Workflow

Create a file `example.go`:

```go
package main

import (
    "context"
    "distributed-queue/pkg/repository"
    "log"
    "github.com/google/uuid"
)

func main() {
    ctx := context.Background()
    
    // Connect to database
    pgPool, _ := repository.NewPostgresPool(ctx, repository.PostgresConfig{
        ConnString: "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable",
    })
    jobRepo := repository.NewJobRepository(pgPool)
    
    // Create workflow
    workflowID, _ := jobRepo.CreateWorkflow(ctx, "video-pipeline")
    log.Printf("Created workflow: %s", workflowID)
    
    // Create Task A (Upload)
    taskA := repository.Task{
        ID:         uuid.New(),
        WorkflowID: workflowID,
        ParentIDs:  []uuid.UUID{},
        TaskType:   "VIDEO_UPLOAD",
        Payload:    []byte(`{"file": "video.mp4"}`),
    }
    jobRepo.CreateJob(ctx, "queue:video_processing", taskA)
    
    // Create Task B (Transcode) - depends on A
    taskB := repository.Task{
        ID:         uuid.New(),
        WorkflowID: workflowID,
        ParentIDs:  []uuid.UUID{taskA.ID},
        TaskType:   "VIDEO_TRANSCODE",
        Payload:    []byte(`{"resolution": "1080p"}`),
    }
    jobRepo.CreateJob(ctx, "queue:video_processing", taskB)
    
    log.Println("Tasks created! Task B will run after Task A completes")
}
```

Run it:
```bash
go run example.go
```

Watch the logs to see Task A complete, then Task B automatically start!

---

## What Just Happened?

1. **Producer** creates tasks every second
2. **Worker** consumes and processes them
3. **PostgreSQL** tracks task state
4. **Redis** queues tasks for processing
5. **Metrics** exposed at :9090
6. **Janitor** recovers abandoned tasks

---

## Common Issues

### Redis Connection Failed
```bash
# Check if Redis is running
docker ps | grep redis

# Restart if needed
docker-compose restart redis
```

### Database Schema Missing
```bash
# Reapply schema
psql "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable" < db/schema.sql
```

### Port Already in Use
```bash
# Change METRICS_ADDR in .env
echo "METRICS_ADDR=:9091" >> .env
```

---

## Next Steps

- **Understand the architecture** → [Architecture](architecture.md)
- **Configure for production** → [Configuration](configuration.md)
- **Add monitoring** → [Metrics](METRICS_COMPLETE.md)
- **Deploy it** → [System Flows](system-flows.md)

---

## Stopping the System

```bash
# Graceful shutdown (waits for tasks to finish)
Ctrl+C

# Stop dependencies
docker-compose down
```

---

[← Back to README](../README.md) | [Next: Architecture →](architecture.md)
