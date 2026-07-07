# Architecture

Deep dive into the system design and components.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DISTRIBUTED QUEUE SYSTEM                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐          │
│  │ Producer │─────▶│  Redis   │◀─────│  Worker  │          │
│  │  Thread  │      │  Queue   │      │  Threads │          │
│  └──────────┘      └────┬─────┘      └─────┬────┘          │
│                         │                   │                │
│                    (Fast Queue)        (Processors)          │
│                         │                   │                │
│                         ▼                   ▼                │
│                    ┌─────────────────────────────┐          │
│                    │      PostgreSQL             │          │
│                    │  - Job State                │          │
│                    │  - DAG Dependencies         │          │
│                    │  - Workflow Tracking        │          │
│                    └─────────────────────────────┘          │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Background Jobs                             │  │
│  │  - Janitor (abandoned task recovery)                 │  │
│  │  - Delayed Poller (retry scheduler)                  │  │
│  │  - Depth Collector (queue depth metrics)            │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Observability                               │  │
│  │  - Prometheus Metrics (:9090/metrics)                │  │
│  │  - Health Endpoint (:9090/health)                    │  │
│  │  - Structured Logging (slog)                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. **Producer Thread**

**Purpose:** Creates tasks and enqueues them to Redis

**Key Responsibilities:**
- Create workflows in PostgreSQL
- Generate tasks with dependencies
- Write task metadata to database (write-ahead log)
- Enqueue tasks to Redis queue

**Code Location:** `main.go` - `producerThread()`

**Example:**
```go
// Every second:
1. workflowID := jobRepo.CreateWorkflow(ctx, "video-pipeline")
2. taskID := uuid.New()
3. jobRepo.CreateJob(ctx, queueName, task)  // DB write first!
4. store.EnqueueTask(ctx, queueName, taskID) // Redis enqueue second
```

---

### 2. **Redis Queue**

**Purpose:** Fast, ephemeral task queue

**Data Structures:**
```
queue:video_processing            → LIST (main queue)
queue:video_processing:inflight   → LIST (being processed)
queue:video_processing:delayed    → ZSET (retry backoff)
queue:video_processing:dlq        → LIST (dead letters)
queue:video_processing:timestamps → HASH (processing start time)
queue:video_processing:retries    → HASH (retry count)
```

**Why Redis?**
- Sub-millisecond latency
- Atomic operations (BLMOVE, ZADD)
- Pub/Sub for real-time updates
- Persistence options (AOF/RDB)

---

### 3. **Worker Threads**

**Purpose:** Consume and process tasks

**Processing Flow:**
```go
1. ConsumeTask()           // BLMOVE from queue → :inflight
2. TryStartJob()           // Check DB: can we process this?
3. simulateProcessing()    // Do the work
4. CheckAndEnqueueChildren() // Enqueue dependent tasks
5. CompleteJob()           // Mark as COMPLETED in DB
6. UpdateWorkflowStatus()  // Update workflow state
7. AcknowledgeTask()       // Remove from :inflight
```

**Idempotency Protection:**
```sql
UPDATE processed_jobs
SET status = CASE
  WHEN status = 'COMPLETED' THEN 'COMPLETED'  -- Already done, skip
  ELSE 'RUNNING'                              -- Claim it
END
WHERE id = $1
RETURNING status;
```

**Code Location:** `main.go` - `workerThread()`

---

### 4. **PostgreSQL Database**

**Purpose:** Durable job state and DAG dependencies

**Schema:**

```sql
-- Workflow tracking
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,  -- PENDING, RUNNING, COMPLETED, FAILED
    created_at TIMESTAMP DEFAULT NOW()
);

-- Job state
CREATE TABLE processed_jobs (
    id UUID PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id),
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,  -- PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    current_retries INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    payload JSONB,
    error_log TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

-- DAG dependencies (junction table for multi-parent support)
CREATE TABLE job_dependencies (
    child_job_id UUID REFERENCES processed_jobs(id),
    parent_job_id UUID REFERENCES processed_jobs(id),
    PRIMARY KEY (child_job_id, parent_job_id)
);
```

**Why PostgreSQL?**
- ACID transactions
- Complex queries (recursive CTEs for cascading)
- Strong consistency
- Proven reliability

---

### 5. **Circuit Breaker**

**Purpose:** Protect against cascading database failures

**State Machine:**
```
        ┌─────────┐
        │ CLOSED  │  (Normal operation)
        │ (Allow) │
        └────┬────┘
             │
        5 failures
             │
             ▼
        ┌─────────┐
        │  OPEN   │  (Fast-fail, skip DB)
        │ (Block) │
        └────┬────┘
             │
        30 seconds
             │
             ▼
        ┌──────────┐
        │ HALF-OPEN│  (Test if recovered)
        │  (Probe) │
        └────┬─────┘
             │
      ┌──────┴──────┐
Success │           │ Failure
        │           │
        ▼           ▼
    CLOSED       OPEN
```

**Usage:**
```go
cbErr := dbBreaker.Call(func() error {
    status, tryErr = jobRepo.TryStartJob(ctx, queueName, taskId)
    return tryErr
})

if cbErr == circuitbreaker.ErrCircuitOpen {
    // DB is down, fail fast
    slog.Warn("circuit breaker open, skipping DB call")
}
```

**Code Location:** `pkg/circuitbreaker/circuitbreaker.go`

---

### 6. **Background Jobs**

#### **Janitor (Abandoned Task Recovery)**

**Purpose:** Recover tasks held too long in `:inflight`

**Logic:**
```go
FOR EACH task in :timestamps:
    IF (now - timestamp) > 20 seconds:
        MOVE task from :inflight → main queue
        DELETE from :timestamps
        Metric: TasksSwept.Inc()
```

**Runs:** Every 5 seconds

#### **Delayed Poller (Retry Scheduler)**

**Purpose:** Move delayed tasks back to main queue

**Logic:**
```go
tasks := ZRANGEBYSCORE :delayed 0 now
FOR EACH task:
    IF ZREM :delayed task == 1:  // CAS: only one poller claims it
        LPUSH main_queue task
```

**Runs:** Every 2 seconds

#### **Depth Collector (Metrics)**

**Purpose:** Track queue depth for monitoring

**Logic:**
```go
depth := LLEN main_queue
QueueDepth.Set(float64(depth))
```

**Runs:** Every 5 seconds

**Code Location:** `main.go` - background job functions

---

## Data Flow

### **Task Creation Flow**

```
Producer
    │
    ├─ 1. CreateWorkflow()
    │      └─ INSERT INTO workflows
    │
    ├─ 2. CreateJob(task)
    │      ├─ INSERT INTO processed_jobs
    │      └─ INSERT INTO job_dependencies (if has parents)
    │
    └─ 3. EnqueueTask(taskID)
           └─ LPUSH queue taskID
```

### **Task Processing Flow**

```
Worker
    │
    ├─ 1. ConsumeTask()
    │      └─ BLMOVE queue → :inflight
    │
    ├─ 2. TryStartJob() (with Circuit Breaker)
    │      └─ UPDATE processed_jobs SET status='RUNNING'
    │
    ├─ 3. simulateProcessing()
    │      └─ Do the actual work
    │
    ├─ 4. CheckAndEnqueueChildren()
    │      ├─ SELECT children WHERE ALL parents COMPLETED
    │      └─ LPUSH queue childID (for each)
    │
    ├─ 5. CompleteJob() (with Circuit Breaker)
    │      └─ UPDATE processed_jobs SET status='COMPLETED'
    │
    ├─ 6. UpdateWorkflowStatus()
    │      └─ UPDATE workflows SET status (based on all jobs)
    │
    └─ 7. AcknowledgeTask()
           └─ LREM :inflight, HDEL :timestamps
```

### **Failure Handling Flow**

```
Worker (on error)
    │
    ├─ 1. RecordFailure() (with Circuit Breaker)
    │      └─ UPDATE processed_jobs SET current_retries++
    │
    ├─ 2. IF retries < max_retries:
    │      └─ RetryTaskWithBackoff()
    │         └─ ZADD :delayed (now + 2^retries) taskID
    │
    └─ 3. ELSE (max retries exceeded):
           ├─ MarkAsCancelledCascading()
           │  └─ UPDATE ALL descendants SET status='CANCELLED'
           ├─ Move to DLQ
           │  └─ LPUSH :dlq taskID
           └─ UpdateWorkflowStatus()
```

---

## Design Patterns

### **Write-Ahead Log (WAL)**

**Problem:** What if Redis enqueue succeeds but DB write fails?

**Solution:** Always write to DB first (source of truth)

```go
// ✅ Correct order
jobRepo.CreateJob(ctx, queueName, task)    // DB write first
store.EnqueueTask(ctx, queueName, taskID)  // Redis enqueue second

// ❌ Wrong order (can create orphans)
store.EnqueueTask(ctx, queueName, taskID)  // Redis first
jobRepo.CreateJob(ctx, queueName, task)    // DB second (might fail!)
```

### **Atomic DAG Orchestration**

**Problem:** Race condition between marking parent complete and enqueuing children

**Solution:** Enqueue children BEFORE marking parent complete

```go
// ✅ Correct order (atomic)
1. CheckAndEnqueueChildren()  // Enqueue first
2. CompleteJob()              // Mark complete second

// ❌ Wrong order (race condition)
1. CompleteJob()              // Mark complete first
2. CheckAndEnqueueChildren()  // Child might see inconsistent state
```

### **Compare-And-Swap (CAS) for Idempotency**

**Problem:** Multiple workers might consume the same task

**Solution:** Atomic state transition in database

```sql
-- Only ONE worker will set status to RUNNING
UPDATE processed_jobs
SET status = CASE
  WHEN status = 'PENDING' THEN 'RUNNING'
  ELSE status
END
WHERE id = $1
RETURNING status;
```

---

## Scalability

### **Horizontal Scaling**

**Workers:**
```bash
# Add more workers
WORKER_COUNT=10 ./distributed-queue
```

**Multiple Processes:**
```bash
# Run on different machines
./distributed-queue &  # Machine 1
./distributed-queue &  # Machine 2
./distributed-queue &  # Machine 3
```

**Kubernetes:**
```bash
kubectl scale deployment queue-worker --replicas=10
```

### **Partitioning**

**Queue Partitioning:**
```go
// Different queues for different task types
queue:video_processing
queue:email_sending
queue:data_pipeline
```

**Database Sharding:**
```sql
-- Partition by workflow_id
CREATE TABLE processed_jobs_shard1 PARTITION OF processed_jobs
FOR VALUES FROM (0) TO (1000000);
```

---

## Reliability Features

### **1. Idempotency**
- Task processed exactly once even if retried
- CAS pattern in database

### **2. Circuit Breaker**
- Protects against database overload
- Fast-fail when DB is down

### **3. Graceful Shutdown**
- Waits for in-flight tasks to complete
- Configurable timeout (default 30s)

### **4. Janitor Recovery**
- Recovers abandoned tasks
- Handles worker crashes

### **5. Dead Letter Queue**
- Captures permanently failed tasks
- Manual investigation and replay

---

## Performance Characteristics

### **Throughput**

**Single Worker:**
- ~500 tasks/second (simple tasks)
- ~50 tasks/second (complex tasks with DB writes)

**10 Workers:**
- ~3,000 tasks/second (near-linear scaling)

### **Latency**

- **Enqueue latency:** < 1ms (Redis LPUSH)
- **Processing latency:** 2-5s (simulated work)
- **DAG child propagation:** < 10ms (SQL query + Redis enqueue)

### **Resource Usage**

**Per Worker:**
- Memory: ~50MB
- CPU: 10-20% (during processing)
- Network: ~1MB/s

---

## Security Considerations

### **Database**
- Use connection pooling (prevent exhaustion)
- Parameterized queries (prevent SQL injection)
- Read-only replicas for metrics queries

### **Redis**
- Enable password authentication
- Use TLS for connections
- Set maxmemory policy (evict old tasks)

### **Observability**
- Metrics endpoint doesn't expose sensitive data
- Structured logs for audit trail
- Health checks for load balancers

---

## Next Steps

- **Understand task lifecycle** → [System Flows](system-flows.md)
- **Learn DAG orchestration** → [DAG Orchestration](ATOMIC_DAG_ORCHESTRATION.md)
- **See complete flows** → [System Flows](system-flows.md)

---

[← Back: Quick Start](quick-start.md) | [Next: System Flows →](system-flows.md)
