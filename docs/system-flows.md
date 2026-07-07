# System Flows

Complete flow diagrams showing how the system works end-to-end.

---

## Overview

This chapter provides detailed flow diagrams for every major operation in the system. Use these to:

- **Debug issues** - Trace where a task is in its lifecycle
- **Understand timing** - See when each component is invoked
- **Design new features** - Follow existing patterns
- **Onboard new developers** - Visual system documentation

---

## Available Flows

### **1. Complete System Flow Diagrams**

For comprehensive flow diagrams covering all aspects of the system, see:

📄 **[SYSTEM_FLOW_DIAGRAMS.md](SYSTEM_FLOW_DIAGRAMS.md)**

This document includes:
- System architecture overview
- Workflow & task creation flow
- Worker processing flow (with circuit breaker)
- Background job flows (Janitor, Delayed Poller)
- DAG workflow example with timeline
- Graceful shutdown sequence
- Method call chains reference

---

## Quick Reference

### **Task Creation**
```
Producer Thread
 └─ CreateWorkflow()      → PostgreSQL: INSERT workflows
 └─ CreateJob()           → PostgreSQL: INSERT processed_jobs
 └─ EnqueueTask()         → Redis: LPUSH queue
```

### **Task Processing**
```
Worker Thread
 └─ ConsumeTask()         → Redis: BLMOVE queue → :inflight
 └─ TryStartJob()         → PostgreSQL: UPDATE status='RUNNING'
 └─ simulateProcessing()  → (Business logic)
 └─ CheckAndEnqueueChildren() → PostgreSQL: SELECT ready children
 └─ EnqueueTask(children) → Redis: LPUSH queue
 └─ CompleteJob()         → PostgreSQL: UPDATE status='COMPLETED'
 └─ UpdateWorkflowStatus() → PostgreSQL: UPDATE workflows
 └─ AcknowledgeTask()     → Redis: LREM :inflight
```

### **Failure Handling**
```
Worker Thread (on error)
 └─ RecordFailure()       → PostgreSQL: UPDATE retries++
 └─ IF retries < max:
     └─ RetryTaskWithBackoff() → Redis: ZADD :delayed
 └─ ELSE:
     └─ MarkAsCancelledCascading() → PostgreSQL: UPDATE descendants
     └─ Move to DLQ → Redis: LPUSH :dlq
```

---

## State Transitions

### **Task States**

```
  PENDING
     │
     ├─ Worker consumes
     ▼
  RUNNING
     │
     ├─────────────┬─────────────┐
     │             │             │
  Success      Retryable     Max Retries
     │          Failure       Exceeded
     ▼             │             │
 COMPLETED        │             ▼
                  │          FAILED
                  │             │
                  │             ▼
                  │      CANCELLED (cascade)
                  │
                  └─ Retry with backoff
                       │
                       ▼
                   PENDING (retry)
```

### **Workflow States**

```
  PENDING
     │
     ├─ First task starts
     ▼
  RUNNING
     │
     ├─────────────┬─────────────┐
     │             │             │
  All tasks   Any task      All tasks
  complete    failed       complete
     │             │        (none failed)
     ▼             ▼             │
  COMPLETED    FAILED            │
                                 ▼
                            COMPLETED
```

---

## Circuit Breaker Flow

### **State Machine**

```
┌─────────────────────────────────────────────────┐
│                  CLOSED                         │
│  (Normal operation - all DB calls allowed)      │
│                                                 │
│  On each DB call:                               │
│    - If success: reset failure count            │
│    - If error: increment failure count          │
└─────────────────┬───────────────────────────────┘
                  │
                  │ failures >= 5
                  ▼
┌─────────────────────────────────────────────────┐
│                   OPEN                          │
│  (Failing fast - block all DB calls)            │
│                                                 │
│  Return: ErrCircuitOpen immediately             │
│  Start timer: 30 seconds                        │
└─────────────────┬───────────────────────────────┘
                  │
                  │ after 30s
                  ▼
┌─────────────────────────────────────────────────┐
│                HALF-OPEN                        │
│  (Testing recovery - allow ONE probe call)      │
│                                                 │
│  On next call:                                  │
│    - If success: → CLOSED                       │
│    - If error: → OPEN                           │
└─────────────────────────────────────────────────┘
```

### **Usage in Code**

```go
// Wrap DB call with circuit breaker
cbErr := dbBreaker.Call(func() error {
    status, tryErr = jobRepo.TryStartJob(ctx, queueName, taskId)
    return tryErr
})

// Handle circuit breaker states
switch cbErr {
case nil:
    // Success - continue processing
case circuitbreaker.ErrCircuitOpen:
    // DB is down - fail fast without timeout
    slog.Warn("circuit breaker open, skipping task")
    return
default:
    // Actual DB error - record failure
    slog.Error("db error", "error", cbErr)
}
```

---

## DAG Execution Timeline

### **Example: Video Processing Pipeline**

```
Workflow: video-pipeline

Tasks:
  A: Upload
  B: Transcode (depends on A)
  C: Thumbnail (depends on B)

Timeline:

T=0s   │ Producer creates workflow
       │ INSERT workflows (status=PENDING)
       │
T=1s   │ Producer creates Task A, B, C
       │ INSERT processed_jobs (A: status=PENDING, parents=[])
       │ INSERT processed_jobs (B: status=PENDING, parents=[A])
       │ INSERT processed_jobs (C: status=PENDING, parents=[B])
       │ INSERT job_dependencies (B→A, C→B)
       │
T=2s   │ Producer enqueues ONLY Task A (root)
       │ LPUSH queue A
       │
T=3s   │ Worker-1 consumes A
       │ BLMOVE queue → :inflight (A)
       │ UPDATE processed_jobs (A: status=RUNNING)
       │
T=5s   │ Worker-1 finishes processing A
       │ SELECT children WHERE ALL parents COMPLETED
       │   → Result: [B] (A is now COMPLETED)
       │ LPUSH queue B
       │ UPDATE processed_jobs (A: status=COMPLETED)
       │
T=6s   │ Worker-2 consumes B
       │ BLMOVE queue → :inflight (B)
       │ UPDATE processed_jobs (B: status=RUNNING)
       │
T=8s   │ Worker-2 finishes processing B
       │ SELECT children WHERE ALL parents COMPLETED
       │   → Result: [C] (B is now COMPLETED)
       │ LPUSH queue C
       │ UPDATE processed_jobs (B: status=COMPLETED)
       │
T=9s   │ Worker-3 consumes C
       │ BLMOVE queue → :inflight (C)
       │ UPDATE processed_jobs (C: status=RUNNING)
       │
T=11s  │ Worker-3 finishes processing C
       │ SELECT children WHERE ALL parents COMPLETED
       │   → Result: [] (no children)
       │ UPDATE processed_jobs (C: status=COMPLETED)
       │ UPDATE workflows (status=COMPLETED)
       │
       │ 🎉 Workflow complete!
```

---

## Retry Flow with Exponential Backoff

```
Task fails (retries=0)
    │
    ├─ RecordFailure()
    │   UPDATE current_retries = 1
    │
    ├─ Calculate delay: 2^1 = 2 seconds
    │
    └─ ZADD :delayed (now+2s) taskID
        │
        │ ... 2 seconds pass ...
        │
        ├─ DelayedPoller finds task
        │   ZRANGEBYSCORE :delayed 0 now
        │
        ├─ Move back to queue
        │   ZREM :delayed taskID
        │   LPUSH queue taskID
        │
        ├─ Worker consumes again (retries=1)
        │
        └─ IF fails again:
            │
            ├─ RecordFailure()
            │   UPDATE current_retries = 2
            │
            ├─ Calculate delay: 2^2 = 4 seconds
            │
            └─ ZADD :delayed (now+4s) taskID
                │
                │ ... continues until max_retries ...
```

---

## Cascading Cancellation Flow

```
Task B fails (max retries exceeded)
    │
    ├─ RecordFailure()
    │   UPDATE processed_jobs (B: status=FAILED)
    │
    └─ MarkAsCancelledCascading(B)
        │
        ├─ PostgreSQL Recursive Query:
        │   WITH RECURSIVE descendants AS (
        │     SELECT child_job_id FROM job_dependencies
        │     WHERE parent_job_id = B
        │     UNION
        │     SELECT jd.child_job_id
        │     FROM job_dependencies jd
        │     JOIN descendants d ON jd.parent_job_id = d.child_job_id
        │   )
        │   UPDATE processed_jobs
        │   SET status = 'CANCELLED'
        │   WHERE id IN (SELECT * FROM descendants);
        │
        ├─ Result: C, D, E all set to CANCELLED
        │
        └─ UpdateWorkflowStatus()
            UPDATE workflows (status=FAILED)
```

---

## Janitor Recovery Flow

```
Worker crashes while processing Task X
    │
    ├─ Task left in :inflight (never acknowledged)
    │
    ├─ Timestamp recorded: HSET :timestamps X 1625097600
    │
    │ ... 25 seconds pass (> HoldTTL) ...
    │
    ├─ Janitor runs (every 5s)
    │
    ├─ Check: now - 1625097600 = 25s > 20s
    │
    ├─ Recovery:
    │   MULTI
    │     LREM :inflight 1 X
    │     HDEL :timestamps X
    │     LPUSH queue X
    │   EXEC
    │
    ├─ Metric: TasksSwept.Inc()
    │
    └─ Another worker picks up Task X
        (Idempotency check prevents duplicate processing)
```

---

## Shutdown Flow

```
User sends SIGTERM (Ctrl+C)
    │
    ├─ Signal caught by signal.NotifyContext
    │
    ├─ Context cancelled: ctx.Done() closes
    │
    ├─ All goroutines notified:
    │   ├─ Worker: checks ctx.Err() → exits after current task
    │   ├─ Producer: checks ctx.Err() → exits
    │   ├─ Janitor: checks ctx.Err() → exits
    │   ├─ DelayedPoller: checks ctx.Err() → exits
    │   └─ DepthCollector: checks ctx.Err() → exits
    │
    ├─ Main thread waits with timeout:
    │   SELECT:
    │     CASE <-done:
    │       "all threads exited gracefully" ✅
    │     CASE <-time.After(30s):
    │       "shutdown timeout, forcing exit" ⚠️
    │
    ├─ Cleanup:
    │   ├─ pgPool.Close()
    │   └─ store.Close()
    │
    └─ EXIT
```

---

## Complete Method Call Chains

### **Creating a Workflow and Task**

```
main.go: producerThread()
  │
  ├─ jobRepo.CreateWorkflow(ctx, name)
  │   │
  │   └─ jobRepo.go: CreateWorkflow()
  │       │
  │       └─ pgPool.Exec()
  │           SQL: INSERT INTO workflows (id, name, status)
  │           VALUES ($1, $2, 'PENDING')
  │
  ├─ jobRepo.CreateJob(ctx, queueName, task)
  │   │
  │   └─ jobRepo.go: CreateJob()
  │       │
  │       ├─ pgPool.Exec()
  │       │   SQL: INSERT INTO processed_jobs (...)
  │       │   VALUES ($1, $2, $3, ...)
  │       │
  │       └─ FOR EACH parent:
  │           pgPool.Exec()
  │             SQL: INSERT INTO job_dependencies
  │             VALUES (child_id, parent_id)
  │
  └─ store.EnqueueTask(ctx, queueName, taskID)
      │
      └─ store.go: EnqueueTask()
          │
          └─ rdb.LPush(ctx, queueName, taskID)
              Redis: LPUSH queue:video_processing taskID
```

### **Processing a Task**

```
main.go: workerThread()
  │
  ├─ store.ConsumeTask(ctx, queueName)
  │   │
  │   └─ store.go: ConsumeTask()
  │       Redis: BLMOVE queue → :inflight
  │
  ├─ dbBreaker.Call(jobRepo.TryStartJob)
  │   │
  │   ├─ circuitbreaker.go: Call()
  │   │   └─ Check state: CLOSED/OPEN/HALF_OPEN
  │   │
  │   └─ jobRepo.go: TryStartJob()
  │       SQL: UPDATE processed_jobs
  │       SET status = CASE
  │         WHEN status='COMPLETED' THEN 'COMPLETED'
  │         ELSE 'RUNNING'
  │       END
  │
  ├─ simulateProcessing(ctx, taskID)
  │   └─ time.Sleep(2 * time.Second)
  │
  ├─ jobRepo.CheckAndEnqueueChildren(ctx, queueName, taskID)
  │   │
  │   ├─ jobRepo.go: CheckAndEnqueueChildren()
  │   │   SQL: SELECT child_job_id WHERE ALL parents COMPLETED
  │   │
  │   └─ FOR EACH child:
  │       store.EnqueueTask(ctx, queueName, childID)
  │
  ├─ dbBreaker.Call(jobRepo.CompleteJob)
  │   │
  │   └─ jobRepo.go: CompleteJob()
  │       SQL: UPDATE processed_jobs
  │       SET status='COMPLETED', processed_at=NOW()
  │
  ├─ jobRepo.UpdateWorkflowStatus(ctx, workflowID)
  │   │
  │   └─ jobRepo.go: UpdateWorkflowStatus()
  │       SQL: UPDATE workflows SET status = (computed)
  │
  └─ store.AcknowledgeTask(ctx, queueName, taskID)
      │
      └─ store.go: AcknowledgeTask()
          Redis: MULTI
            LREM :inflight 1 taskID
            HDEL :timestamps taskID
            HDEL :retries taskID
          EXEC
```

---

## Debugging with Flows

### **Task Not Processing?**

Check the flow:
1. Is task in Redis queue? → `redis-cli LRANGE queue:name 0 -1`
2. Is task in DB? → `SELECT * FROM processed_jobs WHERE id=?`
3. Is task in :inflight? → `redis-cli LRANGE queue:name:inflight 0 -1`
4. Check logs for circuit breaker state
5. Check metrics: `TasksConsumed` vs `TasksEnqueued`

### **Workflow Stuck?**

Check the flow:
1. What's the workflow status? → `SELECT * FROM workflows WHERE id=?`
2. Which tasks are incomplete? → `SELECT * FROM processed_jobs WHERE workflow_id=? AND status NOT IN ('COMPLETED', 'CANCELLED')`
3. Check dependencies: → `SELECT * FROM job_dependencies WHERE child_job_id=?`
4. Are parent tasks complete? → Check parent statuses

### **High Retry Rate?**

Check the flow:
1. Check delayed queue: → `redis-cli ZRANGE queue:name:delayed 0 -1 WITHSCORES`
2. Check retry counts: → `SELECT id, current_retries FROM processed_jobs WHERE current_retries > 0`
3. Check error logs: → `SELECT error_log FROM processed_jobs WHERE status='FAILED'`
4. Check metrics: `ProcessingFailures`, `TasksRetried`

---

## Next Steps

- **Understand architecture** → [Architecture](architecture.md)
- **Learn task lifecycle** → [Configuration](configuration.md)
- **Troubleshoot issues** → [Troubleshooting](troubleshooting.md)

---

[← Back: Architecture](architecture.md) | [Next: Troubleshooting →](troubleshooting.md)
