# Troubleshooting

Common issues, debugging strategies, and solutions.

---

## Quick Diagnosis

### **System Health Check**

Run this checklist first:

```bash
# 1. Check if services are running
docker ps | grep -E "redis|postgres"

# 2. Check application health
curl http://localhost:9090/health

# 3. Check metrics
curl http://localhost:9090/metrics | grep -E "queue_tasks|circuit_breaker"

# 4. Check Redis connectivity
redis-cli PING

# 5. Check PostgreSQL connectivity
psql "postgres://postgres:password@localhost:5432/task_queue" -c "SELECT 1"

# 6. Check logs for errors
tail -100 logs.txt | grep -i error
```

---

## Common Issues

### **Issue 1: Tasks Not Processing**

**Symptoms:**
- Metrics show `TasksEnqueued` increasing but `TasksConsumed` flat
- Redis queue has items: `redis-cli LLEN queue:video_processing` > 0
- No worker activity in logs

**Diagnosis:**

```bash
# Check worker count
ps aux | grep distributed-queue | wc -l

# Check if workers are blocked
curl http://localhost:9090/metrics | grep workers_active

# Check database connection
psql "postgres://..." -c "SELECT COUNT(*) FROM pg_stat_activity"
```

**Possible Causes:**

#### **A. Circuit Breaker is Open**

```bash
# Check circuit breaker state
curl http://localhost:9090/metrics | grep circuit_breaker_state
```

**Solution:**
```bash
# Wait for reset timeout (default 30s)
# Check database is healthy
psql "postgres://..." -c "SELECT 1"

# If DB is healthy, circuit will close automatically
# Watch logs: "circuit breaker transitioning to HALF_OPEN"
```

#### **B. Database Connection Pool Exhausted**

```bash
# Check active connections
psql "postgres://..." -c "SELECT COUNT(*) FROM pg_stat_activity WHERE datname='task_queue'"
```

**Solution:**
```bash
# Increase DB_MAX_CONNS
export DB_MAX_CONNS=20
./distributed-queue

# Or restart with new config
```

#### **C. Workers Not Started**

**Solution:**
```bash
# Check WORKER_COUNT in .env
grep WORKER_COUNT .env

# Should be > 0
# Restart if needed
```

---

### **Issue 2: High Retry Rate**

**Symptoms:**
- Metrics show `TasksRetried` increasing rapidly
- `ProcessingFailures` counter high
- Delayed queue has many items

**Diagnosis:**

```bash
# Check delayed queue size
redis-cli ZCARD queue:video_processing:delayed

# Check failed tasks
psql "postgres://..." << EOF
SELECT task_type, error_log, COUNT(*) 
FROM processed_jobs 
WHERE status='FAILED' 
GROUP BY task_type, error_log
ORDER BY COUNT(*) DESC
LIMIT 10;
EOF

# Check retry distribution
psql "postgres://..." << EOF
SELECT current_retries, COUNT(*) 
FROM processed_jobs 
WHERE current_retries > 0
GROUP BY current_retries
ORDER BY current_retries;
EOF
```

**Possible Causes:**

#### **A. External Service Down**

**Solution:**
```bash
# Check external service health
curl http://external-api.com/health

# If down, pause producers until service recovers
# Tasks will retry automatically with exponential backoff
```

#### **B. Task Timeout Too Short**

**Solution:**
```bash
# Increase processing timeout in code
# Or optimize task processing logic
```

#### **C. Resource Exhaustion**

```bash
# Check system resources
top
df -h
free -m

# Check database performance
psql "postgres://..." -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10"
```

---

### **Issue 3: Tasks Stuck in Inflight**

**Symptoms:**
- Redis `:inflight` list has items that never clear
- Tasks not progressing
- Janitor not recovering them

**Diagnosis:**

```bash
# Check inflight tasks
redis-cli LRANGE queue:video_processing:inflight 0 -1

# Check timestamps
redis-cli HGETALL queue:video_processing:timestamps

# Check when task was added
redis-cli HGET queue:video_processing:timestamps <task-id>
```

**Possible Causes:**

#### **A. Worker Crashed Mid-Processing**

**Solution:**
```bash
# Janitor should recover after JANITOR_HOLD_TTL
# Check if janitor is running
tail -f logs.txt | grep janitor

# If not, ensure background jobs are enabled
# Wait for janitor sweep (default 5s interval)
```

#### **B. Janitor Hold TTL Too High**

```bash
# Check current setting
grep JANITOR_HOLD_TTL .env

# If too high, reduce it
export JANITOR_HOLD_TTL=20s
./distributed-queue
```

#### **C. Task Processing Hanging**

**Solution:**
```bash
# Add timeouts to task processing logic
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()

err := simulateProcessing(ctx, taskID)
```

---

### **Issue 4: Workflow Never Completes**

**Symptoms:**
- Some tasks in workflow show `COMPLETED`
- Workflow status remains `RUNNING`
- Child tasks never start

**Diagnosis:**

```bash
# Get workflow status
psql "postgres://..." << EOF
SELECT id, name, status, created_at 
FROM workflows 
WHERE status='RUNNING' 
ORDER BY created_at DESC 
LIMIT 10;
EOF

# Check tasks in workflow
psql "postgres://..." << EOF
SELECT id, task_type, status, current_retries
FROM processed_jobs
WHERE workflow_id='<workflow-id>'
ORDER BY created_at;
EOF

# Check dependencies
psql "postgres://..." << EOF
SELECT jd.child_job_id, jd.parent_job_id, pj.status as parent_status
FROM job_dependencies jd
JOIN processed_jobs pj ON jd.parent_job_id = pj.id
WHERE jd.child_job_id IN (
    SELECT id FROM processed_jobs WHERE workflow_id='<workflow-id>'
);
EOF
```

**Possible Causes:**

#### **A. Parent Task Stuck in PENDING**

**Solution:**
```bash
# Check if parent has ALL its parents completed
# If not, check grandparents

# Check if parent is in queue
redis-cli LRANGE queue:video_processing 0 -1 | grep <parent-id>

# If not in queue and not completed, manually enqueue:
redis-cli LPUSH queue:video_processing <parent-id>
```

#### **B. Cascading Cancellation Triggered**

```bash
# Check for CANCELLED tasks
psql "postgres://..." << EOF
SELECT id, task_type, status 
FROM processed_jobs 
WHERE workflow_id='<workflow-id>' AND status='CANCELLED';
EOF
```

**Solution:**
- Check which parent failed
- Fix the issue
- Re-create workflow if needed

#### **C. Circular Dependency**

```bash
# Check for cycles
psql "postgres://..." << EOF
WITH RECURSIVE dependency_chain AS (
    SELECT child_job_id, parent_job_id, 1 as depth
    FROM job_dependencies
    WHERE child_job_id = '<task-id>'
    UNION ALL
    SELECT jd.child_job_id, jd.parent_job_id, dc.depth + 1
    FROM job_dependencies jd
    JOIN dependency_chain dc ON jd.child_job_id = dc.parent_job_id
    WHERE dc.depth < 10
)
SELECT * FROM dependency_chain;
EOF
```

**Solution:**
- Remove circular dependency
- Redesign DAG structure

---

### **Issue 5: Dead Letter Queue Growing**

**Symptoms:**
- DLQ has many tasks: `redis-cli LLEN queue:video_processing:dlq`
- Metrics show `TasksDeadLettered` increasing
- Same task types failing repeatedly

**Diagnosis:**

```bash
# Check DLQ contents
redis-cli LRANGE queue:video_processing:dlq 0 -1

# Check failed task details
psql "postgres://..." << EOF
SELECT id, task_type, error_log, current_retries
FROM processed_jobs
WHERE id IN (
    -- Paste DLQ task IDs here
);
EOF

# Check error patterns
psql "postgres://..." << EOF
SELECT error_log, COUNT(*)
FROM processed_jobs
WHERE status='FAILED'
GROUP BY error_log
ORDER BY COUNT(*) DESC;
EOF
```

**Possible Causes:**

#### **A. Invalid Payload**

**Solution:**
```bash
# Check payload structure
psql "postgres://..." << EOF
SELECT id, payload 
FROM processed_jobs 
WHERE id IN (SELECT task_id FROM dlq)
LIMIT 5;
EOF

# Fix payload format in task creation logic
# Manually fix and reprocess:
redis-cli RPOPLPUSH queue:video_processing:dlq queue:video_processing
```

#### **B. Persistent External Failure**

**Solution:**
- Fix external service
- Drain DLQ: `redis-cli DEL queue:video_processing:dlq`
- Or reprocess: `redis-cli RPOPLPUSH queue:dlq queue:main`

---

### **Issue 6: High Memory Usage**

**Symptoms:**
- Application using excessive memory
- OOM errors in logs
- System slowdown

**Diagnosis:**

```bash
# Check process memory
ps aux | grep distributed-queue

# Check Go heap
curl http://localhost:9090/debug/pprof/heap > heap.prof

# Check Redis memory
redis-cli INFO memory

# Check database cache
psql "postgres://..." -c "SELECT * FROM pg_stat_database WHERE datname='task_queue'"
```

**Possible Causes:**

#### **A. Too Many Workers**

**Solution:**
```bash
# Reduce WORKER_COUNT
export WORKER_COUNT=5
./distributed-queue
```

#### **B. Connection Pool Too Large**

**Solution:**
```bash
# Reduce pool sizes
export DB_MAX_CONNS=10
export REDIS_POOL_SIZE=10
./distributed-queue
```

#### **C. Redis Memory Exhaustion**

```bash
# Check Redis maxmemory
redis-cli CONFIG GET maxmemory

# Set eviction policy
redis-cli CONFIG SET maxmemory-policy allkeys-lru
redis-cli CONFIG SET maxmemory 1gb
```

---

## Debugging Strategies

### **Enable Debug Logging**

```bash
export LOG_LEVEL=debug
./distributed-queue 2>&1 | tee debug.log
```

### **Trace a Specific Task**

```bash
# Get task ID from logs
task_id="<task-uuid>"

# Check Redis presence
redis-cli LPOS queue:video_processing $task_id
redis-cli LPOS queue:video_processing:inflight $task_id

# Check database state
psql "postgres://..." << EOF
SELECT * FROM processed_jobs WHERE id='$task_id';
EOF

# Check dependencies
psql "postgres://..." << EOF
SELECT * FROM job_dependencies 
WHERE child_job_id='$task_id' OR parent_job_id='$task_id';
EOF
```

### **Monitor in Real-Time**

```bash
# Watch queue depth
watch -n 1 'redis-cli LLEN queue:video_processing'

# Watch metrics
watch -n 1 'curl -s http://localhost:9090/metrics | grep -E "tasks_enqueued|tasks_consumed|tasks_acknowledged"'

# Watch database activity
watch -n 2 'psql "postgres://..." -c "SELECT status, COUNT(*) FROM processed_jobs GROUP BY status"'
```

### **Replay Failed Tasks**

```bash
# Get failed task IDs
failed_ids=$(psql "postgres://..." -t -c "SELECT id FROM processed_jobs WHERE status='FAILED'")

# Reset status and retry count
for id in $failed_ids; do
    psql "postgres://..." -c "UPDATE processed_jobs SET status='PENDING', current_retries=0 WHERE id='$id'"
    redis-cli LPUSH queue:video_processing $id
done
```

---

## Performance Issues

### **High Latency**

**Check:**
```bash
# Database query times
psql "postgres://..." -c "SELECT query, mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10"

# Redis latency
redis-cli --latency

# Network latency
ping redis-host
ping postgres-host
```

**Solutions:**
- Add database indexes
- Use Redis pipelining
- Co-locate services

### **Low Throughput**

**Check:**
```bash
# Worker utilization
curl http://localhost:9090/metrics | grep workers_active

# Queue depth trend
redis-cli LLEN queue:video_processing
```

**Solutions:**
- Increase `WORKER_COUNT`
- Optimize task processing logic
- Partition work across multiple queues

---

## Emergency Procedures

### **Stop All Processing**

```bash
# Graceful shutdown
pkill -TERM distributed-queue

# Force kill (last resort)
pkill -KILL distributed-queue
```

### **Drain Queue**

```bash
# Move all tasks to backup
redis-cli RENAME queue:video_processing queue:video_processing:backup

# Or delete
redis-cli DEL queue:video_processing
```

### **Reset System**

```bash
# Stop workers
pkill distributed-queue

# Clear Redis
redis-cli FLUSHDB

# Reset database
psql "postgres://..." << EOF
TRUNCATE processed_jobs, job_dependencies, workflows CASCADE;
EOF

# Restart
./distributed-queue
```

---

## Getting Help

### **Collect Diagnostic Info**

```bash
# Create diagnostic report
cat > diagnostics.txt << EOF
=== System Info ===
$(uname -a)
$(go version)

=== Process Info ===
$(ps aux | grep distributed-queue)

=== Redis Info ===
$(redis-cli INFO | head -20)

=== Database Info ===
$(psql "postgres://..." -c "\dt")
$(psql "postgres://..." -c "SELECT status, COUNT(*) FROM processed_jobs GROUP BY status")

=== Metrics ===
$(curl -s http://localhost:9090/metrics | grep queue_)

=== Recent Errors ===
$(tail -100 logs.txt | grep -i error)
EOF
```

### **Report an Issue**

Include:
1. Diagnostic report above
2. Configuration (sanitize passwords!)
3. Steps to reproduce
4. Expected vs actual behavior
5. Relevant logs

---

## Next Steps

- **Optimize performance** → [Configuration](configuration.md)
- **Understand metrics** → [Metrics](METRICS_COMPLETE.md)
- **Review configuration** → [Configuration](configuration.md)

---

[← Back: System Flows](system-flows.md) | [← Back to README](../README.md)
