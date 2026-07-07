# Configuration

Complete guide to configuring the distributed task queue system.

---

## Configuration Overview

All configuration is done through **environment variables**, making it easy to deploy across different environments (development, staging, production).

---

## Environment Variables

### **Database Configuration**

```bash
# PostgreSQL connection
DB_HOST=localhost                    # Database host
DB_PORT=5432                        # Database port
DB_USER=postgres                    # Database username
DB_PASSWORD=password                # Database password
DB_NAME=task_queue                  # Database name
DB_MAX_CONNS=10                     # Max connection pool size
DB_MAX_IDLE_CONNS=5                 # Max idle connections
DB_MAX_CONN_LIFETIME=300s           # Max connection lifetime
```

**Tuning Guidance:**
- `DB_MAX_CONNS`: Workers × 2 (each worker uses ~2 connections)
- `DB_MAX_IDLE_CONNS`: 50% of `DB_MAX_CONNS`
- `DB_MAX_CONN_LIFETIME`: 5-10 minutes to refresh connections

---

### **Redis Configuration**

```bash
# Redis connection
REDIS_ADDR=localhost:6379           # Redis address (host:port)
REDIS_PASSWORD=                     # Redis password (empty for none)
REDIS_DB=0                          # Redis database number
REDIS_MAX_RETRIES=5                 # Max retry attempts for Redis ops
REDIS_MIN_RETRY_BACKOFF=1s          # Min backoff between retries
REDIS_MAX_RETRY_BACKOFF=16s         # Max backoff between retries
REDIS_DIAL_TIMEOUT=5s               # Connection timeout
REDIS_READ_TIMEOUT=3s               # Read operation timeout
REDIS_WRITE_TIMEOUT=3s              # Write operation timeout
REDIS_POOL_SIZE=10                  # Connection pool size
REDIS_MIN_IDLE_CONNS=5              # Min idle connections
```

**Tuning Guidance:**
- `REDIS_POOL_SIZE`: Workers + 5 (extra for background jobs)
- `REDIS_MIN_IDLE_CONNS`: 50% of pool size for low latency
- Use timeouts to prevent hanging operations

---

### **Worker Configuration**

```bash
# Worker behavior
WORKER_COUNT=5                      # Number of concurrent workers
MAX_RETRIES=3                       # Max retries before moving to DLQ
TASK_PRODUCER_INTERVAL=1s           # Producer task generation interval
QUEUE_NAME=queue:video_processing   # Main queue name
```

**Tuning Guidance:**
- `WORKER_COUNT`: Start with CPU cores × 2
- `MAX_RETRIES`: 3-5 for transient errors, 1 for idempotent ops
- `TASK_PRODUCER_INTERVAL`: Adjust based on load (faster = more tasks)

---

### **Circuit Breaker Configuration**

```bash
# Circuit breaker settings
CB_MAX_FAILURES=5                   # Failures before opening circuit
CB_RESET_TIMEOUT=30s                # Time to wait before half-open state
```

**Tuning Guidance:**
- `CB_MAX_FAILURES`: 5-10 (too low = false positives)
- `CB_RESET_TIMEOUT`: 30-60s (give DB time to recover)

---

### **Background Jobs Configuration**

```bash
# Janitor settings
JANITOR_INTERVAL=5s                 # How often janitor runs
JANITOR_HOLD_TTL=20s                # Max time before sweeping task

# Delayed poller settings
DELAYED_POLLER_INTERVAL=2s          # How often to check delayed queue

# Depth collector settings
DEPTH_COLLECTOR_INTERVAL=5s         # How often to collect queue depth
```

**Tuning Guidance:**
- `JANITOR_HOLD_TTL`: 2× expected max task processing time
- `DELAYED_POLLER_INTERVAL`: Balance between retry latency and Redis load
- `DEPTH_COLLECTOR_INTERVAL`: Match your metrics scrape interval

---

### **Graceful Shutdown Configuration**

```bash
# Shutdown behavior
SHUTDOWN_TIMEOUT=30s                # Max time to wait for workers to finish
```

**Tuning Guidance:**
- `SHUTDOWN_TIMEOUT`: 2× max task processing time
- Too short = abandoned tasks (janitor recovers them)
- Too long = slow deployments

---

### **Metrics & Observability**

```bash
# Metrics server
METRICS_ADDR=:9090                  # Prometheus metrics endpoint
METRICS_PATH=/metrics               # Metrics HTTP path
HEALTH_PATH=/health                 # Health check path

# Logging
LOG_LEVEL=info                      # Log level: debug, info, warn, error
LOG_FORMAT=json                     # Log format: json, text
```

**Tuning Guidance:**
- `LOG_LEVEL`: Use `debug` for troubleshooting, `info` for production
- `LOG_FORMAT`: Use `json` for log aggregators (Datadog, ELK)

---

## Configuration File

### **.env.example**

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=task_queue
DB_MAX_CONNS=10
DB_MAX_IDLE_CONNS=5
DB_MAX_CONN_LIFETIME=300s

# Redis
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_MAX_RETRIES=5
REDIS_MIN_RETRY_BACKOFF=1s
REDIS_MAX_RETRY_BACKOFF=16s
REDIS_DIAL_TIMEOUT=5s
REDIS_READ_TIMEOUT=3s
REDIS_WRITE_TIMEOUT=3s
REDIS_POOL_SIZE=10
REDIS_MIN_IDLE_CONNS=5

# Workers
WORKER_COUNT=5
MAX_RETRIES=3
TASK_PRODUCER_INTERVAL=1s
QUEUE_NAME=queue:video_processing

# Circuit Breaker
CB_MAX_FAILURES=5
CB_RESET_TIMEOUT=30s

# Background Jobs
JANITOR_INTERVAL=5s
JANITOR_HOLD_TTL=20s
DELAYED_POLLER_INTERVAL=2s
DEPTH_COLLECTOR_INTERVAL=5s

# Shutdown
SHUTDOWN_TIMEOUT=30s

# Metrics & Logging
METRICS_ADDR=:9090
METRICS_PATH=/metrics
HEALTH_PATH=/health
LOG_LEVEL=info
LOG_FORMAT=json
```

---

## Environment-Specific Configurations

### **Development**

```bash
# .env.development
WORKER_COUNT=2                      # Fewer workers for local dev
LOG_LEVEL=debug                     # Verbose logging
LOG_FORMAT=text                     # Human-readable logs
DB_MAX_CONNS=5                      # Smaller pool
TASK_PRODUCER_INTERVAL=2s           # Slower task generation
```

### **Staging**

```bash
# .env.staging
WORKER_COUNT=5
LOG_LEVEL=info
LOG_FORMAT=json
DB_MAX_CONNS=20
REDIS_POOL_SIZE=15
SHUTDOWN_TIMEOUT=60s                # More time for long-running tasks
```

### **Production**

```bash
# .env.production
WORKER_COUNT=10                     # More workers
LOG_LEVEL=warn                      # Less logging
LOG_FORMAT=json                     # Structured logs
DB_MAX_CONNS=50                     # Larger pool
REDIS_POOL_SIZE=30
SHUTDOWN_TIMEOUT=120s               # Extra time for safe shutdown
CB_MAX_FAILURES=10                  # More tolerant
JANITOR_HOLD_TTL=60s                # Longer TTL for complex tasks
```

---

## Configuration Validation

The system validates configuration on startup:

```go
func (c *Config) Validate() error {
    if c.Worker.Count <= 0 {
        return fmt.Errorf("WORKER_COUNT must be positive")
    }
    if c.Worker.MaxRetries < 0 {
        return fmt.Errorf("MAX_RETRIES must be non-negative")
    }
    if c.CircuitBreaker.MaxFailures <= 0 {
        return fmt.Errorf("CB_MAX_FAILURES must be positive")
    }
    // ... more validations
}
```

Startup will fail with clear error message if config is invalid.

---

## Loading Configuration

### **From .env File**

```bash
# Copy example
cp .env.example .env

# Edit values
vim .env

# Run (automatically loads .env)
./distributed-queue
```

### **From Environment Variables**

```bash
# Set variables
export WORKER_COUNT=10
export MAX_RETRIES=5

# Run
./distributed-queue
```

### **Docker Compose**

```yaml
services:
  queue-worker:
    image: distributed-queue:latest
    env_file:
      - .env.production
    environment:
      - WORKER_COUNT=8  # Override specific values
```

### **Kubernetes ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: queue-config
data:
  WORKER_COUNT: "10"
  MAX_RETRIES: "3"
  LOG_LEVEL: "info"
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: queue-worker
        envFrom:
        - configMapRef:
            name: queue-config
```

---

## Tuning for Different Workloads

### **High-Throughput (Many Small Tasks)**

```bash
WORKER_COUNT=20                     # More workers
DB_MAX_CONNS=50                     # Larger DB pool
REDIS_POOL_SIZE=30                  # Larger Redis pool
JANITOR_HOLD_TTL=10s                # Shorter TTL (tasks are fast)
SHUTDOWN_TIMEOUT=30s                # Tasks finish quickly
```

### **Heavy Processing (Few Large Tasks)**

```bash
WORKER_COUNT=5                      # Fewer workers (CPU-bound)
DB_MAX_CONNS=15                     # Smaller pool
JANITOR_HOLD_TTL=120s               # Longer TTL (tasks are slow)
SHUTDOWN_TIMEOUT=300s               # Give tasks time to finish
MAX_RETRIES=1                       # Don't retry expensive tasks
```

### **DAG-Heavy (Complex Dependencies)**

```bash
WORKER_COUNT=10                     # Moderate workers
DB_MAX_CONNS=30                     # Larger pool (more queries)
DELAYED_POLLER_INTERVAL=1s          # Faster child propagation
DEPTH_COLLECTOR_INTERVAL=2s         # Monitor depth closely
```

### **Batch Processing (Scheduled Jobs)**

```bash
WORKER_COUNT=15                     # Burst capacity
JANITOR_HOLD_TTL=60s                # Longer TTL
TASK_PRODUCER_INTERVAL=10s          # Less frequent production
SHUTDOWN_TIMEOUT=180s               # Wait for batch to finish
```

---

## Dynamic Configuration (Future Enhancement)

For hot-reloading without restart:

```go
// Watch config file
watcher, _ := fsnotify.NewWatcher()
watcher.Add(".env")

go func() {
    for {
        select {
        case event := <-watcher.Events:
            if event.Op&fsnotify.Write == fsnotify.Write {
                // Reload config
                cfg = LoadConfig()
                // Apply new values
                updateWorkerCount(cfg.Worker.Count)
            }
        }
    }
}()
```

---

## Configuration Best Practices

### **Security**

✅ **DO:**
- Use environment variables for secrets
- Store `.env` files outside version control
- Use secret management tools (Vault, AWS Secrets Manager)
- Rotate database passwords regularly

❌ **DON'T:**
- Commit `.env` to git
- Hardcode passwords in code
- Share production credentials

### **Reliability**

✅ **DO:**
- Set timeouts for all external calls
- Use connection pooling
- Configure circuit breakers
- Set graceful shutdown timeout

❌ **DON'T:**
- Use unlimited connections
- Disable retries completely
- Set timeouts too low

### **Performance**

✅ **DO:**
- Scale workers with workload
- Monitor and adjust pool sizes
- Use appropriate janitor TTL
- Test configuration under load

❌ **DON'T:**
- Over-provision workers (context switching overhead)
- Set pool sizes too large (resource exhaustion)
- Use same config for all environments

---

## Monitoring Configuration

Track these metrics to tune configuration:

```promql
# Worker utilization
queue_workers_active / queue_workers_total

# Connection pool usage
db_connections_in_use / db_connections_max

# Circuit breaker state
circuit_breaker_state{state="open"}

# Queue depth trend
rate(queue_depth[5m])
```

---

## Next Steps

- **Monitor the system** → [Metrics](METRICS_COMPLETE.md)
- **Optimize performance** → [Troubleshooting](troubleshooting.md)
- **Deploy to production** → [System Flows](system-flows.md)

---

[← Back: Architecture](architecture.md) | [Next: Troubleshooting →](troubleshooting.md)
