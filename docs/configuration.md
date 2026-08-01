# Configuration

The application is configured with environment variables. Invalid values fall back to their defaults before startup validation.

## PostgreSQL

```env
POSTGRES_CONN_STRING=postgres://postgres:password@localhost:5432/task_queue?sslmode=disable
POSTGRES_MIGRATION_PATH=file://db/migrations
MAX_RETRIES=3
MAX_CONNS=10
MIN_CONNS=2
MAX_CONN_IDLE=5m
MAX_CONN_LIFETIME=1h
MAX_CONN_LIFETIME_JITTER=30s
```

`MAX_CONNS` is the per-process pool limit. `MIN_CONNS` keeps a small warm pool. Connection lifetime jitter prevents all connections from being replaced simultaneously.

## Redis Streams

```env
REDIS_ADDRESS=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
QUEUE_NAME=queue:video_processing
```

The application derives these keys from `QUEUE_NAME`:

- `<queue>:stream` — delivery stream
- `<queue>:workers` — consumer group
- `<queue>:dlq` — dead-letter stream

## Workers and recovery

```env
WORKER_COUNT=5
HOLD_TTL=20s
JANITOR_INTERVAL=5s
TASK_PRODUCER_INTERVAL=1s
SHUTDOWN_TIMEOUT=30s
```

`HOLD_TTL` is the PostgreSQL lease duration and Redis pending-entry idle threshold. Set it above the longest expected task duration; workers renew healthy leases every third of that duration.

## Observability

```env
METRICS_ADDR=:9090
```

The application exposes `/metrics` and `/health` on this address. Docker Compose publishes Grafana on port `3000` and Prometheus on port `9091`.

## Example

```env
REDIS_ADDRESS=localhost:6379
POSTGRES_CONN_STRING=postgres://postgres:password@localhost:5432/task_queue?sslmode=disable
MAX_CONNS=10
MIN_CONNS=2
MAX_CONN_IDLE=5m
MAX_CONN_LIFETIME=1h
MAX_CONN_LIFETIME_JITTER=30s
WORKER_COUNT=5
HOLD_TTL=20s
JANITOR_INTERVAL=5s
METRICS_ADDR=:9090
```
