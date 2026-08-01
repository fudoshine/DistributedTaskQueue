# Troubleshooting

## Health checks

```bash
curl http://localhost:9090/health
curl -s http://localhost:9090/metrics | grep -E '^(queue_|pgxpool_)'
redis-cli PING
psql "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable" -c 'SELECT 1'
```

## Deliveries are not being processed

```bash
redis-cli XLEN queue:video_processing:stream
redis-cli XPENDING queue:video_processing:stream queue:video_processing:workers
redis-cli XINFO GROUPS queue:video_processing:stream
```

Check that the application log contains `stream worker started`. A growing stream with no consumers usually means the worker is down; a growing pending count means a worker has read entries but has not acknowledged them.

## Pending entries do not clear

```bash
redis-cli XPENDING queue:video_processing:stream queue:video_processing:workers - + 10
```

An entry may remain pending while its worker is healthy. After `HOLD_TTL`, the janitor uses `XAUTOCLAIM` and checks the PostgreSQL lease before recovery. Do not delete pending entries manually: first inspect the job state.

```sql
SELECT id, status, lease_expires_at, attempt_number, delivery_generation
FROM processed_jobs
WHERE id = '<task-id>';
```

## Jobs are retrying or terminally failing

```sql
SELECT id, task_type, status, current_retries, max_retries, error_log
FROM processed_jobs
WHERE status IN ('QUEUED', 'RUNNING', 'FAILED')
ORDER BY created_at DESC
LIMIT 20;
```

Retryable failures create a new outbox generation. Terminal failures are appended to `<queue>:dlq` and cancel downstream jobs.

```bash
redis-cli XREVRANGE queue:video_processing:dlq + - COUNT 10
```

## PostgreSQL pool saturation

```bash
curl -s http://localhost:9090/metrics | grep '^pgxpool_'
```

If `pgxpool_acquired_conns` stays near `pgxpool_max_conns`, increase `MAX_CONNS` only after confirming PostgreSQL has capacity. Keep `MIN_CONNS` no greater than `MAX_CONNS` and restart the app after changing Compose or environment values.

## Trace one task

```bash
task_id='<task-uuid>'
redis-cli XREVRANGE queue:video_processing:stream + - COUNT 100 | grep "$task_id"
psql "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable" \
  -c "SELECT id, status, current_retries, lease_expires_at FROM processed_jobs WHERE id='$task_id'"
```

Use the PostgreSQL row as the authoritative state. Stream deliveries can be duplicated or stale by design.
