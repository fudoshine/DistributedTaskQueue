# GoQueue — Agent Context

> Read this file first, before doing anything else in this repo.
> At the end of every session, update the relevant sections below with what changed, what was decided, and what's still open. Keep entries short and dated — this file is a working log, not a polished doc.

---

## Project Summary

GoQueue is a distributed task queue built from primitives in Go, using PostgreSQL (durability / outbox / DAG metadata) and Redis (delivery via Lists + ZSETs). Core architecture: transactional outbox pattern, DAG orchestration via junction table (`job_dependencies`), circuit breaker on all DB calls, exponential backoff on Redis errors, startup recovery sweep, recursive CTE for cascading cancellation. Prometheus metrics wired up for live observability via Grafana.

**Module:** `distributed-queue`
**Go version:** 1.25.6
**Key dependencies:** `pgx/v5`, `go-redis/v9`, `golang-migrate/v4`, `prometheus/client_golang`

---

## Architecture

```
REST API → outbox table (atomic tx) → Outbox Relay → Redis Queue → Workers → PostgreSQL
```

- **Task service** validates the catalog task type and payload before the repository writes `processed_jobs` + `outbox` in one transaction. Workers do not generate demo jobs.
- **Outbox Relay** polls `outbox WHERE delivered=FALSE` every 2s, enqueues to Redis, marks delivered.
- **Workers** consume from Redis, claim job in DB (CAS), process, enqueue children, mark complete.
- **Janitor** sweeps tasks stuck in `:inflight` beyond `HOLD_TTL` back to main queue.
- **Delayed Poller** moves retry tasks from `:delayed` ZSET back to main queue when due.
- **Circuit Breaker** wraps all DB calls — opens after 5 failures, resets after 30s.

### Redis Key Layout
```
queue:video_processing            → LIST  (main queue)
queue:video_processing:inflight   → LIST  (being processed)
queue:video_processing:delayed    → ZSET  (retry backoff)
queue:video_processing:dlq        → LIST  (dead letters)
queue:video_processing:timestamps → HASH  (inflight start times)
queue:video_processing:retries    → HASH  (retry counts)
```

### Database Schema
```
workflows           — workflow lifecycle tracking
processed_jobs      — job state (PENDING→RUNNING→COMPLETED/FAILED/CANCELLED)
job_dependencies    — DAG edges (child_job_id, parent_job_id) — junction table
outbox              — transactional outbox (task_id, queue_name, delivered)
```

Migration files in `db/migrations/` — applied automatically on startup via `golang-migrate`.

---

## Current State (last updated: 2026-10-26)

### From Kiro sessions (Jul 6 – Aug 3):

- **Documentation**: Full docs in `docs/` — `architecture.md`, `quick-start.md`, `configuration.md`, `system-flows.md`, `troubleshooting.md`, `Tradeoff.md`, `METRICS_COMPLETE.md`, `ATOMIC_DAG_ORCHESTRATION.md`, `GRACEFUL_SHUTDOWN.md`, `SYSTEM_FLOW_DIAGRAMS.md`. Docs folder is NOT gitignored (was previously, fixed).
- **Docker**: Full stack in `docker-compose.yml` at root — Postgres, Redis, app, Prometheus, Grafana. Old `pkg/deploy/` removed. Schema no longer mounted into `initdb.d` — migrations handle it.
- **Migrations**: `golang-migrate` added. 6 migration files (000001–000006). `RunMigrations()` called in `main()` before pool opens. Migrations path: `file://db/migrations`.
- **Task submission**: The API service validates catalog task types and payloads. The automatic demo producer was removed in S1-01.
- **Outbox pattern**: `CreateJob` writes `processed_jobs` + `outbox` row atomically. `runOutboxRelay` handles Redis enqueue.
- **Orphan reconciler**: Removed — replaced by outbox relay which is the correct fix.
- **README**: Rewritten — has ASCII diagram, real usage example with actual API, "why not X" section, correct Go version (1.25.6), docs table split into Guides/Reference.
- **`.gitignore`**: `docs/METRICS_GUIDE.md` and `docs/SYSTEM_FLOW_DIAGRAMS.md` gitignored (not referenced in README). `docs/` folder itself is tracked.
- **`pkg/repository/postgres.go`**: `NewPostgresPool` now accepts full `config.PostgresConfig` and wires pool settings (`MaxConns`, `MinConns`, `MaxConnLifetime`, `MaxConnIdle`, `MaxConnLifetimeJitter`). Registers `PgxPoolCollector` for Prometheus.

### From Codex sessions (Jul 26 – Aug 1):

- **Postgres pool**: `MaxConns=10`, `MinConns=2-4`. Confirmed non-bottleneck via live metrics — `acquired_conns=0`, `idle_conns=7`, acquire time ~2.8µs.
- **Worker concurrency**: Appears capped at 5 (`queue_inflight_depth` pinned at exactly 5 while queue depth grew 8,794→13,954). Primary bottleneck — not investigated yet.
- **Processing failure rate**: Reproducible ~20% across restarts (19.9% and 19.3%). Deterministic cause suspected — not yet root-caused.
- **Cascade cancel**: Fixed `CYCLE` syntax error. Now uses manual path-array pattern (`WHERE NOT x = ANY(d.path)`) — PG 8.4+ compatible, no version gate.
- **Migration 000006**: `durable_delivery_outbox` — latest schema change from Codex session.
- **Migration 000005**: `add_job_leases` — leases table added.
- **Migration 000004**: `job_claim_state` — claim state tracking.
- **Migration 000003**: `outbox_task_unique` — unique constraint on outbox.

---

## Open Threads

- [ ] **Root-cause the ~20% `video_processing` failure rate.** Pull actual error payloads from DLQ (51 jobs) and bucket by error type. The precision of the repeat rate across restarts means there's one dominant cause, not random failure.
- [ ] **Raise worker concurrency above 5** and verify `queue_inflight_depth` climbs and `queue_depth` growth slows. Re-check `pgxpool` acquire metrics — still have headroom at `MaxConns=10`.
- [ ] **Confirm cascade-cancel atomicity** — verify child jobs can't be claimed between parent failure and cascade UPDATE completing (possible race window).
- [ ] **Add pgxpool Grafana panels** — queries and alert rules already drafted in this file, not yet wired into dashboard JSON.
- [ ] **Test API-submitted DAGs end-to-end** — verify workflow completion and child scheduling through the submission API.

---

## Decisions Made (don't re-litigate)

- **Outbox over orphan reconciler**: Orphan reconciler (scan PENDING rows + Redis LPOS) was O(N) on queue size. Replaced with transactional outbox — O(1) lookup on `delivered=FALSE` index.
- **WAL ordering**: DB write always before Redis enqueue. Outbox relay is the only path to Redis for new tasks. Workers enqueue children directly (not via outbox) since child enqueue is part of the completion transaction.
- **AND semantics for DAG**: Any parent failure cancels ALL descendants. Not OR semantics.
- **Junction table for DAG**: `job_dependencies(child_job_id, parent_job_id)` — supports multi-parent tasks.
- **Row-Level Security (RLS)**: Not using. Single-tenant, one binary owns all writes.
- **Circuit breaker config**: 5 failures → OPEN, 30s → HALF_OPEN probe.
- **Graceful shutdown**: 30s timeout (`SHUTDOWN_TIMEOUT`), waits for in-flight tasks.
- **`CYCLE` clause**: Don't use PG14 `CYCLE col SET is_cycle USING path` syntax. Use manual path-array + `WHERE NOT x = ANY(path)` — portable since PG 8.4.

---

## Gotchas / Things That Bit Us

- **`//go:embed` can't use `../` paths** — embed directive must stay within the module subtree. Switched from `iofs` embed to `file://` source for migrations.
- **`CYCLE` clause in recursive CTEs** requires all three parts (`CYCLE col SET is_cycle_col USING path_col`). Also PG14+ only. Prefer manual path-array cycle detection.
- **Docker Hub rate limits** — 100/6hr anonymous, 200/6hr authenticated. If `docker pull` times out with IPv6 error, wait 60-90s and retry before touching daemon config.
- **`go.sum` missing entries** after adding `golang-migrate` — run `go mod tidy` to resolve.
- **macOS linker warning** (`malformed LC_DYSYMTAB`) — harmless, from Go toolchain vs Xcode symbol table mismatch. Build still succeeds (exit 0). Suppress with `-ldflags="-w"` if needed.

---

## Useful Prometheus Queries

```promql
# Queue health
queue_depth{queue="queue:video_processing"}
queue_inflight_depth{queue="queue:video_processing"}
rate(queue_tasks_acknowledged_total[1m])
rate(queue_processing_failures_total[1m])

# DAG activity
rate(queue_children_enqueued_total[1m])
rate(queue_tasks_cancelled_cascade_total[1m])
rate(queue_workflows_completed_total[1m])

# Outbox relay
rate(queue_outbox_relayed_total[1m])

# pgxpool
goqueue_pgpool_acquired_conns
goqueue_pgpool_idle_conns
rate(goqueue_pgpool_acquire_duration_seconds_total[5m]) / rate(goqueue_pgpool_acquire_count[5m])
rate(goqueue_pgpool_canceled_acquires_total[5m])
```

Alert rules:
```yaml
- alert: PgPoolNearSaturation
  expr: goqueue_pgpool_acquired_conns / goqueue_pgpool_max_conns > 0.85
  for: 5m

- alert: PgPoolAcquireCanceled
  expr: increase(goqueue_pgpool_canceled_acquires_total[5m]) > 0
  for: 0m

- alert: QueueDepthGrowing
  expr: deriv(queue_depth[5m]) > 10
  for: 10m
```

---

## Session Log

- 2026-07-06: Initial architecture — circuit breaker, DAG junction table, write-ahead log, cascading cancellation, graceful shutdown, Prometheus metrics.
- 2026-07-07: Restructured docs into separate chapter files, cleaned up README, fixed broken links, removed numbered chapter files. Added orphan reconciler (later replaced).
- 2026-07-07: Added `retryWithBackoff` + circuit breaker to producer. Implemented transactional outbox pattern — replaced orphan reconciler. Added `RunMigrations` via `golang-migrate`. Full Docker stack with Prometheus + Grafana auto-provisioning.
- 2026-07-08: Migration files split (000001 base schema, 000002 outbox). `golang-migrate` wired into startup. `initdb.d` mount removed from docker-compose.
- 2026-07-11: Diagnosed flat metrics — producer was creating 1 workflow forever, not real DAGs. Rewrote producer to create 3-step `VIDEO_UPLOAD→VIDEO_TRANSCODE→VIDEO_NOTIFY` pipelines per tick. Each pipeline is its own workflow.
- 2026-08-01 (Codex): Diagnosed pool sizing (confirmed non-issue), found worker concurrency likely bottleneck (inflight pinned at 5), confirmed 20% failure rate is reproducible/deterministic, fixed `CYCLE` syntax error in cascade-cancel query.
- 2026-08-03 (Kiro): Updated CLAUDE.md to consolidate context across all Kiro + Codex sessions.
- 2026-10-18: Fenced idle-delivery recovery with PostgreSQL `NOW()`, added cursor-based Redis PEL recovery, removed incomplete REST idempotency route enforcement, and added the protocol breaking-check gate.
- 2026-10-26: Removed the automatic demo producer, its retry helper and configuration; adjusted worker shutdown accounting to four background goroutines. Reference-executor replacement and persisted catalog routing remain open in S1-01.
