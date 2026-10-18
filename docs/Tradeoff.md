# Tradeoffs

## 1. Redis instead of RabbitMQ

**Problem:** RabbitMQ is a dedicated message broker — it handles routing, acknowledgements, and dead lettering well. But it is a separate system with its own operational complexity: cluster setup, queue declarations, exchange bindings, and a separate persistence layer. More critically, it has no native way to store job state alongside delivery state. You still need a database for job metadata, which means two systems to keep in sync — and the same delivery guarantee problem exists between RabbitMQ and your database.

**Decision:** Redis Streams with consumer groups gives persistent, replayable, acknowledgement-based delivery — the core of what a task queue needs from a broker — while staying within a system the project already uses. This system's routing logic lives in PostgreSQL via DAG edges. Redis is purely a delivery mechanism, and RabbitMQ's routing features would go unused.

| Advantages | Disadvantages |
|---|---|
| One fewer system to deploy and monitor | Redis is not a dedicated broker — less mature queue tooling |
| Queue depth, inflight counts, DLQ all in one place | No built-in priority queues or exchange routing |
| Streams give replayability and PEL out of the box | Redis memory-bound — very large backlogs need tuning |
| Consumer groups handle competing workers natively | Clustering Redis for HA is more complex than RabbitMQ |

---

## 2. Redis Streams instead of Redis Lists

**Problem:** With a Redis List, the naive approach is `BLMOVE` to move a task from the pending list to an inflight list. But if the worker crashes after the move and before acknowledging, the task sits in the inflight list with no automatic recovery mechanism. There is no built-in way to know how long a task has been inflight or which consumer owns it — you need a separate TTL mechanism, and you risk losing tasks silently.

**Decision:** Deliveries are written to a Redis Stream and consumed through a consumer group. `XREADGROUP` records a pending entry (PEL) before a worker processes it, and `XACK` removes it only after the durable outcome is recorded in PostgreSQL. The janitor uses `XAUTOCLAIM` to reclaim idle pending entries after a worker crash, with idle time tracked automatically by Redis.

| Advantages | Disadvantages |
|---|---|
| PEL tracks every unacknowledged delivery automatically | More complex API than List push/pop |
| `XAUTOCLAIM` gives crash recovery without a separate TTL | Stream entries accumulate — need periodic trimming |
| Idle time per consumer visible natively | Slightly higher memory per entry than a List |
| Replayable — entries persist until explicitly acknowledged | Consumer group management adds operational overhead |

---

## 3. PostgreSQL Outbox instead of Direct Publish

**Problem:** The straightforward approach is to write the job to PostgreSQL and immediately publish it to Redis in the same application call. But these are two separate systems — if the process crashes after writing to PostgreSQL but before publishing to Redis, the task is stuck in `PENDING` forever with no one to process it. There is no atomicity guarantee across two different stores.

**Decision:** Job state and an outbox row are committed in a single PostgreSQL transaction. A relay process reads undelivered outbox rows and publishes them to Redis Streams, then marks them delivered. This guarantees that every job written to PostgreSQL will eventually reach Redis, even across crashes.

| Advantages | Disadvantages |
|---|---|
| Zero task loss across crashes — guaranteed by DB transaction | Adds a relay process and polling overhead |
| Duplicate deliveries on relay crash are structurally harmless | Slight delivery latency (relay poll interval, default 1s) |
| `FOR UPDATE SKIP LOCKED` allows multiple relay replicas safely | Outbox table grows — needs periodic cleanup of delivered rows |
| Single source of truth — DB is always authoritative | More moving parts to understand and debug |

---

## 4. Polling Outbox instead of CDC (Change Data Capture)

**Problem:** Polling the outbox table on an interval introduces delivery latency equal to the poll interval (default 1s). Under high throughput, constant polling adds unnecessary DB load even when there are no new rows. A more event-driven alternative is CDC — using PostgreSQL's WAL (Write-Ahead Log) via tools like Debezium to stream every INSERT to the outbox table in near real-time.

**Decision:** Polling was chosen over CDC deliberately. CDC requires running a separate Debezium connector (or similar), a Kafka/Kafka Connect cluster to consume WAL events, and WAL replication slots configured on PostgreSQL. This is a significant operational dependency for what is essentially a delivery mechanism. The outbox relay with `FOR UPDATE SKIP LOCKED` achieves the same correctness guarantee with a 1s latency overhead and zero additional infrastructure.

| Advantages | Disadvantages |
|---|---|
| Zero additional infrastructure — no Debezium, no Kafka | Poll interval adds up to 1s delivery latency |
| `FOR UPDATE SKIP LOCKED` safely supports multiple relay replicas | Constant DB queries even when outbox is empty |
| Simple to reason about, debug, and tune | Does not scale to extremely high insert rates without tuning |
| Relay crash recovery is trivial — just restart and re-poll | Poll interval is a fixed latency floor, not adaptive |

**When CDC makes sense:** If the system needs sub-100ms delivery latency, or outbox insert rates are in the tens of thousands per second, CDC via WAL streaming eliminates both the latency floor and the polling overhead. For this system's workload, 1s latency is acceptable and the operational simplicity outweighs the difference.

---

## 5. PostgreSQL Lease as the Execution Authority

**Problem:** If Redis alone controls ownership, a worker declared dead by the janitor (because its stream heartbeat went idle) could still complete its job and write a stale outcome — racing with the new worker that just claimed the same task. Redis has no way to prevent this because it has no knowledge of what the original worker is actually doing.

**Decision:** Redis tracks delivery ownership for routing; PostgreSQL leases fence actual execution. Every completion, failure, and heartbeat must present the exact lease token issued at claim time. A stale worker whose token was revoked cannot commit any outcome regardless of what Redis says.

| Advantages | Disadvantages |
|---|---|
| Stale worker commits are structurally impossible | Extra DB round-trip on every heartbeat |
| Lease expiry and revocation handled purely in SQL | Lease TTL must be tuned — too short causes false recoveries |
| No distributed lock manager needed | Worker must renew lease on interval or lose ownership |
| Recovery decision is deterministic — DB is the authority | Slightly more complex claim query |

**On external side effects:** The lease prevents a stale worker from writing to PostgreSQL, but it cannot undo work already done outside the database. If a handler sent an email or called an external API before its lease was revoked, the fence does not roll that back. Handlers must be idempotent — the system guarantees at-most-once database writes per lease, not at-most-once execution of arbitrary side effects.

---

## 6. Advisory Lock on Workflow ID for DAG Fanout

**Problem:** When a parent task completes, the system must find all children whose every parent is now `COMPLETED` and promote them to `QUEUED`. In a multi-parent DAG, two parents can complete concurrently. Without coordination, both completion transactions observe the other parent as still `RUNNING` — neither promotes the shared child, and it waits forever.

**Decision:** `CompleteJobAndScheduleChildren` acquires a `pg_advisory_xact_lock` on the workflow ID inside the transaction before checking children. This serializes all completions within a workflow, ensuring exactly one transaction sees all parents as `COMPLETED` and promotes the child.

| Advantages | Disadvantages |
|---|---|
| Eliminates the multi-parent race without application-layer coordination | Serializes all completions within a single workflow |
| Lock is transaction-scoped — released automatically on commit or rollback | A wide workflow with many concurrent completions becomes a bottleneck |
| No separate lock table or Redis lock needed | Lock contention grows with workflow fan-in degree |
| Postgres advisory locks are lightweight — no row-level blocking | Single-workflow throughput is capped by serial completion rate |

**Known limit:** For a workflow with N tasks completing simultaneously, the advisory lock forces serial execution of all N completion transactions. This is acceptable for typical DAG workflows (depth 3-10). For extremely wide workflows (hundreds of concurrent completions), partitioning completions across multiple workflows or using optimistic concurrency would be needed.

---

## 7. Dual Heartbeat (PostgreSQL + Redis)

**Problem:** A worker holding a task must signal liveness to two systems: PostgreSQL (which holds the lease) and Redis (which tracks the PEL idle time). Using only one creates a gap.

- **Redis heartbeat only:** The janitor uses cursor-advancing `XAUTOCLAIM` to
  find idle PEL entries. If a worker renews the Redis idle timer but its PostgreSQL lease expires, the janitor's DB check will recover the job — but if the Redis idle timer was recently reset, `XAUTOCLAIM` won't surface it until the next idle window. The worker and the recovered job run concurrently.
- **PostgreSQL heartbeat only:** The DB lease stays alive, but the Redis PEL idle timer keeps counting. Eventually `XAUTOCLAIM` surfaces the entry to the janitor. The janitor checks the DB lease — it's still valid — so it correctly leaves it alone. But this requires the janitor to do a DB round-trip for every surfaced entry, including live ones.

**Decision:** Both heartbeats fire independently on the same interval. The PostgreSQL lease renewal (`RenewLease`) is the authority — a failure here cancels the handler context immediately. The Redis `XCLAIM` touch is supplementary — a failure is logged as a warning and processing continues. If only the Redis touch fails, the janitor will surface the entry earlier than necessary but the DB lease check will correctly prevent recovery.

| Advantages | Disadvantages |
|---|---|
| Janitor surfaces genuinely idle entries, not live ones | Two network round-trips per heartbeat interval |
| DB lease loss cancels handler immediately — no orphaned work | Redis heartbeat failure adds log noise without action |
| Redis idle timer stays accurate — reduces unnecessary janitor DB checks | Two failure modes to reason about instead of one |
| PostgreSQL remains the single authority for all correctness decisions | |

## 8. Recursive Cancellation with Path Tracking

**Problem:** When a parent task fails, all downstream dependents must be cancelled. A naive approach of cancelling only direct children misses multi-level dependencies. A recursive CTE without cycle detection can loop infinitely if the dependency graph has a cycle. PostgreSQL 14 does not support the `CYCLE` keyword in recursive CTEs.

**Decision:** When a task exhausts its retries, a recursive CTE walks all descendants. The query stores a path array and rejects any node that already appears in the path, making it safe against cycles without relying on unsupported SQL syntax.

| Advantages | Disadvantages |
|---|---|
| Entire descendant tree cancelled in a single query | Path array grows with dependency depth |
| Cycle-safe without application-level graph traversal | Very deep DAGs could hit query complexity limits |
| No separate cancellation job or polling needed | Locking behaviour under high concurrency needs monitoring |
| Works on PostgreSQL 14 without `CYCLE` support | |

---

## 9. Two-Binary Architecture (Worker + API)

**Problem:** Running the HTTP API and the queue worker in the same process couples their failure modes and scaling. A spike in API traffic can starve worker goroutines, and a worker crash takes down the API. A single binary cannot be scaled independently — you cannot add more workers without also adding more API instances.

**Decision:** The system is split into two separate binaries: `worker` (task execution, janitor, outbox relay) and `api` (HTTP ingestion and status queries). They share PostgreSQL and communicate only through durable state — no direct RPC between them.

| Advantages | Disadvantages |
|---|---|
| Worker and API scale independently | Two processes to deploy, monitor, and manage |
| Worker crash does not affect API availability | Configuration must be kept in sync across both |
| API traffic spikes do not starve worker goroutines | Slightly more complex local development setup |
| Clear separation of concerns — ingestion vs execution | |
