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

---

## 6. Recursive Cancellation with Path Tracking

**Problem:** When a parent task fails, all downstream dependents must be cancelled. A naive approach of cancelling only direct children misses multi-level dependencies. A recursive CTE without cycle detection can loop infinitely if the dependency graph has a cycle. PostgreSQL 14 does not support the `CYCLE` keyword in recursive CTEs.

**Decision:** When a task exhausts its retries, a recursive CTE walks all descendants. The query stores a path array and rejects any node that already appears in the path, making it safe against cycles without relying on unsupported SQL syntax.

| Advantages | Disadvantages |
|---|---|
| Entire descendant tree cancelled in a single query | Path array grows with dependency depth |
| Cycle-safe without application-level graph traversal | Very deep DAGs could hit query complexity limits |
| No separate cancellation job or polling needed | Locking behaviour under high concurrency needs monitoring |
| Works on PostgreSQL 14 without `CYCLE` support | |

---

## 7. Two-Binary Architecture (Worker + API)

**Problem:** Running the HTTP API and the queue worker in the same process couples their failure modes and scaling. A spike in API traffic can starve worker goroutines, and a worker crash takes down the API. A single binary cannot be scaled independently — you cannot add more workers without also adding more API instances.

**Decision:** The system is split into two separate binaries: `worker` (task execution, janitor, outbox relay) and `api` (HTTP ingestion and status queries). They share PostgreSQL and communicate only through durable state — no direct RPC between them.

| Advantages | Disadvantages |
|---|---|
| Worker and API scale independently | Two processes to deploy, monitor, and manage |
| Worker crash does not affect API availability | Configuration must be kept in sync across both |
| API traffic spikes do not starve worker goroutines | Slightly more complex local development setup |
| Clear separation of concerns — ingestion vs execution | |
