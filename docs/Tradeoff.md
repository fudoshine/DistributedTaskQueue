# Tradeoffs

## Redis Streams instead of Redis lists

Deliveries are written to a Redis Stream and consumed through a consumer group. `XREADGROUP` records a pending entry before a worker processes it, and `XACK` removes it only after the durable outcome is recorded.

This adds pending-entry management and stream storage, but avoids the untracked in-flight gap of a list move. The janitor uses `XAUTOCLAIM` to examine idle pending entries after a worker failure.

## PostgreSQL outbox instead of direct publish

Job state and an outbox row are committed in one PostgreSQL transaction. A relay publishes the outbox row to Redis Streams and then marks it delivered.

This adds a relay and permits duplicate physical deliveries after a relay crash. Delivery IDs, generations, and the PostgreSQL claim guard make those duplicates harmless.

## PostgreSQL lease as the execution authority

Redis tracks delivery ownership; PostgreSQL leases fence actual execution. A stream heartbeat improves recovery timing, but a worker must still hold the database lease to complete or fail a job.

This means an extra database operation during processing, in exchange for preventing a stale worker from committing after its delivery is recovered.

## Recursive cancellation with path tracking

When a task reaches its retry limit, PostgreSQL recursively cancels its descendants. The query stores a path array and rejects repeated IDs rather than relying on unsupported recursive-CTE cycle syntax.

The path grows with dependency depth, which is appropriate for the intended DAG workflows. Extremely deep graphs should enforce a depth limit at creation time.
