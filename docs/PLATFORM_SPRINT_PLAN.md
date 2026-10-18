# Queue Platform Sprint Plan

**Planning assumption:** two-week sprints and a four-to-six engineer team.
With a smaller team, run the listed tracks sequentially; do not remove the
exit gates. The purpose of this plan is to reach a reliable Go Queue Platform,
then add Go workers and Saga orchestration without changing core
guarantees halfway through implementation.

## Team lanes

| Lane | Responsibility |
| --- | --- |
| Platform | job state machine, routing, relay, storage, Worker Gateway |
| Worker SDK | Go SDK, Go reference workers, capability checks |
| Reliability | integration environment, conformance tests, observability, load tests |

One engineer owns a story end to end. Lanes review the protocol and data-model
changes together; they are not independent silos.

## Sprint 1 — Real execution and a trustworthy baseline

**Goal:** replace the demo queue with a real, server-routed Go handler path and
measure the actual end-to-end system.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Platform | Add the static task catalog; remove client `queue_name`; resolve and persist the route, accepted payload schema/version/size, and result mode; remove simulated processing and automatic producer; register one real `email.send` no-op/reference handler. | A caller submits `task_type`; the server selects the pool and rejects an unknown or oversized payload before it creates work. |
| API security | Add tenant, `api_keys`, and idempotency-record migrations with an expiry index; add an operator-only key issuer, constant-time Gin authentication middleware, scope checks, tenant ownership, transactional `Idempotency-Key` handling, and a bounded idempotency janitor. | Every public task/workflow request has an authenticated tenant; a key cannot read or create work for another tenant; a retried POST creates one workflow/job and outbox row without unbounded retention. |
| Reliability | Make integration tests use an isolated database; build the HTTP → outbox → Redis → claim → handler → completion benchmark; write the multi-relay `SKIP LOCKED` test. | `go test ./...` is safe, and the team has a baseline for throughput and queue-to-start p50/p95/p99. |
| Team | Run the Protocol Gate: `make proto-check` validates `api/proto/queue/v1/worker.proto`; approve the mTLS-principal/session, capability authorization, payload-schema, and result-upload contracts; record initial SLOs from the baseline rather than inventing numbers. | One reproducibly validated, authenticated contract before remote-worker code starts. |

**Exit gate:** `email.send` executes once per valid lease; no client can select an arbitrary queue; all public workflow/task routes require a scoped API key, enforce tenant ownership, and deduplicate concurrent POST retries; test data cannot truncate a developer database; multi-relay ownership is proven; the Protocol Gate passes.

**Do not build:** a second worker language, gRPC Gateway, Saga tables, dynamic queues, or autoscaling.

## Sprint 2 — Correct lifecycle and fast internal delivery

**Goal:** make the queue correct under failure and remove the known one-second
relay latency floor.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Platform | Make terminal failure, cascade cancellation, and workflow status one transaction; reject or correctly handle late dependency creation; add server-owned execution profiles, capability checks, and per-service/tenant/workflow in-flight limits. | No runnable descendant survives a terminal parent failure; incompatible workers cannot consume a task or monopolize a shared pool. |
| Platform | Make the relay wake on new outbox work, drain full batches, and batch Redis/Postgres acknowledgements while retaining polling fallback. | Relay performance is no longer capped by one 100-row batch per second. |
| Reliability | Add fault tests for lost lease, duplicate delivery, failed relay publish, late child, and circuit-breaker-safe duplicate delivery; add Stream/outbox retention plan and truthful depth metrics. | Existing durability guarantees hold under fault injection. |

**Exit gate:** failure/recovery tests pass; measured queue-to-start latency improves against Sprint 1; route, lease, and handler capability are immutable per accepted job.

**Do not build:** remote workers until active delivery metadata is designed and persisted.

## Sprint 3 — Worker Gateway vertical slice

**Goal:** let one independently deployed Go worker execute a task without direct
Redis or PostgreSQL access.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Platform | Implement the Protocol-Gate-approved `api/proto/queue/v1/worker.proto`: mTLS worker registration/renewal, durable `worker_sessions`, session-bound lease/heartbeat/complete/fail endpoints, and persisted authenticated lease ownership plus active Stream-delivery metadata. | Any Gateway replica can authenticate the worker and safely acknowledge a remote worker's completed delivery. |
| Worker SDK | Build the smallest Go SDK: handler registration, long-poll lease loop, heartbeat, cancellation, and outcome classification. | An external Go process can call `Run(ctx)` and handle `email.send`. |
| Reliability | Add mTLS/session authorization, session-expiry/renewal, spoofed-identity, cross-session completion, result-upload, trace-propagation, and Gateway-restart integration tests. | An unauthorized or expired worker session cannot mutate a lease; restart does not produce a lost acknowledgement or stale completion. |

**Exit gate:** the Go email worker has no Queue Platform database/Redis credentials, authenticates with mTLS, and passes claim, heartbeat, completion, stale-lease, cross-session, and restart tests.

**Do not build:** bidirectional streaming, a second worker SDK, or production Saga behaviour.

## Sprint 4 — Go SDK hardening and service adoption

**Goal:** turn the vertical slice into a supported Go worker contract.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Worker SDK | Publish Go SDK API, error taxonomy, deadlines, shutdown behavior, and handler version support; add examples and API documentation. | A Go application team can build a worker without reading platform internals. |
| Platform | Enforce service-to-capability authorization and handler-version routing; add route activation only after an authenticated, recently polling compatible worker is healthy. | The platform never leases a pinned handler version to an incompatible or inactive worker. |
| Reliability | Build the language-neutral conformance suite; require inbox/outbox/idempotency examples for email/provider effects; add load tests for remote Go workers. | The Go SDK behavior is executable specification, not prose only. |

**Exit gate:** one production-like Go worker uses an inbox/idempotency key; all mandatory conformance cases pass; platform storage remains private.

**Do not build:** a second worker language until a concrete workload justifies it.

## Sprint 5 — Go worker workload isolation

**Goal:** prove the Go SDK supports both latency-sensitive and resource-heavy
work without allowing one workload to degrade the other.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Worker SDK | Harden Go subprocess cancellation, shutdown, deadline propagation, and resource instrumentation for handlers that invoke external tools. | A Go worker can safely stop a media/transcode child process when it loses a lease. |
| Application | Build one real Go media worker, such as video transcoding or image thumbnailing, in a separate execution profile from email. | Resource-heavy work is isolated rather than placed on a generic short-job queue. |
| Reliability | Run the conformance suite against email and media workers; load-test profile isolation, tenant limits, and noisy-neighbour recovery. | A media backlog cannot consume the email pool or hide lease/retry defects. |

**Exit gate:** email and media Go workers pass every required conformance test, have zero direct Queue Platform storage access, and meet their separate latency SLOs.

**Do not build:** a Python SDK, direct Redis client, or per-service Saga implementation.

## Sprint 6 — Orchestrated Saga

**Goal:** support compensating business workflows across independently deployed
services.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Platform | Add Saga and Saga-step state, approved templates, forward/compensation transitions, and operator-visible `COMPENSATING`/`COMPENSATION_FAILED` states. | The platform alone decides the next forward or compensation command. |
| Services | Implement two compensable example participants, such as inventory reserve/release and payment charge/refund, each with local inbox/outbox. | Participants are independently idempotent and own their local transactions. |
| Reliability | Test crash-after-effect, duplicate command, lost response, failed compensation, and manual remediation. | A Saga reaches a durable terminal or operator-actionable state; it does not silently hang. |

**Exit gate:** a failed multi-service workflow compensates completed compensable steps in reverse order, with every command safely retryable.

**Do not build:** arbitrary client-authored compensations or claims of universal exactly-once external execution.

## Sprint 7 — Capacity and operational readiness

**Goal:** scale only the proven path and operate it safely.

| Focus | Tasks | Expected result |
| --- | --- | --- |
| Platform | Profile the actual bottleneck; tune relay/worker batch sizes, pool sizes, indexes, and retention. | A documented capacity envelope replaces assumptions. |
| Reliability | Load test multiple relay/Gateway/worker replicas under tenant and workflow concurrency limits; verify recovery, DLQ, dashboard alerts, backup/restore, and rolling handler-version deployment. | The system stays correct during replica loss, deployment, and a noisy-neighbour workload. |
| Operations | Scale worker pools by queue age, backlog, and handler latency; define alert thresholds and runbooks. | On-call can identify whether delay is admission, relay, Gateway, or handler capacity. |

**Exit gate:** published throughput/latency numbers include hardware, payload size, worker count, durability settings, and failure behavior.

**Do not build:** sharding, Kafka/CDC, or self-service queue creation unless Sprint 7 measurements show a concrete limit they solve.

## Mandatory gates

The team must not skip these dependencies:

```text
safe benchmark + real Go handler
  → correct lifecycle + fast relay
  → remote Go worker
  → Go conformance suite
  → second isolated Go workload
  → Saga
  → capacity scaling
```

Any feature that bypasses server-owned routing, lease fencing, task idempotency,
or the conformance suite is rejected in review.
