# Go Worker SDK and Remote Execution Contract

**Status:** approved for implementation after the Protocol Gate in Sprint 1.
The checked-in protobuf source is the wire-contract source of truth; this
document defines its platform and SDK behaviour.

## 1. Decision

The Queue Platform remains a Go service and exclusively owns PostgreSQL,
Redis Streams, workflow state, lease state, retries, DLQ handling, and Saga
orchestration. Application workers are Go services and only communicate with
the platform through the worker protocol.

Workers never receive PostgreSQL or Redis credentials for Queue Platform
storage. A task caller submits a `task_type` and payload; it never selects a
raw queue, lease duration, or retry count. The platform's task catalog resolves
those values and persists the resulting route with the job.

```text
client → Queue API → Postgres + outbox → Redis Streams
                                      ↓
                              Worker Gateway (Go)
                                      ↓ gRPC
                              Go worker services
```

The Worker Gateway is the only component that translates the internal Redis
delivery and Postgres lease into a remote worker lease.

## 2. Goals and non-goals

### Goals

- Every Go worker has identical delivery, retry, and fencing rules.
- The platform can reject a worker that is not allowed to run a task type.
- A platform or gateway crash cannot cause a stale worker to commit an outcome.
- The platform delivers effectively-once business outcomes when the participant
  or its provider accepts the supplied idempotency key.
- A business worker can be deployed independently of the Queue Platform.
- The wire contract supports Saga forward and compensation steps without making
  the SDK responsible for orchestration.

### Non-goals

- A universal exactly-once guarantee for an external side effect whose provider
  has no durable idempotency or reconciliation capability.
- User-created queues, arbitrary handler code upload, or dynamic plugins.
- A generic workflow DSL in the Go SDK.
- A streaming worker protocol in v1. Unary long-polling is easier to implement,
  debug, and support in Go. The SDK hides transport so a later
  streaming transport is an internal optimization.
- A Python or other-language worker SDK. A Go service boundary is sufficient
  for the workloads in scope; revisit only with a concrete workload that cannot
  use a Go worker service.

## 3. Guarantees every SDK exposes

| Guarantee | Meaning |
| --- | --- |
| Delivery | At least once. A handler can be invoked again after a crash or expired lease. |
| Fencing | Only the current lease token may complete, fail, or heartbeat a task. |
| Retry | The platform decides whether and when to retry. A handler only classifies its failure. |
| Idempotency | A handler uses the stable `idempotency_key`; an inbox, unique constraint, or idempotent provider makes the logical business effect effectively once. Attempt number is never part of this key. |
| Deadline | The SDK cancels handler execution when the server-provided deadline or lease is lost. |
| Saga | `saga_id` and `step_id` are context. The platform, not the worker, schedules the next or compensation step. |

The platform commits its own state exactly once when that state changes in its
PostgreSQL transaction. For an external effect, the target system must honor
the task ID/idempotency key or the worker must persist an inbox/outbox record
and reconcile uncertainty. Without one of those mechanisms, a duplicate effect
is possible and must be reported as an unsupported task type.

### Why delivery cannot universally be exactly once

There is an unavoidable failure window:

```text
worker sends provider request → provider accepts it → worker crashes
                                      before CompleteTask reaches the platform
```

Retrying can repeat the effect; not retrying can lose it. The platform cannot
know which happened unless the provider or worker has a durable record keyed by
the idempotency key. A distributed two-phase commit does not solve this for an
email provider, payment API, or arbitrary HTTP endpoint because those systems
do not participate in the same transaction.

## 4. Ownership boundaries

| Owner | Owns | Must not own |
| --- | --- | --- |
| Queue Platform | routing, leases, retries, Saga state, Redis/Postgres, DLQ | business database writes |
| Go worker service | business handler, provider clients, local inbox/outbox, business DB | queue storage or global workflow state |
| Task caller | task type, payload, dependency intent, caller idempotency key | queue name, retries, lease TTL, worker selection |

A worker capability is deployment-controlled:

```text
email-worker-go      → email.send:v1, webhook.deliver:v1
media-worker-go      → video.transcode:v1
ml-worker-go         → image.classify:v1
```

The platform's catalog maps a task type/version to one execution pool. A
worker cannot claim a task outside its declared and authorized capability.

## 5. Protocol v1

[`api/proto/queue/v1/worker.proto`](../api/proto/queue/v1/worker.proto) is the
source of truth. REST remains the public submission/status API; gRPC is private
to the platform and workers. The Protocol Gate requires `make proto-check` to
lint and compile this schema before any Gateway or SDK code is accepted; CI
also compares it with the prior protocol to reject breaking changes. Go stub
generation begins with the Gateway implementation in Sprint 3.

The contract has six unary RPCs: `RegisterWorker`, `RenewSession`,
`LeaseTask`, `Heartbeat`, `CompleteTask`, and `FailTask`. `RegisterWorker`
declares descriptive worker metadata and advertised capabilities. It returns
only the intersection of advertised capabilities and the capabilities allowed
by server policy.

`LeaseTask` returns at most one task. The request itself represents one free
handler slot; there is deliberately no client-provided queue or slot count in
the request. `TaskLease` includes an opaque lease token, a deadline, and an
optional result-upload grant. Every lease mutation includes both the worker
session and lease token.

`WorkerMetadata` is not an authentication claim. It helps operations identify
a deployment, but the Gateway derives the workload principal from the mTLS
client certificate and authorizes it on the server.

### Authentication, authorization, and sessions

mTLS is mandatory for every production worker-to-Gateway RPC. Certificate
issuance is an infrastructure responsibility (for example SPIFFE or the
service mesh), not an SDK feature. The Gateway maps the authenticated workload
principal to the services and capabilities it may run.

On a successful `RegisterWorker`, the Gateway creates a short-lived opaque
logical session bound to the mTLS principal and the registered instance ID. The
SDK sends its token on every lease, heartbeat, completion, and failure request.
The token is 32 cryptographically random bytes; the Gateway persists only its
HMAC, never the raw token.

`worker_sessions` is durable PostgreSQL state shared by every Gateway replica:

```text
id, token_hmac (unique), mTLS_principal, instance_id,
authorized_capabilities, expires_at, hard_expires_at, revoked_at, created_at,
last_seen_at
```

An active task lease persists its `worker_session_id` alongside its broker
delivery metadata. Every mutation validates the session HMAC, expiry,
revocation, mTLS principal, session ID, and lease token in the same PostgreSQL
transaction that fences the task state. This lets another Gateway replica—or a
restarted Gateway—validate the worker without an in-memory session map.

The SDK calls `RenewSession` before expiry. A session has both a renewable
`expires_at` and an immutable `hard_expires_at` (for example, a maximum
24-hour lifetime from registration). Renewal requires the current, unexpired
token and the same mTLS principal, re-authorizes the principal and
capabilities against current policy, and may extend `expires_at` only up to
`hard_expires_at`; it keeps the opaque token unchanged. This avoids racing a
token rotation against an in-flight heartbeat or completion. A new
`RegisterWorker` call creates a different session and cannot mutate an old
session's lease. Registration and renewal return `hard_expires_at` so the SDK
can stop leasing before that boundary. The Gateway must reject new leases whose
deadline would exceed `hard_expires_at` and must fence active leases at that
boundary. Once expired, a session is rejected for every RPC, including
heartbeat, completion, and failure. If renewal cannot complete, the SDK stops
leasing new tasks and cancels active handlers at session expiry; the normal
lease-recovery path then creates a new attempt.

### Payload and result contracts

The server-owned task catalog pins the accepted `(content_type, schema_name,
schema_version, maximum bytes)` for each task type and handler version. The
submission API rejects an unregistered or oversized payload before it creates
a job/outbox record. A worker executes only the exact schema/version in its
lease; business-level field validation remains the handler's responsibility.

An empty `TaskResult` means the task has no output. Inline result data is
limited to 64 KiB. For a catalog entry that permits a larger result, the
Gateway supplies a short-lived `ResultUploadGrant` in `TaskLease`. The worker
uploads only to that grant and returns the grant's exact `object_key`, byte
size, and SHA-256 in `ObjectReference`. Before accepting completion, the
Gateway verifies the reference against the grant and object storage. Workers
must never submit arbitrary URLs or object keys.

### Server responses and gRPC status

- `UNAUTHENTICATED` / `PERMISSION_DENIED`: do not retry until credentials or
  deployment configuration changes. A session that is near expiry may be
  renewed; an expired session must register anew and cannot finish its old work.
- `INVALID_ARGUMENT`: SDK bug or malformed request; fail fast and report it.
- `UNAVAILABLE` / `DEADLINE_EXCEEDED`: SDK reconnects with bounded jitter.
- `accepted = false`: the lease is stale. Cancel the local handler and never
  retry completion with that lease token.

The platform may treat a `PERMANENT` failure as retryable only when its
server-owned task policy explicitly says so. Workers may request a failure
classification; they do not control retry limits.

## 6. Go SDK API

The public SDK API is deliberately small. It has no queue-name option and no
database/Redis client.

```go
w, err := worker.New(worker.Config{
    Endpoint: "queue-gateway.internal:8443",
    Service:  "email-worker",
    Slots:    32,
    TLS:      mtlsConfig,
})
if err != nil { return err }

w.Register("email.send", "v1", func(ctx context.Context, task worker.Task) error {
    return emailClient.Send(ctx, task.IdempotencyKey, task.Payload)
})

return w.Run(ctx)
```

`Run` owns leasing, heartbeat, deadline cancellation, completion, and failure
reporting. Handlers return either `nil`, `worker.Retryable(err)`, or
`worker.Permanent(err)`. The default for an unknown error is retryable.

## 7. Lease lifecycle

```text
1. SDK registers over mTLS and receives its authorized capabilities and session.
   It renews that logical session before expiry.
2. It maintains at most `Slots` combined active handlers and outstanding
   `LeaseTask` long polls—one request per free local slot. The wait timeout is
   at most 15 seconds; `NoTask.retry_after` prevents a hot empty-poll loop.
3. Gateway validates the mTLS principal, session, and server-owned capability;
   claims the internal delivery; and returns at most one `TaskLease`.
4. SDK starts a heartbeat at the supplied interval and calls the handler.
5. SDK calls `CompleteTask` or `FailTask` with its session and opaque lease
   token. The Gateway fences the mutation in Postgres, acknowledges the
   internal delivery, and responds whether the lease was accepted.
6. A stale or disconnected worker loses its lease. Recovery creates a new
   attempt. Gateway-side execution profiles cap per-service, per-tenant, and
   per-workflow in-flight work independently of SDK `Slots`.
```

The SDK must stop handler work when a heartbeat is rejected. It cannot forcibly
stop a thread or external subprocess; the handler must honor its cancellation
context and child processes must be started in a cancellable process group.

The platform must persist the active broker delivery metadata (`stream_id`,
queue, consumer/worker ID, and generation) with the active lease. That allows
any Gateway replica to acknowledge the correct Redis Stream entry after a
remote worker completes. In-memory gateway maps are not durable enough.

## 8. Business-service implementation rule

Each worker service uses an Inbox + Transactional Outbox pattern for effects
that touch its own database or an external provider:

```text
receive task
  → insert/check inbox(idempotency_key)
  → perform local business transaction
  → persist local outcome/outbox
  → return completion to Queue Platform
```

For a provider with native idempotency support, pass the task's idempotency key
to the provider. For a provider without it, persist a local send/operation
record before attempting the external effect and reconcile uncertain outcomes.

## 9. Saga behaviour

The SDK does not expose `nextStep()` or `compensate()` methods. It receives a
normal task envelope whose `saga_id` and `step_id` are observability and
idempotency context. The Queue Platform Saga Orchestrator decides whether to
schedule forward work or a compensation task.

Compensation is another task type, for example:

```text
payment.charge:v1  → payment.refund:v1
inventory.reserve:v1 → inventory.release:v1
```

The participant still uses the same stable idempotency key for the logical
compensation step. An email or notification normally has no compensation.

## 10. Security and observability

- Require mTLS/workload identity for worker-to-Gateway authentication; reject
  plaintext connections outside explicitly isolated local development.
- Map the authenticated workload principal—not SDK metadata—to allowed
  services and task capabilities.
- Never log payloads, provider credentials, worker-session tokens, or lease
  tokens.
- Propagate W3C `traceparent`; create spans for lease, handler, heartbeat, and
  completion.
- Emit metrics for task type, execution pool, outcome, attempt, lease loss,
  handler duration, and queue-to-start latency.
- Bound payload, result, and diagnostic error sizes. Large data travels by
  object-storage reference, not as a queue payload.

## 11. Compatibility rules

- Protobuf changes are additive only. Never reuse a field number.
- A task type plus handler version is immutable once tasks have been accepted.
- Deploy a worker supporting both old and new handler versions before routing
  new work to the new version.
- SDK major versions may break source compatibility; protocol v1 remains
  compatible across supported SDK majors.
- The platform rejects a task if no healthy authorized worker supports its
  pinned handler version.

A worker is healthy for route activation only while it has an unexpired,
authenticated session, has polled or heartbeated within two long-poll windows,
and advertises the pinned handler version. Registration alone is insufficient.

## 12. Required conformance suite

The Go SDK runs these black-box tests against a fake Gateway and an integration
Gateway:

1. An unauthenticated connection is rejected; spoofed worker metadata cannot
   gain a capability.
2. A session from a different mTLS principal cannot lease, heartbeat, fail, or
   complete another worker's task.
3. An expired session cannot heartbeat, fail, or complete a lease;
   `RenewSession` extends the same session without changing its token and
   cannot extend it past `hard_expires_at`.
4. A newly registered session cannot mutate an old session's lease.
5. A duplicate lease invokes the business operation with the same idempotency key.
6. A lost `CompleteTask` response can be retried safely.
7. A rejected heartbeat cancels the handler and blocks stale completion.
8. An unknown handler version or payload schema is never executed.
9. The SDK never exceeds its configured handler-slot limit.
10. A result reference outside its issued upload grant is rejected; a valid
   inline and object-backed result can complete.
11. A retryable failure follows server retry policy; a permanent failure follows
   server terminal policy.
12. A worker cannot claim a task outside its allowed capability.
13. Gateway restart after lease dispatch retains session validation and does
   not lose acknowledgement metadata.
14. A compensation task has its own stable idempotency key and obeys normal
   lease fencing.

## 13. Repository layout and implementation order

```text
api/proto/queue/v1/worker.proto
sdk/go/worker/
internal/gateway/          # Go WorkerGatewayService implementation
internal/execution/        # task catalog and server-owned route policy
internal/saga/             # later; platform-only orchestration
tests/conformance/
examples/email-worker-go/  # executable reference, not production platform code
examples/media-worker-go/  # executable reference
```

Build in this order:

1. Run `make proto-check` and approve the checked-in protocol for Go; approve the
   mTLS/session, payload-schema, and result-upload contracts at the same gate.
2. Persist server-resolved route, `worker_sessions`, authenticated lease
   ownership, and active broker-delivery metadata.
3. Implement the Gateway in Go behind the current Redis/Postgres worker path.
4. Implement the Go SDK and Go conformance suite.
5. Move one low-risk worker, `email.send`, to the Go SDK.
6. Move a second, resource-isolated Go workload such as media processing.
7. Add the Saga Orchestrator only after remote worker completion/recovery is proven.

Do not introduce Kafka, a plugin runtime, bidirectional gRPC streams, dynamic
queue creation, or a second worker language before a concrete workload justifies it.
