# Go Worker SDK: Implementation Contract

Status: design-only engineering handoff, 19 October 2026. Nothing in this
document marks a runtime feature or sprint complete. The current repository
does not yet implement the authenticated remote-worker system described here.
Go is the only worker SDK in scope.

Read this with [the sprint plan](PLATFORM_SPRINT_PLAN.md) and
[REST authentication](REST_API_AUTH_DESIGN.md). Existing protobuf definitions
describe today's message shapes, not proof of the behavior below. Section 5
lists the required contract additions before engineers freeze protocol v1.

## 1. Architecture and decisions

```text
caller --HTTPS/API key--> REST API --transaction--> PostgreSQL jobs + outbox
                                                        |
                                                   relay workers
                                                        |
                                                  Redis Streams
                                                        |
                                   Go Worker Gateway --mTLS/gRPC--> Go business workers
                                                        |
                                           durable outcomes + ACK repair
```

PostgreSQL is the source of truth. Redis is a recoverable delivery accelerator.
A business worker has no Queue Platform PostgreSQL/Redis credentials. Business
services may own their own databases, inboxes, and outboxes.

| Decision | V1 contract |
| --- | --- |
| Routing | Caller submits task type and payload. Server selects and persists tenant, handler version, capability stream, execution pool, schema and policy revision. |
| Graph activation | Build a DRAFT workflow, then seal it atomically. No handler runs before seal; the graph is immutable afterward. |
| Isolation | Email and media use separate pools, deployments and resource budgets. A queue name is not a runtime predictor or a security boundary. |
| Delivery | At least once; a handler can run again. PostgreSQL lease fencing protects platform state, not an external provider's side effect. |
| External effects | Effectively once only with a durable business inbox/unique constraint or provider idempotency and reconciliation. |
| Transport | Six unary gRPC RPCs, including bounded long polling; no streaming protocol in v1. |
| Authentication | REST: scoped API key and secret. Workers: mTLS workload principal plus opaque, durable session. These credentials are not interchangeable. |
| Retry ownership | Platform schedules retries. SDK retries transport/outcome delivery, never reruns a business handler within one received attempt. |
| Saga | Platform orchestrates; participants implement normal forward and compensation handlers. |
| Deployment | Keep one platform repository. API, relay/recovery and Gateway are process boundaries; business workers are independently deployable services. A folder is not automatically a microservice. |

Do not add Kafka, self-service queues, uploaded plugins, a generic workflow DSL,
a Python SDK, or sharding before measurements identify a requirement.

### Exactly-once boundary

If a provider accepts a request and the worker crashes before recording its
outcome, retrying may repeat the effect. Not retrying may lose it. A platform
lease cannot resolve that uncertainty. A local inbox alone also cannot make a
non-idempotent remote provider exactly once.

Each effectful task must document one supported strategy:

1. Local business effect and inbox outcome committed in one local transaction.
2. Provider accepts the stable idempotency key and can return/reconcile the
   original outcome.
3. Durable operation intent plus provider lookup/reconciliation resolves an
   uncertain result before resending.

If none applies, declare duplicate-effect risk explicitly; do not enable an
automatic-retry production route claiming effectively-once behavior.

## 2. Server-owned catalog and safe routing

A catalog entry is an immutable revision containing:

```text
task_type, handler_version, route_revision, capability_stream, execution_pool,
payload_schema(content_type, name, version, max_bytes),
result_schema(content_type, name, version, output_required, inline_max_bytes, object_max_bytes),
lease_ttl, heartbeat_every, handler_timeout, max_attempt_duration, max_job_age, max_attempts,
retry_backoff, tenant/workflow/service limits, effect_strategy
```

Payload/result schemas and business semantics are immutable for a
(task_type, handler_version) capability. Changing them requires a new handler
version as well as a new catalog revision. Retry policy, timeout or route
changes create a new revision for new jobs without changing that capability's
schemas; they never rewrite accepted jobs. Increasing/decreasing an operational
pool cap is allowed independently, but cannot change a job's pinned contract.
Retries, children, recovery and DLQ replay use each job's persisted route,
never the current producer's or janitor's queue. DLQ replay is an audited new
job in a new already-sealed single-task workflow, with a new submission identity
but the original pinned contract and stable business effect key. Saga command
replay must go through the orchestrator, not a generic DLQ operation. Reject replay after the participant/provider's durable
idempotency evidence expires. An intentional new business effect is a new
business command, not a DLQ replay override.

Caller JSON must reject `queue_name`, `max_retries`, lease settings and tenant
overrides instead of silently ignoring them. Handler version is server-resolved
in v1. Unknown/disabled task types and schema violations fail before any
job/outbox write. Submission/status responses reflect persisted defaults and
actual state, not a reconstruction from request fields.

### Capability streams, not one mixed unfiltered queue

Use one server-provisioned stream/consumer group per
`(task_type, handler_version, route_revision)`, mapped to a shared execution
pool where appropriate. Do not create a queue per tenant, workflow or job.
The static catalog bounds the number of streams; retain an old route until its
jobs, PEL and repair work have drained.

A Gateway selects streams only from the intersection of the session's
advertised, server-authorized capabilities and active/historical catalog
routes. Poll these streams round-robin: a bounded nonblocking pass, then block
on one stream for at most one second before rotating, within the overall
15-second LeaseTask wait. Cap registration at 32 capabilities in v1. This
avoids a multi-stream read returning several tasks for one SDK slot.
No strict FIFO or tenant-fair scheduling guarantee is made.

Before returning a lease, re-check the exact job capability, route revision,
tenant policy and distributed admission in the claim transaction. A mismatched
message is quarantined with an operator alert; never execute it, silently
acknowledge/drop it, or repeatedly move it through an incompatible worker.

For remote routes, activation requires a compatible authenticated session
with an unexpired lease on presence and a poll/heartbeat within 30 seconds.
Presence refresh is coalesced, not a row update on every empty poll. Draft
assembly pins and validates catalog contracts without requiring workers online.
At seal, require every pinned route to have a healthy compatible worker;
otherwise return retryable 503 and leave the draft unchanged with no outbox.
Once sealed, accepted jobs survive temporary worker absence and wait for
recovery. Busy workers heartbeat; idle workers poll. Registration alone does
not prove readiness.
Sprint 1's temporary in-process reference executor uses explicit readiness;
it is removed from remote-enabled routes at cutover.

### Short versus long jobs

A media job cannot become an email job by selecting an email queue. The
catalog selects separate pools and max attempt durations; worker CPU/memory
limits and subprocess controls enforce resource isolation. A slow email
provider can still exceed its budget: deadline cancellation, bounded retries
and metrics handle that case. Runtime prediction and hard CPU preemption of
an arbitrary Go goroutine are not promises.

## 3. Durable state and transaction rules

These are required logical records, not prescribed Go struct names. Add new
migrations; never edit a migration already applied to an environment.

| Record | Required persisted information |
| --- | --- |
| Workflow | tenant/ID, DRAFT or execution state, sealed_at, draft_expires_at, job count, timestamps |
| Job | tenant/workflow/task identity, immutable catalog snapshot, payload, stable effect key, state, generation, current lease owner/token hash, attempt number, attempt deadline and expiry |
| Publication | unique (task, generation), delivery ID, persisted route, not-before time, relay ownership token/expiry, publication status |
| Worker session | token HMAC + pepper version, workload principal, service/instance/build, immutable authorized capability subset, registered slot cap, soft/hard expiry, revocation |
| Active attempt | task/generation/attempt, session ID, lease-token hash, stream/group/message ID and consumer, admission-token IDs |
| Outcome receipt | unique attempt identity, owner/session, lease-token hash, operation + canonical outcome digest, accepted result/failure and commit time |
| ACK repair | stream/group/message ID, related outcome/recovery receipt, retry time and completion state; unique physical message identity |
| Admission token | scope kind/key, token number, owning attempt; unique (scope, token number) |
| Result grant | attempt, exact staging key, allowed schema/bytes/content type, expiry and storage policy |
| Committed result | task/attempt/tenant, schema, inline bytes or immutable object key/version, byte size, verified SHA-256, retention/tombstone and latest download-grant expiry |
| Download grant | token HMAC/key version, issuing API-key ID, tenant/result/version, expiry and bounded read lifetime; no raw URL/token |

Required indexes/constraints include unique delivery ID and (task,generation),
unique (task,attempt) receipt plus token-hash lookup, tenant/object lookup,
workflow/job-state lookup, partial due-outbox and expired-running-lease indexes,
free admission tokens by (scope,key,token), session-token HMAC uniqueness and
indexed expiry/next-repair cursors. Verify query plans using representative
history volume, not an empty test database.

A lease token is 32 random bytes; persist its keyed hash and key version, not a reusable token.
Session token HMACs use a separate deployment pepper/key domain from REST keys.
Never put API-key/session/lease credentials in logs, receipts, metrics, task
payloads or URLs. HTTPS artifact URLs may carry only their own short-lived,
object-scoped grant token; redact the entire grant URL and store only its HMAC.

### Lifecycle invariants

- Workflow `DRAFT` accepts graph construction but cannot execute. A task is
  `PENDING` while its workflow is DRAFT or dependencies block it; `QUEUED` includes scheduled
  retries whose `available_at` is in the future; `RUNNING` has one current
  fenced attempt; `COMPLETED/FAILED/CANCELLED` are terminal.
- A generation identifies a publication, not a business effect. Retries and
  lease recovery create a new generation and delivery ID transactionally.
  The task's effect key never changes across these attempts.
- Completing a task commits its receipt, result, child readiness/outbox,
  workflow status, admission release and ACK-repair intent together.
- Retryable failure commits the old receipt, admission release, scheduled next
  generation/outbox and ACK repair together.
- Terminal failure commits failure, descendant cancellation/fencing, workflow
  status, durable DLQ intent, token releases and ACK repair together. DLQ
  publishing is itself an outbox effect, not a prerequisite for DB commit.
- Branch-local failure policy: fence/cancel all descendants of a failed job,
  but let unrelated branches settle. The workflow stays nonterminal while
  existing jobs remain nonterminal. When all settle, aggregate FAILED if any
  failed, otherwise CANCELLED if any cancelled, otherwise COMPLETED. A workflow
  is DRAFT before seal. Once sealed, while work remains report RUNNING after
  any attempt has started/settled, otherwise PENDING. Recompute under the
  workflow lock; later success cannot overwrite a settled failure. Explicit
  operator cancellation fences all unfinished jobs and closes the draft.
- Append is allowed only in an unexpired DRAFT workflow. Seal makes the whole
  graph immutable: reject every later append with 409 workflow_sealed, even
  while the workflow is still running. Terminal/cancelled workflows never
  reopen. Cross-workflow/cross-tenant/missing parents and duplicate edges are
  rejected. Parents must already exist in the draft; immutable backward edges
  prevent forward-reference cycles.
- Creating draft tasks writes no publication. Seal atomically activates the
  graph and inserts initial publications only for roots. Child readiness and
  publication happen later in their parent-outcome transaction.
- Drafts expire 24 hours after creation. Append and seal check current DB time
  under the workflow lock even before the expiry janitor runs. The bounded
  janitor cancels expired drafts and their pending jobs without publication,
  retains ordinary terminal history, and does not immediately delete accepted
  IDs. Sealed jobs' max_job_age starts at sealed_at, not during graph assembly.
- Enforce a v1 maximum of 1,000 jobs/workflow and 32 parents/job to bound atomic
  cascade/lock duration. Reject overflow before writes. Larger DAGs need a
  separately designed durable chunked-cancellation barrier, not unbounded SQL.

### Workflow assembly and seal

This is the required public submission flow from S1-03 onward:

1. POST /api/v1/workflows with name returns 201 and status DRAFT, sealed_at=null,
   draft_expires_at. Authenticate before creating the tenant-owned record.
2. POST /api/v1/workflows/:id/tasks repeatedly adds parent-first tasks, returning
   201, server-assigned IDs and status PENDING. Pin each job's catalog snapshot;
   create no outbox. A caller can safely use returned parent IDs even when
   their handlers would finish immediately.
3. POST /api/v1/workflows/:id/seal with body {} requires workflows:write and
   Idempotency-Key. Under the workflow lock, check ownership, DRAFT expiry,
   a nonempty valid graph and every pinned route's readiness. In one transaction,
   set sealed_at, initialize job-age deadlines, queue roots, insert their unique
   initial publications and store the 200 workflow response.
4. Execution may start only after that commit. GET workflow reports its current
   execution state; GET task reports progress/result using section 8.

All three POST flows use the REST transactional idempotency contract. Same-key
seal retries replay the original response before evaluating current workflow
state or worker health, after current authentication/scope/ownership checks.
A second seal using a different key returns 409 workflow_sealed; it never
publishes roots again. Concurrent append versus seal is serialized: either
append commits before seal and is included, or seal wins and append is 409.

Empty graph returns 400 empty_workflow; an expired draft returns 409
draft_expired and cannot activate. Another tenant gets 404. Invalid parents or
graph bounds fail before mutation. Unhealthy pinned routes return 503 with
Retry-After: 1, no activation/publications or committed submission receipt;
the same key/body can be retried. Expiry versus seal uses DB time under the
same lock: a draft already expired cannot win by beating the janitor.

WorkflowResponse adds sealed_at and draft_expires_at; existing workflow states
gain DRAFT. This is an intentional pre-release contract change, not a change
to the currently running API. S1-05 approves it with the wire contract.
An atomic bulk graph API and appending to an executing graph are not v1 goals.

Use the existing workflow advisory-transaction lock for append, seal, expiry,
claim, outcome, cancellation and recovery. Re-check facts after obtaining it. This
serializes state transitions within one workflow, not every workflow globally.
Heartbeats only fence their current attempt and do not acquire this lock.

Lock order is authenticated policy/session rows (shared for validation), then
workflow advisory lock, affected jobs in ID order, then admission token rows in
scope/token order. Renewal/revocation never acquire workflow/job locks.
All paths follow this order; retry deadlock/serialization failures only with
a bounded transaction retry and unchanged operation identity.

### Distributed admission, without scanning all historical jobs

Provision durable semaphore token rows for pool, service, tenant, workflow and
session scopes. Claim a free token in every applicable scope with row-level
`SKIP LOCKED`; if any scope is full, roll back all acquisitions and do not
start the handler. Create workflow/session tokens with those records; provision
tenant/service/pool tokens by operator policy, not from untrusted requests.
Job claim and token ownership commit together.

Completion, failure, cancellation and lease recovery release only tokens owned
by the exact attempt, in the same transaction as its state change. Token rows
are not reclaimed independently on a wall-clock timeout: fence/recover the
owner job first. A bounded reconciler repairs inconsistent ownership under the
same locks. Decreasing a cap retires free tokens and waits for occupied
retiring tokens; it does not kill or oversubscribe active work.

On admission failure after a broker read, atomically defer the still-queued
job by a short server-policy backoff, create a new generation on its own route,
and persist ACK repair for the old entry. This is not a business attempt and
does not consume its retry budget. Track admission delay separately; repeated
deferral is bounded in rate, not a hot requeue loop. These tokens cap authorized RUNNING
attempts, not physical provider calls or CPU after a stale/uncooperative worker
ignores cancellation. Process isolation, handler cooperation and provider-side
controls are still required. This is not weighted fair sharing;
starvation/fairness is a measured scale gate.

## 4. Identity, sessions and timing

mTLS validates the server and client chain and maps an authenticated certificate
identity to an operator-managed principal/service/capability policy. Metadata
such as service name, instance ID or build string is descriptive, never
authority. Revocation/capability policy is checked on every RPC and at mutation
commit; no process-local positive authorization cache is a security boundary.

RegisterWorker returns the intersection of advertised and allowed
capabilities. That subset and slot cap are immutable for the session. Renewal
can preserve it or fail if it is no longer authorized; it never silently adds
capabilities. Removed authorization fences affected work; new capabilities
require a new session. If any capability in a session is withdrawn, reject
renewal and drain/cancel that session rather than pretending to return an
updated subset in the existing renewal response.

Every lease mutation validates principal, session HMAC, soft/hard expiry,
revocation, policy, owner session and token in the same transaction. A new
session never adopts the old session's task. Session expiry/revocation makes
all old mutations invalid; recovery releases their capacity.

| Timing | Initial policy, to be measured rather than advertised as SLO |
| --- | --- |
| Session soft TTL / renew | 10 minutes / renew halfway, with jitter |
| Session hard lifetime | 24 hours; immutable |
| Long poll | Maximum 15 seconds; per-stream blocking at most 1 second |
| Lease TTL / heartbeat | 20 seconds / 5 seconds; TTL must exceed 3 heartbeat intervals |
| Attempt duration | Catalog-owned; email reference 30 seconds, media reference at most 30 minutes, including upload/finalization |
| Handler timeout | Separate pinned budget: email reference 20 seconds; media reference 20 minutes, reserving the rest for result upload/finalization |
| Unary control RPC timeout | At most 5 seconds, clipped to current safety budget; excludes long poll and object finalization |
| Object finalization RPC | At most 60 seconds, clipped to attempt/session bounds; independent heartbeat and safety monitor stay active |
| Empty-poll retry | Honor server delay, clamped to 100 ms–5 seconds |
| Transport backoff | Full jitter, initial ceiling 100 ms, doubling to maximum 2 seconds |
| Local clock safety margin | At least 250 ms; unhealthy timing/clock conditions fail closed |

Lease expiry is renewable; the attempt deadline is immutable. Handler deadline
is also immutable and earlier: claim time + pinned handler_timeout, bounded by
the attempt deadline. The difference reserves upload/finalization time; catalog
activation rejects a result mode whose tested transfer/sealing budget cannot
fit. Handler context uses this earlier deadline plus a separately cancellable
lease-safety monitor; upload/finalization uses its own context and does not
inherit an already-expired handler context. A heartbeat
cannot extend expiry beyond the attempt deadline, current session expiry or
hard expiry. A maximum job age and attempts budget bound total retries;
defaults belong to the catalog, not SDK configuration.

Timed replies carry DB-derived `server_time`. On each request the SDK measures
round-trip time with Go's monotonic clock. Remaining safe time is no greater
than `expiry - server_time - round_trip - safety_margin`, anchored at receipt
with a monotonic timer. This is deliberately conservative and does not depend
on the worker's wall clock agreeing with the DB. A non-positive budget cancels
work. Use current DB time at the fenced write, not transaction-start time after
waiting on locks.

Transient heartbeat errors do not immediately prove lease loss, but never
extend the last confirmed expiry. Retry within that budget; on rejection or
budget exhaustion cancel execution and prohibit new outcome mutations.
Heartbeat continues during handler execution, object upload and outcome retries.
After an outcome commits, a heartbeat may report stale: stop execution. If an
earlier outcome reply is ambiguous, only an explicit receipt-only request may
resolve it while credentials are valid. After local safety-budget exhaustion,
never send an ordinary Complete/Fail request: the conservative timer may have
expired before the DB lease, so freezing the bytes alone is not a read-only
operation. Receipt-only queries have a separate maximum 30-second resolution
budget, still bounded by session validity and any remaining ShutdownGrace,
and cannot create a transition. Receipt resolution never extends shutdown.

Begin session rollover before hard expiry minus the largest authorized
attempt duration and a control-RPC margin. Register a new session for new work,
keep old-session contexts for their own attempts until drained, and share one
local slot budget across both. Renew the old session while draining. If
renewal fails, stop new intake; on expiry cancel its remaining attempts.
New leases must fit their whole attempt deadline before hard expiry.
Certificate rotation reconnects through the TLS provider; losing the old
principal never transfers old leases to a different principal.

## 5. Protocol v1: freeze gate and RPC behavior

Source: [worker.proto](../api/proto/queue/v1/worker.proto).
This task does not modify it. Sprint S1-05 must add and validate these
currently missing fields/semantics before implementation:

- A server-authored tenant_id on TaskEnvelope, copied only from the persisted
  job's authenticated tenant. Expose it as Task.TenantID; absent/invalid tenant
  identity is a contract error and the handler must not run. Caller payloads
  cannot override it. Append a fresh field number without renumbering fields.
- A `ResultContract` in each TaskLease: allowed schema/content type, maximum
  inline/object bytes and whether output is required. Empty results are
  permitted only when the catalog says so.
- An immutable handler_deadline on TaskLease, distinct from the total attempt
  deadline, to reserve time for upload/finalization.
- DB-derived `server_time` on registration, renewal, lease and heartbeat
  replies; lease and session TTL/hard bounds remain explicit.
- Stable validation rules, presence/length limits and canonical outcome-digest
  rules below. Preserve existing field numbers; reserve removed numbers.
- A receipt_only boolean on CompleteTaskRequest and FailTaskRequest. When true,
  perform authenticated receipt lookup only; never transition a job even if its
  DB lease is still live. Return accepted=false if no matching receipt exists.
- A result-upload contract that covers the entire admitted attempt; no hidden
  refresh RPC and no arbitrary returned object URL.
- Generated stubs, reproducible pinned generator versions, a reviewed baseline
  and breaking-change CI. A successful proto build alone does not validate
  authentication, idempotency or durability.

| RPC | Server action and SDK recovery |
| --- | --- |
| RegisterWorker | Require mTLS, bounded metadata, 1–32 unique capabilities, 1–1,024 slots within principal policy; reject excess rather than silently changing SDK capacity. Persist session, return authorized subset and timing. Require all locally registered handlers to be authorized or fail SDK startup. A lost reply may leave an unused expiring session; no task is adopted. |
| RenewSession | Same principal, current unexpired token and current authorization; keep token unchanged, cap extension at hard expiry. Exact retry is safe. Expired credentials require registration, never old-lease adoption. |
| LeaseTask | One free local slot, one task at most, authorized streams only. Persist attempt/admission/delivery ownership before returning. If the reply is lost, no handler runs without its token; the abandoned lease expires. Server session/global caps include such unseen leases. A fresh poll is not an idempotent retry of the lost claim. |
| Heartbeat | Fence current live attempt/session, extend only within bounds. Repetition is safe; Redis PEL touch failure cannot extend DB authority. |
| CompleteTask | Validate schema/result, verify/seal object if present, then atomically commit receipt + lifecycle effects. Exact receipt replay returns accepted=true even if the job has since advanced; it never re-applies effects. |
| FailTask | Classify request under catalog policy, then commit receipt + retry/terminal effects. Exact replay returns accepted=true without incrementing retries twice. |

Gateway ingress also bounds concurrent polls/control RPCs by session/principal
and global connection budgets; a malicious client cannot turn one advertised
slot into unbounded long-poll goroutines. Registration rate limits and session
expiry bound abandoned-session storage.

A task envelope exposes task/tenant/workflow IDs, optional Saga IDs, type/version,
payload/schema, 1-based attempt number, stable effect key, immutable deadline
and validated trace context. Do not expose queue credentials or raw routing
controls. IDs are canonical UUID strings; schema names/types/versions are
bounded to 128 UTF-8 bytes each; diagnostic failure text is at most 4 KiB after
sanitization. Inline input and output are at most 64 KiB each, with stricter
catalog limits allowed. REST's 64 KiB body limit also includes the JSON
envelope. Object output has a v1 ceiling of 128 MiB, with lower catalog limits allowed.
Large input is a validated business-owned object identifier, never
an SDK-fetched arbitrary URL. Bound gRPC messages to 256 KiB and metadata to
16 KiB; no unbounded decompression or diagnostic strings.

### Immutable outcomes and ACK recovery

Freeze an outcome before the first Complete/Fail call. Its digest covers the
operation and explicitly length-delimited typed values: schema, inline bytes
or object key/size/SHA-256, or failure kind and sanitized text. Use a documented
canonical encoding, not Go map iteration or unspecified protobuf byte order.

For the same attempt and valid credentials:

- No receipt + current live token: validate, commit once, return accepted=true.
- Matching receipt + same token/owner/digest: return accepted=true with no new
  fanout, retries or side effects, even if its former lease expiry has passed.
- Receipt + a different operation/digest: `ALREADY_EXISTS`; report an SDK bug,
  do not replace the outcome.
- receipt_only=true + no receipt: accepted=false, even with a live current
  token. This request cannot create a receipt, seal an object or schedule work.
- No matching receipt + stale/expired/foreign lease: accepted=false for stale
  ownership, or the appropriate authentication/authorization error. Never
  resurrect a dead attempt.

Receipt replay still requires an unexpired, unrevoked session and current
principal authorization. Keep receipts at least as long as job history and
never less than seven days after outcome; cleanup cannot invalidate a live
session's legal replay horizon.

DB commit is the acceptance point. Persist ACK repair before responding;
Redis need not be reachable to acknowledge a committed platform outcome.
A repair loop retries XACK/delete idempotently and handles duplicate physical
deliveries. Repair identity includes stream/group/message plus broker epoch
and expected delivery identity; a reused message ID after stream recreation
must never acknowledge/delete a different job. Verify identity atomically in
the broker adapter before resolving an entry. A duplicate for a completed/old generation may be acknowledged
only after PostgreSQL proves it terminal/stale; a live current attempt is never
stolen just because PEL idle time elapsed. Keep repair records until success
or verified broker-epoch retirement. Gateway restart requires no in-memory
session or stream-ID map to finish this process.

### Status handling

| Signal | SDK behavior |
| --- | --- |
| UNAUTHENTICATED / PERMISSION_DENIED | Stop intake for that session, cancel unauthorized attempts; report configuration/identity failure. Do not endlessly register around a denied principal. |
| INVALID_ARGUMENT / ALREADY_EXISTS | Fail closed and surface a contract bug; never retry malformed or changed outcomes. |
| UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED | Retry only the safe operation, with bounded jitter and server backoff, within its confirmed budget. A timeout does not prove the server rolled back. |
| accepted=false | Cancel and stop new mutations; only an explicit receipt_only request may resolve an ambiguous earlier outcome. |
| NoTask | No handler invocation. Respect retry_after and release/reuse the slot. |
| CANCELLED | Distinguish local drain/cancellation from a transport failure; never convert platform cancellation into successful completion. |

FailureKind is a business outcome request, not a gRPC status: RETRYABLE retries
while attempt/job-age budgets remain, otherwise fails/DLQs; PERMANENT ends the
job unless the pinned catalog explicitly overrides that classification;
CANCELLED from a still-current worker means infrastructure interruption and
retries within the same budgets. An operator/workflow cancellation has already
fenced the job and cannot be undone by a worker failure request. UNSPECIFIED is
invalid, not an implicit retry or success.

Do not stack automatic gRPC application retries under SDK retry loops. The
SDK owns bounded retries explicitly; no hedged LeaseTask/CompleteTask calls.

## 6. Go SDK public API

These are proposed public signatures for engineers, not existing package code.
Expose business concepts, not generated protobufs. Configurations and handler
registrations become immutable when Run starts.

```go
type Config struct {
    Endpoint        string
    Service         string
    InstanceID      string // generated once per process if empty
    BuildVersion    string
    Slots           int
    TLSConfig       func(context.Context) (*tls.Config, error)
    ShutdownGrace   time.Duration
    Logger          *slog.Logger
    Observer        Observer // optional; callbacks must not block
}

type Schema struct {
    ContentType string
    Name        string
    Version     uint32
}
type Payload struct {
    Schema Schema
    Data   []byte
}
type Task struct {
    ID, TenantID, WorkflowID, SagaID, StepID string // TenantID is server-derived
    Type, HandlerVersion          string
    Attempt                       uint32
    IdempotencyKey                string
    Payload                       Payload
    ResultContract                ResultContract
    Deadline                      time.Time // total attempt deadline
    HandlerDeadline               time.Time // ctx has this earlier deadline
}
type Registration struct {
    TaskType, HandlerVersion string
    PayloadSchema            Schema
    ResultSchema             Schema // zero value for no-output handler
}
type ResultContract struct {
    Schema Schema
    Required bool
    InlineMaxBytes, ObjectMaxBytes int64 // zero disables that storage mode
}
type Handler func(context.Context, Task) (Result, error)
type EventKind string // lease, handler, upload, outcome, retry, lease_lost, session, drain
type Event struct {
    Kind EventKind
    TaskType, HandlerVersion string
    Outcome string // bounded enum: ok, retryable, permanent, cancelled, rejected, unavailable
    Duration time.Duration
}
type Observer interface { Observe(Event) }
// Events contain no tokens, payloads, error strings or job/tenant IDs.

// errors.Is classifications; concrete wrapping preserves the underlying cause:
// ErrInvalidConfig, ErrInvalidRegistration, ErrAlreadyStarted,
// ErrUnauthorized, ErrContract, ErrShutdownTimeout.

func New(Config) (*Worker, error)
func (*Worker) Register(Registration, Handler) error
func (*Worker) Run(context.Context) error

// Result has private representation; handlers cannot forge upload grants.
func NoResult() Result
func Inline(Schema, []byte) (Result, error)
type ObjectSource struct {
    Size    int64
    Open    func(context.Context) (io.ReadCloser, error)
    Cleanup func() error // optional; SDK calls once after returned-result ownership transfers
}
func Object(Schema, ObjectSource) (Result, error)
func Retryable(error) error
func Permanent(error) error
```

Config rules: endpoint/service/TLS provider are required; Slots is 1–1,024.
ShutdownGrace defaults to 30 seconds and must be positive. New validates
configuration without network calls. Register rejects empty/duplicate
type-version pairs, nil handlers and invalid schemas, and may only run before
Run. Run may be called once; concurrent/repeated calls return a defined
ErrAlreadyStarted. No handlers is a startup error. TLS configuration must
verify the server and supply client identity; InsecureSkipVerify/plaintext is
not a public production option. Local tests inject a private fake transport,
not an unsafe public flag. Generated clients and credentials remain private.

ResultContract lets handlers choose a permitted output mode without seeing a
grant URL or forging a reference. A zero-schema, zero-limit contract permits
only NoResult. Payload bytes are owned by this invocation, not reused by a
concurrent handler. Observer delivery uses one bounded best-effort queue;
drop/count overflow rather than blocking heartbeat or outcome paths on a
callback. A blocked observer cannot hold execution authority alive.

The TLS provider is invoked for each new transport connection, allowing
mounted certificate rotation without restarting the process. Clone returned
configurations; never mutate the caller's configuration or log credentials.
Use the repository's existing Go module while developing. Before external
publication, S4-03 must replace the local-only `distributed-queue` module name
with the repository owner's canonical import path and prove installation from
a clean external consumer. The repository's hosting URL is a release input,
not something this design invents.

### A no-output email handler

```go
w, err := worker.New(worker.Config{
    Endpoint: "queue-gateway.internal:8443",
    Service: "email-worker", Slots: 32, TLSConfig: loadWorkloadTLS,
})
if err != nil { return err }
err = w.Register(worker.Registration{
    TaskType: "email.send", HandlerVersion: "v1",
    PayloadSchema: worker.Schema{
        ContentType: "application/json", Name: "email.send", Version: 1,
    },
}, func(ctx context.Context, task worker.Task) (worker.Result, error) {
    // SendOnce tenant-scopes business reads before effects and durably reuses the outcome.
    if err := emailService.SendOnce(ctx, task.TenantID, task.IdempotencyKey, task.Payload.Data); err != nil {
        return worker.NoResult(), worker.Retryable(err)
    }
    return worker.NoResult(), nil
})
if err != nil { return err }
return w.Run(ctx)
```

Inline validates the schema and 64 KiB universal bound, then copies the bytes;
mutating the caller's slice cannot change the eventual receipt. Object
validates schema, nonnegative size up to 128 MiB and a non-nil opener. Catalog
limits are checked again when handling a lease. If a handler returns both a
result and an error, the error wins; do not upload the result, but still clean
up any returned ObjectSource once.

A result-producing handler can return Inline(schema, bytes), or
Object(schema, source). The object opener must return the same immutable
bytes on every invocation and honor cancellation; it allows bounded upload
retries without retaining an entire video in memory. Do not delete the file in a handler defer or when the handler context ends:
finalization uses a different context. After a valid object result is returned,
the SDK owns its source until upload/outcome resolution stops and calls Cleanup
once on every exit path. Each opened reader is closed separately. If result
construction fails or the handler never returns it, the handler retains cleanup
responsibility. Cleanup errors are observed but cannot undo a committed result;
cleanup must be bounded/cooperative and temporary-directory quotas remain a
service responsibility.

Result validation failure never becomes a successful completion. Report it as
a permanent contract failure under platform policy, with no payload in logs.
Unknown handler errors default to retryable. Permanent(nil)/Retryable(nil)
return nil. A panic becomes a sanitized permanent failure and an alert; isolate
resource-unsafe code in a subprocess. The SDK does not infer provider success
from nil HTTP transport error alone.

## 7. SDK execution and shutdown state machines

```text
Created -> registrations frozen -> Authenticating -> Running -> Draining -> Stopped
                                                   |
                                      session rollover (new intake,
                                       old attempts drain separately)

slot: polling -> executing -> uploading -> finalizing -> released
```

One local permit covers an outstanding poll, handler, upload or outcome retry.
Across all sessions:
`polls + executing + uploading + finalizing <= Slots`.
No detached handler or unbounded goroutine per retry. Registration does not
authorize the SDK to claim more server-side capacity than the registered cap.
A lost lease reply can leave server capacity occupied without a local handler;
distributed session tokens prevent new polls bypassing that limit.

Start heartbeat and monotonic safety timers before invoking a handler. Give
the handler the earlier handler-deadline context, not the finalization context. Verify
the exact registered handler and payload/result schema before invocation;
unknown/mismatched leases are reported and never executed. Call the handler
once per received lease, freeze its result/failure, then keep heartbeat alive
while uploading and finalizing. Never call the business handler again to retry
CompleteTask. A redelivery is a separate attempt with the same effect key.

An execution context is not the Run intake context. Cancelling Run stops
new polls immediately and cancels outstanding poll RPCs. ShutdownGrace is the
overall shutdown deadline from that cancellation, not just a handler budget.
Until it expires, active work keeps heartbeat and finalization intact.

Before that deadline, best-effort FailTask(CANCELLED) is allowed only if no
outcome was previously frozen/issued and ownership remains confirmed. Once
Complete or Fail is frozen, shutdown never changes its operation/digest.
Retry only that exact outcome while authority/grace remain. After lease
rejection or local safety-budget exhaustion, resolve an earlier ambiguous
reply only via receipt_only, bounded by its 30-second resolution budget and
the remaining shutdown grace. These lookups never extend shutdown.

At grace expiry cancel handler/subprocess, upload, heartbeat and outcome/lookup
RPC contexts. Send no new mutation or receipt query afterward and start no
detached retry. Unfinished attempts recover normally; return ErrShutdownTimeout
if the drain is incomplete. Transport cancellation is not proof of DB rollback:
an already-issued Complete may still have committed. Never replace it with a
new Fail to guess the outcome. Actual claimed attempts, including shutdown
cancellations/timeouts, count toward max_attempts; admission deferrals do not.

Go cannot kill a handler that ignores context. Keep its permit occupied; do
not launch replacement work over the configured limit. Return a shutdown
timeout after the grace period, mark readiness false, and let the process
supervisor terminate the process. Document this operational requirement.
No guarantee permits an uncooperative handler to continue external effects
safely after lease loss.

Run returns nil after a successful requested drain, a typed startup/config
error on an invalid contract, and a typed shutdown-timeout error on incomplete
drain. Transient reconnects are observed, not returned as false task success.
Infrastructure outages cancel attempts at confirmed expiry; they do not cause
infinite keepalive or unbounded in-memory retries.

## 8. Results, uploads and storage ownership

No output is a distinct allowed result, not an empty object reference. An
empty inline value is still a schema-bearing inline result. Handler and
Gateway both check pinned schema/size; the server is authoritative.

Object-backed output is optional per catalog revision. Gate route activation
on a configured storage adapter supporting versioned writes, exact-version
reads/copies, and immutable final objects. Issue one attempt-scoped staging
grant at lease time, expiring after the attempt deadline plus 60 seconds.
Only lease work whose maximum duration fits the storage grant's provider
limit and session hard lifetime. There is no grant-refresh RPC in v1.

V1 uses a quota-enforcing HTTPS artifact frontend within the Gateway boundary,
not an unrestricted direct-store PUT URL. Its opaque grant authorizes only the
staging object. Validate grant/owner-session status, reserve tenant staging
quota durably, allow one concurrent write and at most three write attempts,
and enforce max_size_bytes while streaming, including chunked bodies. Abort
oversized uploads and incomplete multipart writes. Reserve up to three maximum
versions per grant; release quota only after physical cleanup is verified.
Workers have no general object-store write credentials. Separate upload
connection/buffer budgets from gRPC heartbeat/control capacity; no DB connection
is held while streaming. A native storage adapter may replace this frontend
only after proving equivalent byte, write-count and quota enforcement.

The SDK PUTs to the exact HTTPS grant URL with its pinned content type, limits
bytes while streaming, computes SHA-256 and verifies exact byte count. It
does not follow redirects or attach mTLS/session headers to object storage.
Grant hosts/storage configuration come from trusted deployment policy.
Never log/persist signed URLs. Retry uploads with the same opener only within
the attempt's safe remaining budget. A retry must upload identical bytes.

On CompleteTask, the Gateway:

1. Checks grant ownership, expiry, exact staging key, schema, bytes and hash.
2. Captures an immutable storage version and verifies its real bytes/checksum,
   using the trusted frontend's streamed hash or storage-verified checksum,
   never a worker-controlled metadata header.
3. Copies that exact version to a server-owned immutable final key; verifies
   final size/hash. An idempotent seal key is derived from attempt + outcome.
4. Commits only the final key/version and metadata with the outcome receipt.

External verification/copy happens outside a long-held DB transaction; the
final transaction re-checks grant, lease, deadline and authorization. If
ownership was lost, reject completion and clean the unreferenced final object
later. A repeat completion first checks the receipt and does not re-read an
expired staging object. The staging reference in the wire request is mapped
to the committed immutable final reference; callers cannot overwrite results
through a still-live upload URL.

A finalization RPC may run up to 60 seconds while independent heartbeats
renew the lease; its live safety monitor cancels the RPC if confirmed authority
expires. Unlike control RPCs, it is not clipped once to the initial 20-second
lease TTL. Route activation must prove its maximum object can be verified and
sealed within this bound and the attempt deadline under the supported storage
profile. Otherwise reduce the catalog object limit or keep that route disabled;
do not accept results that can never finish verification.

Orphan cleanup uses a grace period beyond grant expiry and checks references
before deletion. Committed result retention follows job history, minimum seven
days in v1. Tombstone under a DB lock before deletion, stop new download grants,
then hold the object through the latest issued grant expiry plus a bounded
five-minute read grace. Grant issuance records that expiry under the same lock;
the artifact frontend enforces the read-duration bound. This prevents cleanup
from racing a supported live read. Reads use an authenticated tenant-scoped
task/status API; download grants target only the recorded final version. No
public bucket URLs in status.

### REST result reads

S3-03 owns the REST models, handlers, scope checks, repository reads and artifact
frontend integration as well as worker completion. The Go worker SDK does not
become a public submission/download client.

GET /api/v1/tasks/:id requires tasks:read and tenant ownership. Its existing task
response gains a result object with exactly one of these shapes:

| result.state | Required fields and meaning |
| --- | --- |
| not_available | No other result fields; task has not completed successfully |
| none | Successful NoResult; no schema, bytes or grant |
| available + storage=inline | schema, size_bytes, sha256, data_base64, retained_until |
| available + storage=object | schema, size_bytes, sha256, retained_until, download_grant_path |
| expired | Result tombstoned/retention ended while task metadata still exists; no bytes or grant |

schema has content_type, name and numeric version. size_bytes counts decoded
bytes; sha256 is 64 lowercase hexadecimal characters. Inline bytes use standard
padded Base64, never an ambiguous raw JSON/string interpretation. Object keys,
bucket names, storage versions and upload URLs remain server-private.
retained_until is an RFC3339 UTC timestamp; it is the earliest retention boundary,
not a promise to keep issuing grants after tombstoning. Task/status returns 200
for an owned task, including none/expired; missing or foreign tasks return 404.
Task JSON responses allow up to 128 KiB to fit a 64 KiB inline result after
Base64 plus metadata. The 64 KiB POST-body/stored-write-response limit is unchanged.

Example inline result fragment (the surrounding task fields are omitted):

```json
{
  "result": {
    "state": "available",
    "storage": "inline",
    "schema": {"content_type": "text/plain", "name": "reference.echo", "version": 1},
    "size_bytes": 5,
    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
    "data_base64": "aGVsbG8=",
    "retained_until": "2026-10-26T12:00:00Z"
  }
}
```

For an object result, download_grant_path is
/api/v1/tasks/:id/result/download-grant. GET that path requires tasks:read,
current API-key validity and tenant ownership, and returns 200 JSON:

```json
{
  "url": "https://artifacts.example.invalid/read/example-grant-token",
  "expires_at": "2026-10-19T12:01:00Z"
}
```

A grant lasts at most 60 seconds from issuance, clipped to issuing key expiry.
GET issues fresh read authority without changing any job/result outcome;
it does not require Idempotency-Key and never returns a cached POST receipt.
Both status and grant responses use Cache-Control: no-store; never put a grant
URL in task status, logs or persisted REST response bodies.

Grant issuance and result tombstoning lock the same result row. Record only a
grant-token HMAC, issuing key/tenant/result/version and expiry; advance the
result's latest-grant expiry before returning the URL. Retain the object through
that expiry plus the enforced five-minute maximum read duration and the safety
margin. Rate-bound issuance and prune expired grant records only after their
read/cleanup horizon; repeated GETs cannot create permanent grant history.

The frontend authenticates the opaque grant at read start using DB time,
re-checks the issuing key's expiry/revocation/tasks:read scope and result tenant,
and serves only the recorded immutable version. No API key/session/lease secret
is sent to the artifact URL. An already-issued grant may read a tombstoned result
within its existing hold; tombstoning blocks new grants, not valid protected
reads. Expired/revoked grants return generic 401; an accepted read is forcibly
bounded to five minutes. This URL is delegated read authority, not a public
bucket or a Queue Platform worker credential.

For the grant endpoint use the existing JSON error shape with stable error
strings: 409 result_not_ready for nonterminal tasks, 409 result_unavailable for
FAILED/CANCELLED, 409 result_not_object for successful none/inline, 410
result_expired for an expired object result, 503 result_storage_unavailable
when backing storage cannot serve it. Authentication is 401, missing scope 403,
and missing/foreign task 404 before result inspection. A retryable 503 includes
Retry-After: 1. No signed URL is returned on these failures.

Required S3-03/C21 evidence: binary inline round-trip/hash/size, every result
state, full-size Base64 response, object grant authorization and issuer-key
revocation, foreign-tenant 404, expired grant 401, tombstone 410, storage 503,
and grant-issuance versus deletion races. S5-03 repeats the storage/read-lifetime
cases with the real artifact adapter.

## 9. Delivery, recovery and latency design

Relay claims commit short durable ownership leases using the existing
ClaimPendingOutbox mechanism; Redis I/O is outside the claiming transaction.
Publish at least once, then conditionally mark published with the exact relay
token. A stale relay cannot mark another relay's work complete.

Use PostgreSQL NOTIFY as a wake-up hint only, plus a 250 ms initial fallback
poll. Drain successive due batches (initial batch 100) until empty or a bounded
fairness yield; do not wait another tick after a full batch. Pipeline bounded
Redis publishes and batch conditional PostgreSQL marks. Preserve per-entry
success/unknown outcomes; a lost pipeline response causes safe duplicates,
not optimistic marking. No DB transaction waits for a long poll or network
publish. Bound outstanding batches and pause claims when Redis is unhealthy.

One batch of 100 per second caps one current relay at roughly 100
publications/second and adds polling delay; it is not a fixed one-second
minimum latency. Benchmark the replacement under the same durability profile,
including backoff and recovery, before setting a throughput target.

Redis ConsumeDelivery must check errors before treating an empty response as
NoTask. Expected duplicate/stale/not-ready/admission outcomes do not count as
dependency failures in the circuit breaker; consecutive infrastructure
failures do, and a successful dependency call resets the streak. Breaker
half-open probes are bounded; it is not a substitute for request deadlines.

### Broker loss and retention

A published flag does not prove Redis will retain data through a crash or
failover. On broker restart/group recreation, and periodically thereafter,
a bounded cursor-based DB reconciler revisits QUEUED jobs whose last dispatch
is older than the configured redelivery horizon (initially 60 seconds). Under
the workflow/job locks, if still QUEUED and due with no active attempt and no
live relay owner, re-arm the existing current-generation publication using the
same delivery ID and persisted route. Record next-check time; normal repair
does not increment generation or reset original due age. Thus a valid older
copy remains claimable even during a long backlog, rather than being repeatedly
invalidated and pushed to the tail. Claim fencing permits one live owner even
if both copies arrive. Bound background repair to at most 5% of configured
publication capacity; broker-loss catch-up has an explicit bounded recovery
budget. Validate old Stream/PEL entries against the DB before ACK. Recovery of
RUNNING jobs additionally requires expired or revoked ownership and creates a
new generation; PEL idle alone never proves that.

For one platform-owned group per capability stream, ACK then delete known
resolved physical entries in bounded batches, with durable repair for partial
failure. A repair is complete only after both XACK and XDEL succeeded or the
entry/group absence or broker-epoch retirement was verified; XACK success alone
cannot drop cleanup debt. Never MAXLEN-trim unread/pending entries as a capacity shortcut.
A bounded cleanup pass also removes terminal/stale duplicates left by crashes.
Adding a second independent consumer group requires redesigning deletion
around all groups; it is not a config-only change.

Retain outbox rows needed for current-generation validation until the job
moves beyond them; archive/delete resolved history in indexed bounded batches.
Keep receipts/history/results at least seven days, REST submission receipts
24 hours, unreferenced uploads until expiry plus a one-hour grace, and unresolved
ACK/DLQ repair until resolved or audited broker-epoch retirement. Pin business
idempotency evidence at least as long as retries and permitted manual replay;
never let platform cleanup silently erase the provider's deduplication window.

Depth metrics distinguish due QUEUED, delayed QUEUED, RUNNING, dependency-blocked,
PEL, Stream history, ACK debt and oldest due age. XLEN is not ready depth;
a constant zero is not a delayed-depth implementation.

## 10. Business workers and Saga

A local participant transaction inserts/checks an inbox unique on stable effect
key, applies its local business change, and stores its outcome/outbox together.
Task.TenantID comes from the authenticated persisted job, not payload fields.
Scope business-object lookup and participant inbox uniqueness by that tenant
plus effect key; reject an object belonging to another tenant before any side
effect. A globally privileged worker must not infer authority merely from a
payload object ID. Duplicate commands return the stored outcome. For remote providers, use the
supported strategy in section 1; a transactional outbox does not close the
provider-accepts/crash-before-record window by itself.

The SDK never exposes nextStep/compensate. SagaID and StepID are optional
context; the platform records durable forward outcomes and schedules approved
compensation task types. Compensation has its own stable effect key, distinct
from the forward action, and reuses that key across compensation retries.

The Saga record is the orchestration container. Each forward or compensation
command owns a separate already-sealed ordinary single-task workflow, created
and activated in its orchestration transaction without the public draft round
trips. Saga-step records hold
the cross-service dependency graph. Their tasks share SagaID/StepID context,
not cross-workflow parent edges. Commit step intent, command workflow/job and
outbox together with unique (saga, step, phase) identity. A compensation never
appends to or reopens a terminal ordinary workflow. The orchestrator acquires
Saga lock before command-workflow locks in sorted order; ordinary lifecycle
transactions write durable Saga outcome events and do not acquire Saga locks.
The orchestrator deduplicates those events before advancing step state.

Stop scheduling new forward steps on Saga failure; cancel/fence active work,
then settle or reconcile uncertain forward effects before deciding what to
compensate. Compensate successful reversible steps in reverse dependency order
of the approved DAG, not merely reverse wall-clock completion order.
Irreversible actions such as email need a declared pivot/no-compensation
policy. A failed compensation stays durably COMPENSATION_FAILED with alerts
and an audited operator retry/resolve flow; do not report the Saga as rolled
back or automatically restart an irreversible forward action.

## 11. Folder structure and programming patterns

Proposed ownership, not a request to generate these directories now:

```text
api/proto/queue/v1/worker.proto   # wire source; generators pinned in build tooling
api/gen/queue/v1/                # generated transport code, never hand-edited
cmd/api/                        # composition root: REST only
cmd/worker/                     # relay/recovery composition root; remove demo producer
cmd/gateway/                    # private worker RPC composition root
cmd/admin/                      # operator-only key/catalog/remediation tools
internal/api/{handlers/v1,service,middleware}/
internal/auth/                  # REST keys, scoped principal and tenant policy
internal/execution/             # immutable catalog, profiles and route validation
internal/gateway/               # auth/session/RPC adaptation; calls lifecycle repository
internal/saga/                  # Sprint 6 orchestration only
internal/domain/                # shared platform states and transition rules
pkg/repository/                 # existing DB boundary; all lifecycle transactions
pkg/redis/                      # existing broker adapter; no business handlers
sdk/go/worker/                  # public API, private lifecycle/transport/result code
tests/conformance/              # black-box SDK/Gateway contract tests
examples/email-worker-go/       # service-owned inbox/provider integration example
examples/media-worker-go/       # isolated subprocess and object-result example
db/migrations/                 # additive forward migrations and rollback guidance
deploy/                        # process/resource/TLS/observability configuration
```

Go's internal-package boundary prevents SDK consumers importing platform
internals. Keep business services' own domain/inbox/provider code in their
service repositories; the examples teach those boundaries. Do not share ORM
models or a mutable database across business microservices. Retain pkg paths
already used here rather than undertaking an unrelated folder rewrite.

| Programming step | Pattern to apply | Concrete boundary, not extra framework |
| --- | --- | --- |
| Parse config and wire dependencies | Composition root | cmd only; constructors receive explicit dependencies |
| Authenticate and authorize | Middleware/interceptor + immutable principal | Validate once at ingress and re-check authority inside fenced writes |
| Resolve task type | Immutable registry/Strategy | Catalog chooses policy/route; caller never chooses a queue |
| Validate state changes | State machine + compare-and-set fencing | Central transition functions; legal predecessor + generation + token |
| Commit job and dispatch intent | Transactional Outbox / Unit of Work | One repository transaction, not multiple independently committing calls |
| Bound concurrency | Bulkhead + durable semaphore | Local SDK permits plus shared platform admission tokens |
| Retry dependency failures | Bounded retry + circuit breaker | Separate infrastructure failures from semantic outcomes |
| Deduplicate outcomes/effects | Receipt + Inbox/Outbox | Platform receipt and business effect key solve different boundaries |
| Adapt remote execution | Ports/adapters | Gateway translates wire to lifecycle calls; SDK hides generated messages |
| Coordinate workers on failure | Structured concurrency | Separate intake/execution contexts, bounded drain and joins |
| Persist large output | Claim check + immutable artifact | Staging grant -> verified sealed result; no untrusted URL fetch |
| Coordinate cross-service rollback | Orchestrated Saga | Durable platform commands, participant-owned transactions |

Do not introduce a generic Repository/Factory/EventBus abstraction for every
file. A pattern is useful only where the table names an actual invariant.

## 12. Conformance and release gates

Each case runs against a deterministic fake Gateway where suitable and real
PostgreSQL/Redis/Gateway for storage/security assertions. Fake-only passing
does not prove fencing, isolation or crash durability. Story IDs are in the
sprint plan.

| ID | Required executable evidence | First gate |
| --- | --- | --- |
| C01 | No client cert, wrong CA/server name and spoofed service metadata rejected | S3-01 |
| C02 | Cross-principal/cross-session/wrong-token lease mutations rejected | S3-01 |
| C03 | Soft expiry, renewal, revocation and hard-expiry rollover across replicas | S3-01, S4-01 |
| C04 | Lost lease reply invokes no unknown handler; unseen leases consume admission | S3-02 |
| C05 | Duplicate generations preserve one stable effect key and one current claim | S2-01, S3-02 |
| C06 | Lost Complete/Fail reply replays one immutable receipt; changed outcome conflicts | S3-03 |
| C07 | Rejected/dropped heartbeat cancels at last confirmed expiry, not indefinitely | S3-04 |
| C08 | Unknown capability/version/schema never runs; mixed-capability pool cannot leak tasks | S3-02 |
| C09 | Slots bound polls/execution/upload/finalization across session rollover; authorized-attempt caps survive replica races | S2-02, S4-01 |
| C10 | Empty/inline/object limits, handler/finalization budgets and cleanup ownership; hash mismatch, wrong grant, overwrite and expired grant rejected | S3-03 |
| C11 | Retryable/permanent/cancelled policy, deadline and attempt budget applied once | S2-01, S3-04 |
| C12 | Gateway crash after dispatch/DB commit preserves ownership, result and ACK repair | S3-03 |
| C13 | Provider accepts then worker crashes: duplicate uses stored/reconciled business outcome | S4-02 |
| C14 | Compensation has a distinct stable key, ordinary fencing and reverse-DAG ordering | S4-02 fixture; S6-03 end-to-end |
| C15 | Drain keeps heartbeat/upload/finalize alive; grace expiry cancels child process without slot leak | S4-01, S5-01 |
| C16 | Clock offset, delayed replies, receipt-only lookup cannot mutate a live lease, and expired receipt credentials fail closed | S3-04 |
| C17 | Redis errors are visible; acknowledged history cleaned; broker/group loss repaired from DB | S2-04 |
| C18 | Tenant REST auth, concurrent idempotency replay and dependency ownership | S1-02, S1-03 |
| C19 | Server-derived TenantID reaches the handler; payload cannot override it; participant rejects another tenant's object | S3-02/S3-04 envelope; S4-02 business fixture |
| C20 | No execution/outbox in DRAFT; seal/append/expiry races; seal replay publishes each root once | S1-03, S2-01 |
| C21 | REST result union/Base64 and tenant-scoped grants; revoked/expired reads and tombstone/deletion holds | S3-03, S5-03 |
| C22 | 60-second completion versus 30-second shutdown never switches frozen outcome; lookup cannot extend grace | S3-04, S4-01 |

S3 is an internal authenticated vertical slice, not a public SDK release. S4
release requires C01–C22, except actual media subprocess evidence in S5 and
the complete Saga engine in S6; their SDK behaviors still need synthetic
fixtures at S4. Disabled object/result/capability modes cannot be advertised
as supported before their corresponding tests pass.

Release checklist: reproducible code generation; proto breaking checks;
Go unit/race/vet and real integration/conformance CI; clean external-module
example build; API reference and error/cancellation cookbook; changelog;
minimum Go version and supported SDK/server compatibility matrix; security
review; on-call ownership and rollback drill. No production-readiness claim
comes from documentation alone.

## 13. Performance evidence and rollout

Measure separately: API acceptance, outbox commit-to-publish, due-to-handler
start, handler runtime, outcome commit and end-to-end completion. Scheduled
wait is not relay latency. Report p50/p95/p99, throughput, error/duplicate rate,
oldest due age, recovery time, DB lock/WAL/pool wait and broker memory.

S1 records a baseline; S2 measures relay/lifecycle changes; S4 repeats through
mTLS/Gateway/SDK; S5 proves mixed-load isolation; S7 publishes the capacity
envelope. Use open-loop offered-load sweeps and report unfinished work, not
only completed-request latency. A repository INSERT benchmark is not queue
end-to-end performance. Do not claim parity with Temporal, River or BullMQ
without a reproducible equivalent workload and durability/feature profile.

Roll out additively: expand schema, backfill explicit legacy tenant and route
assignments, verify counts, deploy dual-read compatible platform, enable one
canary route, then expand. Never guess ownership of existing data or generate
credentials for an unknown owner. Unknown legacy rows block cutover until an
operator mapping is provided. Suspend legacy writes during the final mapping
window. Prevent old unauthenticated API/old worker processes from continuing
after authenticated cutover.

Rollback stops new-route intake and drains/fences current leases; it does not
route remote tasks back to the simulator or remove security. Keep expanded
columns/old handlers until accepted work drains. Destructive down-migrations
are not the production rollback strategy.

## 14. Technical references

The queue-claim design uses the queue-table use case of
[PostgreSQL 14 SKIP LOCKED](https://www.postgresql.org/docs/14/sql-select.html#SQL-FOR-UPDATE-SHARE);
it is not an ordering/fairness guarantee. [Redis XACK](https://redis.io/docs/latest/commands/xack/)
acknowledges PEL ownership, not Stream-history retention. Explicit bounded
deadlines and operation-aware retries follow the
[gRPC deadline](https://grpc.io/docs/guides/deadlines/) and
[gRPC retry](https://grpc.io/docs/guides/retry/) guidance.
