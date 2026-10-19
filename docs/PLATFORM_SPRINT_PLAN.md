# Queue Platform: Prioritized Sprint Plan

Status: planned engineering work, 19 October 2026. All checkboxes below are
uncompleted requirements, not claims about implemented behavior. This plan
does not ask for implementation in the current documentation change.

Canonical SDK contract: [WORKER_SDK_DESIGN.md](WORKER_SDK_DESIGN.md).
REST security contract: [REST_API_AUTH_DESIGN.md](REST_API_AUTH_DESIGN.md).
This file is the project's sprint plan; keep these links as the single source
rather than maintaining a second divergent SPRINT_PLAN.md.

## 1. Delivery strategy

Build a reliable Go queue and authenticated Go worker SDK before Saga and
scale work. Optimize measured scheduling overhead, not just database INSERTs.
Temporal, River and BullMQ are comparison points, not interchangeable feature
sets or evidence of this platform's capacity.

Planning assumption: two-week sprints, four-to-six engineers, approximately
25–35 engineering days of committed work per sprint after review/on-call/test
work. Estimates below are initial engineer-day ranges, not delivery promises;
dependencies and correctness gates take precedence over the sprint calendar.
If measured work exceeds capacity, move whole dependent stories to the next
sprint. Do not reduce security, fault tests or durability to preserve a date.

| Lane | Suggested staffing | Owns |
| --- | --- | --- |
| Platform (P) | 2 engineers | catalog, API/storage lifecycle, relay, Gateway, Saga |
| SDK/application (W) | 1–2 engineers | Go public API, worker lifecycle, business examples |
| Reliability/security (R) | 1–2 engineers | CI, independent fault/conformance evidence, performance and rollout |

One named engineer owns each story end to end and supplies tests, docs and
rollout notes. The listed lane is accountable, not permission to hand off
untested code. Platform and SDK owners jointly approve wire/data decisions;
a different engineer reviews security and state-machine changes.

### Priorities and release boundaries

- P0: prevents unauthorized work, data loss, false success or unsafe retry.
  Blocks the affected release.
- P1: required worker usability, bounded operations and measured latency.
  Required before supported SDK/production release.
- P2: capacity tuning and comparative evidence after correctness.
- Sprints 1–2: internal correctness/performance foundation.
- Sprint 3: authenticated internal Gateway/SDK vertical slice.
- Sprint 4: supported SDK release candidate and one real low-risk worker.
- Sprint 5: media isolation validation.
- Sprint 6: Saga feature release candidate, separate from basic queue release.
- Sprint 7: operational/capacity sign-off, not a promise of unlimited scale.

Existing primitives include durable outbox generations, DB leases,
ClaimPendingOutbox and protobuf definitions. Reuse them; do not mark the
corresponding stories done without proving the connected flow. Existing
deleted placeholder packages and unrelated worktree edits are not an
instruction to recreate them.

### Definition of done for every story

- Check in the expected artifact and link the relevant SDK/REST contract.
- Test success, boundary, concurrent race, retry and relevant crash window.
  Never postpone a security/correctness test to a later sprint.
- Interpret concurrency caps as bounds on authorized attempts, not proof that
  a fenced/uncooperative worker has physically stopped external effects.
- Preserve persisted routing, tenant ownership, lease fencing, stable effect
  keys and bounded resource use.
- Add diagnostics without secret/payload logging or unbounded metric labels.
- Include migration/backfill/rollback notes when storage or accepted API
  behavior changes. Expand/contract; never edit deployed migrations.
- Provide reproducible commands and actual evidence in the story/PR. An
  integration test skipped for missing environment is not a passing gate.
- Review caller, repository, broker, config, examples and docs together where
  the change crosses those boundaries. Do not add an abstraction per file.

## 2. Sprint 1 — Safe boundaries and reproducible baseline

Goal: eliminate demo ambiguity, establish REST trust, freeze the remote-worker
contract, and measure the actual system.

### S1-01 — Immutable catalog and persisted server routing

P0 · Owner P · 5–7 days · Dependencies: none.
Scope: internal/execution, API models/services, repository, additive migration,
current cmd/worker reference-executor wiring.

Tasks and expected outputs:

- [ ] Define static catalog revisions and schemas exactly as SDK section 2;
  persist each accepted job's route, handler version, policy and stable effect
  key. Unknown/oversized payloads create no job or outbox row.
- [ ] Reject queue_name, max_retries, TTL and tenant overrides at the REST
  boundary; return persisted defaults/state in responses.
- [ ] Add DRAFT/sealed_at/draft_expires_at workflow state. Draft task creation
  pins policy and records PENDING dependencies without an outbox or execution;
  seal, not append, gates worker health and activation.
- [ ] Provision bounded capability streams/groups from server policy, not user
  input. Keep old routes until historical jobs and repair debt drain.
- [ ] Remove automatic producer and simulated arbitrary-task success. Add only
  reference.echo:v1 for the baseline; email.send remains disabled until a real
  idempotent provider path is delivered in S4-02.
- [ ] Add an expand/backfill plan with operator-provided legacy tenant/route
  mapping. Unknown ownership blocks cutover; never assign all old data to an
  arbitrary tenant. Prevent old writers from bypassing the new contract.

Patterns: immutable registry/Strategy, composition root, transactional outbox.
Expected result: every job has one trustworthy historical execution contract.
Acceptance evidence: child/retry/recovery use their own persisted route across
a catalog change; unknown type/override/oversize rejected; no simulator or
background demo producer runs in a normal deployment.

### S1-02 — REST authentication, scopes and tenant ownership

P0 · Owner P, security review R · 4–6 days · Dependencies: S1-01 schema agreement.
Scope: internal/auth, API middleware/services, repository, migrations, admin CLI.

Tasks and expected outputs:

- [ ] Implement REST contract key IDs, random secrets, versioned HMAC storage
  and constant-time validation; operator-only issuance prints a secret once.
- [ ] Derive tenant from the authenticated key, enforce every route's declared
  scope including workflow seal, and tenant-scope every read/write/dependency
  lookup. Reserve result-read scope coverage for the S3-03 endpoints.
- [ ] Return generic 401, scope 403, foreign-object 404; use errors.Is for
  wrapped not-found errors instead of direct equality.
- [ ] Require TLS/trusted ingress; leave only health public. Add rotation,
  immediate cross-replica revocation and redacted audit events.
- [ ] Define ingress authentication-abuse limits and deny unsafe startup
  configuration. Do not use a local auth cache as the security boundary.

Patterns: middleware + immutable principal; tenant-scoped repository.
Expected result: unauthenticated/cross-tenant requests cannot reach work data.
Acceptance evidence: REST tests 1–6 and C18's authentication/tenant subset
against two API replicas; wrong
secret/expiry/revocation reveal no credential detail.

### S1-03 — Transactional submission idempotency and accurate responses

P0 · Owner P/W · 4–6 days · Dependencies: S1-01, S1-02.
Scope: API middleware/services/handlers, repository, migrations and API docs.

Tasks and expected outputs:

- [ ] Enforce bounded body/header parsing and hash exact request bytes before
  binding; require Idempotency-Key only with the complete replay store.
- [ ] Atomically claim (tenant, method, exact path, key), create DRAFT workflow
  or pending draft task, and store the response. Those writes create no outbox;
  concurrent losers replay the same accepted response.
- [ ] Implement POST /api/v1/workflows/:id/seal with workflows:write and body {}:
  lock the draft, check expiry/nonempty graph/pinned-route health, initialize
  job-age deadlines and atomically activate roots/outbox plus stored response.
  Same-key replay precedes workflow/health re-evaluation after current auth.
- [ ] Return 409 only for changed bytes in that same scope. The same key on a
  different path is independent; re-check current authorization on replay.
- [ ] Persist actual response defaults/state, including workflow DRAFT,
  sealed_at/draft_expires_at and task PENDING until seal; never reconstruct
  incompatible state/defaults from request fields.
- [ ] Add indexed, bounded 24-hour idempotency cleanup and expiry documentation.

Patterns: idempotent consumer/receipt + Unit of Work.
Expected result: callers can assemble fast-task DAGs safely, and network
retries cannot create duplicate jobs or activate roots twice.
Acceptance evidence: a fast root cannot execute before its child is submitted
and seal commits; draft writes produce zero publications, concurrent seal
replay produces one per root. Different-key reseal/late append is 409; empty
seal is 400; expired draft cannot activate; unhealthy seal is retryable 503
without writes. C18/C20 auth/idempotency cases and body/janitor limits pass;
S2-01 completes the lifecycle/expiry race matrix.

### S1-04 — Safe integration harness and multi-consumer baseline

P0 · Owner R · 3–5 days · Dependencies: catalog reference route from S1-01;
full authenticated benchmark also requires S1-02/S1-03.
Scope: repository/Redis tests, CI, benchmark harness and evidence report.

Tasks and expected outputs:

- [ ] Use disposable databases or isolated schemas and uniquely named streams.
  Remove shared-database TRUNCATE/default developer credentials from tests.
  An unsafe target fails before any migration or cleanup.
- [ ] Write the SKIP LOCKED multi-consumer test: several concurrent relay
  claimers race on due rows, with a start barrier, disjoint ownership and full
  coverage assertions; locked/not-due rows are excluded.
- [ ] Add relay-token expiry/reclaim tests: old owner cannot mark published,
  crashed ownership becomes claimable, and duplicates do not become two claims.
- [ ] Benchmark authenticated draft/task assembly -> seal commit -> outbox ->
  Redis -> reference handler -> outcome. Record graph-assembly/API acceptance
  separately from sealed-root due-to-start; never wait for execution before
  sealing. Keep repository microbenchmarks separate.
- [ ] Add CI unit/race/vet and real DB/Redis integration jobs; missing service
  prerequisites fail the integration job rather than silently skip it.

Patterns: isolated fixtures, barrier-based concurrency tests, stage tracing.
Expected result: reproducible throughput/latency and ownership evidence.
Acceptance evidence: baseline records fixture/hardware/durability and offered
load; race detector and multi-relay assertions pass repeatedly. The basic
claim race test may take half a day; the full safe harness/baseline is not a
half-day story.

### S1-05 — Protocol/data-model freeze gate

P0 · Owner P + W, independent R review · 3–4 days · Dependencies: catalog,
auth and baseline design; can draft in parallel.
Scope: worker.proto, build/generation CI, SDK design and schema ADR.

Tasks and expected outputs:

- [ ] Reconcile existing six RPCs with SDK sections 3–8; add server-derived
  tenant_id, ResultContract, server_time, handler_deadline and non-mutating
  receipt_only fields additively.
  Freeze validation limits and outcome-digest
  encoding, deadline semantics and result-upload lifetime.
- [ ] Approve mTLS policy, session HMAC/expiry/rollover, capability streams,
  distributed tokens, immutable receipts and durable ACK repair together.
- [ ] Record indexes/constraints, lock order, retention and crash boundaries.
  Define how each outcome/recovery path releases its exact admission tokens.
- [ ] Pin generators, generate reproducible Go stubs in the implementation
  story, establish a reviewed proto baseline and run lint/build/breaking CI.
- [ ] Approve Task.TenantID and tenant-scoped participant ownership checks,
  handler (Result,error), result-source lifetime and immutable-outcome shutdown.
- [ ] Freeze public DRAFT/seal and REST result union/download-grant contracts,
  scopes, errors and retention holds. List C01–C22 owners/fixtures.

Patterns: contract-first design, state machine, ports/adapters.
Expected result: one signed-off contract before remote execution is built.
Acceptance evidence: make proto-check and breaking comparison pass; review
walks through provider-accept/crash, lost RPC reply, session rollover and
cross-capability dispatch. Compilation alone cannot satisfy this story.

Sprint 1 exit: safe test targets, authenticated/tenant-scoped idempotent REST,
persisted catalog routing, honest reference execution, baseline report and
approved wire/data contract. Existing unauthenticated deployments are not
production-ready while this gate is incomplete.

## 3. Sprint 2 — Atomic lifecycle, bounded admission, faster delivery

Goal: correct the existing path before moving execution across a network.

### S2-01 — One atomic lifecycle and dependency policy

P0 · Owner P · 5–7 days · Dependencies: S1-01, S1-04, S1-05.
Scope: repository transitions, domain states, new migration/indexes, worker path.

Tasks and expected outputs:

- [ ] Centralize success, retry and terminal-failure transactions. Commit
  receipts/child outbox/workflow state/DLQ intent and token release together;
  no separate post-commit cascade or workflow-status update. Failure cancels
  descendants, not unrelated branches; the workflow stays active until all
  existing jobs settle, then FAILED outranks CANCELLED and COMPLETED. Explicit
  operator cancellation fences all unfinished work and closes appends.
- [ ] Use the documented workflow/job lock order and generation/token fences.
  Heartbeat and every outcome reject expired/stale ownership.
- [ ] Permit append only in unexpired DRAFT and reject every sealed append.
  Serialize append/seal/draft expiry/cancellation under the same workflow lock;
  reject missing/cross-workflow/cross-tenant parents and repeated edges.
- [ ] Cancel expired 24-hour drafts and pending jobs in a bounded janitor,
  retaining history. Seal checks DB expiry even before that janitor runs;
  job-age deadlines start at seal, never during drafting.
- [ ] Enforce 1,000 jobs/workflow and 32 parents/job, immutable dependency edges,
  max attempts and catalog job-age/handler/attempt deadlines. Worker CANCELLED
  means interruption/retry within budget, not silent terminal discard.
- [ ] Implement durable terminal/DLQ repair intent and idempotent retry
  generation creation; use each affected job's persisted route.

Patterns: fenced state machine, Unit of Work, transactional outbox.
Expected result: no committed terminal failure leaves a runnable descendant
or a falsely nonterminal workflow.
Acceptance evidence: two-parent completion, append versus seal, expiry versus
seal, terminal failure versus completion, and crashes at transaction boundaries;
C05/C11/C20 pass. No draft delivery can claim, sealed graphs never reopen, and
successful workflow terminal status is durable too. Internal DLQ replay creates
an already-sealed single-task workflow atomically with its publication.

### S2-02 — Distributed execution budgets and token reconciliation

P0 · Owner P · 3–5 days · Dependencies: S2-01 transition structure.
Scope: execution policy, repository admission/maintenance, migrations.

Tasks and expected outputs:

- [ ] Provision pool/service/tenant/workflow tokens and reserve session-token
  support for S3. Claim all required scopes transactionally with SKIP LOCKED;
  no historical COUNT(*) query on each execution.
- [ ] Roll back partial acquisitions on a full scope. Defer admission-rejected
  queued work at bounded rate without charging a business attempt.
- [ ] Release exact-owner tokens on every outcome, cascade, deadline and
  recovery; add reconciliation under the same locks.
- [ ] Implement cap increases/decreases without reassigning occupied tokens;
  expose occupied/retiring tokens and admission-wait metrics.

Patterns: bulkhead, durable semaphore, compensating repair.
Expected result: replicas cannot multiply a configured tenant/workflow cap.
Acceptance evidence: C09 races across replicas; crash/recovery/cascade does
not leak or double-release capacity; full tenant does not execute past its
cap or consume the email pool through a media route.

### S2-03 — Wake-and-drain outbox relay

P1 · Owner P/W · 3–4 days · Dependencies: S1-04 and persisted routing.
Scope: relay loop, repository relay methods, Redis adapter and metrics.

Tasks and expected outputs:

- [ ] Add commit-triggered NOTIFY wake hints and 250 ms fallback polling.
  Notifications are hints, never the durable delivery source.
- [ ] Drain full due batches immediately, starting at 100 with bounded yields;
  commit relay ownership before Redis I/O and conditionally mark by token.
- [ ] Pipeline bounded publishes and batch conditional DB marks while retaining
  per-item unknown/error handling; unknown publish means safe replay.
- [ ] Bound in-flight batches/connection use and stop claiming during Redis
  outage. Keep heartbeat/recovery pools from being starved by relay drain.
- [ ] Compare stages under the unchanged baseline durability profile.

Patterns: transactional outbox, bounded batching, backpressure.
Expected result: no one-batch-per-second throughput cap.
Acceptance evidence: lost notification, Redis partial publish, stale relay,
restart and multi-relay tests pass; measure p95/p99 improvement at the same
offered load. A one-second poll is polling delay, not a fixed minimum latency.

### S2-04 — Broker repair, retention and truthful depth

P0 · Owner R + P · 4–6 days · Dependencies: S2-01; coordinate with S2-03.
Scope: repository maintenance, broker adapter, migrations, metrics/runbook.

Tasks and expected outputs:

- [ ] Repair missing broker/group deliveries from due QUEUED DB records using
  bounded cursors and current-generation republication. Preserve original
  queue age; never invalidate older valid copies on every repair scan.
- [ ] Keep RUNNING recovery fenced by actual DB expiry/revocation, not PEL idle;
  create new generations only for real retry/lease-recovery transitions.
- [ ] Persist ACK/delete and DLQ repair; retain cleanup debt until both ACK
  and deletion/verified absence succeed. Check broker epoch/delivery identity
  so a recreated stream cannot cause deletion of a different job.
  For the single platform group, delete only DB-proven resolved physical
  entries, never unread/pending work by an arbitrary MAXLEN.
- [ ] Implement indexed batch cleanup for resolved outbox/history/receipts,
  preserving current-generation validation and seven-day history minimum.
- [ ] Report due/delayed/blocked/running/PEL/history/repair debt and oldest due
  age independently; fix delayed depth and stop calling XLEN ready backlog.

Patterns: reconciliation loop, idempotent repair, bounded retention.
Expected result: Redis/group loss cannot permanently strand accepted DB work;
history/repair growth is visible and bounded under the supported policy.
Acceptance evidence: C17 covers Redis restart, group loss, idle live lease,
cleanup crash between ACK/delete and backlog amplification under repair.

### S2-05 — Correct dependency-error handling

P0 · Owner W/R · 1–2 days · Dependencies: baseline fixtures S1-04.
Scope: circuit breaker, Redis ConsumeDelivery, caller classification/tests.

Tasks and expected outputs:

- [ ] Check Redis errors before classifying empty reads as NoTask; cover
  NOGROUP, backend disconnect and malformed messages distinctly.
- [ ] Exclude duplicate/stale/not-ready/admission outcomes from breaker
  failures; reset consecutive failures on real dependency success.
- [ ] Bound half-open probes, surface real outages and preserve typed wrapped
  errors through callers. No retry storm when a dependency is down.

Patterns: failure taxonomy, circuit breaker, bounded retry.
Expected result: healthy duplicates cannot open the DB breaker and Redis
failures cannot masquerade as an empty queue.
Acceptance evidence: duplicate bursts plus interleaved successes stay healthy;
injected consecutive infrastructure failures open/recover predictably.

Sprint 2 exit: atomic lifecycle/dependency/routing tests, hard global caps,
broker repair/retention and correctness tests pass. Relay p95/p99 is measured
against S1 under equivalent load; regressions need a profiled explanation and
fix before accepting the optimization.

## 4. Sprint 3 — Authenticated Gateway and SDK vertical slice

Goal: reference.echo runs in an independent Go process with no platform
storage credentials. Authorization, results and durable outcomes are part of
this gate, not deferred hardening.

### S3-01 — mTLS identity and durable worker sessions

P0 · Owner P, security review R · 5–7 days · Dependencies: S1-05, S2 exit.
Scope: cmd/gateway, internal/gateway auth/session, repository/migrations, TLS deploy.

Tasks and expected outputs:

- [ ] Implement principal-derived capability/service policy, mTLS-only ingress
  and Register/Renew with durable token HMACs and immutable authorization.
- [ ] Validate current principal/policy, expiry and revocation on every RPC
  and mutation commit across replicas; no metadata-based privilege.
- [ ] Implement soft/hard expiry and current-policy renewal; no new session
  can mutate old-session leases. Add per-session admission tokens.
- [ ] Bound registration/session churn per principal/instance and RPC sizes;
  add safe certificate/pepper rotation and principal revocation runbooks.
- [ ] Persist delivery ownership/session metadata needed by another replica.

Patterns: interceptors, immutable principal, durable session.
Expected result: any Gateway replica authenticates the same session safely.
Acceptance evidence: C01/C02 and C03 expiry/renewal/revocation/fencing pass
before any remote route is enabled. C03 SDK rollover completes in S4-01 before
the supported SDK release, not in this server-session story.

### S3-02 — Capability-aware lease and heartbeat service

P0 · Owner P · 2–4 days · Dependencies: S3-01, S2-01/S2-02.
Scope: Gateway RPC adapters, execution registry, repository/broker adapters.

Tasks and expected outputs:

- [ ] Poll only authorized capability streams, fairly rotate bounded stream
  reads, and return one lease per free-slot request.
- [ ] Atomically re-check route/version/schema, sealed workflow, session and all
  admission scopes; persist stream/group/message ownership. Populate tenant_id
  solely from the job, never worker metadata/payload, before replying.
- [ ] Implement server-time lease/heartbeat budgets and immutable attempt
  deadline. No DB connection remains checked out during a long poll.
- [ ] Gate route activation on authenticated recent worker presence;
  registration alone is not health. Accepted jobs survive worker absence.
- [ ] Handle lost lease replies as abandoned bounded leases, not duplicate
  handler invocations or permission to bypass distributed capacity.

Patterns: ports/adapters, fenced claim, bulkhead.
Expected result: an email-only session cannot consume media/webhook work.
Acceptance evidence: C04/C05/C08/C09 plus C19 trusted tenant propagation;
lost replies and live-lease PEL reclaim remain safe across Gateway replicas.

### S3-03 — Idempotent outcomes, results and durable ACK repair

P0 · Owner P/W · 7–10 days · Dependencies: S3-01, S2 lifecycle/repair, S1 REST auth.
Scope: Gateway complete/fail, REST result models/handlers/services, repository
receipts/read grants, artifact frontend/storage adapter and migrations.

Tasks and expected outputs:

- [ ] Implement canonical frozen Complete/Fail digests and per-attempt receipt
  lookup. Exact valid replay returns accepted=true; changed outcome conflicts.
  receipt_only lookup must never mutate, even if the DB lease is still live.
- [ ] Commit outcome, retry/fanout/workflow change, admission release and ACK
  repair together; Redis failure after DB commit cannot reverse acceptance.
- [ ] Validate no-output/inline result schemas and byte limits; issue durable
  attempt-scoped object grants only for storage-ready catalog revisions.
- [ ] Add the quota-enforcing artifact frontend: bounded streaming writes,
  per-grant write-count limits, durable tenant quota reservations and trusted
  hashes. An unrestricted signed PUT is not a security-complete substitute.
- [ ] Verify exact staging versions/real checksums, seal immutable final
  objects outside DB locks, then re-check ownership in the outcome transaction.
- [ ] Preserve receipt replay after lease expiry while credentials remain
  valid; reject expired/revoked session replay. Add orphan-safe cleanup.
- [ ] Extend GET task with the specified result-state/storage union, padded
  Base64/size/SHA-256 and object download_grant_path; bound JSON at 128 KiB.
- [ ] Implement GET /api/v1/tasks/:id/result/download-grant under tasks:read:
  tenant-scoped, no-store, at most 60-second delegated read authority. Re-check
  issuing key revocation/scope on read; hold immutable objects through grant
  expiry plus bounded read grace, serialize tombstoning and prune grant debt.

Patterns: idempotent receipt, Unit of Work, claim check/immutable artifact.
Expected result: lost replies cannot double-fanout or double-count retries;
an upload URL cannot overwrite a committed result.
Acceptance evidence: C06/C10/C12 with crash before/after DB commit, ACK
failure, wrong hash/key/size, staging overwrite, duplicate upload and stale seal.
C21 proves full-size inline decoding/hash, none/not-ready/expired states, foreign
404, revoked/expired grant 401, tombstone 410, storage 503 and read/deletion races.
If object mode is incomplete, disable its routes and do not call SDK v1 complete.

### S3-04 — Go SDK lifecycle and public result API

P0 · Owner W · 4–6 days · Dependencies: approved S1-05 API; fake Gateway work
can run in parallel, integration requires S3-01 through S3-03.
Scope: sdk/go/worker public API and private transport/execution/result lifecycle.

Tasks and expected outputs:

- [ ] Implement Config/Register/Run and (Result,error) handlers; hide generated
  clients and reject invalid/duplicate registration and insecure TLS.
- [ ] Apply one slot permit through polling, execution, upload and finalization;
  invoke a handler once per received attempt, preserving trusted TenantID and
  rejecting a missing/invalid tenant before invocation.
- [ ] Implement session renewal, monotonic server-time budgets, heartbeat
  cancellation, bounded transport retries and immutable outcome replay.
- [ ] Implement inline and repeatable streaming object-result handling without
  unbounded memory or arbitrary URL fetch/redirect. Copy inline bytes, define
  error-over-result precedence and transfer ObjectSource cleanup ownership.
  Use separate handler/finalization contexts and concurrent heartbeat during
  the bounded, longer object-finalization RPC.
- [ ] Separate intake cancellation from execution/drain; expose typed startup,
  authorization/contract and shutdown errors plus bounded observer events.
  Never replace a frozen Complete/Fail with shutdown CANCELLED. Clip receipt
  lookups and every request to remaining ShutdownGrace; send nothing after it.

Patterns: structured concurrency, explicit state machine, adapter.
Expected result: a service author supplies handlers, not lease/retry plumbing.
Acceptance evidence: C07/C11/C16/C19/C22 and API validation/slot tests; lost
Complete never reruns business logic. The legal 60-second finalization versus
30-second shutdown race cannot switch outcome, extend grace or fabricate success.

### S3-05 — Integrated protocol fault matrix

P0 · Owner R, P/W support · 3–5 days · Dependencies: S3-01 through S3-04.
Scope: tests/conformance, CI containers/TLS fixtures and internal demo.

Tasks and expected outputs:

- [ ] Build real multi-Gateway fixtures, deterministic fake time/transport
  fixtures and fault injection for each commit/reply boundary.
- [ ] Run phase-appropriate C01–C12/C16–C22 cases with real PostgreSQL/Redis
  and REST/artifact fixtures; S4 supplies C19's business participant proof.
  Collect evidence rather than relying on mocked auth/storage.
- [ ] Restart Gateway after dispatch and after outcome commit; prove durable
  session validation and ACK repair without in-memory ownership maps.
- [ ] Demonstrate draft/parent/child/seal -> remote reference.echo using only
  workload credentials at workers; audit mounts/env/network so platform
  DB/Redis stays private.

Patterns: executable contract, black-box fault testing.
Expected result: the vertical slice's claims are independently reproducible.
Acceptance evidence: deterministic CI green and a reviewed gap list limited
to S4 adoption/drain/packaging, S5 media and S6 Saga—not deferred core security.

Sprint 3 exit: all remote execution routes enforce mTLS/session/capability/
version/tenant policy from their first release; stale ownership cannot commit.
This is internal validation, not yet the supported SDK publication.

## 5. Sprint 4 — Supported SDK and real email adoption

### S4-01 — Drain, rollover and compatibility hardening

P1 · Owner W · 4–6 days · Dependencies: S3 exit.
Scope: SDK contexts/timers/TLS, Gateway compatibility, conformance fixtures.

Tasks and expected outputs:

- [ ] Implement old/new session overlap before hard expiry with one shared
  slot budget; old tasks retain their own session identity.
- [ ] Verify graceful drain keeps heartbeat/upload/finalize alive only within
  remaining grace. Frozen outcomes never become CANCELLED; receipt-only
  resolution cannot extend grace. At expiry cancel contexts and return a
  typed timeout for incomplete/uncooperative work without detached retries.
- [ ] Reconnect on certificate rotation; revalidate stable handler/schema
  contracts and support simultaneous old/new handler versions.
- [ ] Test goroutine leaks, reconnect storms, delayed replies, clock offsets
  and bounded observers under the race detector.

Patterns: structured concurrency, explicit rollover state machine.
Expected result: routine shutdown/rotation does not abandon healthy work early
or claim that Go can forcibly kill an arbitrary goroutine.
Acceptance evidence: C03/C09/C15/C16/C22; no slot leak or changed outcome across
rollout/timeout, including a slow completion with a lost reply.

### S4-02 — Idempotent email service reference

P0 · Owner W, business integration review P · 4–6 days · Dependencies: S3 exit.
Scope: examples/email-worker-go, participant-owned inbox/outbox/provider adapter,
SDK cookbook and integration fixtures.

Tasks and expected outputs:

- [ ] Implement a real provider adapter or deterministic provider simulator
  with documented durable idempotency/query semantics. A no-op is not email.
- [ ] Tenant-scope business/provider-account lookups using Task.TenantID before
  effects; reject cross-tenant object IDs even when the payload schema is valid.
  Commit inbox unique on (tenant,effect key), business outcome/outbox together;
  return stored outcomes and pass the stable effect key to the provider.
- [ ] Reconcile provider-accepts/crash-before-record before any resend. If the
  chosen provider cannot support this, declare the route's risk/disable
  automatic effect retries instead of claiming effectively once.
- [ ] Add forward/compensation context fixtures with distinct stable keys;
  do not implement the Saga orchestrator in the SDK.
- [ ] Document error classification, provider timeout budgets and per-attempt
  artifact/context lifetime using the public API only. Shutdown CANCELLED
  retries within platform policy, whereas operator cancellation stays fenced.

Patterns: Inbox + Transactional Outbox, provider idempotency, reconciliation.
Expected result: an engineer can copy the service boundary safely.
Acceptance evidence: C13, C14 metadata/key fixture and C19 cross-tenant object
rejection; crash after provider acceptance produces one logical effect under
the stated provider contract, never under a payload-selected tenant.

### S4-03 — Publishable SDK package and engineer guide

P1 · Owner W/R · 3–5 days · Dependencies: S4-01, S4-02.
Scope: SDK package docs, examples, module/release tooling and CI.

Tasks and expected outputs:

- [ ] Set the owner's real canonical Go module/import path before external
  publication; retain one module initially, no speculative multi-module repo.
- [ ] Publish complete public signatures, typed errors, result opener lifetime,
  payload schemas, credential rotation and shutdown/retry cookbooks.
- [ ] Pin/reproduce generated code, check no manual generated edits, and
  publish SDK/server/Go support matrix and changelog.
- [ ] Build examples from a clean external consumer module using release
  artifacts, not replace directives into this workspace.
- [ ] Make all applicable C01–C22 cases release-blocking; synthetic fixtures
  cover media/Saga envelope behavior until their real features arrive.

Patterns: minimal public facade, contract testing, reproducible release.
Expected result: engineers can install and use the SDK without reading internals.
Acceptance evidence: clean-consumer build, documented upgrade path, security
review and conformance report. Hosting identity is an owner-supplied release
input, not an invented import path in this plan.

### S4-04 — Canary, observability and remote performance budget

P1 · Owner R/P · 3–4 days · Dependencies: S4-01 through S4-03.
Scope: deploy, metrics/traces/dashboards, canary/rollback and benchmark report.

Tasks and expected outputs:

- [ ] Trace API/outbox/relay/claim/handler/outcome stages; redact credentials,
  grants, payloads and diagnostics. Use bounded labels, not task/tenant IDs.
- [ ] Correct Prometheus scrape service names and add alerts for oldest due
  age, lease loss, auth failure, ACK debt, blocked workflows and pool saturation.
- [ ] Enable one low-risk route after readiness, shadow only read-only
  observations, then canary real effects with the business owner.
- [ ] Demonstrate rollback that stops intake/drains/fences but never returns
  to simulator success or unauthenticated writes.
- [ ] Repeat baseline through mTLS and real SDK; publish transport overhead,
  errors, sustainable rate and lock/connection costs.

Patterns: strangler/canary, observability, bulkheads.
Expected result: the SDK can be adopted and rolled back without hiding failure.
Acceptance evidence: C01–C22 applicable cases, canary/rollback drill and
measured performance regression review; external effects are never duplicated
for a shadow benchmark.

Sprint 4 exit: supported Go SDK release candidate, production-like idempotent
email reference, clean install, usable docs, observable authenticated rollout.
Public release remains blocked by missing compatibility/security evidence.

## 6. Sprint 5 — Heavy-work isolation and artifacts

### S5-01 — Cancellable Go media worker

P1 · Owner W · 3–5 days · Dependencies: S4 release candidate.
Scope: examples/media-worker-go, subprocess wrapper, deploy resource profile.

Tasks and expected outputs:

- [ ] Implement a real thumbnail/transcode handler with bounded workspace,
  input identifier validation and no arbitrary server-side URL fetch.
- [ ] Run external tools in a cancellable process group; terminate descendants
  on deadline/lease loss and escalate to kill after a bounded interval.
- [ ] Isolate CPU/memory/disk/network and worker slots from email; propagate
  context through tool startup, upload and finalization.
- [ ] Retain output through SDK upload/finalization using ObjectSource cleanup
  ownership; do not delete it when the earlier handler context expires.

Patterns: process isolation/bulkhead, structured concurrency, claim check.
Expected result: a long media task cannot consume email resources by naming a
short queue or leave orphan subprocesses after ownership loss.
Acceptance evidence: real C15 process-tree tests, resource-limit failure tests
and all applicable SDK conformance cases.

### S5-02 — Mixed-load tenant and workload isolation

P1 · Owner R/P · 3–5 days · Dependencies: S5-01, S2 admission.
Scope: load fixtures, pool policy, dashboards and isolation report.

Tasks and expected outputs:

- [ ] Load email alone, media alone, then both with a hot tenant/workflow;
  keep the email offered workload identical in the comparison.
- [ ] Test multiple Gateway/relay/worker replicas, exhausted tokens, cap
  decrease, provider slowdown and an uncooperative worker.
- [ ] Measure admission delay, queue age, handler start, recovery and resource
  pressure independently; identify starvation rather than hiding it in p99.
- [ ] Prove no cap violations or pool leakage; tune only documented budgets.

Patterns: bulkhead, controlled noisy-neighbor experiment.
Expected result: workload and tenant protection is evidence-backed.
Acceptance evidence: initial engineering target is email due-to-start p99 no
more than 20% above its isolated run at the same email offered load; zero hard
cap violations. If missed, fix/isolate before broad adoption. This is a test
target, not a published customer SLA.

### S5-03 — Object-storage failure and cleanup validation

P0 · Owner R/W · 3–4 days · Dependencies: S3-03, S5-01.
Scope: result adapter/SDK integration fixtures and object lifecycle operations.

Tasks and expected outputs:

- [ ] Inject slow upload, expired grant, changed opener bytes, checksum
  mismatch, overwrite and crash during sealing/DB commit.
- [ ] Prove bounded SDK memory, repeatable uploads and that committed reads
  always use the recorded immutable version.
- [ ] Run orphan/retention cleanup under concurrent completion/read and after
  worker/Gateway loss; reject unauthorized result-download grants. Test
  tombstoning versus grant issuance and deletion holds through grant/read grace.
- [ ] Record storage permissions, grant maximum lifetime, object cleanup cost
  and quota/alert thresholds for each deployment.

Patterns: immutable artifact, least privilege, reference-aware garbage collection.
Expected result: large results do not weaken fencing or leave unbounded debt.
Acceptance evidence: C10/C12/C15 with real storage semantics; no result deleted
while referenced, and no staging overwrite changes a committed outcome.

Sprint 5 exit: separate email/media performance evidence, real process
cancellation and artifact lifecycle validated. Do not add a second language
or SDK-local Saga implementation.

## 7. Sprint 6 — Orchestrated Saga

### S6-01 — Durable Saga engine and approved templates

P0 for Saga release · Owner P · 5–7 days · Dependencies: S4 stable outcomes;
S5 isolation for heavy participants.
Scope: internal/saga, domain/migrations, repository transition/outbox, admin API.

Tasks and expected outputs:

- [ ] Define approved versioned DAG templates with forward/compensation task
  types, reversible/irreversible pivot policy and participant schema versions.
- [ ] Persist Saga/step states and commands; commit transition/outbox together
  under a deterministic lock order and unique command identity.
- [ ] Give each forward/compensation command its own ordinary single-task
  already-sealed workflow atomically with its publication, bypassing public
  draft round trips; keep Saga dependencies in Saga-step records. Never reopen a
  terminal workflow or insert cross-workflow parent edges. Deduplicate durable
  outcome events; Saga lock precedes command-workflow locks, never the reverse.
- [ ] On failure stop new forwards, fence/cancel active work and reconcile
  uncertain effects before choosing compensations.
- [ ] Schedule completed reversible steps in reverse dependency order; use
  distinct stable compensation keys and ordinary task lease/retry policy.
- [ ] Expose durable COMPENSATING/COMPENSATION_FAILED and auditable operator
  retry/resolve without mutating immutable prior outcomes.

Patterns: orchestrated Saga, state machine, outbox/receipt.
Expected result: platform alone schedules Saga work; workers stay ordinary
business participants.
Acceptance evidence: branched-DAG transition/ordering tests, duplicate events
and crash recovery; no false fully-rolled-back state after compensation failure.

### S6-02 — Two independently idempotent participants

P0 for Saga release · Owner W · 4–6 days · Dependencies: S6-01 template contract.
Scope: inventory reserve/release and payment charge/refund examples, local stores.

Tasks and expected outputs:

- [ ] Implement forward and compensation with service-owned transactions and
  local inbox/outbox; no shared platform database or cross-service ORM.
- [ ] Return/reconcile previous outcomes by stable keys, including provider
  uncertainty and repeated compensation commands.
- [ ] Define allowed business reversals, partial failure and irreversible
  operations explicitly; never promise rollback of email or arbitrary APIs.
- [ ] Document SDK envelope/trace usage without nextStep/compensate SDK calls.

Patterns: participant Inbox/Outbox, domain transaction, idempotency.
Expected result: orchestration does not bypass participant correctness.
Acceptance evidence: one logical charge/refund/reservation/release under
duplicate commands and crash-after-effect fault fixtures.

### S6-03 — Saga failure, remediation and recovery drills

P0 for Saga release · Owner R/P · 4–6 days · Dependencies: S6-01, S6-02.
Scope: tests/conformance, operator runbooks/tools, Saga observability.

Tasks and expected outputs:

- [ ] Inject lost replies, duplicate/reordered commands, active-forward
  cancellation, uncertain provider effect and orchestrator restart.
- [ ] Test reverse-DAG compensation with parallel branches and shared
  dependencies; no compensation runs before dependent reversals settle.
- [ ] Force compensation exhaustion and demonstrate permissioned audited
  operator retry/resolve; alerts identify exactly the blocked business step.
- [ ] Verify retention preserves participant idempotency evidence through
  retry/replay windows; never blindly rerun a forward pivot.

Patterns: fault injection, durable remediation, Saga observability.
Expected result: every Saga is terminal or explicitly operator-actionable.
Acceptance evidence: actual C14 end-to-end report and recovery drill. A generic
workflow passing C14 envelope fixtures in S4 is not Saga-engine completion.

Sprint 6 exit: compensable multi-service DAG survives duplicate/crash/partial
failure and reaches a truthful terminal or remediation state.

## 8. Sprint 7 — Capacity envelope and operational sign-off

### S7-01 — Profile before scaling

P2 · Owner P/R · 3–5 days · Dependencies: stable S4/S5 path.
Scope: profiling, query plans/indexes, relay/pool configuration and evidence.

Tasks and expected outputs:

- [ ] Profile CPU/allocation, DB WAL/commit/lock/pool wait, Redis/network and
  RPC scheduling; identify the actual bottleneck at each load step.
- [ ] Tune batches/connection/worker budgets without starving heartbeat or
  recovery; reserve maintenance capacity and check DB connection totals across
  all replicas, including listeners.
- [ ] Remove duplicate lease-expiry indexes with a new reviewed migration,
  checking deployed definitions and concurrent-index operational constraints.
- [ ] Re-measure after each coherent change set. Consider partitioning only
  if data/lock/IO evidence demonstrates a limit it addresses.

Patterns: measurement-driven optimization, bounded resource budgeting.
Expected result: a repeatable capacity envelope, not a guessed single-node ceiling.
Acceptance evidence: throughput gain and p99/lock/CPU tradeoff published under
unchanged durability; no lost jobs, stale commits or hard-cap violations.

### S7-02 — Durability, backup/restore and incident operation

P0 for production · Owner R/P · 3–5 days · Dependencies: S2 repair and S4 canary.
Scope: deploy persistence/TLS/secrets, runbooks, alerts, recovery fixtures.

Tasks and expected outputs:

- [ ] Record PostgreSQL synchronous-commit/replication and Redis persistence/
  failover guarantees. Benchmark and production profiles must be explicit.
- [ ] Drill worker/Gateway/relay loss, Redis data/group loss, DB reconnect,
  disk/full storage and restore into an isolated environment.
- [ ] Verify queued DB replay, lease/token recovery, result-object consistency,
  DLQ repair and alert/action ownership after restore.
- [ ] Have the product/operations owner approve actual RPO/RTO and retry/
  history/idempotency retention before launch; backup-only restore cannot
  honestly promise zero loss of writes newer than that backup.
- [ ] Add recovery, credential compromise, stuck workflow, failed compensation,
  saturated pool and cleanup backlog runbooks with permission boundaries.

Patterns: reconciliation, disaster recovery, least privilege.
Expected result: failures have a tested operator path and truthful durability claim.
Acceptance evidence: timed restore/recovery drill and signed recovery
objectives; no production sign-off from unit tests alone.

### S7-03 — Repository and release documentation reconciliation

P1 · Owner W/R · 2–4 days · Dependencies: implemented contracts and deploy layout.
Scope: README, quick-start, configuration, DAG/shutdown docs, Dockerfile/CI.

Tasks and expected outputs:

- [ ] Build actual cmd binaries rather than a nonexistent root main; verify
  compose service names, health/readiness, TLS mounts and Prometheus targets.
- [ ] Replace unsafe manual schema.sql/bootstrap instructions with the
  reviewed migration procedure; show explicit safe test/benchmark flags.
- [ ] Remove client queue/retry examples and simulator claims; describe DB
  transaction/outbox fanout, not direct child Redis publishing.
- [ ] Reconcile graceful shutdown and exact-once wording with tested behavior;
  publish module support/deprecation and operator ownership.
- [ ] Perform a whole-flow review from clean checkout through authenticated
  submission, worker execution, result and recovery.

Patterns: single source of truth, composition-root build, executable examples.
Expected result: launch docs describe the shipped system, not historical demos.
Acceptance evidence: clean-checkout walkthrough/CI and no contradictory
security, durability or performance statements.

### S7-04 — Published benchmark and launch decision

P1 for capacity claim · Owner R, P/W review · 2–4 days · Dependencies: S7-01/S7-02.
Scope: benchmark scripts/raw results/report and release decision record.

Tasks and expected outputs:

- [ ] Execute the common benchmark protocol below for reference, email fixture,
  media and mixed tenant/workflow workloads with multiple replicas.
- [ ] Publish offered/accepted/completed rates, p50/p95/p99, errors, duplicates,
  backlog slope, recovery time and resource costs; include failed runs.
- [ ] Separate scheduler overhead from business/provider time and repository
  microbenchmarks from end-to-end behavior.
- [ ] If comparing Temporal/River/BullMQ, pin versions and match hardware,
  payloads, persistence, retry/dependency semantics and measurement boundaries.
  Label unsupported comparisons; do not remove durability to win a graph.
- [ ] Approve a capacity envelope and overload policy; launch only within it.

Patterns: reproducible experiment, explicit service-level objectives.
Expected result: engineers/operators know the supported operating range.
Acceptance evidence: reproducible raw results and signed production readiness
review; no unconditional “faster than Temporal/BullMQ” claim.

## 9. Benchmark protocol and measurable gates

This is a required experiment specification, not a performance guarantee.

1. Use disposable targets, fixed random seed/workload, same payload sizes and
   fixed handler/provider fixtures. Never send real email/payments in load tests.
2. Record commit, Go/PostgreSQL/Redis versions, CPU/RAM/disk, network,
   replicas/slots, pool caps, connection limits, payload/result sizes, DAG
   width/depth, durability/retention and TLS settings.
3. Warm for two minutes. Sweep open-loop offered load from below saturation
   upward, reporting rejected/timed-out/unfinished work to avoid survivor and
   coordinated-omission bias. Repeat comparison points three times.
4. S1/S2 smoke points run at least ten minutes with at least 10,000 successful
   starts for percentile reporting. S7's claimed operating point sustains
   30 minutes and stable backlog, plus a separately reported overload run.
5. Report API acceptance, commit-to-publish, due-to-handler-start, handler,
   outcome commit and total completion separately. Use synchronized stage
   timestamps or trace durations with measured clock uncertainty; do not
   subtract unrelated host wall clocks and call it latency.
6. Correctness gate: zero tenant/capability/fence/admission violations and no
   missing accepted work after the documented recovery horizon. Count duplicate
   deliveries/invocations; they are allowed, duplicate logical effects are not
   under an idempotent fixture.
7. Capacity gate: stable due backlog and less than 0.1% infrastructure errors
   at the claimed sustained point, within the application owner's signed p99
   budget. Overload rejection/errors are reported, never silently excluded.
8. S1 establishes measured numeric latency/capacity budgets; S4 quantifies
   remote overhead; S5 uses its explicit 20% isolation target. Changes to
   performance budgets require a recorded tradeoff/review, not silent waiver.
   Security/durability/correctness gates cannot be waived by faster results.
9. Inject replica/broker loss at a non-saturated load and report accepted work
   recovery, ACK/DLQ debt and backlog-drain time. No benchmark is complete with
   only a healthy-process steady-state number.

Cross-sprint dependencies:

```text
S1: routing + authenticated REST + isolated baseline + protocol freeze
  -> S2: atomic lifecycle + shared admission + relay/repair
  -> S3: authenticated Gateway + SDK + durable results/outcomes
  -> S4: supported SDK + idempotent service + canary
  -> S5: resource-heavy workload isolation
  -> S6: Saga (separate feature gate)
  -> S7: capacity/operations sign-off

S7 operational preparation may start earlier; launch cannot bypass its gate.
Saga is not required to ship the basic queue/worker SDK, but cannot be advertised
until S6 passes.
```

## 10. Audit-to-story traceability

| Reviewed gap | Required story/evidence |
| --- | --- |
| 1. REST unauthenticated, no tenant/idempotency boundary | S1-02, S1-03; C18 |
| 2. Simulator and automatic producer falsely complete work | S1-01; real effect strategy S4-02 |
| 3. Parent/janitor queue leaks across routes | S1-01, S2-01; persisted-route recovery tests |
| 4. Late child and cross-workflow parents | S1-03/S2-01; draft/seal/expiry races and immutable sealed graph, C20 |
| 5. Failure/cascade/workflow/DLQ split commits | S2-01, S3-03; C06/C12 |
| 6. Semantic duplicates open breaker, success streak not reset | S2-05; deterministic breaker tests |
| 7. Redis read errors hidden by empty-read branch | S2-05; C17 |
| 8. One batch/tick caps relay and adds polling delay | S2-03; common benchmark |
| 9. Unbounded Stream history and false depth metrics | S2-04; C17 |
| 10. Handler result API/upload/commit/read lifecycle incomplete | S1-05, S3-03/S3-04, S5-03; C10/C21 |
| 11. Authorization/version checks postponed after Gateway | S3-01/S3-02; C01/C02/C08 before enablement |
| 12. Wrapped not-found and inaccurate POST response defaults | S1-02/S1-03; HTTP response assertions |
| 13. CI missing real lifecycle/auth/concurrency gates | S1-04, S3-05, S4-03 |
| 14. Prometheus scrape target doesn't match compose service | S4-04, S7-03; actual scrape evidence |
| 15. REST idempotency path-scope contradiction | S1-03; same-path conflict/different-path independence |
| 16. Shutdown cancels execution instead of draining | S3-04, S4-01; C15/C22 and frozen-outcome invariant |
| 17. Duplicate lease-expiry indexes | S7-01; new migration + query-plan check |
| 18. Docker/quickstart/DAG/benchmark documentation drift | S7-03; clean-checkout walkthrough |

Non-audit resilience requirements are explicit too: durable session rollover
S3-01/S4-01; lost lease reply S3-02; immutable receipt/ACK repair S3-03;
broker data-loss repair S2-04; bounded DAG/cascade S2-01; object immutability
S3-03/S5-03; uncertain Saga forward effects S6-01/S6-03. The latest review adds
trusted tenant context C19 (S3-02/S3-04/S4-02), reliable draft/seal C20
(S1-03/S2-01), public result reads C21 (S3-03/S5-03) and frozen-outcome shutdown
C22 (S3-04/S4-01). These are required, not post-release follow-ups.

## 11. First sprint planning meeting: required decisions and assignments

Use this design as the default; do not reopen queue naming, language choice,
auth model or retry ownership during individual implementation PRs.

- Assign named owners/reviewers and actual capacity to S1-01 through S1-05.
- Have the data owner supply legacy tenant/route mapping, infrastructure owner
  supply workload identity/storage constraints, and release owner supply the
  canonical repository import path. Missing inputs block only their cutover/
  publication gates; no engineer should invent them.
- Confirm the explicit v1 limits and approved effect strategies against real
  business workloads. Changing them is a reviewed contract change.
- Schedule S1 baseline/protocol review and reserve review/fault-test capacity.
- Open stories using the task checklists above; attach expected artifact and
  acceptance evidence, not “SDK done” as an untestable status.

No implementation, new dependency, migration application or SDK publication
is authorized by this documentation handoff.
