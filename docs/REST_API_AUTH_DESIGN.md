# REST API Authentication and Tenant Authorization

**Status:** planned contract for Sprint 1; authentication and idempotency are
not implemented in the current REST server.

Until the API-key and idempotency store are implemented together, POST
endpoints do not require `Idempotency-Key`. Do not add header enforcement or
body buffering without also adding the transactional store and replay flow.

## Decision

The public Queue REST API uses a long-lived API key pair, not JWTs. This API is
used by server-to-server task callers; it has no browser session or delegated
end-user identity to encode in a JWT. A caller receives a public key ID and a
random secret once, and presents them over TLS on every request.

This is separate from Worker Gateway authentication:

| Caller | Authentication | Why |
| --- | --- | --- |
| Queue REST client | API key ID + secret | Stable service-to-service caller identity, scopes, tenant ownership, and immediate revocation. |
| Go worker | mTLS workload identity + Gateway session | A worker is a deployed workload that leases and completes tasks; it must not use a static REST secret. |

JWT, OAuth, user login, and request-signing are out of scope for V1. Add them
only when the product has delegated user access or a concrete replay-protection
requirement that TLS cannot cover.

## Wire contract

Every protected endpoint requires exactly one header:

```http
Authorization: ApiKey dqk_<public-id>.<base64url-secret>
```

- `dqk_<public-id>` is an opaque, non-secret lookup ID.
- The secret is 32 cryptographically random bytes encoded as unpadded base64url.
- The separator is a dot; IDs and encoded secrets must not contain a dot.
- Never put either value in a URL, query parameter, task payload, or log.
- TLS is mandatory at the public ingress. A directly exposed API listener must
  reject plaintext; when TLS terminates at a trusted proxy, only that proxy may
  forward traffic to the API service.

`GET /api/v1/health` is the only unauthenticated application endpoint and
returns no tenant or queue data. Metrics remain on an operator-only network,
not behind a customer API key.

Every `POST` also requires an `Idempotency-Key` header. It is an opaque
client-generated value with at least 128 bits of randomness. The key is scoped
to the authenticated tenant, HTTP method, and exact request path; it is not the
task's worker-facing `idempotency_key`.

When this contract is implemented, the API will reject an `Idempotency-Key`
longer than 256 bytes and reject request bodies larger than the endpoint's
configured limit (64 KiB for V1 JSON writes) before allocating an unbounded
buffer.

## Authorization model

Successful authentication places this immutable principal in request context:

```text
key ID + tenant ID + scopes
```

The caller never supplies a tenant ID. The server derives it from the key and
persists it with created workflows and tasks. Initial scopes are:

| Endpoint | Required scope |
| --- | --- |
| `POST /api/v1/workflows` | `workflows:write` |
| `GET /api/v1/workflows/:id` | `workflows:read` |
| `POST /api/v1/workflows/:id/tasks` | `tasks:write` and workflow belongs to the key's tenant |
| `GET /api/v1/tasks/:id` | `tasks:read` and task belongs to the key's tenant |

A missing, malformed, unknown, expired, revoked, or bad-secret key returns
`401` with the same generic body. A valid key without the required scope
returns `403`. A valid key reading another tenant's object returns `404` to
avoid revealing that the object exists.

### Write idempotency

For `POST /api/v1/workflows` and `POST /api/v1/workflows/:id/tasks`, middleware
must apply the body limit, read the body exactly once into a bounded byte slice,
compute SHA-256 over those bytes, and replace `Request.Body` with a new reader
over the same bytes before Gin binding runs. A retry must send the same bytes.
It writes an idempotency record in the same PostgreSQL transaction as
the workflow/job and its outbox row. Reusing the same key with a different body
or path returns `409 Conflict`.

The transaction first claims the key with `INSERT ... ON CONFLICT DO NOTHING`.
The winner creates the workflow/job and outbox row, stores the response, and
commits all of it together. A loser reads the committed record, compares its
request hash, and returns the stored response. It never creates work before it
has claimed the key.

The task's stable worker-facing `idempotency_key` is platform-generated and
stored with the task. It protects a business effect after delivery; the REST
header protects the caller's submission from creating duplicate work. These
are separate keys with separate retention and scopes.

## Storage and validation

Sprint 1 adds an `api_keys` table with:

```text
id, public_id (unique), tenant_id, secret_hmac, pepper_version,
scopes, created_at, expires_at, revoked_at, last_used_at
```

It also adds an `api_idempotency_records` table with:

```text
tenant_id, method, request_path, idempotency_key, request_sha256,
response_status, response_body, created_at, expires_at
```

`(tenant_id, method, request_path, idempotency_key)` is unique. The stored
response is capped at 64 KiB and retained for 24 hours. An index on
`expires_at` supports the API idempotency janitor, which deletes expired rows
in small batches. Clients must use a new key after expiry; once cleanup removes
the old record, a reused old key is treated as a new request.

The database never stores a raw secret. On creation, the platform stores
`HMAC-SHA-256(API_KEY_PEPPER[pepper_version], secret)`. `API_KEY_PEPPER` is a
KMS-managed deployment secret, never a database value or application log.
Validation looks up only `public_id`, rejects revoked/expired keys, computes
the matching HMAC, and uses a constant-time comparison before scope checks.

V1 performs this indexed lookup on every authenticated REST request and does
not keep a process-local positive-authentication cache. This makes revocation
effective for the next request on every API replica. If benchmarks later show
this lookup is material, introduce only a shared cache with durable versioning
and cross-replica invalidation; a TTL-only cache is not acceptable for keys.

Pepper rotation is an explicit API-key reissue migration: keep the old pepper
version available until every key using it has been replaced or revoked. Do not
silently rotate the pepper and invalidate all callers.

The platform displays a newly created secret once. Rotation creates a second
key, callers switch traffic, and an operator revokes the old key after a short
overlap. There is no endpoint that reads a secret back.

## Middleware and audit rules

Authentication runs once as Gin middleware on the protected `/api/v1` group,
before binding request payloads or calling services. Handlers receive the
principal only from request context; they do not parse headers themselves.

The middleware must:

1. Reject multiple or malformed `Authorization` headers and bound header size.
2. Validate the API key as above, then attach the immutable principal.
3. Enforce endpoint scope before handler execution.
4. Emit an audit record with public key ID, tenant ID, route, response class,
   request ID, and timestamp—never the secret or complete Authorization value.
5. Update `last_used_at` asynchronously or rate-limited, so a hot key does not
   create a database write per request.

Ingress/WAF rate-limits failed authentication by source IP. The platform later
adds tenant/key rate limits as part of operational capacity work; it must not
use a process-local limiter as its multi-replica security boundary.

## Required tests

1. Missing, malformed, unknown, revoked, expired, and wrong-secret keys all
   receive the same `401` response without leaking secret data.
2. A valid key reaches only routes covered by its scopes.
3. A tenant cannot read or append tasks to another tenant's workflow.
4. The stored value is an HMAC, not the issued secret; application and audit
   logs contain neither secret nor Authorization header.
5. During key rotation, both intended keys work; revoking the old key blocks it
   immediately across API replicas.
6. Health remains unauthenticated and every task/workflow endpoint is covered
   by the middleware.
7. A valid JSON body remains bindable after idempotency hashing and buffering.
8. Concurrent identical POST retries create one workflow/job and one outbox
   row, then return the same stored response.
9. Reusing an idempotency key with a changed body or request path returns `409`.
10. The idempotency janitor deletes only expired rows in bounded batches.
11. An oversized body returns `413` and an oversized idempotency key returns
    `400` before the handler or database write runs.

## Delivery order

1. Add tenant, API-key, and idempotency-record migrations plus an operator-only
   bootstrap/issue command that prints the secret once.
2. Add the constant-time key validator and Gin middleware.
3. Thread tenant ownership and transactional POST idempotency through
   workflow/task writes and reads.
4. Add the bounded idempotency janitor and its expiry index.
5. Add authorization, cross-tenant, concurrent-retry, and janitor integration
   tests.
6. Enforce TLS/ingress configuration and publish rotation/runbook guidance.

Do not issue API keys from an unauthenticated public endpoint, store secrets in
environment variables, accept POST requests without `Idempotency-Key`, or use
the REST API key for Worker Gateway calls.
