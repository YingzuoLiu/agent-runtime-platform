# Durable Action gateway

Phase 7D gives an existing Agent, script, or workflow one narrow way to delegate an external side
effect without surrendering its Planner, memory, session, or main loop. The public Action façade
submits a private, single-step domain into the existing durable Run lifecycle, which continues to
use `ExternalActionCoordinator` for prepared intent, dispatch fencing, restart recovery,
cancellation arbitration, terminal uncertainty, and sanitized evidence.

The first contract supports only:

```text
POST /actions
GET  /actions/{action_id}
GET  /actions/{action_id}/events

action_type: webhook.send
```

It does not expose arbitrary URLs, methods, headers, tokens, Python or shell handlers, framework
adapters, approval, compensation, or a provider-status query. The Travel Runtime Console remains
the repository's primary local demonstration.

## Register a destination

`destination` is an alias in a deployment-owned provider registry. The endpoint, credentials,
timeout, response limit, stable provider identity, idempotency capability, and definitive-status
classification are server configuration and never caller input.

Configure HTTP destinations with `RUNTIME_ACTION_PROVIDERS_JSON` before starting the API:

```bash
export RUNTIME_ACTION_PROVIDERS_JSON='{
  "demo": {
    "endpoint": "https://provider.example/actions",
    "provider_identity": "demo-provider-v1",
    "bearer_token": "replace-with-a-server-owned-token",
    "supports_idempotency": true,
    "definitive_status_codes": [400, 401, 403, 404],
    "timeout_seconds": 5,
    "max_response_bytes": 65536
  }
}'
```

HTTPS is required. An HTTP loopback endpoint is available only for explicit local development by
setting `allow_insecure_localhost` to `true`. `provider_identity` must be a stable, non-secret name
for the concrete provider deployment/account: rotate credentials without changing it, but change
it when the external account or execution boundary changes.

`supports_idempotency` is a capability assertion, not an instruction to add a header and hope. Set
it to `true` only when the provider contract guarantees that repeated requests with the same key
and payload deduplicate the effect. Provider-idempotent destinations may use at most two transport
attempts with the same server-derived key after an ambiguous boundary. Unsafe destinations are
never blindly re-dispatched after an ambiguous attempt or a restart that finds `dispatching`.

Classify a 4xx status as definitive only when the provider contract proves it rejected the request
without applying the effect. In particular, do not configure `408`, `409`, `425`, or `429` as
definitive merely because they are 4xx responses. A mistaken classification can turn an uncertain
write into an ordinary failure.

`RUNTIME_ACTION_WAITER_LIMIT` controls the number of requests allowed to use bounded waiting at
once (default `16`, valid range `1..1000`). Requests above that limit return the current `202`
representation immediately instead of waiting for a slot.

## Provider HTTP envelope

`HttpExternalActionProvider` sends `POST` to the configured endpoint with JSON content, an
`Idempotency-Key` header containing the server-derived provider key, and the configured Bearer
credential when present. It does not follow redirects or inherit process proxy settings.

The JSON body is the complete server-owned `ExternalActionRequest`, not the caller's raw payload:

```json
{
  "action_id": "internal-action-ledger-id",
  "run_id": "owning-durable-run-id",
  "step_id": "dispatch",
  "tenant_id": "authenticated-tenant",
  "subject_id": "authenticated-subject",
  "workflow_type": "durable-action:webhook.send:1",
  "tool_name": "webhook.send",
  "arguments": {
    "payload": {
      "text": "hello"
    }
  },
  "idempotency_key": "server-derived-provider-key"
}
```

The provider must return `Content-Type: application/json`, status `200` or `201`, and this strict
success shape:

```json
{
  "provider_reference": "delivery_123",
  "result": {}
}
```

The public `webhook.send` output allowlist contains only `provider_reference`, so Phase 7D requires
the provider's `result` object to be empty. The reference must match
`[A-Za-z0-9][A-Za-z0-9._:-]{0,199}`. Extra result fields, invalid references, reflected credentials,
malformed or oversized JSON, and non-JSON bodies are not persisted as success.

Only `200` and `201` with a valid response are synchronous success. `202`, `204`, redirects,
unclassified 4xx responses, 5xx responses, timeouts, connection interruptions, invalid content,
and schema failures are ambiguous because the provider may already have applied the effect. This
adapter is therefore not a raw-body forwarder or a drop-in client for ordinary Slack-style webhook
URLs; the endpoint must implement this envelope contract.

## Submit an Action

An Operator credential needs the existing `runs:create`, `tools:execute`, and
`external-actions:execute` permissions. Tenant, subject, role, execution authority, thread,
provider route, and provider configuration all come from authenticated or server-owned state.

```bash
curl -i -X POST 'http://127.0.0.1:8000/actions?wait=5' \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "action_type": "webhook.send",
    "destination": "demo",
    "idempotency_key": "agent-job-42",
    "input": {
      "payload": {
        "text": "hello"
      }
    }
  }'
```

The payload must be a JSON object of at most 32,768 canonical UTF-8 bytes and no more than 16
levels deep. Non-finite numbers and extra fields are rejected. The caller cannot add authority or
transport fields such as `tenant_id`, `subject_id`, `role`, `thread_id`, `url`, `method`, `headers`,
`token`, `timeout`, `provider_identity`, `supports_idempotency`, or `definitive_status_codes`.

The optional `wait` query parameter is numeric seconds in the range `0..5`, defaulting to `0`. It
does not enter the Action fingerprint or change the durable lifecycle:

- a terminal Action returns `200`;
- a nonterminal Action returns `202`, `Location: /actions/{action_id}`, and `Retry-After: 1`;
- the response body always contains the current Action representation and `action_id`;
- a client disconnect or wait timeout does not cancel the underlying Run.

A nonterminal response has the same typed resource shape used by later reads:

```json
{
  "action_id": "opaque-action-id",
  "action_type": "webhook.send",
  "destination": "demo",
  "idempotency_key": "agent-job-42",
  "status": "running",
  "result": null,
  "error_code": null,
  "error_message": null,
  "created_at": "2026-08-15T10:00:00+00:00",
  "updated_at": "2026-08-15T10:00:01+00:00",
  "completed_at": null
}
```

The standalone integration in [`examples/external_agent.py`](../examples/external_agent.py) meets
the target of at most ten executable physical lines. A `202` response from that example is normal;
five seconds is not a completion guarantee.

## Idempotency

The client `idempotency_key` is stable within a tenant and should identify one logical requested
effect. The runtime hashes it into a reserved internal Run request namespace and stores a canonical
fingerprint over the versioned contract, action type, destination, and normalized typed input. The
raw client key is never used as the provider key.

- The same tenant, key, and canonical request return the same Action, including after restart.
- JSON object key order and explicit typed defaults do not change the fingerprint.
- Reusing the key with a different action type, destination, or input returns `409
  idempotency_key_reused` and does not dispatch the losing request.
- Tenants have independent idempotency namespaces.
- Unknown action types and destinations fail before a Run is created.

On an ordinary successful path, concurrent identical submissions create one Run and one provider
dispatch. A provider-idempotent recovery may make a second transport attempt with the same
server-derived provider key after an ambiguous first attempt. This is bounded provider-assisted
deduplication, not transport exactly-once.

## Read status and evidence

The public Action ID is the owning Run ID treated as an opaque identifier; the private Action
domain and its Run are hidden from `/agents`, `/runs`, Run events, cancellation, generic thread
state, and direct tool execution.

```bash
curl -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/actions/<action_id>

curl -H "Authorization: Bearer $RUNTIME_API_KEY" \
  'http://127.0.0.1:8000/actions/<action_id>/events?after_sequence=0'
```

Public status is `queued`, `running`, `reconciling`, `succeeded`, `failed`, `cancelled`, or
`outcome_unknown`; the last four are terminal. A terminal provider failure is represented by a
normal `200` GET response with `status="failed"` or `status="outcome_unknown"`, not by turning the
GET into a provider-derived HTTP error. Only `succeeded` exposes a result.

Action events come from the authoritative workflow event store and project only:

```text
sequence, event_type, status, destination, dispatch_count, retry_mode,
provider_reference, error_code, created_at
```

They exclude tenant, subject, client and provider idempotency keys, internal IDs, provider
identity, endpoint, credentials, dispatch tokens, raw provider bodies, and traceback data. The
optional `after_sequence` cursor must be a non-negative integer.

Lookup is tenant-scoped before authorization. A missing Action and a cross-tenant ID both return
the same `404 action_not_found`; a same-tenant Viewer can read an Action and its events using the
existing `runs:read` and `run-events:read` permissions. Phase 7D has no subject-level isolation or
per-destination permission within a tenant.

Action routes use a stable `{"error":{"code":"...","message":"..."}}` envelope:

| HTTP | Code | Meaning |
| ---: | --- | --- |
| `401` | `invalid_api_key` | The Bearer credential is absent or invalid. |
| `403` | `operation_not_permitted` | The authenticated role lacks the required existing permission. |
| `404` | `action_not_found` | The Action is absent or belongs to another tenant. |
| `409` | `idempotency_key_reused` | The tenant-scoped key is bound to different canonical Action input. |
| `422` | `action_type_not_registered` | The requested Action type is not registered. |
| `422` | `destination_not_registered` | The destination is not in the Action provider registry. |
| `422` | `invalid_action_input` | The body or Action query value is invalid. |
| `500` | `action_evidence_incomplete` | Durable evidence cannot support a safe public projection. |

The ordinary `/runs` API also rejects Action-owned `action-request:` client-request IDs with `422
reserved_client_request_namespace`. Provider failures discovered asynchronously remain terminal
Action representations rather than being converted into GET errors.

### Quarantined private Actions

If checkpoint drift places an Action-owned Run in
`thread_checkpoint_conflict_reconciliation_pending`, the private Run remains hidden from generic
Run routes. An Operator may target the public `action_id` through
`POST /operator/quarantine-resolutions`. The shared resolution service returns an Action-facing
target and Action reference; it does not expose the private Thread ID, Run route, input,
idempotency keys, provider binding, arguments, result body, or execution tokens.

Only terminal and internally consistent `succeeded`, `failed`, or `outcome_unknown` action evidence
can make the plan eligible. Prepared, dispatching, or reconciling Actions remain quarantined. The
resolution never calls the provider and does not change the public provider outcome: for example,
a durably succeeded Action remains publicly `succeeded` even though its stale-checkpoint Run is
terminalized as failed. See
[`operator-quarantine-resolution.md`](operator-quarantine-resolution.md).

## Recovery and limits

The private domain owns one durable `dispatch` step and reuses the existing external-action ledger.
A prepared action can make its first dispatch after restart. A `dispatching` provider-idempotent
action may re-dispatch once with the same key; an unsafe one becomes `outcome_unknown` without a
second call. Terminal success, definitive failure, and unknown are reused and never redispatched.
A provider result that is already durably succeeded takes precedence over a later Run failure or
cancellation, while inconsistent post-dispatch evidence fails safe to `outcome_unknown`.

There is no terminal-unknown status query, automatic reconciliation scanner, compensation,
rollback, human approval, arbitrary handler execution, distributed lease, or distributed
exactly-once guarantee. SQLite plus the existing unique constraints and fences define the current
single-database concurrency boundary.
