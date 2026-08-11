# Agent Runtime Reliability Platform

[![CI](https://github.com/YingzuoLiu/agent-runtime-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/YingzuoLiu/agent-runtime-platform/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB)
![Status](https://img.shields.io/badge/status-Phase%207A%20complete-2ea44f)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-oriented reference implementation for reliable Agent execution: typed Planner
decisions, policy-governed registered tools, durable state and events, restart-safe recovery,
multi-tenant authorization, governed cross-thread memory, and explicit failure semantics.
Phase 7A adds prepare-before-dispatch durable external actions with server-derived
idempotency, provider references, and explicit uncertain-outcome recovery.

Travel is the first reference application, not the architecture boundary. A separate
release-validation domain exercises the same runtime through a deterministic DAG and selective
replay.

The default path is deterministic and offline: no LLM key, Redis, PostgreSQL, or Kubernetes is
required. An optional OpenAI Responses adapter drives the same typed loop with live model
decisions. The three Travel planning tools use synthetic, read-only data. The opt-in
`travel-agent:1.2.0` action writes only to a deterministic local trip-hold provider test double;
this repository does not search live inventory, book travel, take payment, or claim an official
vendor sandbox.

## Why this project exists

Agent failures are not only model-quality problems. They also come from state drift, silently
skipped constraints, duplicate execution, cancellation races, restart recovery, and unsafe tool
boundaries.

An early ablation in this repository produced a useful warning:

```text
full runtime completion rate: 50%
no-validator completion rate: 75%
```

The higher score was misleading. Trace inspection showed that the additional “successful” plan
violated the user's budget. The validator reduced nominal completion because it exposed an
invalid result instead of silently accepting it.

That finding became the design rule for the platform:

> Do not let the system look successful while failing somewhere the user or operator cannot see.

See [`FINDINGS.md`](FINDINGS.md) for the scenarios, traces, and ablation results.

## At a glance

| Concern | Implemented behavior | Where to inspect |
| --- | --- | --- |
| Planning | Strict `CALL_TOOL`, `REQUEST_CLARIFICATION`, and `FINISH` decisions | [`runtime_service/dynamic_loop.py`](runtime_service/dynamic_loop.py) |
| Policy | Fixed step-limit, allowlist, permission, and argument-schema checks before execution | [`docs/dynamic-tool-loop.md`](docs/dynamic-tool-loop.md) |
| Durability | SQLite-backed runs, events, checkpoints, Planner decisions, tool calls, and attempts | [`runtime_service/store.py`](runtime_service/store.py), [`runtime_service/workflow_store.py`](runtime_service/workflow_store.py) |
| Recovery | Decision replay, completed-result reuse, interrupted-step recovery, and pinned execution authority | [`tests/test_dynamic_tool_loop.py`](tests/test_dynamic_tool_loop.py) |
| External actions | Prepared intent, provider dispatch fencing, bounded idempotent recovery, and explicit unknown outcomes | [`runtime_service/external_action_coordinator.py`](runtime_service/external_action_coordinator.py), [`docs/durable-external-actions.md`](docs/durable-external-actions.md) |
| Security | Fail-closed API-key auth, tenant isolation, Viewer/Operator RBAC, registered-tool sandboxing | [`runtime_service/auth.py`](runtime_service/auth.py), [`docs/cloud-runtime.md`](docs/cloud-runtime.md) |
| Memory | Subject-scoped versioned preferences, sealed run snapshots, audit events, and operational forgetting | [`docs/governed-memory.md`](docs/governed-memory.md) |
| Domains | Five Travel runtime versions plus fixed-order and DAG release-validation versions | [`runtime_service/registry.py`](runtime_service/registry.py) |
| Evidence | Workflow-first projection for Planner, policy, tool, action, and loop-outcome evidence; manager recovery events remain direct | [`runtime_service/evidence.py`](runtime_service/evidence.py), [`tests/test_durable_external_action_api.py`](tests/test_durable_external_action_api.py) |
| Verification | Full suite; CI on Python 3.11 and 3.12 | [CI workflow](.github/workflows/ci.yml) |

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q
```

Configure one local Operator credential and start the API:

```bash
export RUNTIME_API_KEY="replace-with-a-random-local-key"
export RUNTIME_API_KEYS_JSON='[{"credential_id":"local-demo","api_key":"replace-with-a-random-local-key","tenant_id":"tenant-demo","subject_id":"local-user","role":"operator"}]'
uvicorn api.main:app --reload
```

PowerShell equivalent:

```powershell
$env:RUNTIME_API_KEY = "replace-with-a-random-local-key"
$env:RUNTIME_API_KEYS_JSON = '[{"credential_id":"local-demo","api_key":"replace-with-a-random-local-key","tenant_id":"tenant-demo","subject_id":"local-user","role":"operator"}]'
uvicorn api.main:app --reload
```

Open [the interactive API docs](http://127.0.0.1:8000/docs), or verify the public health
endpoints:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
```

Submit a Phase 7A Travel run with an explicit synthetic hold request:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "dynamic-tokyo-trip-001",
    "agent_id": "travel-agent",
    "agent_version": "1.2.0",
    "input": {
      "user_message": "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
      "requested_action": "create_hold"
    }
  }'
```

`requested_action` defaults to `plan_only`. Omitting it runs the same planning
path without preparing or dispatching an external action. The explicit
`create_hold` path validates the selected plan against persisted search,
ranking, cost, budget, and preference evidence before it can write a synthetic
hold.

Use the returned `run_id` to inspect the durable result and evidence:

```bash
curl -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/runs/<run_id>

curl -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/runs/<run_id>/events

curl -N -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/runs/<run_id>/events/stream
```

The observable loop is:

```text
planner.decision -> policy.decision -> external_action.* -> tool.result -> loop.outcome
```

Read-only steps omit `external_action.*`. A successful explicit hold emits
`external_action.prepared`, `external_action.dispatch_started`, and
`external_action.succeeded` with a sanitized provider reference.

The `1.1.0` and `1.2.0` paths also emit `memory.retrieved` and, when the message contains an explicit
allowlisted stable preference, `memory.created` or `memory.superseded`. A new thread for the same
authenticated subject retrieves that preference automatically:

```bash
curl -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/memories

curl -X DELETE -H "Authorization: Bearer $RUNTIME_API_KEY" \
  http://127.0.0.1:8000/memories/<memory_id>
```

If required information is missing, the completed run returns
`state.current_stage="needs_clarification"`. Submit another run of the same pinned Travel version on the
same tenant-qualified thread to continue from its durable checkpoint.

## Architecture

```mermaid
flowchart TB
    C[Client] --> A[FastAPI control plane]
    A --> I[Authentication and typed RBAC]
    I --> M[RuntimeManager and AgentRegistry]
    M --> S[(SQLite runs, events, checkpoints)]
    M --> T[Travel runtimes]
    M --> R[Release-validation runtimes]
    T --> X[Governed memory]
    X --> Q[(SQLite memories and run snapshots)]
    T --> L[DynamicToolLoop]
    R --> G[Static validated DAG]
    L --> P[Planner and policy gate]
    P -->|read_only| B[Registered-tool sandbox]
    P -->|external_write| EC[ExternalActionCoordinator]
    EC --> E[(External-action ledger)]
    E --> V[Registered provider boundary]
    L --> O[EvidenceProjector]
    G --> B
    B --> S
    E --> S
    O --> S
```

Every protected request derives `Principal`, `TenantContext`, and `RuntimeRole` from a server-side
credential record. Request bodies cannot provide tenant identity, role, executable paths, handler
modules, Python source, or shell commands.

Dynamic runs persist the authenticated subject and the server-evaluated permission snapshot.
Async and restarted workers use that original execution authority rather than trusting the
request body or recomputing a later role.

### The Phase 5A/7A execution loop

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Runtime
    participant P as Planner and policy
    participant T as Sandbox or provider
    participant D as Durable store

    C->>R: Submit run
    R->>D: Persist queued run and authority
    loop Bounded dynamic steps
        R->>P: Typed context and prior observations
        P-->>R: Typed decision
        R->>D: Persist indexed decision
        alt CALL_TOOL allowed
            R->>D: Claim durable tool step
            alt read_only
                R->>T: Execute registered handler
            else external_write
                R->>D: Prepare action and arbitrate cancellation
                R->>T: Dispatch registered provider
            end
            T-->>R: Structured result
            R->>D: Persist result and events
        else Clarification, finish, or rejection
            R->>D: Persist explicit outcome
        end
    end
    R-->>C: Polling or SSE evidence
```

The domain-neutral loop owns decision parsing, policy order, durable step identity, recovery, and
stable runtime failures. Each domain owns Planner context construction, observation mapping, and
final validation. A Planner `FINISH` decision cannot bypass the Travel budget and preference
validator.

For an external write, `ToolEffect` selects the provider path and
`ToolRetryMode` controls ambiguous-result recovery. Intent is durable before
provider entry, the idempotency key is derived by the server, and successful
action/result finalization updates the action row and parent tool call together.
The direct tool endpoint cannot execute external writes.

### The Phase 6A memory boundary

`travel-agent:1.1.0` retrieves only allowlisted, active preferences for the authenticated
`tenant_id + subject_id`. The first retrieval, including an empty result, is sealed per run before
Planner execution. Recovered attempts reuse that exact typed snapshot rather than querying current
memory again.

Current-message preferences override retrieved values. Values injected by `1.1.0` are execution
overlays, not thread-checkpoint preferences, so superseding or deleting them does not leave a stale
memory copy in that thread. Exact mutation history remains versioned and auditable. See
[`docs/governed-memory.md`](docs/governed-memory.md) for conflict, forgetting, and evidence
semantics.

`travel-agent:1.2.0` inherits this exact sealed memory behavior. Its
`requested_action` and external-action evidence do not create a second memory
store or change the published `1.1.0` schema.

### The Phase 7A external-action boundary

`travel-agent:1.2.0` uses a separate private four-tool registry and input schema.
The wrapper removes `create_trip_hold` from the tool descriptors passed to the
base/model Planner, so that Planner always sees only the three read-only tools.
`plan_only` is the default: valid read-only planning decisions are preserved and
no action is prepared, while a base Planner that fabricates
`CALL_TOOL create_trip_hold` is rejected. Only `DurableActionTravelPlanner` may
insert that call, after explicit `requested_action="create_hold"`, a base
`FINISH`, and deterministic validation of the final candidate. It then requires
a matching successful provider observation before accepting the original
`FINISH`.

Independently of Planner filtering, the `create_trip_hold` `ToolSpec` has a
server-only `runtime_input_gate`. If the durable loop receives that action
decision while the run input is `plan_only`—including a forged decision already
persisted for replay—it fails with `external_action_not_requested` before
claiming any tool step or action row, and performs zero provider dispatches.

The `1.0.0` and `1.1.0` loops retain separate three-tool registry instances and
their unchanged `TravelMessageInput` schemas. They cannot select
`create_trip_hold` and do not accept `requested_action`.

The internal `external_actions` ledger records prepared intent, dispatch state,
retry mode, the logical provider route, a stable concrete provider identity,
provider reference, and terminal status. Provider-idempotent
recovery reuses the stable key with at most two total dispatches; unsafe
ambiguous work becomes `outcome_unknown` without blind retry. If cancellation
commits before dispatch, provider code is not called. Once dispatch has started,
a successful action can outlive a later run cancellation and is not
automatically compensated. See
[`docs/durable-external-actions.md`](docs/durable-external-actions.md).

After the provider result is committed, the runtime makes up to two complete
attempts to mirror the action and tool evidence into public run events. If both
fail, the action remains `succeeded` with its provider reference, while the run
fails with `external_action_evidence_incomplete`; cancellation cannot hide that
already-completed action/evidence gap.

Restart recovery also treats post-dispatch drift as an uncertainty boundary. If
thread-state/workflow identity, the runtime-input gate, provider configuration,
availability or capability, persisted provider identity, permission, tool
effect, or argument/output schema no longer matches an in-flight dispatched
action, the provider is not called again and the action is finalized/reported as
`outcome_unknown`, rather than downgraded to an ordinary not-requested,
configuration, or validation error. An already terminal action retains its
stronger succeeded, failed, or unknown reconciliation semantics.

## Registered runtime versions

Versions remain registered because durable runs pin exact behavior for recovery and replay.

| Agent | Version | Execution model | Purpose |
| --- | --- | --- | --- |
| `travel-agent` | `0.3.0` | Deterministic application runtime | Original typed state, reducer, validation, and partial replanning path |
| `travel-agent` | `0.5.0` | Deterministic review workflow | Budget and preference evidence review with validator-gated local replanning |
| `travel-agent` | `1.0.0` | Policy-governed dynamic loop | Observation-driven tool selection, clarification, and validated finish |
| `travel-agent` | `1.1.0` | Governed cross-thread memory | Subject-scoped typed preferences, sealed retrieval snapshots, and auditable forgetting |
| `travel-agent` | `1.2.0` | Durable external actions | Explicit synthetic trip holds with prepared intent, provider idempotency, and uncertain-outcome recovery |
| `release-validation` | `1.0.0` | Fixed-order workflow | Compatibility contract for existing durable runs |
| `release-validation` | `1.1.0` | Static validated DAG | Step persistence, selective replay, signature-safe evidence reuse |

The release-validation domain is independent of Travel. Its manifests, artifacts, compatibility
records, and tool results are synthetic fixtures. Scheduling is intentionally serial; no parallel
execution claim is made.

## Reliability and safety properties

### Typed state and visible failure

- Pydantic input, state, tool-argument, and Planner-decision contracts;
- explicit `StatePatch` transitions and deterministic nested-state reduction;
- strict rejection of unknown or cross-domain state fields;
- stable failure codes for invalid tools, arguments, permission, timeout, handler failure, step
  exhaustion, invalid Planner decisions, provider failure, unknown external outcomes, and
  incomplete run-evidence mirroring after a successful action;
- domain validation after Planner `FINISH`, with `FAILED` and `BLOCKED` kept distinct;
- typed evidence showing checked rules, skipped checks, blockers, and replanning directives.

### Durable execution and recovery

- asynchronous run lifecycle with append-only events and thread checkpoints;
- idempotent submission scoped by tenant and `client_request_id`;
- exact Agent-version pinning;
- atomic cancellation/completion guard;
- startup recovery for queued and running work;
- exclusive step claims and attempt-token protection;
- immutable selective-replay child runs with typed source and step lineage;
- replay of indexed Planner decisions and reuse of completed tool results;
- explicit interrupted-step recovery without silently retrying a persisted terminal tool failure;
- sealed per-run memory retrieval, including empty snapshots, across restart and retry;
- versioned preference conflict handling with idempotent same-value writes;
- prepare-before-dispatch external-action intent linked to its durable tool step;
- stable server-derived provider idempotency keys and dispatch-token fencing;
- atomic external action/tool-result finalization with provider-reference evidence;
- stable non-secret provider deployment/account identity bound into prepared intent;
- strict external-write output models that persist only normalized allowlisted provider fields;
- bounded provider-idempotent recovery and explicit no-retry `outcome_unknown` for unsafe work;
- cancellation arbitration before first dispatch;
- fail-closed post-dispatch reconciliation for identity, provider, capability, permission, and
  schema drift;
- crash-equivalent recovery when uncertain-outcome terminalization itself cannot be committed;
- stable `external_action_evidence_incomplete` reporting without undoing a succeeded action.

The external-action claim is intentionally narrow. The bundled write is a deterministic
provider-side SQLite test double, not a live booking, payment, or inventory integration. Phase 7A
does not promise exactly-once execution, compensation, or human approval. A successful action may
remain durable even if cancellation later wins the run's completion boundary.

### Tenant and tool boundaries

- fail-closed Bearer API-key authentication;
- tenant-qualified runs, idempotency keys, events, checkpoints, tool linkage, and replay sources;
- tenant-and-subject-qualified memory records, history, retrieval snapshots, and APIs;
- same-shape `404` responses for cross-tenant and unknown resources;
- centralized default-deny Viewer/Operator authorization;
- independent `external-actions:execute` permission for provider dispatch;
- service-derived execution authority excluded from API responses;
- server-owned tool registry and Pydantic arguments with unknown fields rejected;
- fixed subprocess worker, fresh temporary working directory, scrubbed environment, timeout,
  bounded output, process-group termination, and POSIX resource limits;
- `tini` configured as container PID 1 for signal forwarding and orphan reaping;
- external-write tools excluded from direct sandbox/API execution and routed only through the
  prepared action ledger.

The process sandbox protects trusted registered tools from accidental or unauthorized selection
and resource leakage. It is not a general untrusted-code sandbox: it does not isolate host
networking or create a private mount namespace.

## API surface

| Endpoint | Purpose | Viewer | Operator |
| --- | --- | ---: | ---: |
| `GET /health`, `GET /ready` | Liveness and readiness | public | public |
| `GET /agents`, `GET /tools` | Discover Agent contracts and the three-tool public read-only catalog | yes | yes |
| `GET /runs/{id}` | Read one tenant-scoped run | yes | yes |
| `GET /runs/{id}/events` | Read durable events | yes | yes |
| `GET /runs/{id}/events/stream` | Stream the same event model over SSE | yes | yes |
| `GET /threads/{id}/state` | Read the tenant-scoped checkpoint | yes | yes |
| `GET /memories` | Read the current subject's active or historical memory records | yes | yes |
| `POST /runs` | Create or selectively replay a run | no | yes |
| `POST /runs/{id}/cancel` | Request cooperative cancellation | no | yes |
| `POST /tools/{tool}/execute` | Execute a registered read-only tool directly; external writes return `409` | no | yes |
| `POST /agent/message` | Call the synchronous Travel compatibility path | no | yes |
| `DELETE /memories/{id}` | Forget one logical memory key for future runs | no | yes |

`create_trip_hold` is intentionally absent from `GET /tools`; it belongs only
to the private `travel-agent:1.2.0` durable registry. The direct POST route still
recognizes that name so an authorized attempt receives `409` instead of falling
through to sandbox execution.

`RUNTIME_API_KEYS_JSON` is the local credential provider. Every credential must declare
`viewer` or `operator`; missing or unknown roles fail configuration loading without retaining the
plaintext key in the validation exception chain. Plaintext keys are hashed when loaded and are not
retained by the authenticator. If the variable is absent or empty, every protected endpoint fails
closed with `401`.

This is deliberately small static RBAC, not a general policy platform.

By default, the logical `travel-trip-hold` provider route is bound to a
deterministic SQLite test double in a file next to, but separate from, the
runtime database. A deployment can instead inject the generic JSON-over-HTTP
provider boundary:

```bash
export RUNTIME_TRAVEL_ACTION_PROVIDER_URL="https://provider.example/v1/actions"
export RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY="travel-hold-deployment-v1"
export RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN="server-owned-optional-token"
export RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY="true"
```

When the URL is set, the adapter sends the normalized action request with the
stable `Idempotency-Key` header and optional Bearer token. The token and endpoint
are server configuration, never Planner input or public evidence. This is a
configurable boundary, not a claim that a real airline, booking, inventory, or
payment provider has been validated.

`RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY` is required in HTTP mode. It is a
stable, non-sensitive deployment/account identifier—not an endpoint or
credential—and must stay constant across restarts and token rotation for the
same backing account. If it changes while an action is in flight, recovery
reports `outcome_unknown` and does not send the action to the new identity.

The HTTP adapter defaults `supports_idempotency` to false. Because
`create_trip_hold` is registered as `PROVIDER_IDEMPOTENT`, dispatch fails closed
before preparing an action unless the deployment explicitly sets
`RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY=true` after verifying that
the endpoint actually honors the key. Merely sending `Idempotency-Key` is not
evidence of provider-side deduplication.

HTTPS is the default and the only production transport. Every non-loopback `http://`
endpoint is rejected, with or without Bearer authentication. Loopback HTTP is
accepted only for explicit development when
`RUNTIME_TRAVEL_ACTION_PROVIDER_ALLOW_INSECURE_LOCALHOST=true`; the switch is
required even without a Bearer token and never permits plaintext dispatch to a
remote host.

Only HTTP `200` and `201` responses are eligible for synchronous success, and
their bounded JSON bodies must still pass the typed result checks. Every other
status is ambiguous by default. The provider constructor has a server-only
`definitive_status_codes` option for an explicitly verified, deliberately small
set of `4xx` responses whose provider contract guarantees no effect. The API
exposes no environment setting for that option, so the configured HTTP provider
uses the empty set by default.

Every external-write tool declares a Pydantic `output_model` with
`extra="forbid"`. Only its normalized allowlisted fields plus the trusted
provider reference can reach the durable tool/action result or public evidence.
Before a Travel hold is committed as succeeded, its result must exactly match
the prepared destination, selected option, quoted total, and hold duration; its
reference must be an opaque `hold_` identifier. Extra provider fields—including
debug or secret material—and mismatched results are rejected before persistence;
raw responses are never added to run events.

## Optional live-model Planner

The default scripted Planner is deterministic and is the path used by normal CI. To drive the
same `travel-agent:1.0.0` loop through the OpenAI Responses adapter:

```bash
pip install -r requirements-model-demo.txt
export RUNTIME_PLANNER_PROVIDER=openai
export OPENAI_API_KEY="..."
export OPENAI_MODEL="..."
python examples/model_driven_travel_demo.py
```

The adapter exposes strict function schemas for the same three decisions and stores only typed
decisions and results. It does not persist the system prompt, raw provider response, or
provider-supplied terminal reasoning. CI uses a fake Responses client and does not claim to make a
live model request.

The Travel catalog remains synthetic in this mode.

## Selective replay example

`release-validation:1.1.0` creates a new immutable child run when replay is requested:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "release-replay-2.4.0",
    "agent_id": "release-validation",
    "agent_version": "1.1.0",
    "input": {
      "manifest": {"...": "typed synthetic manifest fields"},
      "replay": {
        "source_run_id": "run_source",
        "step_ids": ["run_unit_tests"]
      }
    }
  }'
```

Requested nodes and their descendants rerun. Source evidence is copied only when node, tool, and
input signatures still match; the source run is never mutated.

## What to inspect first

| Path | Why it matters |
| --- | --- |
| [`runtime_service/dynamic_loop.py`](runtime_service/dynamic_loop.py) | Domain-neutral Planner/policy/tool state machine and recovery logic |
| [`runtime_service/external_actions.py`](runtime_service/external_actions.py) | Provider contracts, registry, and sanitized dispatch boundary |
| [`runtime_service/http_external_action.py`](runtime_service/http_external_action.py) | Optional server-configured JSON-over-HTTP provider adapter |
| [`runtime_service/manager.py`](runtime_service/manager.py) | Durable run lifecycle, worker queue, cancellation, and restart handoff |
| [`runtime_service/auth.py`](runtime_service/auth.py) | Principal derivation, typed permissions, and default-deny authorization |
| [`runtime_service/memory.py`](runtime_service/memory.py) | Versioned records, audit events, sealed snapshots, and run evidence |
| [`domains/travel/dynamic_runtime.py`](domains/travel/dynamic_runtime.py) | Reference adapter and domain-owned final validation |
| [`domains/travel/external_action_runtime.py`](domains/travel/external_action_runtime.py) | Explicit Travel hold planning and provider-evidence validation |
| [`domains/travel/tools/trip_hold_provider.py`](domains/travel/tools/trip_hold_provider.py) | Deterministic provider-side SQLite test double |
| [`domains/travel/memory.py`](domains/travel/memory.py) | Allowlisted extraction and typed Travel preference mapping |
| [`domains/release_validation/runtime.py`](domains/release_validation/runtime.py) | Independent DAG workflow and selective replay adapter |
| [`tests/test_execution_authority.py`](tests/test_execution_authority.py) | Trusted authority, restart, tampering, and idempotent-resubmission proofs |

## Documentation map

- [`docs/dynamic-tool-loop.md`](docs/dynamic-tool-loop.md): Planner contracts, policy order,
  failure codes, recovery boundaries, and model adapter;
- [`docs/durable-external-actions.md`](docs/durable-external-actions.md): action ledger,
  idempotency, provider boundary, cancellation arbitration, and uncertain outcomes;
- [`docs/governed-memory.md`](docs/governed-memory.md): subject isolation, versioning, sealed
  retrieval, forgetting, RBAC, and audit evidence;
- [`docs/cloud-runtime.md`](docs/cloud-runtime.md): durable lifecycle, API, sandbox, deployment,
  and security model;
- [`docs/release-validation-workflow.md`](docs/release-validation-workflow.md): DAG validation,
  selective replay, identity hashing, retry, and interrupted recovery;
- [`docs/evidence-review-workflow.md`](docs/evidence-review-workflow.md): review contracts,
  partial results, local replanning, and semantic-analyzer boundary;
- [`FINDINGS.md`](FINDINGS.md): evaluation methodology and behavioral findings;
- [`docs/sample_trace.md`](docs/sample_trace.md): an annotated application-runtime trace.

Historical evaluation and reinforcement-learning artifacts remain under `eval/` and `rl/`; they
are not on the cloud-runtime request path.

## Tests and CI

Run the same static and behavioral gates used by GitHub Actions:

```bash
python -m compileall agent api runtime_service domains
ruff check agent api runtime_service tests domains
mypy
pytest -q
```

The full suite covers typed reduction and validation, golden traces, review evidence, multi-turn
continuation, run lifecycle, cancellation races, restart recovery, idempotency, tenant isolation,
RBAC, schema migration, DAG validation, selective replay, tool sandboxing, all eight Phase 5A
failure codes, policy-order precedence, decision replay, cached tool results, execution authority,
REST/SSE evidence equality, and the fake OpenAI Responses boundary.
Phase 6A additionally covers cross-thread restart continuity, same-tenant subject isolation,
version conflicts, empty-snapshot sealing, memory RBAC, and deletion without checkpoint
resurrection.
Phase 7A covers version/schema isolation, explicit action intent, prepare-before-dispatch
durability, provider-side deduplication and conflicts, bounded provider-idempotent recovery,
unsafe unknown outcomes, cancellation/dispatch arbitration, direct-endpoint blocking, provider
references, post-dispatch drift reconciliation, unrecoverable run-evidence gaps,
provider-identity continuity, strict output filtering, HTTPS/loopback policy,
HTTP-boundary sanitization, and action-event REST/SSE parity.

GitHub Actions runs compile checks, Ruff, scoped Mypy, and pytest on Python 3.11 and 3.12.

## Deployment boundary

Docker, Docker Compose, and a deliberately single-replica Kubernetes manifest are included.
SQLite and the in-process queue keep the project self-contained, but they are not horizontally
scalable.

Durable workflow consumers target the structural `WorkflowStore` contract, while the service
composition root still supplies the repository's only implementation, `SQLiteWorkflowStore`.
This separates runtime typing from SQLite without claiming that another backend already preserves
the required cross-ledger transactions, fencing, ordering, and restart-recovery semantics.

Before increasing replicas, the architecture would need:

```text
PostgreSQL runs/checkpoints/events/memories/external_actions
+ Redis, Pub/Sub, or another distributed queue
+ worker leases and heartbeats
+ provider-specific reconciliation and compensation
+ OpenTelemetry traces and metrics
+ container-backed sandbox workers
```

Docker Compose requires `RUNTIME_API_KEYS_JSON` from the caller environment. The Kubernetes
manifest expects the same JSON in Secret `travel-agent-runtime-auth`, key `api-keys.json`; the
repository does not contain or generate credential material.

## Deliberate limitations

This is a completed portfolio/reference milestone, not a claim of a complete production Agent
platform.

- SQLite rather than PostgreSQL, and an in-process queue rather than distributed workers;
- no worker lease, heartbeat, quota, token accounting, key rotation, or external secret manager;
- only two static roles, with no custom or per-Agent/per-tool grants;
- process isolation rather than a container, gVisor, or microVM sandbox;
- no arbitrary user-code or untrusted third-party MCP execution;
- serial DAG and dynamic-tool scheduling;
- synthetic Travel and release-validation data;
- no real flight, hotel, booking, payment, or inventory integration and no official vendor
  sandbox claim;
- the optional HTTP provider is a configurable transport boundary, not a validated live Travel
  provider;
- no OpenTelemetry backend or evaluation dashboard;
- no prompt-injection detector beyond typed decisions, policy checks, and registered tools;
- memory is limited to explicit allowlisted preferences, without embeddings, inferred facts, or
  erasure of immutable historical run evidence;
- no exactly-once guarantee, compensation/rollback workflow, human approval, or automated
  reconciliation for unknown external-action outcomes.

Phase 7A is the completed portfolio milestone. Human approval, semantic memory retrieval, bounded
parallel read-only calls, a live read-only Travel adapter, multi-model fallback, and durable
multi-Agent delegation are possible follow-up slices, not prerequisites for the runtime
demonstrated here.

## License

This project is licensed under the [MIT License](LICENSE).
