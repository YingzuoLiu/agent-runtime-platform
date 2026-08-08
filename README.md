# Multi-Domain Agent Runtime Demo

A runnable reference implementation of six related layers:

1. an **application-level Agent Runtime** for structured multi-turn travel planning;
2. a small **self-hosted cloud runtime** for durable run lifecycle management;
3. **policy-enforced registered-tool sandboxing** using restricted subprocess workers.
4. an optional **evidence-review workflow** with typed handoff, partial results, and
   validator-gated local replanning.
5. a second **release-validation domain** using the same durable run lifecycle and
   unified HTTP API with an explicit DAG and selective replay.
6. a **policy-governed dynamic tool loop** with typed Planner decisions, durable
   evidence, restart-safe tool calls, and Travel as its first reference adapter.

The project is offline-first. Its default scripted Planner does not require an LLM key,
Redis, PostgreSQL, or Kubernetes. An opt-in OpenAI Responses adapter demonstrates the
same loop with live model decisions while retaining synthetic, non-booking Travel tools.

## Why this project exists

Long-horizon Agent failures are not only model-quality problems. They also come from state drift, silently skipped constraints, ambiguous completion metrics, duplicate execution, cancellation races, restart recovery, and unsafe tool boundaries.

The project started as an application-runtime experiment around typed state, validation, retry, and partial replanning. An ablation study then produced a counterintuitive result:

```text
full runtime completion rate: 50%
no-validator completion rate: 75%
```

The higher no-validator score was misleading. Trace inspection showed that the apparently completed plan violated the user's budget. The validator reduced nominal completion rate because it surfaced an invalid plan instead of silently accepting it.

The detailed scenarios, configurations, traces, and findings are documented in [`FINDINGS.md`](FINDINGS.md).

That experiment established the reliability theme that now connects the whole repository:

> Do not let the system look successful while failing somewhere the user or operator cannot see.

The later engineering work extends that same objective across additional boundaries:

- typed state and deterministic validation make constraint violations visible;
- durable run records make execution state observable across requests and restarts;
- atomic completion and cancellation updates prevent stale writes from hiding races;
- idempotent submission prevents network retries from silently duplicating work;
- API-key authentication derives a trusted tenant context before control-plane work begins;
- tenant-qualified persistence prevents runs, events, checkpoints, and replay evidence from crossing tenant boundaries;
- a typed default-deny role policy separates read-only observation from runtime mutation;
- persisted execution-authority snapshots keep async and restarted workers on the original permissions;
- tool allowlists and schema validation prevent unapproved execution;
- typed Planner decisions and indexed evidence make dynamic tool selection inspectable;
- subprocess timeouts, resource limits, and `tini` prevent runaway work from leaking resources.

The engineering layer is therefore not a replacement for the evaluation work. It is the next layer of the same reliability problem.

## Architecture

```text
Client
  |
  v
Bearer API key -> Principal / TenantContext / RuntimeRole
  |
  v
RoleAuthorizer -> typed permission / persisted ExecutionAuthority
  |
  v
FastAPI control API
  |- POST /agent/message
  |- POST /runs
  `- POST /tools/{tool}/execute
          |
          +-------------------------------+
          |                               |
          v                               v
RuntimeManager ---- AgentRegistry     ToolSandbox ---- ToolRegistry
  |                                      |
  +---- worker queue                     `- restricted subprocess
  |
  +---- SQLiteRunStore
  |       |- tenant-scoped runs
  |       |- run_events
  |       `- tenant-scoped thread_states / checkpoints
  |
  +--> TravelAgentRuntime 0.3 / 0.5
  |      `- typed patch/replan/review/validation
  |
  +--> DynamicTravelRuntime 1.0
  |      `- DynamicToolLoop -> Planner -> policy -> durable tool call
  |
  `--> ManagedReleaseValidationRuntime
         `- durable DAG + selective replay
```

Both Agent API paths read and write the same durable `thread_states` store. Authenticated tenant context scopes run lookup, idempotency, checkpoints, events, tool-to-run linkage, and selective-replay sources. Dynamic runs also persist the authenticated subject and server-evaluated effective permissions; workers never derive execution authority from request body fields. Tool executions may optionally attach their start and finish events to a durable `run_id` visible to that tenant.

## What the project demonstrates

### Evaluation and behavioral analysis

- controlled ablations for validator and retry behavior;
- scenario-level completion, blocker, replan, and validation measurements;
- trace inspection to distinguish real success from silent constraint violations;
- a concrete finding that completion rate alone is not a sufficient Agent metric;
- a bug discovered through evaluation: confirmation intent was previously treated as an unsupported request;
- a remaining product gap: budget failure needs alternative suggestions rather than repeated invalid replans.

See [`FINDINGS.md`](FINDINGS.md) for the full study.

### Application runtime

- typed `AgentState` with Pydantic;
- explicit `StatePatch` transitions;
- reducer-based nested-state updates;
- partial replanning after changed constraints;
- deterministic budget, itinerary, and flight validation;
- optional geography grounding;
- a feature-flagged Budget + Preference review workflow;
- one canonical `PlanEvidence` snapshot with role-limited typed projections;
- structured evidence, checked-rule coverage, and explicit skipped checks;
- deadline-aware concurrent review with `completed_partial` semantics;
- deterministic finding reduction and typed local-replan directives;
- visible blockers and application trace events.

See [`docs/evidence-review-workflow.md`](docs/evidence-review-workflow.md) for the review
contracts, retry semantics, offline semantic-analyzer boundary, and deliberate next steps.

### Cloud runtime

- asynchronous `POST /runs` API;
- typed, registry-discovered input and state schemas for multiple domains;
- fail-closed Bearer API-key authentication with typed `Principal` and `TenantContext`;
- tenant-scoped runs, idempotency keys, events, checkpoints, and replay references;
- Viewer/Operator authorization through a centralized typed, default-deny policy;
- strict domain-state validation that rejects unknown or cross-domain fields;
- durable `run_id` lifecycle;
- exact Agent-version pinning;
- worker-based execution;
- SQLite-backed runs, events, and thread checkpoints;
- restart recovery for queued/running work;
- cooperative cancellation with an atomic completion guard;
- idempotent run submission through `client_request_id`;
- polling and Server-Sent Events APIs;
- immutable replay child runs with typed source/step lineage;
- Docker, Docker Compose, and a deliberately single-replica Kubernetes manifest.

### Registered-tool sandbox

- server-side tool allowlist;
- Pydantic argument validation with unknown fields rejected;
- fixed executable and fixed worker script;
- fresh temporary working directory per execution;
- scrubbed environment that does not forward runtime secrets;
- wall-clock timeout and process-group termination;
- bounded returned output;
- POSIX CPU, memory, file-descriptor, and core-dump limits;
- `tini` as container PID 1 to reap orphaned descendants;
- structured execution results;
- optional linkage to append-only run events.

The sandbox intentionally does **not** accept Python source, shell commands, executable paths, or arbitrary module names.

### Policy-governed dynamic tool loop

- strict `CALL_TOOL`, `REQUEST_CLARIFICATION`, and `FINISH` Planner decisions;
- a domain-neutral `DynamicToolLoop` with no Travel or release-validation imports;
- fixed pre-execution checks for step limit, allowlist membership, durable permission,
  and argument schema;
- stable failure codes for invalid tools/arguments, denied permission, timeout, handler
  failure, step exhaustion, invalid decisions, and provider failure;
- indexed Planner decisions and tool calls persisted before execution;
- user-visible `planner.decision`, `policy.decision`, `tool.result`, and `loop.outcome`
  events through both REST and SSE;
- no system prompt, raw provider response, or provider-supplied terminal reason in
  dynamic evidence; only typed decisions/results are stored, with adapter-generated
  terminal reason labels;
- explicit interrupted-step recovery and completed-result reuse after restart;
- deterministic Travel tools whose next arguments and final branch depend on prior results;
- domain-owned final mapping plus `TravelValidator`, so Planner `FINISH` cannot bypass
  budget or preference constraints;
- deterministic scripted planning for CI and an optional generic OpenAI Responses adapter.

Travel's `search_trip_options` returns a synthetic reference catalog, not live inventory.
Nothing in Phase 5A books or purchases travel. See
[`docs/dynamic-tool-loop.md`](docs/dynamic-tool-loop.md) for contracts, policy order,
failure semantics, recovery boundaries, and the model-driven demo.

### Release-validation workflow

A durable registered-tool workflow using shared runtime infrastructure, with an explicit validated DAG, per-step persistence, selective replay, bounded retry, explicit interrupted-step recovery, cached completed results, and deterministic readiness validation.

- a second, independent domain (`domains/release_validation/`) built on the same `SQLiteWorkflowStore` and `ToolSandbox` as the rest of `runtime_service`, with no relationship to Travel;
- five independent root nodes feeding one evidence fan-in node, scheduled serially in deterministic topological order;
- replay child runs that rerun requested nodes and descendants while copying only source evidence whose node, tool, and input signatures still match;
- copied evidence recorded with zero target attempts and append-only replay events, without mutating the terminal source run;
- every executed node persisted through `SQLiteWorkflowStore`'s exclusive step-claim and attempt-token protection;
- a deterministic validator that inspects tool *results*, not just tool-call success, so a release can be BLOCKED even when every tool call completed;
- explicit FAILED/BLOCKED separation: exhausted retries or an unexpected exception are FAILED, a business readiness check that did not pass is BLOCKED.

See [`docs/release-validation-workflow.md`](docs/release-validation-workflow.md) for graph validation, replay invalidation, identity hashing, retry classification, and interrupted-recovery semantics.

This is a static code-defined DAG, not planner- or LLM-driven tool selection. A typed adapter exposes it through `AgentRegistry`, `RuntimeManager`, and the shared `/runs` API, while `SQLiteWorkflowStore` owns step-level attempts and evidence. Scheduling remains serial; no parallel-execution claim is made. All release manifests, artifacts, and compatibility data are synthetic fixtures for this repository.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q
export RUNTIME_API_KEY="replace-with-a-random-local-key"
export RUNTIME_API_KEYS_JSON='[{"credential_id":"local-demo","api_key":"replace-with-a-random-local-key","tenant_id":"tenant-demo","subject_id":"local-user","role":"operator"}]'
uvicorn api.main:app --reload
```

`RUNTIME_API_KEYS_JSON` configures the local credential provider. Every record must declare
`role` as `viewer` or `operator`; a missing or unknown role makes configuration loading fail
without retaining the plaintext key in the validation exception chain. If the variable is absent
or empty, every protected endpoint fails closed with `401`; only `/health` and `/ready` remain
public. Plaintext keys are hashed when loaded and are not retained by the authenticator. This
local provider is deliberately separate from the later external secret-provider slice.

Health endpoints:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
```

## Synchronous compatibility API

```bash
curl -X POST http://127.0.0.1:8000/agent/message \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "tokyo-trip-001",
    "user_message": "I want a 5-day Tokyo trip under 9000 SGD."
  }'
```

This endpoint executes in the request process but saves its updated state to the same checkpoint store used by asynchronous runs.

## Durable run API

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "tokyo-trip-001",
    "agent_id": "travel-agent",
    "agent_version": "0.5.0",
    "input": {
      "user_message": "Change the budget to 10000 and avoid red-eye flights."
    },
    "client_request_id": "request-20260713-001"
  }'
```

The legacy top-level `user_message` remains accepted for Travel clients. New clients use the
domain-neutral `input` object shown above. `GET /agents` publishes each registered runtime's
`domain_id`, `schema_version`, and JSON input schema. Repeating the same
`client_request_id` returns the existing run for the authenticated tenant instead of creating a duplicate.

`travel-agent:0.5.0` opts into the evidence-review path. The original
`travel-agent:0.3.0` version remains registered and is still the request default.
`travel-agent:1.0.0` explicitly opts into the Phase 5A dynamic loop:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "dynamic-tokyo-trip-001",
    "agent_id": "travel-agent",
    "agent_version": "1.0.0",
    "input": {
      "user_message": "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
    }
  }'
```

If required information is missing or the selected synthetic option exceeds budget, the run
still completes and returns a clarification question with
`state.current_stage="needs_clarification"`. Submit another `1.0.0` run on the same thread
to continue from the durable checkpoint.

The default Planner is deterministic and offline. To run the same API slice with a live model
decision boundary:

```bash
pip install -r requirements-model-demo.txt
export RUNTIME_PLANNER_PROVIDER=openai
export OPENAI_API_KEY="..."
export OPENAI_MODEL="..."
python examples/model_driven_travel_demo.py
```

CI injects a fake Responses client; it does not claim to make a live model request. The demo
still uses synthetic Travel tools and performs no booking.

```bash
curl -H "Authorization: Bearer $RUNTIME_API_KEY" http://127.0.0.1:8000/runs/<run_id>
curl -H "Authorization: Bearer $RUNTIME_API_KEY" http://127.0.0.1:8000/runs/<run_id>/events
curl -N -H "Authorization: Bearer $RUNTIME_API_KEY" http://127.0.0.1:8000/runs/<run_id>/events/stream
curl -X POST -H "Authorization: Bearer $RUNTIME_API_KEY" http://127.0.0.1:8000/runs/<run_id>/cancel
```

The release-validation domain uses the same lifecycle endpoints:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "release-2.4.0",
    "agent_id": "release-validation",
    "agent_version": "1.1.0",
    "input": {"manifest": {"...": "typed synthetic manifest fields"}}
  }'
```

`release-validation:1.0.0` remains registered with its Phase 3A fixed-order
input contract so persisted runs keep their pinned recovery behavior.
`release-validation:1.1.0` is the Phase 3B DAG/replay contract used below.

Selective replay also uses `POST /runs`, creating a new immutable child run:

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

## Sandboxed tool API

List the only tools clients are allowed to request:

```bash
curl -H "Authorization: Bearer $RUNTIME_API_KEY" http://127.0.0.1:8000/tools
```

Execute a deterministic tool:

```bash
curl -X POST http://127.0.0.1:8000/tools/route_cost_summary/execute \
  -H "Authorization: Bearer $RUNTIME_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": null,
    "arguments": {
      "transport_cost": 2000,
      "hotel_cost": 3000,
      "activity_cost": 1000,
      "budget": 7000
    }
  }'
```

An unknown tool is denied before any subprocess starts. Invalid arguments are rejected before execution. The default registry exposes:

```text
search_trip_options
rank_trip_options
route_cost_summary
```

## Security boundary

Every control-plane endpoint except `/health` and `/ready` authenticates a Bearer API key before
request execution. The local credential provider maps the key to an immutable `Principal`,
`TenantContext`, and configured `RuntimeRole`; neither `tenant_id` nor `role` is accepted from
request bodies. `RoleAuthorizer` then checks one typed permission at the operation boundary and
denies permissions that are not explicitly granted.

For `POST /runs`, the API also evaluates every declared permission and stores the resulting
subject/tenant/permission snapshot as internal execution authority. Dynamic workers and restart
recovery use that persisted snapshot rather than trusting the request or recomputing a later role.
The authority is excluded from API responses; the stable failure code
`tool_permission_denied` remains visible when a Planner requests a tool without permission.

| Operation | Viewer | Operator |
| --- | ---: | ---: |
| List agents and tools | yes | yes |
| Read run, events/SSE, and thread state | yes | yes |
| Create or replay a run | no | yes |
| Cancel a run | no | yes |
| Execute a tool | no | yes |
| Call the synchronous compatibility endpoint | no | yes |

Same-tenant permission failures return `403`. Cross-tenant run, cancellation, event, stream,
checkpoint, tool-linkage, and replay-source lookups retain the same `404` shape as an unknown
resource. SQLite uniqueness and checkpoint keys are tenant-qualified, so tenants may reuse the
same `thread_id` and `client_request_id` without sharing state.

This is deliberately small static RBAC, not a general policy platform. Per-Agent/per-tool grants,
custom roles, user/role persistence, quotas, key rotation, and an external/AWS-backed secret
provider remain later slices.

The execution backend is a **registered-tool process sandbox**, not a general untrusted-code service.

It protects the runtime from accidental or unauthorized tool selection, malformed inputs, inherited API keys, runaway execution time, and excessive POSIX resource use. Registered tools remain trusted service code.

The descriptor reports `network_mode: host` because the process backend does not claim to block outbound network access. It also does not create a private mount namespace, so it cannot safely run arbitrary user code or untrusted third-party MCP servers.

The Docker image starts the service under `tini`, which runs as container PID 1, forwards signals, and reaps orphaned descendants. This prevents timed-out tools that spawn child processes from leaving unreaped zombies in long-running containers. Running directly on a host still relies on the host init or service manager for orphan reaping.

A stronger boundary should use an ephemeral container, Kubernetes Job, gVisor sandbox, or microVM with:

```text
read-only root filesystem
+ isolated writable workspace
+ no host mounts
+ dropped capabilities
+ seccomp/AppArmor
+ disabled or allowlisted network
+ non-root UID
+ CPU/memory/PID limits
+ execution deadline
+ approved image/dependency set
```

See [`docs/cloud-runtime.md`](docs/cloud-runtime.md) for the detailed execution and security model.

## Agent integration boundary

The sandbox is exposed as an independent control-plane API, used by the static
release-validation DAG, and used by `travel-agent:1.0.0` through the dynamic loop. All three
paths resolve server-owned `ToolSpec` records. Domain handlers are imported only from the
entrypoint attached to that registered spec; planners and request payloads cannot provide an
executable path or handler module.

Phase 5A tools are deterministic and read-only, so the existing attempt-token ledger and cached
results provide restart-safe replay for this slice. Side-effecting tools would additionally need
per-call idempotency contracts, approval policy, and compensation behavior. Prompt-injection
detection, side-effect approval, parallel tool calls, multi-Agent delegation, and a general
per-Agent/per-tool grant model remain deliberate later slices.

## Cancellation semantics

Cancellation is cooperative: code already executing inside an Agent step is not forcibly interrupted. The database sets `cancel_requested` atomically, and completion uses `WHERE cancel_requested = 0`. A cancel arriving at the execution boundary cannot be overwritten by a stale whole-row write.

## Restart recovery

At startup, `RuntimeManager` requeues durable records left in `queued` or `running`. A previously running task receives a `run.recovered` event and executes again. The execution context marks startup-recovered work so the release-validation adapter explicitly recovers a persisted `running` DAG node instead of misclassifying it as a new concurrent execution. Replay children use the same rule; completed copied/executed nodes remain cached.

Dynamic runs use the same boundary. They replay an indexed durable Planner decision, rebuild
observations from completed `tool_calls`, and do not rerun completed subprocesses. A tool step
left `running` is explicitly marked interrupted only when the manager says this is restart
recovery, then claimed with a new attempt token. A real persisted tool failure is terminal rather
than being silently retried. The original execution-authority snapshot is restored with the run.

Terminal `failed` records are not recoverable and remain terminal under idempotent resubmission.

This is safe for the deterministic read-only demo. Booking and payment tools would additionally
require side-effect idempotency keys, approval and compensation semantics.

## Tests and CI

The suite covers:

- state patching and deterministic validation;
- typed review evidence, context isolation, checked-rule coverage, and finding reduction;
- reviewer timeout, bounded retry, partial-result, and feature-flag behavior;
- ten deterministic clean/error review fixtures with exact expected-finding checks;
- multi-turn checkpoint continuation;
- state sharing between synchronous and asynchronous APIs;
- cancellation before start and after an execution boundary;
- restart recovery and two-worker execution;
- idempotent run submission;
- fail-closed API-key authentication and typed principal/tenant derivation;
- Viewer/Operator permission checks, role-spoof rejection, and mutation-free `403` failures;
- tenant isolation across runs, idempotency, events, SSE, checkpoints, tool linkage, and replay sources;
- additive migration of pre-tenant SQLite records into the reserved `legacy` tenant;
- domain-specific input rejection before queueing and multi-domain state round-trips;
- release-validation execution through the shared run, event, and checkpoint APIs;
- DAG validation, deterministic topology, descendant expansion, source immutability, and input-safe selective replay;
- tool allowlisting and argument-schema rejection;
- subprocess timeout termination;
- parent-secret environment scrubbing;
- sandbox API execution and run-event linkage.
- strict Planner decision parsing and all eight Phase 5A failure codes;
- policy-order precedence with proof that denied calls neither claim a step nor start the sandbox;
- observation-driven Travel search, ranking, cost, clarification, and blocked-FINISH branches;
- durable Planner-decision replay, completed-tool cache reuse, and explicit interrupted recovery;
- persisted execution authority across restart and idempotent resubmission;
- REST/SSE equality for dynamic evidence;
- a fake OpenAI Responses client covering strict tools, all three decisions, provider failure,
  invalid JSON, and zero/multiple function-call rejection;
- unchanged Travel `0.3.0`/`0.5.0` golden traces and release-validation DAG/replay behavior.

GitHub Actions runs compile checks, Ruff, scoped mypy, and pytest on Python 3.11 and 3.12. A separate container smoke job builds the Docker image and verifies that `/usr/bin/tini` is the configured entrypoint.

## Deployment boundary

SQLite and the in-process queue keep the project self-contained, but they are not horizontally scalable. The Kubernetes manifest therefore uses one replica and persistent storage.

Docker Compose requires `RUNTIME_API_KEYS_JSON` from the caller environment. The Kubernetes
manifest expects the same JSON in Secret `travel-agent-runtime-auth`, key `api-keys.json`; the
repository does not contain or generate credential material.

Before increasing replicas, replace them with:

```text
PostgreSQL runs/checkpoints/events
+ Redis, Pub/Sub, or another distributed queue
+ worker lease and heartbeat
+ idempotent tool-call ledger
+ OpenTelemetry traces and metrics
+ container-backed sandbox workers
```

## Deliberate limitations

This is a cloud-runtime prototype, not a complete Agent Platform:

- SQLite instead of PostgreSQL;
- local queue instead of distributed workers;
- no worker lease or heartbeat;
- only two static roles; no custom roles, per-Agent/per-tool grants, or quotas;
- local static API-key configuration only; no rotation workflow or external secret manager integration;
- no tool-call idempotency for the ad-hoc `/tools/{tool}/execute` API; release-validation and the dynamic loop use `SQLiteWorkflowStore` attempt-token claims for their read-only registered steps;
- process sandbox does not isolate host networking or the full host filesystem;
- POSIX resource limits are weaker on Windows;
- no arbitrary user-code execution endpoint;
- no OpenTelemetry backend or evaluation dashboard;
- no real flight, hotel, payment, or booking API;
- no real LLM preference analyzer by default; the semantic analyzer is an injectable boundary;
- Schedule/Geography reviewers and workflow-task persistence are not part of the first slice;
- the release-validation DAG is static and serial: there is no parallel scheduler, conditional branching, client-defined graph, or dynamic/LLM-driven tool selection -- see [`docs/release-validation-workflow.md`](docs/release-validation-workflow.md).
- dynamic calls are serial and bounded: no parallel tool calls, multi-Agent delegation,
  multi-model fallback, human approval state, quota/token accounting, or tool marketplace;
- the optional model Planner has no prompt-injection detector beyond strict typed tool policy;
- no exactly-once guarantee for future side-effecting tools.

> An evidence-driven Agent Runtime prototype that connects behavioral evaluation with typed
> Planner decisions, deterministic validation, durable execution and recovery, trusted runtime
> authority, event observability, and policy-enforced registered-tool execution.
