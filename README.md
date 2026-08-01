# Travel Agent Runtime Demo

A runnable reference implementation of four related layers:

1. an **application-level Agent Runtime** for structured multi-turn travel planning;
2. a small **self-hosted cloud runtime** for durable run lifecycle management;
3. **policy-enforced registered-tool sandboxing** using restricted subprocess workers.
4. an optional **evidence-review workflow** with typed handoff, partial results, and
   validator-gated local replanning.

The project is offline-first. It does not require an LLM key, Redis, PostgreSQL, or Kubernetes to demonstrate the runtime mechanics.

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
- tool allowlists and schema validation prevent unapproved execution;
- subprocess timeouts, resource limits, and `tini` prevent runaway work from leaking resources.

The engineering layer is therefore not a replacement for the evaluation work. It is the next layer of the same reliability problem.

## Architecture

```text
Client
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
  |       |- runs
  |       |- run_events
  |       `- thread_states / checkpoints
  |
  v
TravelAgentRuntime
  |- intent -> StatePatch -> reducer
  |- partial replan
  |- optional evidence review workflow
  |    |- PlanEvidenceBuilder -> role-specific contexts
  |    |- BudgetChecker + PreferenceReviewer
  |    `- deterministic FindingReducer -> ReplanDirective
  |- deterministic validator
  `- blocker propagation
```

Both Agent API paths read and write the same durable `thread_states` store. Tool executions may optionally attach their start and finish events to a durable `run_id`.

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
- durable `run_id` lifecycle;
- exact Agent-version pinning;
- worker-based execution;
- SQLite-backed runs, events, and thread checkpoints;
- restart recovery for queued/running work;
- cooperative cancellation with an atomic completion guard;
- idempotent run submission through `client_request_id`;
- polling and Server-Sent Events APIs;
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

### Release-validation workflow

A durable registered-tool workflow using shared runtime infrastructure, with fixed-order multi-step execution, per-step persistence, bounded retry, explicit interrupted-step recovery, cached completed results, and deterministic readiness validation.

- a second, independent domain (`domains/release_validation/`) built on the same `SQLiteWorkflowStore` and `ToolSandbox` as the rest of `runtime_service`, with no relationship to Travel;
- six registered tools always executed in a fixed order, each persisted through `SQLiteWorkflowStore`'s exclusive step-claim and attempt-token protection;
- a deterministic validator that inspects tool *results*, not just tool-call success, so a release can be BLOCKED even when every tool call completed;
- explicit FAILED/BLOCKED separation: exhausted retries or an unexpected exception are FAILED, a business readiness check that did not pass is BLOCKED.

See [`docs/release-validation-workflow.md`](docs/release-validation-workflow.md) for the retry classification, execution/step identity hashing, and interrupted-recovery semantics.

This is fixed-order tool execution, not planner- or LLM-driven tool selection, and it bypasses `RuntimeManager`/`AgentRegistry`/the HTTP API entirely -- see [Agent integration boundary](#agent-integration-boundary) below for what that does and does not mean. All release manifests, artifacts, and compatibility data are synthetic fixtures for this repository.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q
uvicorn api.main:app --reload
```

Health endpoints:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
```

## Synchronous compatibility API

```bash
curl -X POST http://127.0.0.1:8000/agent/message \
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
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "tokyo-trip-001",
    "user_message": "Change the budget to 10000 and avoid red-eye flights.",
    "agent_id": "travel-agent",
    "agent_version": "0.5.0",
    "client_request_id": "request-20260713-001"
  }'
```

Repeating the same `client_request_id` returns the existing run instead of creating a duplicate.

`travel-agent:0.5.0` opts into the evidence-review path. The original
`travel-agent:0.3.0` version remains registered and unchanged for controlled comparisons.

```bash
curl http://127.0.0.1:8000/runs/<run_id>
curl http://127.0.0.1:8000/runs/<run_id>/events
curl -N http://127.0.0.1:8000/runs/<run_id>/events/stream
curl -X POST http://127.0.0.1:8000/runs/<run_id>/cancel
```

## Sandboxed tool API

List the only tools clients are allowed to request:

```bash
curl http://127.0.0.1:8000/tools
```

Execute a deterministic tool:

```bash
curl -X POST http://127.0.0.1:8000/tools/route_cost_summary/execute \
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
route_cost_summary
rank_trip_options
```

## Security boundary

The current backend is a **registered-tool process sandbox**, not a general untrusted-code service.

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

The sandbox is exposed as an independent control-plane API, and it is also used directly by the fixed-order release-validation workflow (see above). Neither of those is a planner- or LLM-driven tool loop: the release-validation workflow always calls its six tools in the same hardcoded order, and `TravelAgentRuntime` still does not invoke tools from an LLM or planner-driven tool loop at all.

This keeps the current threat model narrow and testable: external clients may request only registered tools through the API, and the release-validation workflow only ever selects from its own fixed tool sequence. A later Agent tool-calling integration -- dynamic, planner-selected tool calls driven by Travel or another domain -- must still add per-Agent tool permissions, prompt-injection defenses, approval policies for side effects, per-call idempotency, and trace linkage between planner decisions and sandbox executions.

## Cancellation semantics

Cancellation is cooperative: code already executing inside an Agent step is not forcibly interrupted. The database sets `cancel_requested` atomically, and completion uses `WHERE cancel_requested = 0`. A cancel arriving at the execution boundary cannot be overwritten by a stale whole-row write.

## Restart recovery

At startup, `RuntimeManager` requeues durable records left in `queued` or `running`. A previously running task receives a `run.recovered` event and executes again.

This is safe for the deterministic demo. Booking and payment tools would additionally require per-tool-call idempotency records.

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
- tool allowlisting and argument-schema rejection;
- subprocess timeout termination;
- parent-secret environment scrubbing;
- sandbox API execution and run-event linkage.

GitHub Actions runs compile checks, Ruff, scoped mypy, and pytest on Python 3.11 and 3.12. A separate container smoke job builds the Docker image and verifies that `/usr/bin/tini` is the configured entrypoint.

## Deployment boundary

SQLite and the in-process queue keep the project self-contained, but they are not horizontally scalable. The Kubernetes manifest therefore uses one replica and persistent storage.

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
- no authentication, tenant isolation, or quotas;
- no external secret manager integration;
- no tool-call idempotency for the ad-hoc `/tools/{tool}/execute` API; the release-validation workflow has one, scoped to its own `SQLiteWorkflowStore` attempt-token claims;
- process sandbox does not isolate host networking or the full host filesystem;
- POSIX resource limits are weaker on Windows;
- no arbitrary user-code execution endpoint;
- the sandbox is used by the fixed-order release-validation workflow, but not yet by the Travel runtime, and nowhere by a planner- or LLM-driven dynamic tool loop;
- no OpenTelemetry backend or evaluation dashboard;
- no real flight, hotel, payment, or booking API.
- no real LLM preference analyzer by default; the semantic analyzer is an injectable boundary;
- Schedule/Geography reviewers and workflow-task persistence are not part of the first slice;
- the release-validation workflow has a fixed step order (no dependency graph or scheduler), no selective replay or input-change invalidation, no dynamic/LLM-driven tool selection, and no `RuntimeManager`/`AgentRegistry`/HTTP integration -- see [`docs/release-validation-workflow.md`](docs/release-validation-workflow.md).

> An evidence-driven Agent Runtime prototype that connects behavioral evaluation with structured state, deterministic validation, durable execution lifecycle, checkpoint recovery, cancellation safety, idempotent submission, event observability, and policy-enforced registered-tool sandboxing.
