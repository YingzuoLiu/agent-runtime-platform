# Trusted Domain Extensions

Phase 7C adds one narrow, real extension boundary: a **trusted Python package can register an
Agent version at application startup** and then use the existing durable Run, Event, and
Checkpoint APIs without editing Runtime Core.

The bundled proof is `incident-triage:1.0.0`, a synthetic, offline, read-only recommendation
Agent. It does not contact Prometheus, Alertmanager, Kubernetes, or OpenClaw, and it never executes
a rollback. Its purpose is to prove that a non-Travel domain can supply its own typed input and
state, Planner, private tool allowlist, and final evidence validator while the platform retains the
execution lifecycle.

## Exact boundary

`create_app()` accepts explicit `runtime_extensions`:

```python
from api.main import create_app
from domains.incident_triage import IncidentTriageExtension

app = create_app(runtime_extensions=(IncidentTriageExtension(),))
```

During application lifespan startup, each extension receives:

```python
RuntimeExtensionContext(
    registry=registry,
    workflow_store=workflow_store,
    run_event_sink=store,
)
```

The extension builds its domain runtime from those shared durable services and registers an exact
`agent_id + version + input_model + state_model + factory` tuple. Registration occurs before the
state registry is bound and before workers recover queued work.

This is intentionally a **deployment-time composition seam**, not:

- runtime code upload, plugin discovery, hot loading, or a marketplace;
- execution of untrusted Python or arbitrary MCP servers;
- automatic access to governed memory, external-write providers, or human approval;
- a claim that the Runtime Console can render every custom domain.

An extension is trusted server code. The Tool Registry and loop policy gate limit selection; the
subprocess sandbox constrains execution resources. Neither makes a malicious extension safe.

## Runnable reference

The implementation is split by responsibility:

| Path | Extension responsibility |
| --- | --- |
| [`domains/incident_triage/models.py`](../domains/incident_triage/models.py) | Strict input, tool argument, evidence, result, and state models |
| [`domains/incident_triage/planner.py`](../domains/incident_triage/planner.py) | Observation-driven typed decisions only |
| [`domains/incident_triage/tools.py`](../domains/incident_triage/tools.py) | One server-owned, read-only tool allowlist |
| [`domains/incident_triage/handlers.py`](../domains/incident_triage/handlers.py) | Deterministic synthetic evidence handler |
| [`domains/incident_triage/runtime.py`](../domains/incident_triage/runtime.py) | Loop adapter, state reduction, and final evidence validation |
| [`domains/incident_triage/extension.py`](../domains/incident_triage/extension.py) | Version-pinned runtime composition and registration |
| [`examples/incident_triage_app.py`](../examples/incident_triage_app.py) | Opt-in FastAPI composition root |

The default `api.main:app` does **not** register this Agent. That keeps the normal Travel demo and
default registry stable. Run the explicit example app when validating Phase 7C.

### Start it locally

Create and activate the repository environment first if needed:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

Then start the opt-in app:

```bash
export RUNTIME_API_KEY="phase7c-local-key"
export RUNTIME_API_KEYS_JSON='[{"credential_id":"phase7c-local","api_key":"phase7c-local-key","tenant_id":"local","subject_id":"developer","role":"operator"}]'
export RUNTIME_DB_PATH="runtime_data/phase7c.db"
export RUNTIME_DEMO_MODE="false"
export RUNTIME_PLANNER_PROVIDER="scripted"
python -m uvicorn examples.incident_triage_app:app --host 127.0.0.1 --port 8000
```

PowerShell:

```powershell
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
$env:RUNTIME_API_KEY = "phase7c-local-key"
$env:RUNTIME_API_KEYS_JSON = '[{"credential_id":"phase7c-local","api_key":"phase7c-local-key","tenant_id":"local","subject_id":"developer","role":"operator"}]'
$env:RUNTIME_DB_PATH = "runtime_data/phase7c.db"
$env:RUNTIME_DEMO_MODE = "false"
$env:RUNTIME_PLANNER_PROVIDER = "scripted"
& .\.venv\Scripts\python.exe -m uvicorn examples.incident_triage_app:app --host 127.0.0.1 --port 8000
```

The API walkthrough below uses PowerShell. Bash clients call the same authenticated endpoints and
can use the equivalent `curl` pattern shown in the root README.

Verify registration:

```powershell
$headers = @{ Authorization = "Bearer $env:RUNTIME_API_KEY" }
Invoke-RestMethod http://127.0.0.1:8000/agents -Headers $headers |
    Where-Object agent_id -eq "incident-triage"
```

### Submit through the existing API

Custom domains should use structured `input`. The legacy `user_message` convenience field and
`POST /agent/message` are Travel compatibility paths.

```powershell
$body = @{
    thread_id = "incident-demo-001"
    agent_id = "incident-triage"
    agent_version = "1.0.0"
    client_request_id = "incident-demo-request-001"
    input = @{
        alert_id = "alert-checkout-001"
        service = "checkout-api"
        severity = "critical"
        error_rate_percent = 8.0
        recent_deployment = $true
    }
} | ConvertTo-Json -Depth 5

$submitted = Invoke-RestMethod `
    -Method Post `
    -Uri http://127.0.0.1:8000/runs `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body

do {
    Start-Sleep -Milliseconds 200
    $result = Invoke-RestMethod `
        -Uri "http://127.0.0.1:8000/runs/$($submitted.run_id)" `
        -Headers $headers
} while ($result.status -notin @("completed", "failed", "cancelled"))

$result | ConvertTo-Json -Depth 12
```

Expected accepted result fields include:

```json
{
  "status": "completed",
  "state": {
    "current_stage": "triaged",
    "result": {
      "alert_id": "alert-checkout-001",
      "service": "checkout-api",
      "risk_level": "high",
      "recommended_action": "prepare_rollback_review",
      "evidence_source": "synthetic_incident_fixture",
      "action_executed": false
    }
  }
}
```

`prepare_rollback_review` is a recommendation label only. There is no registered rollback tool or
external side effect.

Read the persisted evidence and custom checkpoint:

```powershell
$events = Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/runs/$($submitted.run_id)/events" `
    -Headers $headers
$events | Select-Object sequence, event_type

Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/threads/incident-demo-001/state?domain_id=incident-triage&schema_version=1" `
    -Headers $headers | ConvertTo-Json -Depth 12
```

The evidence sequence contains the same Runtime-owned boundaries as Travel:

```text
run.queued
run.started
checkpoint.loaded
planner.decision       CALL_TOOL inspect_incident_signal
policy.decision        allowed
tool.result
planner.decision       FINISH
loop.outcome           finished
checkpoint.saved
run.completed
```

Rows come from SQLite-backed Runtime evidence. The reference contract test asserts that the list
returned by `GET /runs/{id}/events` is exactly equal to the persisted event list.

## How the domain is formed

### 1. Define strict input and state

The API validates `IncidentTriageInput` before queueing the run. The state inherits
`BaseRuntimeState` and declares class-level routing metadata:

```python
class IncidentTriageState(BaseRuntimeState):
    domain_id: ClassVar[str] = "incident-triage"
    schema_version: ClassVar[str] = "1"
```

Runtime Core now revalidates both `initial_state()` and the state returned by `execute()` against
the registration's state model. A wrong model or mismatched `thread_id` fails before checkpoint
persistence.

### 2. Let the Planner decide, not execute

The Planner can return only the existing typed decision contracts. It requests clarification when
`service` is absent, calls the registered evidence tool when there is no observation, and proposes
`FINISH` only after it receives persisted evidence.

The Planner never imports or calls the handler. An injected attempt to call `execute_rollback` is
rejected as `unknown_tool` before a tool-call row or handler execution exists.

### 3. Keep tools private and allowlisted

The Agent-local `ToolRegistry` contains exactly one `READ_ONLY` tool. Its service argument is a
typed allowlist (`catalog-api` or `checkout-api`), so an unsupported service is denied with
`invalid_tool_arguments` at `policy.decision`, before the subprocess handler starts.

The submitted severity, error rate, and deployment recency are alert claims. The tool does not echo
them: it reads an independent server-owned fixture keyed by the allowlisted service. This keeps the
sample deterministic and offline while still giving the finish validator evidence that can agree
or disagree with the request.

Agent-private tools are deliberately not aggregated into `GET /tools`. That endpoint remains the
public Travel direct-sandbox catalog. Use `GET /agents` to verify custom Agent registration and
inspect private tool behavior through that Agent's run evidence.

### 4. Validate FINISH from evidence

The finish evaluator parses the persisted tool observation, recomputes risk and recommendation,
and compares them with the request and Planner proposal. A structurally valid but fabricated
recommendation produces a completed Run with:

```text
state.current_stage = blocked
state.result = null
validation_errors = ["Planner recommendation does not match deterministic policy"]
```

This keeps runtime failure (`FAILED`) distinct from a domain conclusion rejected by validation
(`BLOCKED`).

### 5. Register an immutable version

`IncidentTriageExtension.register()` builds its private loop from the supplied shared stores and
registers `incident-triage:1.0.0`. The factory returns a Runtime adapter; `RuntimeManager` remains
domain-neutral.

The same app can run multiple workers. Planner and Runtime objects must therefore be stateless or
thread-safe; process-local mutable fields are neither durable nor authoritative. Persist execution
state only through the supplied Runtime stores and context.

## Version and recovery rules

Durable registration has three different identities:

| Identity | Meaning | Compatibility rule |
| --- | --- | --- |
| Agent version | Planner, tools, validation, and runtime behavior | Do not mutate behavior behind an existing version; retain old registrations while pinned runs may recover |
| State schema version | Serialized checkpoint shape for one `domain_id` | A new incompatible state model needs a new schema version; no automatic migration exists |
| Workflow type | Durable dynamic-loop decision/tool history | Keep it stable for a pinned Agent version so recovery can replay the same contract |

Within one tenant, a `thread_id` is bound to one `domain_id + schema_version`. Reusing that thread
for another domain or schema fails rather than overwriting the checkpoint. Use a new thread or an
explicit, separately designed migration.

Startup must load every extension required by queued or running persisted work. The manager
preflights every recoverable `agent_id + version + state schema` before creating workers; a missing
or incompatible registration fails application startup without claiming or terminalizing the Run.
Restore the exact registration before retrying startup. Version pinning does not freeze Python code
by itself.

`REQUEST_CLARIFICATION` completes the current Run and saves the checkpoint. A caller continues by
submitting another Run on the same Agent version and thread; it is not a durable paused worker.

Runs on the same tenant-qualified thread must also be submitted serially. Multiple workers can
otherwise read the same checkpoint concurrently and the last completion can overwrite the other
update; the current checkpoint store has no per-thread lease or revision compare-and-swap.

## Security and product boundary

- The sample catalog and handler are deterministic fixtures, not live telemetry.
- The tool subprocess always has timeout and bounded-output enforcement. POSIX also applies CPU,
  memory, and file-descriptor limits; Windows does not. The current backend is not a network or
  mount-namespace isolation boundary.
- Read-only tool output has no universal output model gate. This domain's finish evaluator parses
  and validates the observation before accepting a recommendation.
- A schema-valid client-provided `state` is not authenticated evidence. Extension authorization
  must use `RuntimeExecutionContext.authority`, and conclusions must use persisted workflow/tool
  evidence. The sample overwrites request fields and clears caller-provided trace entries before
  execution; public Run Events remain the evidence authority.
- Exactly-once, compensation, human approval, governed memory, and external-action reconciliation
  are not automatically added by `RuntimeExtension`.
- The default Docker Compose command and Runtime Console load `api.main:app`, which remains the
  Travel portfolio demo. They do not load `examples.incident_triage_app:app`.

## Relationship to a future DataOps Guardian

The Phase 7C seam is the Runtime socket a future Guardian adapter could use:

| Future Guardian concern | What Phase 7C proves now |
| --- | --- |
| Incident investigation skill | A non-Travel Planner can consume persisted, typed observations |
| Prometheus / Kubernetes / Alertmanager tools | Agent-private server-owned tool registries can be composed, but no live integration is included |
| Allowlist and evidence gate | Unsupported tool arguments and fabricated FINISH conclusions fail closed |
| Safe rollback and approval | Not implemented; these require the durable external-action and human-approval designs |
| OpenClaw packaging | Not implemented; a later adapter can call the same authenticated Run/Event API |

So 7C does not claim “install Guardian into OpenClaw.” It proves the platform no longer requires a
Core edit to register and execute the domain that such an adapter would target.

## Verification

The contract coverage is in:

- [`tests/test_bring_your_own_domain.py`](../tests/test_bring_your_own_domain.py): opt-in
  registration, same API, persisted event equality, clarification, policy denial, independent
  fixture mismatch, evidence-gated finish, zero external actions, unknown-tool denial, strict
  input, duplicate registration, and missing-extension recovery preflight;
- [`tests/test_runtime_state_contract.py`](../tests/test_runtime_state_contract.py): registered
  state-model validation, thread identity, cancellation-race normalization, checkpoint fail-closed
  behavior, and SQLite round-trip.

Run:

```bash
pytest -q tests/test_bring_your_own_domain.py tests/test_runtime_state_contract.py
```
