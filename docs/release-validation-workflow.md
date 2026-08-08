# Release-Validation Workflow

A synthetic, offline, fixed-order registered-tool workflow built on the
same durable persistence (`SQLiteWorkflowStore`) and registered-tool
sandbox (`ToolSandbox`) as the rest of `runtime_service`. It exists to
prove those two pieces are genuinely domain-agnostic: this domain has no
relationship to Travel and does not import `AgentState`. Phase 3A adds a
typed adapter that registers the workflow with `AgentRegistry` and runs it
through `RuntimeManager` and the shared `/runs` HTTP lifecycle.

All manifests, artifacts, test results, and compatibility data are
synthetic fixtures for this repository. This is not a real release
pipeline and does not represent any organization's actual process.

## Architecture

```text
ReleaseManifest
  |
  v
ReleaseValidationWorkflow.run(run_id, manifest, resume_interrupted=False)
  |
  +-- SQLiteWorkflowStore.create_or_get_execution / mark_running
  |
  +-- fixed step sequence, each step:
  |     claim_step -> ToolSandbox.execute(tool_name, arguments) -> complete_step/fail_step
  |
  |     1. load_release_manifest
  |     2. inspect_build_artifacts
  |     3. run_unit_test_check
  |     4. run_compatibility_check
  |     5. inspect_deployment_configuration
  |     6. generate_release_evidence
  |
  `-- validate_release_readiness(step_results)  -- pure function, not a registered tool
        |
        +-- no findings   -> finalize_ready   -> READY
        `-- any finding   -> finalize_blocked -> BLOCKED
```

The six steps always run in this exact order. There is no dependency
graph, no scheduler, and no conditional branching between them --
`STEP_SEQUENCE` in `domains/release_validation/runtime.py` is a plain,
hardcoded list.

## Execution and step identity

`run_id` plus a stable hash of the manifest is the execution's identity:

- same `run_id` + same manifest hash -> continues or returns the existing
  execution;
- same `run_id` + a different manifest hash -> `ExecutionInputMismatchError`,
  never a silent reuse of the old result;
- each step's identity is `(run_id, step_id)` plus a stable hash of that
  step's arguments (a pure function of the manifest).

The hash is computed by canonically serializing the manifest / step
arguments (`sort_keys=True`) and taking a SHA-256 digest. Nothing
non-deterministic (timestamps, random ids, attempt tokens) is ever part of
what gets hashed, because none of it is part of the manifest or of a
step's arguments in the first place.

There is no selective invalidation: a mismatch is a hard stop for the
caller to resolve, not a signal for this workflow to figure out what to
re-run. That is explicitly deferred.

## Retry classification

Every step call goes through `ToolSandbox.execute`, which reports one of
`COMPLETED / DENIED / INVALID_INPUT / TIMED_OUT / FAILED`. Only two of
those are ever retried in-process (bounded at `MAX_ATTEMPTS = 2`, no
sleep, no backoff):

| Status | Retried? | Why |
|---|---|---|
| `DENIED` | No | Tool not in the allowlist -- a caller/config bug, not a fluke. |
| `INVALID_INPUT` | No | Schema mismatch -- retrying sends the same bad input again. |
| `TIMED_OUT` | Yes | Could be a fluke; worth one more try. |
| `FAILED` | Only if the error text matches an explicit marker | See below. |

A `FAILED` result covers both "the tool has a real bug" and "a test
deliberately injected a flaky-dependency simulation" -- the sandbox
reports both identically. Retrying a real bug just re-triggers it, so
`FAILED` is retried **only** when `execution.error` contains one of
`RETRYABLE_FAILURE_MARKERS` (`"transient_test_failure"`,
`"transient_worker_failure"`) in `domains/release_validation/runtime.py`.
No production tool in this repository ever raises with those markers --
only a test-only wrapper around `ToolSandbox.execute` (see
`ScriptedFaultSandbox` in `tests/test_release_validation_workflow.py`)
produces them. This deliberately keeps "first attempt fails" logic out of
every production tool.

Exhausting `MAX_ATTEMPTS` on a step raises `StepAttemptsExhaustedError`,
which finalizes the whole execution as **FAILED**, not BLOCKED: the tool
never produced a result at all, so there is nothing for the validator to
have judged.

## Interrupted recovery

`ALREADY_RUNNING` from `claim_step` is the normal outcome for a
genuinely still-in-progress step. By default, `run()` raises
`StepAlreadyRunningError` and finalizes nothing -- the execution stays
`RUNNING` for a later call to resolve. Recovery only happens when a direct
caller passes `resume_interrupted=True`, or when `RuntimeManager` marks a
durable run as recovered during startup and the managed adapter supplies
the same explicit recovery intent. That recovery calls
`SQLiteWorkflowStore.recover_interrupted_step` (RUNNING -> FAILED with
`error_code="interrupted"`) and then re-claims, producing a brand-new
`attempt_token`. There is no lease, no heartbeat, and no automatic
detection of a dead process inside `ReleaseValidationWorkflow`; either the
direct caller or the outer durable manager must decide.

## Validator: why a tool succeeding is not the same as ready

`validate_release_readiness` is a plain Python function, not a registered
tool, and it inspects the *content* of each completed step's result, not
just whether the tool call succeeded. Two concrete false-success shapes
this catches:

- every tool call reports success, but `run_compatibility_check`'s own
  result says Python 3.12 was never tested even though the manifest
  requires it -> `python_versions_covered` finding -> BLOCKED;
- `generate_release_evidence` completes, but the evidence it produced
  does not reference all four prior checks -> `evidence_complete`
  finding -> BLOCKED.

It is a flat list of five independent checks (artifacts, unit tests,
Python compatibility, deployment configuration, evidence completeness).
There is no rule priority, no rule composition, and no configurable
policy language -- extending this into a general rules engine is
explicitly out of scope for this phase.

An exception raised by the validator itself (or anywhere else in the
workflow loop) is a third, distinct outcome: the execution finalizes as
**FAILED**, never BLOCKED. BLOCKED means "the validator looked at the
evidence and rejected it"; FAILED means "something broke before a
verdict could be reached."

## What this does not claim

- The step sequence is fixed and hardcoded; there is no dependency graph
  or scheduler.
- Tool selection is fixed at each step; nothing here is planner- or
  LLM-driven.
- The core `ReleaseValidationWorkflow` still owns only fixed step execution;
  `ManagedReleaseValidationRuntime` is the explicit adapter to the generic
  manager. This separation does not make the workflow planner-driven.
- The workflow is submitted through the shared `/runs` endpoint; there is no
  release-specific execution endpoint.
- Interrupted-step recovery is explicit (`resume_interrupted=True` for a
  direct caller, or a startup-recovery context from `RuntimeManager`), not
  automatic crash detection inside the workflow.
- There is no selective replay: an input mismatch is rejected outright,
  never partially reused.
