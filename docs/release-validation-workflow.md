# Release-Validation DAG and Selective Replay

A synthetic, offline registered-tool workflow built on the same durable
`WorkflowStore` contract, `RuntimeManager`, and `ToolSandbox` used elsewhere in
the repository. The service currently supplies `SQLiteWorkflowStore` as the only
implementation. The workflow is independent of Travel and does not import `AgentState`.

All manifests, artifacts, test results, and compatibility data are synthetic
fixtures. This is not a real release pipeline and does not represent any
organization's systems or process.

The registry retains `release-validation:1.0.0` with its Phase 3A input model
and fixed-order execution path for durable-run recovery. Phase 3B is exposed as
`release-validation:1.1.0`; its input schema adds the replay directive below.

## Execution model

`ReleaseValidationWorkflow` executes a validated DAG in deterministic
topological order:

```text
load_manifest -----------+
inspect_artifacts -------+
run_unit_tests ----------+--> generate_evidence --> readiness validator
run_compatibility -------+
inspect_deployment ------+
```

The first five nodes are independent roots. `generate_evidence` depends on
all five. The scheduler is dependency-aware but intentionally serial: Phase 3B
does not claim parallel execution, conditional branching, dynamic graph
construction, or planner-selected tools.

`WorkflowDag` validates the declaration at startup and rejects:

- duplicate or empty node ids;
- missing or duplicate dependencies;
- self-dependencies;
- cycles.

Ready nodes use declaration order as a deterministic tie-breaker.

## Durable execution and step identity

One `workflow_executions` row identifies a run. For a normal run, its input
hash is the canonical manifest hash. For a replay child, the identity also
includes the typed replay directive. Reusing one `run_id` with different
identity data is still an explicit `ExecutionInputMismatchError`; selective
replay never mutates an existing run in place.

Each node persists one `tool_calls` row keyed by `(run_id, step_id)`. Its
signature includes the registered `tool_name` and a stable SHA-256 hash of the
canonical JSON arguments. Attempt tokens protect completion/failure writes
from stale workers. Every existing-row status (`COMPLETED`, `RUNNING`, or
`FAILED`) checks both signature components before cache reuse, recovery, or
retry; a changed tool definition is an explicit failure, never a cache hit.

## Selective replay

A replay is submitted through the same `POST /runs` API as every other
domain execution. It creates a new target run and identifies one terminal
source run:

```json
{
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
}
```

Replay semantics are deterministic:

1. The source must be a different `release_validation` execution in `READY`
   or `BLOCKED`. `PENDING`, `RUNNING`, and `FAILED` sources are rejected.
2. Requested nodes and all of their DAG descendants are re-executed in the
   new run. Node ids are a set semantically and are normalized into a stable
   order by the typed input model.
3. An unselected source node is reusable only when it completed and its
   `step_id`, `tool_name`, and input hash match the target node exactly.
4. A source node that is missing or incompatible is automatically invalidated
   together with all of its descendants. Old evidence is never silently used
   with changed arguments.
5. Reused evidence is copied into the target as a durable completed row with
   `attempt_count = 0` and a `step.replay_reused` event. The source rows remain
   unchanged.

The result and checkpoint include a typed replay summary containing the source
run, requested nodes, actually replayed nodes, reused nodes, and nodes that
were automatically invalidated. This makes replay behavior observable without
inferring it from timing or logs.

For example, replaying `run_unit_tests` with an unchanged manifest reruns only
that node and `generate_evidence`; the other four roots are copied from the
source. If an unselected compatibility input changed, `run_compatibility` and
its downstream evidence node are added to the replay set automatically.

## Retry and interrupted recovery

Every executed node goes through `ToolSandbox.execute`. In-process retry is
bounded by `MAX_ATTEMPTS = 2`:

| Sandbox status | Default retry | Meaning |
|---|---:|---|
| `COMPLETED` | No | Persist the result. |
| `TIMED_OUT` | Once | Treat as transient. |
| `FAILED` | No | Permanent unless a caller injected a stricter classifier for a test. |
| `DENIED` | No | Tool policy/configuration failure. |
| `INVALID_INPUT` | No | Repeating identical invalid arguments cannot help. |

Exhausted attempts finalize the workflow as `FAILED`, not `BLOCKED`, because
no evidence was produced for the validator.

A `RUNNING` step is never assumed dead. Direct callers must pass
`resume_interrupted=True`; startup recovery through `RuntimeManager` supplies
the equivalent domain-neutral recovery context. Recovery marks the abandoned
attempt interrupted, issues a new attempt token, and preserves the stale-token
write guard.

Replay remains restart-safe. Already copied nodes are reused from the target,
already executed replay nodes hit their target cache, and a persisted running
node still requires the explicit startup-recovery marker.

## Validator outcomes

The validator is a pure function over completed node results. Tool-call
success alone does not mean the release is ready:

- no findings -> `READY`;
- one or more readiness findings -> `BLOCKED`;
- execution, retry, graph, replay, or validator exception -> `FAILED`.

The validator remains a flat deterministic checklist, not a policy language.

## Deliberate boundaries

- The DAG is code-defined and static; clients cannot submit arbitrary graphs.
- Scheduling is serial and deterministic, not parallel.
- Replay creates a child run; terminal source runs are immutable.
- Run takeover requires an expired or legacy-unleased top-level Run; interrupted node recovery
  remains explicit and every workflow mutation is fenced by the current Run token.
- Tool choice is fixed per node; there is no planner or LLM-driven tool loop.
- The shared API applies static Viewer/Operator authorization; custom roles,
  per-workflow grants, quotas, PostgreSQL, and external secret management remain future work.
