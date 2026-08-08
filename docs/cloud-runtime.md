# Cloud Runtime Upgrade

Version `0.3.0` adds a self-hosted execution-management layer around the travel application runtime. Version `0.4.0` adds a policy-enforced subprocess backend for registered tools. Version `0.6.0` generalizes the manager, registry, persistence, and `/runs` API across typed domains.

## Architecture

```text
Client
  |
  v
FastAPI API
  |- /agent/message
  |- /runs
  `- /tools/{tool}/execute
         |
         v
RuntimeManager ---- AgentRegistry
  |
  +---- local worker queue
  |
  +---- SQLiteRunStore
  |       |- runs
  |       |- run_events
  |       `- thread_states / checkpoints
  |
  +---- TravelAgentRuntime
  |
  +---- ManagedReleaseValidationRuntime
  |
  `---- ToolSandbox ---- ToolRegistry
             |
             `- restricted subprocess worker
```

Both Travel API paths use the same durable `thread_states` table. The generic
run path persists `domain_id` and `schema_version` separately from state JSON,
and the registry validates concrete input/state models before execution.
Thread identifiers are globally scoped in this SQLite slice: reusing one for a
different domain or schema fails explicitly instead of overwriting a checkpoint.

## Run lifecycle

```text
queued -> running -> completed
                  -> failed
queued/running    -> cancelled
```

Every run records its stable `run_id`, thread, pinned Agent version,
domain/schema identity, structured input/output, timestamps, validation
results, cancellation metadata, optional `client_request_id`, and latest
serialized domain state.

Important transitions are append-only events:

```text
run.queued
run.started
checkpoint.loaded
checkpoint.saved
run.completed | run.failed | run.cancelled
sandbox.execution_started
sandbox.execution_finished
```

## Submission idempotency

Clients may send a `client_request_id` when creating a run. The database applies a unique constraint to this field. Repeating the same submission returns the existing run instead of creating another queued task.

This protects the control API from duplicate runs caused by HTTP retries. It is separate from tool-call idempotency: booking or payment tools still need their own per-call ledger.

## Cancellation race handling

Cancellation remains cooperative because an in-process Agent step cannot be forcibly interrupted safely. However, cancellation state is not protected only by a read-then-write sequence:

1. `request_cancel` sets `cancel_requested = 1` directly in the database;
2. final completion uses a conditional update with `WHERE cancel_requested = 0`;
3. if the condition fails, the run is finalized as cancelled and no thread checkpoint is committed.

This closes the boundary race where a stale `RunRecord` could overwrite a cancel request.

## Known consistency gaps

`finalize_completed_run`, `finalize_cancelled_run`, and `request_cancel_atomically` close the
consistency gap between a run's terminal/cancel-requested status and the event that describes
it: the status write and its describing event commit in a single transaction, so an external
reader can never observe the new status without the event already being visible.

A repository-wide audit for the same shape of gap found four more places that still use two
separate commits -- an `update_run` (or `create_run`) followed by a separate `append_event` --
instead of one transaction:

- `RuntimeManager.submit()`: `SQLiteRunStore.create_run` and the `run.queued` event;
- `RuntimeManager._execute_run()`'s QUEUED -> RUNNING transition: `update_run` and the
  `run.started` event;
- `RuntimeManager._execute_run()`'s RUNNING -> FAILED path: `update_run` and the `run.failed`
  event;
- `RuntimeManager.start()`'s restart recovery, RUNNING -> QUEUED: `update_run` and the
  `run.recovered` event.

**Target invariant for each of these, once fixed:** the moment an external reader observes the
new status via `GET /runs/{run_id}`, the event describing that transition must already be
present in `GET /runs/{run_id}/events` -- the status write and its event must commit together,
the same way `finalize_completed_run`, `finalize_cancelled_run`, and
`request_cancel_atomically` already do.

Today these four are primarily a **visibility window**, not a true compare-and-set race: for
each of these specific transitions, only one worker thread ever touches a given `run_id` at a
time, so there is no second writer genuinely competing for the same transition the way
completion and cancellation can compete for the same RUNNING row. A reader can only observe a
status briefly ahead of its event, not two conflicting terminal outcomes.

That does not mean a future fix should skip the compare-and-set discipline established for
completion and cancellation. Each of these four should still gate its UPDATE on an explicit
source-status condition -- e.g. `WHERE run_id = ? AND status = 'queued'` for QUEUED ->
RUNNING, `WHERE run_id = ? AND status = 'running'` for RUNNING -> FAILED and the
restart-recovery RUNNING -> QUEUED transition -- rather than an unconditional full-row
`UPDATE ... WHERE run_id = ?`. Even without a competing writer today, an explicit source-status
condition keeps the invariant machine-checkable and guards against a future caller (a second
worker pool, a retried recovery pass) re-running the transition and duplicating its event.

## Tool sandbox

The process backend executes only tools registered by the server. Clients cannot submit Python source, shell commands, executable paths, or module names.

The boundary applies:

- a server-side tool allowlist;
- Pydantic input validation with unknown fields rejected;
- a fixed Python executable and fixed worker script;
- a fresh temporary working directory per execution;
- a minimal environment that does not forward API keys or database credentials;
- wall-clock timeout with process-group termination on POSIX;
- stdout/stderr size caps;
- POSIX CPU, address-space, open-file, and core-dump limits;
- structured execution results and optional linkage to a durable `run_id` event history.

Two deterministic example tools are registered:

```text
route_cost_summary
rank_trip_options
```

Example:

```bash
curl http://127.0.0.1:8000/tools

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

This is deliberately a **registered-tool process sandbox**, not an arbitrary-code sandbox. The current backend reports `network_mode: host`: it does not claim to block outbound network access. It also does not provide a private mount namespace or prevent a malicious registered tool from reading files available to the runtime user. Registered tools are therefore still trusted application code.

The sandbox API is currently invoked directly. It is not yet wired into the Agent decision loop or an autonomous tool-calling path, so prompt-injection-driven tool selection is outside the current threat model and must be reviewed when that integration is added.

A production backend for untrusted code or third-party MCP servers should replace the subprocess implementation with an ephemeral container, Kubernetes Job, gVisor sandbox, Firecracker microVM, or equivalent isolation boundary using:

```text
read-only root filesystem
+ explicit writable workspace
+ no host mounts
+ dropped Linux capabilities
+ seccomp/AppArmor profile
+ network disabled or allowlisted
+ non-root UID
+ CPU/memory/PID limits
+ execution deadline
+ image and dependency allowlist
```

## Descendant process cleanup

A registered tool may eventually launch its own subprocesses. When a timed-out sandbox process group is killed, those descendants can be reparented to container PID 1. The production image therefore starts the service through `tini`, which forwards signals and reaps orphaned descendants instead of leaving zombie processes behind.

This guarantee applies to the supplied Docker image. Running `uvicorn` directly on a host still relies on that host's init or service manager to reap orphaned descendants.

## Restart recovery

On startup, the manager scans records left in `queued` or `running`. A previously running run is moved back to `queued`, receives `run.recovered`, and is executed again.

The test suite verifies recovery, cancellation before start, cancellation at an execution boundary, two-worker execution, shared thread state, submission idempotency, tool allowlisting, schema rejection, timeout termination, environment scrubbing, and API event linkage.

## Deliberate limitations

SQLite and an in-process queue keep the repository runnable without external services. Therefore:

- deploy one runtime replica only;
- there is no distributed worker lease or heartbeat;
- cancellation occurs at cooperative execution boundaries;
- there is no tenant authentication, quota, or secret-manager integration;
- external side-effecting tools do not yet have idempotency records;
- the subprocess sandbox does not isolate host networking or the complete host filesystem;
- POSIX rlimits are not available on Windows, where timeout and process separation remain but resource enforcement is weaker;
- there is no arbitrary user-code execution endpoint;
- the sandbox API is not yet connected to the Agent decision loop or autonomous tool calling;
- descendant reaping depends on `tini` in the provided container image or an equivalent host init/service manager.

A production-oriented next step is PostgreSQL for runs/checkpoints/events, Redis or Pub/Sub for distributed dispatch, worker leases and heartbeats, a tool-call idempotency ledger, OpenTelemetry traces, and a container-backed sandbox implementation.
