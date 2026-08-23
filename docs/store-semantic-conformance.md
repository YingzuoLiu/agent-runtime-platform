# Store Semantic Conformance

This suite turns the durable execution plane's storage guarantees into adapter-driven executable
scenarios. It records the SQLite reference semantics through observable outcomes, not through
matching method names, schemas, SQL, indexes, or query plans.

The current passing implementation is the SQLite single-host reference execution plane. This
document does **not** claim that `WorkflowStore` covers the entire execution plane:

- `WorkflowStore` covers workflow executions, tool calls, external actions, and the workflow event
  ledger.
- Run claim, lease, Thread scheduling, checkpoint revision CAS, Run events, and quarantine remain
  public behaviors of `SQLiteRunStore`.
- `RuntimeManager` still depends directly on `SQLiteRunStore`.

Only the SQLite adapter exists today, so this PR is not evidence of cross-backend compatibility. A
future backend is not interchangeable merely because its method signatures resemble SQLite. It must
execute the applicable scenarios through its own test-side adapter and produce the same typed
outcomes, durable state, event order, revisions, attempts, and sanitized error codes.

## Contract layers

### 1. Workflow Store Contract

`tests/conformance/test_workflow_store_contract.py` drives the existing `WorkflowStore` surface.
It covers execution and step identity, attempt-token fencing, dispatch-token fencing,
prepare-before-dispatch, cancellation arbitration, transition/event atomicity, append-only event
ordering, and restart-visible committed writes.

### 2. Run Store Behavioral Contract

`tests/conformance/test_run_store_contract.py` drives public `SQLiteRunStore` behavior through a
test-only backend bundle. It covers Run claim and exact lease expiry, stale lease fencing,
tenant-qualified Thread scheduling, revision-qualified checkpoint loads, completion CAS, atomic
full-Run/projected-checkpoint/event completion, failure/cancellation checkpoint preservation, and
restart-visible writes.

The bundle is deliberately not a production `RunStore` protocol. It exists only to create and
reopen stores, inject a store clock, synchronize races, and inject backend-specific failures.

### 3. Composite Execution-Plane Contract

`tests/conformance/test_execution_plane_contract.py` retains semantics that genuinely cross the Run
store, Workflow store, `RuntimeManager`, and external-action recovery coordinator. It proves that an
ambiguous unsafe effect is reconciled before same-Thread successor work and that checkpoint drift
during external-action reconciliation enters an inspectable, nonterminal quarantine. I9 adds the
narrow operator path across the same Run/checkpoint/workflow/action tables: only an unchanged plan
with terminal external evidence may atomically fail the Run and release its Thread.

## Invariant matrix

Every test below executes through the reference-adapter fixture. Its only current parameter is
`[sqlite]`; the parameter provides stable test IDs but does not by itself prove portability.

| Invariant | Setup | Operation / race / failure injection | Externally observable result | Executable test ID |
| --- | --- | --- | --- | --- |
| **I1 One live owner per Run attempt** | One queued Run; two store instances share durable storage and an injected clock. | Two claims cross an explicit `threading.Barrier`; a second claim is attempted before expiry, then the clock advances to the exact lease boundary. | Exactly one initial claim exists; no unexpired double-owner appears; takeover increments `attempt`, rotates the lease token, and records `lease_expired`. | `tests/conformance/test_run_store_contract.py::test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced[sqlite]`; `...::test_lease_expiry_uses_the_injected_store_clock_exactly[sqlite]` |
| **I2 Stale attempt can never mutate durable state** | A replacement attempt owns the Run after exact clock-driven expiry; a Workflow step also has a newer attempt token after interrupted recovery. | The stale Run token tries an attempt event and completion; the stale tool token tries completion. | Run writes raise/return typed lease loss; tool completion raises `StaleAttemptError`; the winning output/checkpoint persists and no stale event or result appears. | `tests/conformance/test_run_store_contract.py::test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced[sqlite]`; `tests/conformance/test_workflow_store_contract.py::test_i2_stale_tool_attempt_cannot_mutate_durable_state[sqlite]` |
| **I3 One running Run per tenant-qualified Thread** | Two queued Runs share tenant and Thread; two store instances race. | Both claim calls cross an explicit barrier. | Exactly one Run becomes running; the successor remains queued with `attempt == 0`. | `tests/conformance/test_run_store_contract.py::test_i3_one_running_run_per_tenant_qualified_thread[sqlite]` |
| **I4 Thread serialization is tenant-qualified** | Two queued Runs either have different Thread IDs in one tenant or the same Thread ID in different tenants. | Two stores claim concurrently through the same barrier scenario as I3. | Both Run IDs are claimed; one tenant-qualified Thread does not serialize unrelated work. | `tests/conformance/test_run_store_contract.py::test_i4_thread_scope_allows_independent_claims[sqlite-*]` |
| **I5 Checkpoint commit requires expected revision** | A completed predecessor creates revision 1; its successor captures base revision 1. | The backend adapter injects a mixed-writer checkpoint change to revision 2; revision-qualified load and stale completion are attempted. | Load raises `ThreadCheckpointRevisionConflictError(1, 2)`; completion returns `CHECKPOINT_CONFLICT`; the Run fails with `thread_checkpoint_conflict`; revision 2 and its state are not overwritten. | `tests/conformance/test_run_store_contract.py::test_i5_checkpoint_load_and_completion_require_expected_revision[sqlite]` |
| **I6 Terminal Run + checkpoint + required events preserve atomic semantics** | A currently leased Run returns a typed state with trace evidence. A second scenario starts from a legal legacy checkpoint with a non-empty trace before failure and cancellation. | The backend separately fails the checkpoint write, `checkpoint.saved`, and `run.completed` after the completion transaction has started; failure and cancellation then execute normally. | Every injected failure leaves the Run running with its lease, checkpoint absent, and no partial terminal events. A later commit atomically stores the full Run trace, a same-type checkpoint with `execution_trace=[]`, truthful projection counts, revision, `checkpoint.saved`, and `run.completed`. Failed/cancelled Runs preserve the legacy payload and revision. | `tests/conformance/test_run_store_contract.py::test_i6_completion_checkpoint_and_required_events_commit_atomically[sqlite-*]`; `...::test_failure_and_cancellation_preserve_checkpoint_revision[sqlite]` |
| **I7 Recovery never guesses an ambiguous external effect** | An unsafe provider is entered once; terminal evidence write fails, leaving a durable `dispatching` action and reconciliation-pending Run; a same-Thread successor is queued. | Store and coordinator instances are recreated after exact lease expiry; recovery executes with a provider that fails the test if called. | Recovery claims the predecessor first, never calls the provider again, finalizes `outcome_unknown` with dispatch count 1, and only then permits the successor claim. The outcome and sanitized error code survive another reopen. | `tests/conformance/test_execution_plane_contract.py::test_i7_reconciliation_precedes_successor_and_never_retries_unsafe_effect[sqlite]` |
| **I8 Detected semantic conflict fails closed into inspectable quarantine** | A reconciliation-pending predecessor owns a dispatched action at checkpoint base 1; a same-Thread successor waits; the adapter injects checkpoint revision 2. | A recreated `RuntimeManager` recovers the expired predecessor. An explicit event observes quarantine; the test then recreates the store, requests cancellation, and attempts takeover. | Runtime construction never occurs. The predecessor remains nonterminal `running`, has no lease, carries `thread_checkpoint_conflict_reconciliation_pending`, and appends `checkpoint.conflict` with expected/observed revisions and quarantine disposition. Restart/takeover/cancellation do not terminalize it; the successor stays queued while another Thread remains claimable. | `tests/conformance/test_execution_plane_contract.py::test_i8_checkpoint_conflict_is_inspectable_nonterminal_quarantine[sqlite]` |
| **I9 Quarantine release requires an unchanged eligible plan and preserves authoritative evidence** | A predecessor has checkpoint base 1, a durable terminal external action, quarantine at current revision 2, and two queued same-Thread successors. | Dry-run derives an opaque plan without writes; apply rederives it in one transaction and fails the Run. The first released successor then commits revision 3 before the original commit is verified and exactly replayed after reopen. | Dry-run leaves Run/checkpoint/workflow/action/events unchanged. Apply first preserves revision 2 and identical workflow/action evidence, performs zero provider calls, and writes one resolution plus one `run.failed`. Legal successor CAS progress does not turn verification into `500` or poison delayed replay; the exact plan is reused without duplicate events while the later successor remains ordered. | `tests/conformance/test_execution_plane_contract.py::test_i9_unchanged_eligible_plan_releases_quarantine_preserving_evidence[sqlite]` |

## Cross-PR durable checkpoint integrity

The combined system property is:

> For one tenant-qualified Thread, only the current leased Run attempt may atomically advance the
> checkpoint from its captured revision. A successful completion stores the full validated Run
> result and a same-type resumable checkpoint projection together with truthful events. A stale,
> conflicting, failed, cancelled, partially committed, or quarantined attempt cannot overwrite or
> ambiguously reinterpret the authoritative checkpoint; operator repair may release quarantine
> only while its evidence-bound plan remains unchanged.

| Property slice | Production enforcement point | Executable evidence |
| --- | --- | --- |
| Current writer only | Run lease token/deadline predicates in `SQLiteRunStore`; workflow/action attempt and dispatch fencing | I1, I2, I7 |
| One ordered Thread writer | tenant-qualified running-Run unique index and claim arbitration | I3, I4 |
| Correct predecessor | captured `checkpoint_base_revision`, revision-qualified load, completion CAS | I5; `tests/test_thread_serialization.py` |
| Full result versus resumable projection | `project_thread_checkpoint_state()` called inside `commit_completed_run()` after result validation; distinct Run/checkpoint JSON | I6; `tests/test_checkpoint_projection.py`; `tests/test_contracts.py` |
| One atomic observable completion | one `BEGIN IMMEDIATE` transaction covers Run update, checkpoint CAS, `checkpoint.saved`, `run.completed`, and lease clearing | I6 failures at checkpoint write and both event boundaries |
| Truthful projection evidence | `checkpoint.saved.trace_events=0`, `run_trace_events=<full Run count>`, and `projection=execution_trace_reset` | I6; state-boundary characterization v2; API test |
| Conflict and recovery interpretation | revision drift terminalizes ordinary completion, but reconciliation precedence enters inspectable nonterminal quarantine | I5, I7, I8 |
| Controlled repair | plan rederivation, hash equality, terminal external evidence, and atomic resolution/Run failure | I9; quarantine-resolution unit and API tests |

The scan found no production consumer that uses an earlier checkpoint trace for a decision. The
dynamic workflow hash already excludes it; API and Demo consumers treat the completed Run state as
Run evidence and the Thread endpoint as current resumable state. The remaining explicit assumptions
are that SQLite is the single-host reference backend, old/new binaries do not write concurrently,
and Run/workflow event ledgers are not claimed to reproduce every `TraceEvent` payload byte for
byte. Projected checkpoints remain ordinary schema-v1 payloads, so inspection, restart takeover,
conformance loading, and quarantine plan hashing require no alternate decoder.

Upgrade behavior is lazy and failure-safe: an old non-empty trace is supplied unchanged to the
first post-upgrade execution, the first successful commit advances the revision and projects it
out, and failure/cancellation leaves it intact. CAS conflict preserves the observed checkpoint;
transaction failures roll back both representations and their events. Quarantine repair preserves
the checkpoint rather than re-projecting it, and the first later successful successor applies the
normal projection boundary.

## Additional Workflow Store scenarios

| Semantic contract | Observable proof | Executable test ID |
| --- | --- | --- |
| Execution identity | Typed `CREATED`, `EXISTING`, input mismatch, and workflow-type mismatch outcomes. | `test_workflow_execution_identity_outcomes[sqlite]` |
| Step identity | Cached, input-mismatch, and definition-mismatch checks do not append events. | `test_workflow_step_identity_checks_do_not_append_events[sqlite]` |
| Event order, cursor reads, and restart visibility | Sequences are contiguous; cursor reads return the suffix; a reopened store returns the same terminal execution, step, and events. | `test_workflow_event_order_cursor_and_restart_visibility[sqlite]` |
| Prepare-before-dispatch and dispatch-token fencing | Dispatch before preparation fails; retry rotates the token; a late result raises `StaleDispatchError` and cannot finalize the current action or parent step. | `test_prepare_precedes_dispatch_and_dispatch_token_fences_late_result[sqlite]` |
| Cancellation arbitration | Cancellation committed before first dispatch returns `RUN_CANCELLED`; action remains prepared with dispatch count 0 and has no dispatch event. | `test_cancellation_wins_before_first_external_dispatch[sqlite]` |
| Workflow transition/event atomicity | Injected `step.completed` event failure rolls the step transition back; reopen sees the original running step and no terminal event. | `test_workflow_state_transition_and_event_append_are_atomic[sqlite]` |
| External action / parent step / event atomicity | Injected parent `step.completed` event failure rolls back the action success, parent completion, and both terminal events. | `test_external_action_parent_step_and_events_finalize_atomically[sqlite]` |

The test IDs in this table are relative to
`tests/conformance/test_workflow_store_contract.py::` unless a full path is shown.

## Deterministic execution rules

- Lease scenarios use the injected store clock. They never sleep to wait for expiry.
- Claim races use explicit barriers and bounded thread joins/results.
- Recovery closes the logical lifetime of prior objects by creating new store, coordinator, or
  manager instances against the same durable database.
- Composite synchronization uses `threading.Event`; it does not infer that a race occurred because
  enough wall-clock time passed.
- Assertions target typed outcomes, durable status, attempt, revision, event sequence/payload,
  provider call count, and sanitized error code.

## Backend adapter boundary

`tests/conformance/backends.py` contains the current SQLite adapter. Its fault-injection hooks may
use implementation knowledge to create a failure or mixed-writer condition, but the shared scenario
does not inspect or assert SQL, PRAGMA values, index names, migrations, or query plans. Those remain
implementation-specific tests.

The adapter currently returns concrete `SQLiteRunStore` objects and its failure hooks patch private
SQLite methods or issue controlled SQL. That coupling is isolated to the adapter; it is also why the
suite is described as an SQLite reference contract rather than a completed backend abstraction.

Overlapping behavioral tests were migrated from `tests/test_run_leasing.py` and
`tests/test_thread_serialization.py` into this directory. The original files retain SQLite-specific
schema, index, corruption, and RuntimeManager regressions; the conformance directory is now the
canonical home for the shared lease, Thread, CAS, and atomic-terminal semantics.

Adding a future backend means:

1. generalize the test-side surface only as required by the real implementation;
2. add an adapter that creates and recreates the backend and supplies equivalent clock and
   fault-injection hooks;
3. add it to the conformance fixture and run the same observable scenario semantics;
4. keep backend schema/migration/performance tests separate;
5. do not claim semantic interchangeability until all applicable scenarios pass.

## Explicit non-goals

This suite does not add PostgreSQL, Redis, a queue, multi-host coordination, arbitrary operator
repair, a generic production store interface, a checkpoint schema change, automatic unquarantine,
or Runtime/provider retry. It records the SQLite single-host semantics, including the one narrow
operator resolution, in executable reference contracts prepared for reuse when a real second
backend exists.
