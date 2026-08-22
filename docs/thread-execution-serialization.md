# Durable execution plane: thread-scoped execution serialization

Status: implementation candidate on `feat/thread-scoped-execution-cas`; not yet merged to `main`.

Baseline: `8a508493270be87b06abdc4c1b12f14d0c913b14`.

## Problem

Run leasing and fencing establish durable ownership of one Run attempt. They prevent an expired
attempt from overwriting a recovered attempt of the same Run. They do not prevent two different
Runs for the same tenant-qualified thread from executing at the same time.

Today, two workers can claim two Runs whose key is the same `(tenant_id, thread_id)`. Both can load
the same thread checkpoint, execute independently, and then commit in either order. The later
commit wins the `thread_states` upsert even if it was calculated from an older checkpoint. The
result is a lost conversation update and a checkpoint that does not describe the durable order of
the Runs.

Checkpoint overwrite is not the only risk. Both executions can make Planner decisions, call tools,
and authorize external actions before either checkpoint write occurs. Rejecting only the second
checkpoint write would preserve the stored state but would not undo work that was already
performed from stale state.

The legacy synchronous `POST /agent/message` path has the same problem from another direction. It
loads a checkpoint, executes `TravelAgentRuntime` inline, and calls
`save_unmanaged_thread_state()`. That path does not acquire a Run lease and can overwrite the
checkpoint of a managed Run.

The user-visible risks are:

- a later message losing the result of an earlier message;
- conversation turns being applied in an order different from their durable Run order;
- tools or external actions running from a stale checkpoint;
- a recovered Run observing a different base checkpoint from its original attempt;
- a compatibility endpoint bypassing an otherwise fenced execution plane.

## Existing solutions and their limits

Established systems combine two different ideas:

- workflow and actor systems serialize messages for one durable identity;
- versioned stores use compare-and-swap to reject a write derived from an obsolete snapshot.

Neither idea is sufficient by itself here. The relevant implementation choices are:

| Approach | Useful property | Why it is not the selected design |
| --- | --- | --- |
| Process-local lock keyed by thread | Simple and cheap | It does not cross Manager processes, survive restart, or protect another binary using the same SQLite file. |
| SQLite transaction held for the whole Runtime call | Strong serialization | It would hold a database write lock across arbitrary Agent and tool latency and would serialize unrelated threads. |
| Independent thread lease and heartbeat | Durable thread ownership | It duplicates the existing Run lease and creates two tokens, two deadlines, and ambiguous recovery when only one lease expires. |
| Separate `active_run_id` thread record | Explicit active owner | It duplicates truth already present in `runs.status` and requires repair whenever Run and thread records diverge. |
| Checkpoint revision CAS only | Prevents last-writer-wins overwrite | It does not prevent concurrent reasoning, tools, or external actions from stale state. |
| Run-backed thread serialization plus revision CAS | Prevents concurrent execution and stale checkpoint acceptance | Selected. It reuses existing Run authority and adds revision CAS as an independent backstop. |

This milestone does not reproduce a general workflow engine or actor runtime. It adopts only the
minimum semantics the current durable Run model needs.

## Value gap

The current execution boundary answers:

> Which attempt may mutate this Run?

It does not answer:

> Which Run may currently execute against this thread checkpoint?

The missing guarantee is not a second kind of heartbeat. It is a deterministic claim rule that
makes the current `running` Run the durable execution owner of its tenant-qualified thread, plus a
monotonic checkpoint revision that proves the committed state was derived from the expected head.

## Why this component is needed

Thread serialization and checkpoint CAS protect different points in time:

1. serialization prevents a second Run from entering Agent execution while the first Run is
   active;
2. revision CAS rejects a result if another revision-aware writer changed the checkpoint despite
   serialization.

The first is the primary execution guarantee. The second is a fail-closed invariant and migration
guard. A revision conflict should be impossible when every writer uses the new boundary; observing
one is evidence of a revision-aware bypass, store corruption, or an implementation bug. CAS cannot
detect an old writer that changes `state_json` without incrementing `revision`; mixed-version
safety therefore depends on the physical stop-and-drain deployment boundary, not on CAS.

## Deterministic responsibility

All thread ownership and checkpoint revision behavior is deterministic Runtime policy. A Planner,
model, Runtime implementation, tool handler, or provider must never decide whether a thread is
available, which Run executes next, which revision is current, or whether a conflicting result is
accepted.

The durable store owns:

- the thread identity key `(tenant_id, thread_id)`;
- claim eligibility and same-thread ordering;
- recovery-before-successor precedence;
- capture and preservation of a Run's checkpoint base revision;
- checkpoint compare-and-swap and revision increment;
- atomic Run, checkpoint, event, and lease commits;
- classification of lease loss versus checkpoint conflict.

The Manager owns:

- polling for eligible Runs without making in-memory queues authoritative;
- invoking Runtime code only after a successful store claim and revision-qualified load;
- mapping deterministic store outcomes to durable Run failures;
- adapting the legacy synchronous endpoint to the managed Run path.

Agent reasoning remains responsible only for domain decisions within the currently leased Run.

## Scope

This milestone includes:

- one executing Run per `(tenant_id, thread_id)`;
- parallel execution for different thread keys;
- same-thread FIFO selection using the existing persisted `(created_at, run_id)` order;
- recovery of the active `running` Run before any queued successor;
- checkpoint base revision capture at first claim;
- revision-qualified checkpoint loads and compare-and-swap completion;
- removal of the production `save_unmanaged_thread_state()` bypass;
- routing `POST /agent/message` through the managed Run lifecycle;
- an initial-state seed rule that cannot overwrite an existing thread;
- additive SQLite migration guarded by a drain-before-upgrade boundary;
- two-Manager and multi-worker concurrency proofs.

It does not include:

- a second thread lease, token, heartbeat, or timeout;
- PostgreSQL, Redis, Pub/Sub, or a distributed queue;
- a generic execution backend or store abstraction;
- arbitrary checkpoint merge logic;
- user-selected scheduling priorities or fairness policy;
- cross-thread transactions;
- exactly-once computation or external effects;
- forcefully terminating synchronous Runtime code;
- horizontally scalable deployment claims.

## Safety model

The existing Run lease remains the only bearer authority. A `running` Run is the durable active slot
for its tenant-qualified thread, and its current unexpired `lease_token` authorizes attempt-owned
writes. Checkpoint revisions are monotonic concurrency metadata, not secrets and not bearer
capabilities.

No database lock is held while Runtime code executes. SQLite arbitrates only short claim, load, and
commit transactions. Different threads can execute concurrently after their claim transactions
finish.

An expired active Run still occupies its thread slot. Expiry makes that Run recoverable; it does
not make a queued successor eligible. This preserves the existing Action recovery rule: an
interrupted Run must reconcile its durable external-action evidence before a later message can act
on the same thread.

## Required invariants

1. The serialization identity is exactly `(tenant_id, thread_id)`. `subject_id`, Agent version, and
   domain do not create a second execution lane for the same checkpoint key.
2. At most one Run for a serialization identity may have `status = running`.
3. A queued Run is claimable only when no Run for the same identity is `running`.
4. Among uncancelled queued Runs for one identity, the oldest persisted `(created_at, run_id)`
   candidate is claimed first. A later Run cannot skip an earlier uncancelled queued Run.
5. If a same-thread Run is `running`, a queued successor remains blocked even when the running
   lease expires. The running Run must be recovered and terminalized first.
6. An in-memory queue, lock, or wake signal is never thread ownership evidence.
7. Thread ownership uses the current Run lease token. No independent thread token exists.
8. The first successful claim records `checkpoint_base_revision`. A takeover preserves it exactly.
9. A Run submitted without client state captures the latest checkpoint revision when it is first
   claimed, not when it is queued. It therefore observes a predecessor's committed state.
10. A client-provided state is only an initial seed for an empty thread. Its base revision is `0`.
11. Revision `0` means no `thread_states` row. Every persisted row has revision at least `1`.
12. A managed Runtime is invoked only after the store proves that the current checkpoint revision
    equals the Run's base revision.
13. Successful checkpoint persistence requires the same revision equality and increments the
    revision by exactly one.
14. Run completion, checkpoint insertion/update, `checkpoint.saved`, `run.completed`, and Run lease
    clearing commit atomically.
15. Failure and cancellation release the logical thread slot by terminalizing the Run but do not
    change the checkpoint revision.
16. Reconciliation-pending work remains `running` and continues to block queued successors. If it
    also has a checkpoint conflict, it is quarantined without invoking Runtime code or releasing
    the thread slot.
17. A stale Run token cannot save a checkpoint, terminalize a Run, or release another Run's thread
    slot.
18. A revision conflict is permanent for that execution result and is never automatically retried.
19. A blocked thread does not cause head-of-line blocking for eligible Runs on other threads.
20. Same `thread_id` values in different tenants are independent and may execute concurrently.

## Persisted model

### `runs`

Add one nullable internal column:

```text
checkpoint_base_revision    INTEGER
```

The column is excluded from public `RunRecord` serialization and representations. It is not an
input clients may set.

For a normal queued Run without client state, the value remains null until the first claim. The
claim transaction stores the current thread revision. Every later attempt of that Run reuses the
same value.

For an accepted initial-state seed, the value is set to `0` in the create transaction. The
revision-qualified load still verifies that the thread is empty before Runtime code executes.

### `thread_states`

Add:

```text
revision    INTEGER NOT NULL
```

The logical rules are:

```text
no row                         -> revision 0
first successful checkpoint   -> revision 1
next successful checkpoint    -> revision 2
...
```

Existing rows migrate to revision `1`; their exact historical write count is unknowable and not
needed. Only equality and monotonic increments after migration matter.

### Thread-owner constraint

No new thread-owner table is added. The `running` Run row is the durable thread-owner record. Add a
partial unique index as a store-level backstop:

```sql
CREATE UNIQUE INDEX idx_runs_one_running_per_thread
ON runs(tenant_id, thread_id)
WHERE status = 'running';
```

The claim query must still filter blocked candidates before attempting an update. The index catches
implementation mistakes and cross-connection races; it is not the scheduling algorithm.

The implementation retains the existing
`idx_runs_claimable(status, lease_expires_at, created_at)` index. `EXPLAIN QUERY PLAN` for the final
selector shows the same-thread anti-joins using the partial unique index, while the global
`(created_at, run_id)` ordering still uses a temporary B-tree. A speculative thread-shaped index
did not remove that sort in backlog probes, so this milestone adds no unproven index. Revisit the
query and index together only with measured nonterminal-backlog data.

## Client-provided state contract

A schema-valid client state is not authenticated durable history. Under this milestone it has one
safe use: seeding a new thread.

`RuntimeManager.submit()` validates that request state has the same `thread_id` and registered
schema as the Run. `create_run_with_event()` then enforces in one store transaction:

- no `thread_states` row exists for `(tenant_id, thread_id)`;
- no queued or running Run already exists for that key;
- `checkpoint_base_revision` is persisted as `0`.

If any condition is false, submission fails with a stable conflict rather than accepting a future
overwrite. Continuing an existing thread requires omitting `state`; the Manager then loads the
durable checkpoint when the Run becomes active.

A future API may expose an explicit `expected_checkpoint_revision` for controlled external state
replacement. That is not part of this milestone. Unversioned replacement of an existing
checkpoint remains forbidden.

## Claim transaction

`claim_next_run()` keeps its short `BEGIN IMMEDIATE` transaction and extends candidate eligibility.
The selector must filter ineligible same-thread rows in SQL so that one blocked thread cannot cause
the worker to return empty while another thread has eligible work.

Conceptually, an eligible candidate is:

```text
expired running Run
    whose thread has no different running Run

or

oldest uncancelled queued Run for its thread
    whose thread has no running Run
    and no earlier queued Run
```

Under the post-migration unique index, a thread cannot normally contain multiple running Runs. If
store inspection finds such rows before the index exists or during startup validation, execution
fails closed; the Runtime does not guess which concurrent history is correct.

For a fresh queued Run, the transaction:

1. reads the checkpoint revision, using `0` when no row exists;
2. sets `status = running`;
3. increments `attempt`;
4. writes a new owner, Run lease token, heartbeat, and expiry;
5. writes `checkpoint_base_revision` when it is null;
6. appends `run.started`, including the non-secret base revision.

An accepted seed already has base revision `0` from its create transaction. The Manager's
revision-qualified load rechecks that base against the current checkpoint before invoking Runtime
code; the claim transaction does not silently rebase it.

For an expired running Run, the transaction:

1. requires an already persisted non-null `checkpoint_base_revision`;
2. preserves that revision;
3. rotates the Run lease token and increments `attempt`;
4. preserves cancellation and reconciliation markers;
5. appends `run.recovered(reason=lease_expired)` and `run.started`.

A post-migration running Run with a null base revision is mixed-version or corrupt state and is not
claimable. It must not silently adopt whatever checkpoint happens to be current.

## Revision-qualified load

After claim and before Runtime execution, the Manager loads the checkpoint with the expected base
revision:

```text
expected 0 + no row       -> new state
expected N + row N        -> deserialize row
expected 0 + existing row -> conflict
expected N + no row       -> conflict
expected N + row M != N   -> conflict
```

Domain and schema checks remain in the same boundary. A request seed is revalidated but does not
replace an existing checkpoint.

The Manager appends `checkpoint.loaded` only after the revision-qualified load succeeds. Its
payload may contain `source` and `revision`; neither is an authority token.

## Completion transaction

`commit_completed_run()` remains one `BEGIN IMMEDIATE` transaction. It must:

1. verify Run status, tenant, current unexpired Run lease token, and cancellation predicate;
2. require a validated checkpoint state for a successful managed completion;
3. compare the current logical thread revision with `checkpoint_base_revision`;
4. insert revision `1` when the base is `0`, or update `N -> N + 1` with `WHERE revision = N`;
5. transition the Run to `completed` and clear its lease;
6. append `checkpoint.saved` with `base_revision` and `revision`;
7. append `run.completed`;
8. commit all changes together.

Any zero-row checkpoint CAS rolls back the whole transaction. In particular, it must not leave a
completed Run without its checkpoint or append terminal evidence for a rejected result.
Calling the public store method with no state is a programming error: it raises before mutation and
leaves the Run `running` with its current lease and checkpoint unchanged.

`commit_failed_run()` and `commit_cancelled_run()` continue to require the current Run lease but do
not compare or increment checkpoint revision because they do not save state. Their terminal status
atomically releases the thread for its next queued Run.

## Failure semantics

Extend the typed store outcome with:

```text
RunCommitOutcome.CHECKPOINT_CONFLICT
```

Use a stable durable failure code:

```text
thread_checkpoint_conflict
```

Use a distinct nonterminal quarantine code when checkpoint drift is detected while external-action
recovery or reconciliation precedence is active. This includes an in-flight dispatch and a
terminal dispatched Action whose recovery ordering or evidence projection still has to complete:

```text
thread_checkpoint_conflict_reconciliation_pending
```

The behaviors are:

| Condition | Result |
| --- | --- |
| Same-thread predecessor is running | Successor stays queued; this is not a failure. |
| Run lease token is stale or expired | Existing `lease_lost` behavior; do not relabel it as a checkpoint conflict. |
| Revision mismatch before Runtime execution, with no external-action recovery/reconciliation precedence | Do not call the Runtime; fenced terminal failure with `thread_checkpoint_conflict`. |
| Revision mismatch while external-action recovery/reconciliation precedence is active | Do not call the Runtime. Quarantine the Run as nonterminal, clear its lease, exclude it from automatic takeover, and retain the thread slot for a future controlled operator-repair flow. |
| Revision mismatch at completion | Roll back completion and checkpoint writes, then fenced terminal failure with `thread_checkpoint_conflict`. |
| Seed targets a nonempty or busy thread | Reject submission with a stable conflict; no Run is queued. |
| Multiple running Runs violate the unique thread invariant | Fail startup/claim closed; do not select a winner automatically. |
| Conflict terminalization loses the Run lease | Accept no result; normal takeover encounters the same base-revision mismatch and fails before re-execution. |

An ordinary `thread_checkpoint_conflict` is permanent for that execution result. The Manager must
not turn it into a transparent retry because the rejected attempt may already have durable workflow
or external-action evidence. After that Run is terminal, a caller may submit a state-less Run that
loads the current checkpoint. This advice does not apply to
`thread_checkpoint_conflict_reconciliation_pending`: that marker is nonterminal and intentionally
retains the thread slot, so a same-thread replacement remains blocked.

A completion-time mismatch is terminal only after the registered Runtime has returned a completed
state. Supported external-action runtimes do not return while reconciliation is nonterminal: they
raise `ExternalActionReconciliationPendingError` and bypass completion. The conflict transaction
rejects Run success and checkpoint persistence; it does not rewrite or delete provider, Action,
tool, or workflow outcomes. A future Runtime that can return while dispatched work remains
nonterminal would invalidate this assumption and must quarantine at completion instead.

Expected and actual revision numbers may appear in sanitized error/event metadata. State contents,
lease tokens, owner IDs, and credentials must not.

## `POST /agent/message`

The endpoint must stop constructing `TravelAgentRuntime` inline and stop calling
`save_unmanaged_thread_state()`.

Instead it:

1. creates a managed `travel-agent:0.3.0` Run through `RuntimeManager.submit()` using the effective
   authenticated execution authority;
2. uses the same state-seed rule as `POST /runs`;
3. waits at most the requested `wait=0..5` seconds, defaulting to five seconds, under a bounded
   async waiter pool;
4. maps a completed Run's message, state, and validation errors into the legacy `200` response;
5. otherwise returns `202` with `run_id`, current `status`, `Location`, and `Retry-After` while the
   durable Run continues independently.

The endpoint preserves the legacy completed-response shape, not unbounded synchronous waiting or
the old state-replacement behavior. `RUNTIME_AGENT_MESSAGE_WAITER_LIMIT` bounds concurrent waiters;
an exhausted waiter pool returns `202` immediately instead of queueing for a waiter slot. Polling
uses async sleep and offloaded store reads capped by both the remaining request budget and a
250-millisecond SQLite timeout. A busy/locked or over-budget observation ends waiting with the last
known nonterminal `202`; it does not occupy the shared sync endpoint threadpool or fail the durable
Run. A client disconnect or timeout does not authorize inline fallback and does not cancel the Run.

The `wait` budget begins only after durable submission succeeds. Authentication, checkpoint
preflight, and Run creation retain their ordinary storage-operation timeout. The route must not
abandon an in-flight submission merely to satisfy the observation budget: that write could still
commit without returning a `run_id` to the caller. This is the same durable-acceptance boundary used
by `POST /runs`; `wait` bounds terminal observation, not the database submission transaction.

The optional `state` is accepted only when the tenant-qualified thread has no checkpoint and no
queued or running Run. Otherwise the endpoint returns `409`; continuing an existing thread requires
omitting `state` so the managed Run loads durable history. This is an intentional breaking safety
change for callers that previously echoed `updated_state` into every request.

The unrestricted `save_unmanaged_thread_state()` method is removed. Keeping it as a public store
surface, even without a production caller, would retain a bypass around the new invariant.

## Recovery, cancellation, and reconciliation

Recovery precedence for one thread is:

```text
active Run lease expires
    -> recover the same Run with a rotated token
    -> replay/reconcile its durable work
    -> complete, fail, or cancel it
    -> only then claim the queued successor
```

A live unexpired Run is never stolen. Expiry does not clear its thread ownership. A graceful stop
may expire the Run lease after execution threads drain, but the persisted Run remains `running` and
is still the first recovery candidate for its thread.

Queued cancellation is terminalized before claim and therefore releases its queue position without
changing checkpoint revision. Running cancellation continues to require the current Run lease.
When external-action evidence requires reconciliation, the Run remains `running` until that safety
decision is resolved; queued successors stay blocked.

External-action safety and checkpoint safety fail closed together. A recovered Run with active
external-action recovery/reconciliation precedence and a revision mismatch must not enter the
ordinary Runtime: a completion CAS could reject stale state but could not undo tools or external
effects produced before completion.
The Manager therefore writes durable conflict evidence, keeps the Run `running`, clears its lease,
and excludes that quarantine code from automatic takeover. The same-thread successor remains
blocked. This state requires controlled operator repair; the runtime neither guesses a checkpoint
base nor hides an unresolved provider outcome behind an ordinary terminal conflict. This milestone
only creates and preserves the quarantine. It does not add an automatic retry or repair primitive.

### Quarantine operations: detect and contain

`thread_checkpoint_conflict_reconciliation_pending` is an invariant-breach sentinel, not a normal
retry state. In a homogeneous deployment it should be unreachable; observing it indicates a mixed
writer, manual checkpoint change, store corruption, or an implementation defect.

Operators may locate affected rows using read-only inspection of a consistent SQLite backup:

```sql
SELECT run_id, tenant_id, thread_id, agent_id, agent_version,
       status, error_code, cancel_requested, attempt,
       checkpoint_base_revision, created_at, updated_at
FROM runs
WHERE status = 'running'
  AND error_code =
      'thread_checkpoint_conflict_reconciliation_pending';
```

For a public Run, inspect `GET /runs/{run_id}` and `GET /runs/{run_id}/events`, including the
`checkpoint.conflict` event with disposition
`external_action_reconciliation_quarantined`. For an Action-owned private Run, inspect the existing
Action through `GET /actions/{action_id}` and `GET /actions/{action_id}/events`; generic Run routes
intentionally hide private Action Runs. Preserve the workflow/Action ledger and provider-side
evidence even when a late authorized provider result has not yet been mirrored into public events.

Operational handling is containment only:

1. Stop new submissions for the affected `(tenant_id, thread_id)` at ingress. Other threads remain
   usable.
2. Preserve a consistent Runtime database backup and corresponding provider evidence. Do not copy
   only the main SQLite file while WAL is active.
3. Do not directly update Run status, error code, lease fields, `checkpoint_base_revision`, or
   `thread_states`. Do not delete or rewrite tool, Action, workflow, idempotency, or event evidence.
4. Do not use cancellation as an unquarantine operation. It records intent but cannot terminalize
   this lease-free running row.
5. Do not repeat the operation under a new Run or thread. External-action idempotency is Run-scoped,
   so a replacement may receive a different provider key and duplicate the effect.
6. Leave the row quarantined and escalate to a release with an audited repair primitive or a
   provider-specific incident procedure that can prove the external outcome.

This release has no supported unquarantine or force-terminalization operation. A future repair
primitive must compare-and-set the exact quarantine state, require resolved Action evidence, avoid
Runtime/provider invocation and checkpoint rewrites, preserve every ledger, append operator identity
and reason, and atomically terminalize the Run before releasing its thread slot.

Run heartbeat renewal does not update checkpoint revision and does not create thread events.

## Migration and deployment boundary

The old binary did not record checkpoint base revisions and allowed same-thread Runs to overlap.
There is no safe general algorithm for reconstructing which checkpoint an interrupted old Run
originally loaded. Guessing the current revision would turn an unknown history into false evidence.

The upgrade therefore requires a stronger boundary than the additive column changes alone:

1. stop accepting new Run and `/agent/message` submissions;
2. use the old binary to drain all queued and running Runs to terminal states;
3. verify with a store query that the nonterminal Run count is zero;
4. stop and verify exit of every old Runtime process;
5. back up the SQLite database;
6. add `runs.checkpoint_base_revision`;
7. add `thread_states.revision`, assigning `1` to every existing row;
8. update every table-rebuild migration to copy both columns;
9. create the partial unique running-Run index;
10. start only binaries that enforce thread serialization and revision CAS;
11. re-enable submissions.

Initialization must abort before workers start if a pre-migration database contains queued or
running Runs. Operators must drain or explicitly resolve them; startup must not rewrite status,
invent a base revision, or choose among concurrent old Runs.

Existing terminal Runs, events, workflow ledgers, memories, Actions, and checkpoint JSON remain
unchanged. Existing checkpoint rows begin at revision `1` because their pre-migration write count
is not part of the contract.

Mixed old/new execution is unsupported. An old Manager can claim a same-thread Run without the new
predicate, and the old synchronous route can write checkpoint state without incrementing revision.
That state-only write is invisible to CAS because the revision does not change. The unique index may
make some violations fail loudly, but it does not make mixed-version operation safe. Deployment
must stop old processes before schema initialization and keep them stopped. This manual drain rule
still applies if a failed or partial rollout already added the new columns: once the columns exist,
initialization cannot infer that a nonterminal row was created by an old binary.

Rollback is also a stop-the-world operation. Drain all nonterminal new-version Runs, stop every new
binary, back up the database, and use an explicit data-compatible downgrade procedure. Starting an
old binary against a live new-version database removes the guarantee even if the additive columns
remain readable.

## Transaction and crash boundaries

The required ordering is:

```text
submit -> durable queued Run
       -> best-effort wake

worker -> short claim transaction
       -> revision-qualified checkpoint load
       -> Runtime execution outside database transaction
       -> fenced completion transaction
       -> next same-thread Run becomes eligible
```

Crash cases remain deterministic:

- crash after submit but before wake: polling finds the queued Run;
- crash during claim: SQLite commits the whole claim or none of it;
- crash after claim but before load: the Run lease expires and the same Run is recovered with its
  persisted base revision;
- crash during Runtime execution: the successor stays queued while the active Run recovers;
- crash during completion: Run status, checkpoint revision, events, and lease clearing commit
  together or roll back together;
- stale local Runtime return after takeover: existing Run fencing rejects it.

## Test proof matrix

The milestone is not complete without deterministic tests for all of the following.

### Store arbitration

- two store instances cannot claim two queued Runs for the same thread;
- the partial unique index rejects a second `running` Run for the same tenant/thread;
- identical thread IDs in different tenants are independent;
- a queued candidate blocked by its thread does not prevent claiming an eligible different thread;
- same-thread queued Runs are selected in `(created_at, run_id)` order;
- a queued successor remains blocked at, before, and after its predecessor's lease expiry;
- only takeover of the expired predecessor is eligible;
- takeover preserves `checkpoint_base_revision` and rotates only Run attempt authority.

### Checkpoint CAS

- absent checkpoint is revision `0` and first save creates revision `1`;
- successive completed Runs observe and write `1 -> 2 -> 3`;
- failure and cancellation do not increment revision;
- revision mismatch before execution produces zero Runtime calls;
- revision mismatch at completion writes no checkpoint or completion evidence;
- a stale Run token cannot write even when its base revision matches;
- checkpoint save, completion event, and lease clearing are atomic under injected failures.

### Manager concurrency

- with two workers and two Managers, only one same-thread Runtime enters its blocking section;
- a second thread enters concurrently while the first thread is blocked;
- after Run 1 completes, Run 2 loads Run 1's exact state and incremented revision;
- after Run 1 crashes, Run 2 does not start; Run 1 attempt 2 recovers first;
- reconciliation-pending and running-cancellation paths retain the thread slot;
- queued-head cancellation unblocks the next Run without a checkpoint change.

### API and state seeds

- `/agent/message` creates a durable Run and never calls the unmanaged save path;
- concurrent `/agent/message` and `/runs` requests for one thread serialize through the same claim;
- a state seed succeeds only on an empty, idle thread;
- a second seed, a seed against an existing checkpoint, and a seed behind queued work fail closed;
- a request without state waits and then loads the predecessor's checkpoint;
- public Run/OpenAPI serialization excludes `checkpoint_base_revision` and all lease authority.

### Migration and compatibility

- every existing checkpoint row migrates unchanged with revision `1`;
- a database with any queued or running Run refuses the upgrade/startup boundary;
- all table rebuilds preserve the new columns and revisions;
- revision-aware injected writes are detected as conflicts rather than accepted;
- Python 3.11 and 3.12 full suites remain green.

Tests that prove serialization must coordinate with events/barriers rather than timing sleeps. Tests
for parallelism must observe both different-thread Runtime calls entered before either is released.

## Implementation slices

### Slice 1: persisted revision contract

- add columns, models, migrations, public-boundary exclusions, and the unique index;
- implement revision-qualified checkpoint read and atomic checkpoint CAS;
- add typed conflict outcomes and migration tests.

### Slice 2: same-thread claim arbitration

- extend `claim_next_run()` eligibility and repeat the predicate in its conditional update;
- capture base revision on fresh claim and preserve it on takeover;
- prove same-thread exclusion, recovery precedence, no cross-thread head-of-line blocking, and true
  different-thread parallelism.

### Slice 3: remove the compatibility bypass

- enforce the empty-thread state-seed rule;
- route `/agent/message` through `RuntimeManager`;
- remove `save_unmanaged_thread_state()`;
- add API race and durable recovery tests.

These are review slices, not independently shippable dormant features. The branch must not merge
until every checkpoint writer and public execution path obeys the same contract.

## Acceptance criteria

The milestone is complete when:

- two different Runs for one tenant-qualified thread cannot execute concurrently;
- different threads demonstrably still execute in parallel;
- an expired active Run is recovered before its successor starts;
- every managed Run loads and commits against one persisted base revision;
- a successful state-bearing managed Run advances the checkpoint revision exactly once;
- stale revisions and stale Run tokens both fail closed without terminal success evidence;
- client state cannot overwrite an existing thread;
- `/agent/message` has no direct checkpoint-write path;
- crash recovery, cancellation precedence, and Action reconciliation remain unchanged;
- migration refuses ambiguous old nonterminal work instead of inventing history;
- public APIs expose no internal Run authority;
- the full Python and Docker Action recovery proof suites remain green;
- documentation continues to state that SQLite is a single-host reference boundary.

## Follow-up boundaries

A future PostgreSQL implementation can preserve the same semantics using a short transactional
claim and conditional checkpoint update. It must not replace durable ownership with a queue receipt,
advisory lock, or process-local mutex.

An explicit public checkpoint update API may later expose revision tokens or HTTP ETags. That API
must remain a conditional write and must not revive arbitrary unversioned state replacement.

Store conformance tests are most useful after this contract is implemented: at that point they can
encode Run lease fencing, same-thread serialization, checkpoint revision CAS, and recovery ordering
as one backend-independent semantic suite without claiming that a second backend already exists.
