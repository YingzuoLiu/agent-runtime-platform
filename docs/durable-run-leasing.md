# Durable execution plane: run leasing and fencing

Status: historical design record for the merged Run Leasing & Fencing milestone (PR #27).

The later thread-serialization milestone supersedes this document wherever it strengthens claim,
legacy migration, or checkpoint rules. In particular, current code fails closed for a legacy
`running` row without `checkpoint_base_revision`; it no longer automatically recovers every
unleased `running` row described below. See `docs/thread-execution-serialization.md`.

Baseline: `b93914b7dc126f51bf4af7c4de11a2dc45e1918a`.

## Problem

The Runtime already persists Run state, events, checkpoints, workflow steps, and external-action
evidence. It also recovers interrupted work after a process restart. The remaining problem is not
durability of the data. It is durable ownership of live execution.

At the baseline, `RuntimeManager` used an in-process queue. At startup it treated every `running`
Run as abandoned, moved it back to `queued`, and scheduled it again. That assumption is valid only
when the new Manager is known to be the sole process. It is false during an overlapping restart, an
accidentally started second Manager, or a future multi-replica deployment.

A two-Manager probe against the baseline SQLite store demonstrated the failure:

1. Manager A claims attempt 1 and blocks inside the Runtime.
2. Manager B starts, sees the live `running` row, resets it, and starts attempt 2.
3. Attempt 1 can still commit its old output while the row already records attempt 2.
4. A late exception from attempt 1 can overwrite an already completed attempt 2 with `failed`,
   leaving both `run.completed` and `run.failed` in the evidence stream.

The user-visible risks are a wrong terminal result, a stale thread checkpoint, contradictory audit
evidence, and unsafe recovery decisions.

## Baseline protections

The baseline already contained several relevant mechanisms. The implementation reuses them rather
than replacing them:

- the baseline `claim_run_start` used a status compare-and-set, so two workers racing for the same
  `queued` Run could not both perform the initial `queued -> running` transition; the lease-aware
  `claim_next_run` supersedes that narrower primitive.
- completion and cancellation use narrow status/cancellation predicates.
- workflow steps receive attempt-specific tokens; a stale step cannot complete over a newer
  attempt.
- external actions use prepare-before-dispatch state, dispatch tokens, provider idempotency
  declarations, and explicit reconciliation of uncertain outcomes.
- startup recovery replays persisted decisions rather than asking a Planner to invent a new
  history.

These mechanisms protect individual transitions, workflow steps, and external actions. They do not
identify which live worker currently owns the top-level Run. The existing integer `attempt` is
observability data, not a write capability, because finalization does not compare it.

## Existing-system research

The design borrows semantics, not infrastructure, from established systems:

| System | Useful pattern | Important limitation |
| --- | --- | --- |
| Temporal | Each Activity attempt has a task token; a configured Heartbeat Timeout detects missed heartbeats; a late completion from an obsolete attempt is rejected by the service. | Activity retries can repeat external side effects, so Activities still need idempotency. This project does not need to reproduce Temporal. |
| Amazon SQS | Visibility timeout is a renewable claim, and every receive produces a new receipt handle. | Standard queues are at-least-once, and an old receipt handle is not a strong fencing guarantee. |
| Kubernetes Lease | Holder identity, renewal time, duration, and transition count form a useful lease record. | The client-go implementation explicitly says leader election does not provide fencing; two actors can still act concurrently. |
| PostgreSQL | `FOR UPDATE SKIP LOCKED` is explicitly suitable for multiple consumers of a queue-like table. | A row lock lasts only for the transaction and must not be held for the whole Run. Durable ownership still has to be stored as data. |

Primary references:

- [Temporal Activity execution and task tokens](https://docs.temporal.io/activity-execution)
- [Temporal durable Event History](https://docs.temporal.io/workflow-execution/event)
- [Temporal rejection of obsolete task completion](https://github.com/temporalio/documentation/blob/main/docs/troubleshooting/request-failures.mdx)
- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Amazon SQS receipt handles](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html)
- [Kubernetes Lease objects](https://kubernetes.io/docs/concepts/architecture/leases/)
- [Kubernetes client-go non-fencing warning](https://github.com/kubernetes/client-go/blob/master/tools/leaderelection/leaderelection.go)
- [PostgreSQL `SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html)

## Value gap

A heartbeat alone would only answer, approximately, whether an owner still appears alive. It would
not prevent a paused or partitioned owner from waking up and writing after takeover. Conversely, a
token without expiry would prevent stale writes but could leave a crashed Run owned forever.

The missing component therefore has two inseparable parts:

1. a renewable lease for bounded failure detection and takeover;
2. an attempt-specific fencing token checked by every attempt-owned durable mutation.

The durable store, not an in-memory queue and not an Agent decision, must be the execution source of
truth.

## Deterministic responsibility

All leasing behavior is deterministic Runtime policy. A Planner, model, tool handler, or provider
must never decide whether a lease is valid, whether it may be stolen, or whether a stale result is
accepted.

The store owns:

- claim eligibility;
- lease expiry comparison;
- token rotation;
- heartbeat renewal;
- takeover and attempt increments;
- cancellation/finalization arbitration;
- rejection of stale writes;
- atomic Run, checkpoint, and event commits.

Agent reasoning remains responsible only for domain decisions within a currently authorized
execution attempt.

## Scope

This milestone includes:

- Run-level owner, heartbeat, expiry, and fencing-token state;
- atomic claim of new and expired Runs;
- periodic renewal using store-authoritative time;
- durable polling, with an in-process signal used only to reduce latency;
- fenced completion, failure, cancellation, reconciliation markers, and attempt-owned Run events;
- propagation of Run execution authority into workflow, memory, and external-action mutation gates;
- recovery of legacy unleased `running` rows (the behavior introduced by this historical
  milestone and later superseded by the thread-serialization migration boundary);
- two-Manager concurrency and stale-worker tests against one SQLite database;
- preservation of the existing external-action recovery rules.

It does not include:

- PostgreSQL, Redis, Pub/Sub, or a horizontally scalable deployment claim;
- a generic queue or execution-backend interface;
- exactly-once execution or exactly-once external effects;
- forcefully terminating Python threads or arbitrary Runtime code;
- per-thread serialization or checkpoint revision compare-and-set;
- quotas, priorities, autoscaling, or worker placement;
- Docker, gVisor, microVM, or untrusted-code execution changes.

SQLite proves the ownership and fencing semantics for multiple Manager instances sharing one local
database. It does not make SQLite a supported multi-host production database.

## Safety model

The Runtime continues to execute trusted first-party Runtime implementations. A synchronous Python
call cannot be forcibly stopped safely when its lease is lost. The Manager latches a failed renewal
and discards a late terminal result; nested durable mutations independently reject the stale token.
Store-side fencing remains authoritative:

- a stale worker cannot mutate top-level Run state, checkpoint, or attempt-owned Run evidence;
- a stale Run owner cannot record new Planner/workflow decisions, create or recover workflow
  attempts, seal attempt-owned memory, or obtain a new external-dispatch authorization;
- ordinary workflow step completion/failure requires both the current Run token and the existing
  step-attempt token;
- existing dispatch tokens and reconciliation rules fence durable acceptance of a response from a
  provider request that was already authorized and may have been sent;
- consequential effects must continue to use the External Action Gateway.

A lease cannot undo a provider request that was already sent. At-least-once computation and an
uncertain external outcome remain possible, and must remain explicit.

## Required invariants

1. The `runs` table is the source of truth. An in-memory signal may wake workers but may not be the
   only record that work exists.
2. At most one unexpired lease token is current for a Run.
3. `lease_owner_id` is diagnostic identity. Only the current `lease_token`, validated by the store,
   grants write authority.
4. Claim is one transaction: select an eligible Run, rotate its token, increment `attempt`, set
   owner/heartbeat/expiry, and append the describing events.
5. Renew succeeds only while the Run is `running`, the token still matches, and store time is
   strictly before expiry. It sets `heartbeat_at = store_now` and
   `lease_expires_at = store_now + lease_duration`; it never extends from the previous deadline.
6. For each transaction, `store_now == lease_expires_at` is expired and the attempt cannot renew or
   commit in that transaction. Expiry is evaluated against current store time.
7. A Manager permanently latches a failed renewal and never retries that heartbeat, even if the
   wall clock later moves backward. Any later terminal or nested mutation can succeed only if its
   own transaction independently proves that the token is still current.
8. Every takeover creates a new token. Token rotation, not wall-clock history, makes the previous
   authority permanently invalid.
9. Completion, failure, running cancellation, reconciliation markers, checkpoint writes, and
   attempt-owned events require the current unexpired token.
10. Recording Planner/workflow decisions or replay evidence, creating/recovering/completing/failing a
   workflow attempt, terminalizing a workflow, writing attempt-owned memory, and preparing or
   starting an external dispatch require the same current unexpired Run token in the mutation
   transaction.
11. A local reconciliation decision that closes a dispatch as unknown requires the current Run
    lease. Finalizing an actual response or ambiguity from an already-started provider call remains
    guarded by its dispatch and tool-attempt tokens; lease loss does not make valid provider evidence
    false.
12. A successful terminal transition, its checkpoint when applicable, its terminal event, and lease
   clearing commit atomically.
13. Cancellation requests remain tenant-scoped and do not require worker authority. Resolving a
    running cancellation does require the current lease.
14. Expired external-action work enters the existing reconciliation path. Lease takeover never
    authorizes a blind provider replay.
15. Heartbeats do not append durable Run events. Claim, takeover, recovery, and terminal transitions
    do; high-frequency renewal is operational state, not business evidence.

## Persisted model

Add nullable internal columns to `runs`:

```text
lease_owner_id          TEXT
lease_token             TEXT
lease_heartbeat_at      INTEGER
lease_expires_at        INTEGER
```

The time columns are store-generated UTC epoch milliseconds. Comparisons happen inside the same
database transaction that performs the conditional update. Worker wall clocks do not decide expiry.

The existing `attempt` remains the human-readable execution generation and increments on every
successful claim, including takeover. The opaque token is still required; an integer shown in an
API or log must not become a bearer capability.

This opaque equality token fences mutations in stores that validate it. It is not a monotonic token
that a downstream provider can compare, and it does not provide exactly-once external effects.

Lease fields are internal and excluded from public Run responses, events, and error messages. Add a
claim index shaped for the current concrete store:

```text
(status, lease_expires_at, created_at)
```

Migration rules:

These rules describe the original leasing migration. The current thread-serialization migration
adds a stricter drain requirement and rejects a nonterminal legacy Run whose checkpoint base is
unknown.

- existing `queued` rows with null lease fields are normally claimable;
- existing `running` rows with null lease fields are legacy interrupted work and immediately
  eligible for one fenced recovery claim;
- terminal rows remain terminal and do not receive a lease;
- all table-rebuild migrations must copy the new columns.

This migration cannot be deployed with old and new binaries active together. An old Manager ignores
lease columns, rewrites every `running` row at startup, and finalizes without a token. The first
upgrade therefore requires a stop-the-old-runtime boundary:

1. stop and verify exit of every old Runtime process;
2. run the additive migration;
3. start only lease-aware binaries.

Mixed-version rollback is also unsupported. A rollback must first drain or stop the lease-aware
Runtime and follow an explicit data-compatible procedure; simply starting the old binary would
remove the new guarantee.

## Store contract

The concrete `SQLiteRunStore` gains narrow primitives. This does not introduce another execution
backend abstraction.

```python
claim_next_run(
    *, owner_id: str, lease_duration_seconds: int
) -> RunLeaseClaim | None

renew_run_lease(
    run_id: str, *, lease_token: str, lease_duration_seconds: int
) -> bool

append_attempt_event(
    run_id: str, *, lease_token: str, event_type: str, payload: dict
) -> RunEvent

commit_completed_run(run: RunRecord, *, lease_token: str) -> RunCommitOutcome
commit_failed_run(
    run: RunRecord, *, lease_token: str, error_code: str, error: str
) -> RunCommitOutcome
commit_cancelled_run(
    run: RunRecord, *, reason: str, lease_token: str
) -> RunCommitOutcome

commit_reconciliation_pending(
    run_id: str, *, tenant_id: str, lease_token: str, error_code: str, error: str
) -> RunCommitOutcome

assert_current_run_lease(
    connection, run_id: str, *, lease_token: str
) -> None
```

`RunLeaseClaim` contains the Run, the opaque token, the current attempt, and a recovery reason. The
token is never placed in `RunRecord`'s public serialization.

`RunCommitOutcome` must distinguish at least:

- `committed`;
- `cancel_requested`;
- `lease_lost`;
- `already_terminal`.

The current boolean completion result is insufficient because a zero-row update can no longer be
assumed to mean cancellation. Treating lease loss as cancellation would itself be a stale write.

The Manager's failure path must stop using the broad `update_run` operation. Failure becomes a
fenced atomic terminal transaction, including `run.failed`, just like successful completion.

For SQLite, `claim_next_run` uses a short `BEGIN IMMEDIATE` transaction to serialize the candidate
selection and update across connections. The transaction is committed before Runtime code starts.
No database lock is held for the duration of execution. A future PostgreSQL implementation can map
the same semantics to a short `FOR UPDATE SKIP LOCKED` transaction without changing the safety
contract.

The ownership predicate must also be reusable inside the concrete `SQLiteWorkflowStore` and
`SQLiteMemoryStore` transactions. A separate check followed by a later mutation would be a
time-of-check/time-of-use gap. Managed mutations therefore carry the token into the transaction
that records a decision/replay result, changes workflow state, creates, recovers, completes, or fails
a workflow step, seals or changes attempt-owned memory, prepares an action, or starts/retries a
dispatch. A step token alone is insufficient because Run takeover and nested step-token rotation are
not one atomic transaction. The deliberate exception is durable acceptance of an already-authorized
provider outcome, which remains guarded by its dispatch and tool-attempt tokens. Standalone store
tests may use explicit fixtures, but a managed Run row with a missing token fails closed.

## Claim and takeover

This section describes claim behavior at the end of the leasing milestone. Current claim behavior
also enforces per-thread serialization and checkpoint-base requirements; the later design document
is authoritative for those additional constraints.

An eligible claim candidate is:

- `queued` with no pending cancellation; or
- `running` with no token, or with `lease_expires_at <= store_now`.

Candidates are ordered by `created_at, run_id` to keep selection deterministic. A competing claimant
may observe the same candidate, but only the conditional update winner receives a token.

For a fresh queued Run, the claim transaction:

1. sets `status = running`;
2. increments `attempt`;
3. writes a new owner, token, heartbeat, and expiry;
4. appends `run.started`.

For an expired or legacy running Run, the same transaction:

1. preserves cancellation and external-action reconciliation markers;
2. rotates owner and token;
3. increments `attempt`;
4. appends `run.recovered` with a non-secret reason;
5. appends `run.started`.

The recovered execution receives the existing recovery boolean so domain workflows replay persisted
decisions and inspect their ledgers. A later internal context field may carry the more precise reason
`lease_expired` or `legacy_unleased`; it must not change recovery behavior.

Queued cancellation is handled by a separate atomic store transition before claim selection. It
retains the existing contract that a Run cancelled before execution has no `run.started` event. A
running expired Run with cancellation is reclaimed because external-action evidence may need to be
reconciled before cancellation can safely win.

## Manager execution loop

The in-process queue stops carrying authoritative Run IDs. `submit` commits the Run and then signals
a condition/event. Workers also poll at a bounded interval, so a crash between commit and signal
cannot strand work.

```text
submit -> commit queued Run -> best-effort wake signal
                                 |
worker -> finalize queued cancellation if present
       -> atomically claim next Run
       -> register active lease with heartbeat controller
       -> execute persisted Runtime input
       -> fenced terminal/reconciliation commit
       -> remove active lease
       -> repeat, or wait for signal/poll timeout
```

`RuntimeManager.start()` still validates that all nonterminal Runs have registered Agents before it
starts workers. It no longer rewrites every `running` row. Live leases remain owned; only expired or
legacy-unleased work is claimable.

The heartbeat controller renews active leases at a configured interval shorter than the duration.
A normal starting point is a 30-second lease with a 10-second renewal interval; configuration must
enforce a meaningful safety margin. Lease-operation SQLite busy timeouts must be shorter than that
margin; the current general 30-second connection timeout cannot also be the heartbeat retry budget.
Polling can remain subsecond because it is only a local SQLite reference path.

A definite zero-row renewal marks local execution authority lost. A renewal exception makes
authority unconfirmed, so the Manager latches the heartbeat failure and checks that signal before
terminalization. The synchronous Runtime does not receive a cooperative stop signal in this slice;
any later nested or terminal mutation must re-prove the same token in its own store transaction. The
conditional store write remains authoritative, including for a race with the local signal check.

Clock movement changes lease timing: a forward jump may trigger early recovery, while a backward
jump may delay expiry and can make an unrotated deadline appear future again. Once takeover rotates
the token, however, no later clock value can make the old token current. The SQLite reference assumes
a reasonably maintained single-host wall clock; it does not claim a cross-host bounded-skew lease.

`RuntimeExecutionContext` carries the opaque execution token through Core-owned code, but the token
is never included in Planner context, tool arguments, public events, memory values, or provider
requests. The heartbeat controller's lost-lease signal is local to the Manager in this slice. A
synchronous Runtime may therefore continue computing until it reaches a fenced mutation or returns;
the in-transaction token predicate, not cooperative cancellation, is the safety guarantee.

## Shutdown

Shutdown stops new claims first. Heartbeats continue while active workers are given a bounded grace
period to finish. The Manager must not deliberately release a lease while its Runtime thread can
still execute, because that would manufacture concurrent ownership.

If the grace period expires, renewal stops and the lease expires naturally. Any late local result is
rejected by the same token/expiry predicate. Process termination remains the mechanism that actually
stops an uncooperative thread.

## Cancellation

Cancellation keeps its current precedence rules:

- a queued cancellation becomes terminal without acquiring a lease or appending `run.started`;
- a cancellation committed before successful completion prevents the completion transaction;
- only the current lease holder may finalize a running cancellation;
- a stale owner cannot complete, fail, or cancel the Run;
- external-action `outcome_unknown`, evidence-incomplete, and reconciliation requirements retain
  their safety precedence over ordinary cancellation.

If completion loses because cancellation was requested while the lease is still current, the typed
commit outcome tells the Manager to follow the cancellation path. If completion loses because the
token is stale or expired, the Manager discards the result and performs no cancellation write.

## External actions

Run leasing does not replace step-attempt or dispatch fencing.

Persisting authorization for a new consequential operation requires two kinds of authority:

- `prepare_external_action`, first dispatch, and retry dispatch require the current unexpired Run
  lease in the same transaction that records the transition;
- the current tool-attempt token continues to bind the action to the workflow step.

The database transaction cannot be atomic with the subsequent provider call. A worker can commit a
valid dispatch authorization, lose its Run lease, and still enter or complete the network call. That
window is expected. Prepare-before-dispatch evidence, provider idempotency declarations, dispatch
tokens, and reconciliation govern it; Run leasing must not claim to cancel an external effect.

Finalizing a provider response for a dispatch that was authorized before lease expiry is different.
Durable acceptance of that response remains guarded primarily by the dispatch and tool-attempt
tokens. A provider may
have accepted the request just before the Run lease expired; throwing away its valid success
evidence would not undo the effect and could make recovery less safe. If takeover has already rotated
the dispatch token or terminalized the action as `outcome_unknown`, the existing dispatch-token
compare-and-set rejects durable recording of the late response.

An `outcome_unknown` chosen locally by restart policy is not provider evidence. Unsafe-interrupted
closure, exhausted recovery, and routing or binding drift therefore validate the current Run lease
in the same transaction that terminalizes the action and tool step. This prevents an obsolete
recovery attempt from preempting the current owner's reconciliation or a valid late provider result.

The takeover path supplies recovery context to the existing dynamic loop and durable-action Runtime.
They inspect the authoritative workflow/action ledger:

- ledger-proven success is reused;
- a provider-idempotent ambiguous dispatch follows its bounded existing recovery rule;
- an unsafe ambiguous dispatch becomes `outcome_unknown` without another provider call;
- identity or capability drift remains fail closed.

Tests must prove that lease expiry cannot convert an uncertain external action into an ordinary safe
retry. Tokens and owner IDs must never be sent to a provider.

## Events and observability

Preserve the current public event vocabulary where possible:

- `run.started` identifies the new attempt;
- `run.recovered` records `lease_expired` or `legacy_unleased` without owner or token data;
- the existing terminal events remain authoritative;
- heartbeats produce no Run events.

Operational logs or future metrics should count fresh claims, takeovers, renewal failures, and stale
commit rejections. They must not expose lease tokens. No OpenTelemetry backend is introduced in this
milestone.

## Failure and race matrix

The first implementation is incomplete unless automated tests cover these cases:

| Window or race | Required outcome |
| --- | --- |
| Run commit succeeds but wake signal is lost | Polling eventually claims the durable row. |
| Two workers claim one queued Run | One claim and one `run.started`; the loser receives no token. |
| Manager B starts while Manager A has a live lease | B does not recover or execute the Run. |
| Heartbeats continue | Expiry advances and no takeover occurs. |
| Heartbeats arrive more frequently than required | Each deadline is based on current store time; duration does not accumulate. |
| Heartbeats stop | The Run becomes claimable at the exact expiry boundary. |
| Old owner renews at or after expiry | Renewal is rejected even before another claim. |
| New owner takes over | Attempt increments, token rotates, and recovery events occur once. |
| Old owner completes before expiry | Completion may win; no takeover can yet claim. |
| Old owner completes after expiry but before takeover | Completion is rejected and the expired Run remains recoverable. |
| Old owner completes or fails after takeover | No Run state, checkpoint, or event changes. |
| New owner completes, then old owner fails | Run remains completed; no `run.failed` is appended. |
| Cancel commits before a queued claim | Run cancels without `run.started`. |
| Cancel races with current completion | Exactly one terminal transition wins under existing precedence. |
| Cancel is present on an expired external-action Run | New owner reconciles before deciding cancellation. |
| Crash after claim but before Runtime call | Expiry permits a recovered attempt. |
| Crash after Runtime result but before terminal commit | No terminal evidence exists; expiry permits recovery. |
| Crash after terminal transaction commits | The Run stays terminal and is never reclaimed. |
| Provider result is ambiguous when lease expires | Existing reconciliation decides; no blind replay. |
| Old owner records evidence or claims/recovers/completes/fails a step after lease loss | The nested mutation transaction rejects the stale Run token. |
| Old owner requests a new prepare/begin/retry authorization after lease loss | The store transition is rejected. |
| Lease expires after dispatch authorization but before provider I/O | The call may still occur; prepare-before-dispatch evidence and reconciliation govern it. |
| Authorized provider response arrives after Run lease loss | Its durable result may finalize under the current dispatch token; an obsolete token is rejected. |
| Old owner tries to seal or mutate attempt-owned memory | The memory transaction rejects the stale Run token. |
| Renewal storage call errors | Execution authority becomes unconfirmed and terminal commit still fails closed. |
| Legacy `running` row has no token | It is recovered exactly once into a fenced attempt. |
| Graceful shutdown finishes within its window | Current worker commits normally and clears the lease. |
| Graceful shutdown times out | Lease is not released early; it expires and stale output is rejected. |

Tests should use direct store-controlled expiry rather than long sleeps. At least one end-to-end test
must use two distinct `SQLiteRunStore` and `RuntimeManager` instances against the same file so an
in-process mutex cannot make the race proof vacuous.

## Implemented slices

### Slice 1: store ownership contract

- add schema migration, internal models, claim result, and typed commit outcomes;
- implement atomic claim, renew, expiry boundary, and stale-token rejection;
- make completed, failed, cancelled, and reconciliation writes token-aware and atomic;
- make managed workflow, memory, and dispatch-start mutations validate Run authority in their own
  transactions;
- add migration, store-clock, and two-connection race tests.

### Slice 2: durable Manager dispatch

- replace authoritative queue items with store claim plus best-effort wake/poll;
- add worker identity, active-lease tracking, heartbeat, and graceful shutdown;
- stop startup from resetting live `running` Runs;
- preserve registration preflight and queued-cancellation behavior;
- add two-Manager overlap and stale-result tests.

### Slice 3: recovery integration

- propagate takeover recovery context and fence every managed durable mutation;
- prove release-validation, dynamic-loop, memory, and Action gateway behavior;
- add the uncertain-external-action and cancellation race matrix;
- update deployment limitations without claiming horizontal scalability.

These are review slices, not independent dormant features. The branch should not merge until the
end-to-end Manager path actually uses the store contract.

## Acceptance criteria

The milestone is complete when:

- an overlapping Manager cannot steal a live unexpired Run;
- an expired Run is recovered without requiring a whole application restart;
- stale heartbeat, completion, failure, cancellation, checkpoint, and attempt-event writes are
  rejected by the store;
- stale workers cannot record decisions/replay evidence, transition workflows, mutate ordinary
  steps or attempt-owned memory, or obtain new prepare/dispatch/retry authorization;
- provider evidence from an already-authorized dispatch remains governed by its dispatch token;
- a stale worker cannot create contradictory terminal evidence;
- losing the in-memory wake signal cannot strand committed work;
- external-action recovery and cancellation precedence remain unchanged;
- legacy databases migrate without losing Run, event, checkpoint, memory, workflow, or action data;
- the full Python 3.11/3.12 CI suite remains green;
- documentation still states that SQLite is a single-host reference boundary, not a horizontally
  scalable production execution plane.

## Follow-up boundaries

Once these semantics are proven, a PostgreSQL store can implement claim selection with
`FOR UPDATE SKIP LOCKED`, and Redis or Pub/Sub can be added as a wake-up channel. Those components
must preserve the same token predicates and store-authoritative expiry rules. A queue delivery or a
leader lease alone is never proof of current execution authority.

Per-thread serialization remains a separate layer from Run leasing and is now specified and
implemented by [`thread-execution-serialization.md`](thread-execution-serialization.md). It reuses
the current Run lease as tenant-qualified thread ownership and adds checkpoint revision
compare-and-set; Run leasing itself does not imply either guarantee.
