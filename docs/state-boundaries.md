# Persisted state boundaries

This document defines the persisted state boundaries after the narrow checkpoint-trace projection
was introduced. The Travel and other domain schemas remain unchanged. Run completion still uses
the existing lease fence, checkpoint revision compare-and-swap, and one transaction; the change is
that the Run row retains the full validated result while the Thread checkpoint receives a
deterministic projection with `execution_trace=[]`.

The accompanying evaluator is [`eval/state_boundary.py`](../eval/state_boundary.py). Its committed
output is [`eval/results/state_boundary_latest.json`](../eval/results/state_boundary_latest.json).

## Boundary table

| Boundary | Scope | Owner | Durable lifetime | Replay/read source | Consistency and authority | Production mutation path |
| --- | --- | --- | --- | --- | --- | --- |
| Run control | One `run_id`, tenant-qualified; scheduling additionally uses `(tenant_id, thread_id)` | `RuntimeManager` and `SQLiteRunStore` | Submission through terminal Run retention; a nonterminal expired lease remains recoverable | `runs` row plus ordered `run_events` | Store-time lease deadline, current `lease_token`, status/cancellation CAS, and one-running-Run-per-thread constraint fence managed mutations | submit/create, `claim_next_run`, heartbeat renewal, cancellation CAS, and fenced completion/failure/cancellation methods |
| Workflow ledger | Run, workflow execution, step/tool attempt, and external action | `DynamicToolLoop`, `ExternalActionCoordinator`, and `WorkflowStore` | Durable execution and external-effect evidence retained after Run terminalization | `workflow_executions`, `tool_calls`, `external_actions`, and `workflow_events`; `read_run_snapshot` gives one read transaction | Stable workflow/step/action identity; attempt and dispatch tokens fence stale results; the managed path also supplies the current Run lease token | `WorkflowStore` transition methods atomically update their row and workflow event; external-action finalization also binds the parent tool result |
| Event history | Per Run, split between public Run events and internal workflow events | Run store/event sink and workflow store; `EvidenceProjector` mirrors workflow facts | Append-only evidence for the retained Run | `run_events` ordered by per-Run sequence; `workflow_events` ordered separately | Unique ordered sequence per stream. Workflow/action rows are authoritative before their public Run-event mirror; not every cross-store mirror is one transaction | Run atomic transition methods, attempt-fenced append, workflow atomic transitions, and evidence projection/repair |
| Domain checkpoint | One `(tenant_id, thread_id)` with one pinned `domain_id` and `schema_version` | Domain Runtime produces typed state; `RuntimeManager` validates it; `SQLiteRunStore` projects and commits it | Latest cross-turn state until the next successful managed completion replaces it | Latest `thread_states.state_json`; completed Run rows retain the full historical result state | Same-thread serialization, captured base revision, revision CAS at load and completion, current Run lease fencing, and a store-enforced trace projection | First seed only on an empty thread; otherwise only the atomic successful completion path writes `execution_trace=[]` and increments revision |
| Governed memory | Logical `(tenant_id, subject_id, domain_id, kind, key)` | `GovernedMemory`, `MemoryStore`, and the domain memory policy | Cross-thread version history until operational forgetting; expiry may hide an active record | `memory_records` versions and `memory_events` mutation audit | At most one active logical version; different values supersede in one transaction; tenant/subject scope and persisted Run permissions fail closed | Allowlisted extraction followed by `remember`/`upsert_from_run`; administrative APIs list or tombstone the subject's logical key |
| Sealed memory snapshot | One Run, bound to its persisted tenant, subject, and domain | `GovernedMemory` and `MemoryStore` | Immutable Run evidence, including an explicitly empty retrieval | `run_memory_snapshots` | First get-or-create seals the view. Managed retrieval requires the current Run lease and validates persisted Run authority; later memory writes or deletion do not rewrite it | First memory retrieval at the Run boundary; retries and recovered attempts reuse the existing row |

Important qualifications:

- `runs.state_json` receives the full validated final state, including that Run's
  `execution_trace`. `thread_states.state_json` receives the same concrete state type and all the
  same non-trace fields, but persists `execution_trace=[]` for the next Run.
- `run_events` are not the authority for an external action when their mirror is incomplete. The
  workflow/action ledger remains authoritative and drives reconciliation.
- A checkpoint is a latest-state projection, not an append-only history. Revision CAS proves which
  predecessor it was derived from; Run/workflow events and each completed Run's full state retain
  execution evidence without copying prior trace prefixes into mutable Thread state.

Legacy checkpoints with non-empty traces remain readable. They are not rewritten at startup and
their revision does not change merely because the binary was upgraded. The next Run receives the
legacy checkpoint unchanged; only its next successful completion stores an empty checkpoint trace.
Failure or cancellation leaves the legacy row and revision untouched. The following successor then
loads the compacted checkpoint. No schema-version bump or table migration is required.

## Travel field classification

The classifications below describe current enforced reads and writes, not a broader target schema.

| Field | Characterization | Current evidence and remaining gap |
| --- | --- | --- |
| `destination`, `days`, `budget` | Authoritative durable task state for the current thread | Travel planning and validation read them on later turns. The governed-memory allowlist explicitly excludes them. Their lifetime beyond the current task is a product question, but no competing mutable authority exists today. |
| `preferences` | Mixed by Agent version and key | Memory-free versions persist Travel preferences in the checkpoint. In `1.1.0`/`1.2.0`, the three governed keys are a Run overlay from subject memory and are restored to their pre-overlay checkpoint values before completion. Non-governed preference keys can still be checkpoint state. Treating the whole dictionary as either memory or checkpoint authority would be inaccurate. |
| `itinerary` | Derived but needed across turns | It is regenerated from task constraints or tool evidence, yet `confirm_plan` consumes the current checkpoint itinerary. The final Validator uses `itinerary.total_cost`, days, flight type, and destination. |
| `tool_outputs` | Mixed/unclear semantics | In `0.5.0`, `cost_breakdown` is read on a later `confirm_plan` turn by `PlanEvidenceBuilder` as a complete cost ledger. If absent, the existing builder falls back to `itinerary.total_cost`, so the breakdown is enhanced evidence rather than the only admissible source. In dynamic `1.x`, `_prepare_state` clears this field at the start of each Run and the completed state stores a projection of that Run's tool observations while the workflow ledger remains durable execution authority. The whole field cannot be classified as only cache or only cross-turn state. |
| `blockers` | Derived current-thread projection | Validation or clarification writes it and later actionable input clears/replaces it. It affects the current response/stage, but no recovery path uses it as tool/action authority. Whether older blockers should remain available belongs to history/evidence, not yet a proven checkpoint requirement. |
| `current_stage` | Derived but needed across turns and API reads | It is the current domain outcome projection (`planning`, `planned`, `needs_repair`, and so on). Dynamic follow-ups overwrite it. It is not the Run status and must not be used as Run-control authority. |
| `execution_trace` | Run-scoped final-state evidence; projected out of the next checkpoint | Production code appends current execution observations, and the completed Run row retains them. `SQLiteRunStore.commit_completed_run()` enforces a same-type, deep-independent checkpoint projection with an empty trace after Runtime execution and validation. Existing event ledgers remain unchanged, but they are not claimed to reconstruct every `TraceEvent` payload exactly. |
| `retry_count` | Authoritative durable state in current behavior; semantic scope is unclear | `TravelAgentRuntime` reads it against `retry_limit`, increments it on validation failure, persists it in the checkpoint, and never resets it after a successful repair. The eval proves it therefore behaves as a thread-level cumulative counter across independent Runs, while `RunRecord.attempt` separately remains per Run. No published product contract establishes whether the cumulative behavior is intended. |

## Characterization method

The evaluator uses only repository-owned deterministic paths:

- `ScriptedTravelPlanner`, the production Travel ToolRegistry, and the offline synthetic Travel
  handlers through an evaluator-only direct adapter (process sandbox timing is not a state-boundary
  variable);
- real `RuntimeManager`, Run lease, checkpoint load/commit, and revision increments;
- real `SQLiteWorkflowStore`, `SQLiteMemoryStore`, governed-memory policy, and Travel parsers;
- a deterministic review probe that calls the production `PlanEvidenceBuilder` but replaces review
  UUIDs and timing with fixed values, because those values are irrelevant to the state-boundary
  question;
- fixed English inputs, no network, no live LLM, and no comparison of real timestamps.

Run it with:

```bash
python -m eval.state_boundary --output eval/results/state_boundary_latest.json
pytest -q tests/characterization/test_state_boundary_eval.py
```

The pytest verification regenerates the report in a temporary directory and applies semantic
invariants. The committed JSON remains an inspectable evidence artifact, not a byte-for-byte golden
fixture. Checkpoint size remains observational: there is deliberately no maximum-byte CI threshold.

## Results

### Memory supersession

An explicit `avoid_red_eye=true` write created version 1. A later explicit
`avoid_red_eye=false` write superseded version 1 and created active version 2. A new Run without an
explicit preference sealed and applied version 2, producing a `red_eye` itinerary. Its committed
checkpoint contained no governed preference key, which is the expected overlay-removal behavior.

For this fresh `1.1.0` thread, the eval found only one mutable persisted value for that governed
logical key: active memory. The checkpoint stores the thread task, while the Run snapshot stores
immutable evidence of exactly which memory version influenced the Run. This does not erase the
code-level checkpoint fallback: a checkpoint value created by a memory-free version can still be
the third-precedence source on a later memory-aware Run. That cross-version case was not added to
this eval and is not claimed away.

### Sealed snapshot

A Run snapshot sealed with version 1 remained byte-for-byte semantically unchanged after a later
Run created version 2. A subsequent new Run retrieved version 2. The old snapshot is therefore
Run evidence, not a mutable alias of current memory.

### Budget replacement

On one memory-aware thread:

| Observation | Before | After explicit replacement |
| --- | ---: | ---: |
| Checkpoint revision | 1 | 2 |
| Checkpoint budget | 9000 | 12000 |
| Checkpoint governed preferences | `{}` | `{}` |
| Active memory | `flight.avoid_red_eye=true`, v1 | unchanged |

The replacement Run loaded revision 1 from `thread_store`, persisted budget 12000 at revision 2,
and did not create, supersede, or delete memory. Budget is checkpoint task authority, not governed
memory.

### Negation and anti-trap safety

The existing strict parser and real memory-aware runtime produced no memory mutation for:

- `Do you offer red-eye flights?`
- `Tell me about a hotel near subway.`
- `What does a relaxed travel style mean?`

The existing negative-intent cases persisted typed values in the correct direction:

| Text | Persisted logical value |
| --- | --- |
| `I do not mind red-eye flights.` | `flight.avoid_red_eye=false` |
| `I do not want a hotel near subway.` | `hotel.near_subway=false` |
| `I prefer NOT a relaxed travel style.` | `travel.style="balanced"` |

No alternate eval parser was introduced.

### Cross-turn `confirm_plan`

Turn 1 persisted an itinerary total of 7300, budget 9000, and a three-part
`tool_outputs.cost_breakdown` at checkpoint revision 1. Turn 2 loaded that exact revision from the
thread store and detected `confirm_plan`. The review path observed:

```text
candidate plan present
cost ledger status = complete
cost ledger total = 7300
cost source = tool_outputs.cost_breakdown
final validation errors = []
```

The actual dependency is narrower than the whole `tool_outputs` dictionary:

- the current `itinerary` and `budget` are required current-plan state;
- the review evidence builder reads `cost_breakdown` when available;
- the final Validator independently gates `itinerary.total_cost` against `budget`;
- the builder already has a tested plan-total fallback when the breakdown is absent.

Therefore this eval proves that the cost projection is used across turns, but it does not prove
that every tool output must remain in the checkpoint forever.

### Checkpoint growth

Size is the UTF-8 byte length of the exact compact persisted JSON. Checkpoint and Run-result trace
columns are measured separately; `other` includes all non-trace/non-`tool_outputs` checkpoint
values plus JSON keys and punctuation. No persisted row was altered to calculate the breakdown.

| Observation | Characterized result |
| --- | --- |
| Total checkpoint size | 523 bytes on turn 1 and 608 on turn 8; intermediate growth was positive, negative, or zero |
| Checkpoint `execution_trace` | Empty on all eight successful revisions; serialized value remained 2 bytes (`[]`) |
| Run-result `execution_trace` | Non-empty on all eight completed Runs |
| `checkpoint.saved` counts | `trace_events` matched the persisted checkpoint and `run_trace_events` matched the full Run result on every turn |
| `tool_outputs` contribution | Remained constant across the scenario |
| Field accounting | Total bytes equal trace value + tool-output value + other state/JSON structure on every turn |

The exact per-turn byte measurements remain in the committed JSON evidence. They prove that
cumulative trace prefixes are no longer a checkpoint growth source while per-Run trace evidence is
retained. They do not prove that checkpoint size is globally bounded, that other fields cannot
grow, or that every derived field should be projected out.

### `retry_count` scope

Five independent Runs on one thread produced:

| Turn | Durable Run `attempt` | Checkpoint `retry_count` | Stage after completion |
| ---: | ---: | ---: | --- |
| 1 | 1 | 1 | `needs_repair` |
| 2 | 1 | 1 | `planned` |
| 3 | 1 | 2 | `needs_repair` |
| 4 | 1 | 2 | `planned` |
| 5 | 1 | 2 | `blocked` |

Successful repair Runs did not reset the counter. Current behavior is therefore thread-scoped,
not per Run. This is not labeled a correctness bug here because no product contract says whether
`retry_limit` is intended to cap one Run, one repair episode, or the entire thread. The behavioral
impact and a minimal reproduction are now explicit for a later narrow decision.

## Answers and recommendation

1. **Does `execution_trace` still cause monotonic checkpoint growth?** No in the characterized
   eight-turn managed path. Every persisted checkpoint trace is `[]`, while every completed Run
   retains a non-empty current-Run trace.
2. **What is `tool_outputs`?** Mixed. `0.5.0` reads a current-plan cost projection across turns;
   dynamic `1.x` replaces it with the current Run's observation projection while the workflow
   ledger owns durable execution evidence. It is neither only a temporary cache nor one uniform
   permanent authority.
3. **How does `retry_count` behave?** It is thread-scoped today. Successful Runs do not reset it,
   and its limit is distinct from the per-Run lease/recovery `attempt` counter. Whether it should
   move requires product semantics that this evidence line cannot invent.
4. **Do memory and checkpoint duplicate mutable authority?** The fresh memory-aware path did not
   duplicate its governed write into the checkpoint, and the sealed snapshot duplicates only
   immutable historical evidence. The broader code boundary still permits a checkpoint preference
   as the documented third-precedence fallback, especially when a thread previously ran a
   memory-free version. That is intentional layered overlap with explicit precedence, not proof of
   two coequal authorities and not proof that overlap can never become stale. The `preferences`
   container therefore remains version- and key-sensitive.
5. **What cleanup is enforced?** Only the narrow `execution_trace` projection. It does **not**
   project away `tool_outputs`, move `retry_count`, change governed memory, or change the schema.

Splitting `tool_outputs` may become justified later, but its 92-byte value was flat in this scenario
and the exact per-version consumer contract is not characterized enough for that change. Moving
`retry_count` also remains premature until the intended retry episode is defined.
