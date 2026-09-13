# Governed Graph Mutation — feasibility review

**Status: design review only. Nothing described here is implemented, and no claim in this document
is backed by executable evidence in this repository.** It reviews the Governed Graph Mutation
proposal against the code at `ad28ada` and answers the feasibility questions the proposal raises.

## 1. Verdict

The proposal is technically sound and its central principle — the Agent proposes, the Runtime
decides — is the right one and already matches how this repository separates typed decisions from
durable execution. Three findings change the plan materially:

1. **The capability has no host yet.** Graph execution and Planner decisions live in two disjoint
   subsystems that share the durable store and nothing else. There is no component today in which
   an Agent executes a graph, so there is nothing for a `PROPOSE_GRAPH_MUTATION` decision to mutate.
   Building that bridge is the largest single cost in the proposal and it is not in the phase plan.
2. **Mutation should fork, not edit in place.** The runtime's existing selective replay is already
   a fork: it creates a new run that inherits reusable evidence from a terminal parent. Modelling a
   committed mutation the same way satisfies nine of the ten success criteria using mechanisms that
   already exist and are already tested, and deletes the hardest parts of the proposal (partial
   graph transitions, RUNNING-node mutation, erasure of historical side effects) rather than
   solving them.
3. **The demo scenario does not require the capability.** The `choose_provider → fallback_provider`
   example in the proposal is dynamic routing expressed as mutation; it is representable as a
   static graph with a conditional edge, which LangGraph already does. The ablation would measure
   nothing. A scenario where the *set of node identities is not enumerable before the run* is
   needed instead.

Recommended next step is not Phase 1 as written. It is a narrower Phase 0 that settles the value
question (§9) before any durable schema is designed.

## 2. What the runtime provides today

Facts from the current tree, since the proposal's premises depend on them.

**Graph topology exists, and it is a deployment-time constant.** `WorkflowDag`
(`runtime_service/dag.py:18`) validates node ids, duplicate and self dependencies, unknown
dependencies and cycles, and exposes a deterministic `topological_order`, `dependencies_for()` and
`descendants()`. It has exactly one caller: `RELEASE_VALIDATION_DAG`
(`domains/release_validation/runtime.py:245`), a module-level constant built at import time from
`STEP_DEFINITIONS`.

**Node definitions carry Python code, not data.** A `StepDefinition`
(`domains/release_validation/runtime.py`) holds `build_arguments`, a callable that computes a
node's tool arguments from the manifest and upstream results. A node is therefore not serializable
today.

**Selective replay is a fork and never an in-place edit.** `reuse_completed_step`
(`runtime_service/workflow_store.py:807`) rejects `source_run_id == target_run_id`, requires the
source execution to be READY or BLOCKED and the target to be RUNNING, and copies a completed
`tool_calls` row into the target only when tool name and argument hash both match.
`docs/release-validation-workflow.md` states the invariant directly: "selective replay never
mutates an existing run in place."

**Execution identity is immutable with respect to its input.** `create_or_get_execution`
(`runtime_service/workflow_store.py:599`) re-reads a conflicting row and returns an explicit
`INPUT_MISMATCH` rather than accepting a changed `input_hash`. A replay child already folds its
replay directive into that identity hash, so a run whose plan differs is a different run by
construction.

**Side-effect classification is already static.** `ToolSpec.effect`
(`runtime_service/sandbox.py:95`) is `READ_ONLY` or `EXTERNAL_WRITE`, fixed at registration, and
`ToolRegistry.register` rejects incoherent combinations (`sandbox.py:116`, `:128`). Whether a node
can write externally is answerable from the registry alone, with no execution state.

**Completed external effects are already un-copyable.** `reuse_completed_step` refuses to reuse any
step that has an `external_actions` row, with the reasoning in-line at
`runtime_service/workflow_store.py:888`: copying the tool result alone would make the target run
look like it owned a side effect it never performed, and copying the action would make two runs
share one provider idempotency identity.

**Planner-driven execution has no topology at all.** `DynamicToolLoop`
(`runtime_service/dynamic_loop.py`) indexes durable steps as `step_id = f"call-{decision_index:04d}"`
(`dynamic_loop.py:394`) — a monotonic counter. It owns typed decision validation, policy order,
external-action dispatch and failure codes, and knows nothing about dependencies, readiness or
graphs.

**Concurrency is already fenced at three levels.** One RUNNING run per thread
(`idx_runs_one_running_per_thread`, `runtime_service/store.py:325`), run leases with fencing
tokens asserted on every workflow-store mutation (`_assert_current_run_lease`,
`workflow_store.py:618` and elsewhere), and thread checkpoint revision compare-and-swap.

## 3. The decisive finding: the two halves do not meet

| | Graph path | Planner path |
| --- | --- | --- |
| Where | `domains/release_validation/` | `runtime_service/dynamic_loop.py` |
| Topology | validated DAG, fixed at import | none; linear call index |
| Who chooses the next step | deterministic topological scheduler | Planner, via typed decision |
| Step identity | `(run_id, step_id)` from the DAG | `(run_id, "call-NNNN")` |
| External writes | none — every `release_validation` tool is `READ_ONLY` | full external-action ledger |
| Replay | selective, by fork into a new run | cache reuse within one run |

The proposal's Phase 5 ("expose mutation as a typed Planner decision") assumes a runtime where a
Planner is executing a graph. That runtime does not exist. Creating it means giving the dynamic
loop a dependency-aware scheduler and DAG-derived step ids, or giving the graph workflow a Planner
— either way a new execution path with its own recovery, cancellation and lease semantics, and its
own conformance coverage across both store backends.

That work is worth roughly as much as all of Phases 1–4 combined, and it is *prerequisite* to the
only phase that demonstrates the proposal's thesis. It belongs at the front of the plan, scoped
explicitly, or the plan should stop at Phase 4 and claim only deterministic mutation.

Two secondary consequences:

- Phases 1–4 cannot exercise §15 at all. The only DAG domain has zero `EXTERNAL_WRITE` tools, so
  the side-effect boundary is untestable until a new fixture domain exists. Phase 7 needs a domain
  built for it, not a bolt-on to `release_validation`.
- `release_validation` should not be modified. It is pinned at `1.0.0`/`1.1.0` for durable-run
  recovery, is covered by `tests/test_public_contract.py` and a proof document, and its fixed-order
  legacy path exists precisely so old runs stay replayable. Mutation belongs in a new domain.

## 4. Phase 0 — prior art and where the gap actually is

| System | Runtime topology change | Durable versioned topology | Recovery across a change | Side-effect anchoring |
| --- | --- | --- | --- | --- |
| LangGraph | No. A compiled graph is immutable; dynamic behaviour is conditional edges, `Send`/`Command` within a predefined graph, or rebuilding a `StateGraph` before a run. Checkpoints persist state per super-step, not topology as a versioned entity. | No | State resumes; the graph is assumed unchanged | Caller's responsibility; nodes must be engineered as replay boundaries |
| AutoGen GraphFlow | No. The `DiGraph` is fixed at team construction. | No | n/a | n/a |
| Airflow / Dagster | Structure is fixed at parse time. Dynamic task mapping varies *width* from upstream data, not the graph. | Per DAG-run, not per mutation | Per task instance | Caller's responsibility |
| Temporal | Execution path can branch, but replay determinism means changing an in-flight execution requires `patched()` or Worker Versioning — a code-level, human-authored branch, not a proposed structural change. | Code version, not a graph object | Strong: the whole point | Activities + idempotency; no notion of "this node's effect anchors the topology" |
| ATM (arXiv 2607.20488) | Yes, and closest to this proposal: telemetry-triggered agent-team mutation gated on capability monotonicity, routing completeness, and shadow-before-live validation (the proposal's dry-run, independently arrived at). | Not described | Not described | Not described |
| This runtime today | No | No | Strong, per run | Strong: prepare-before-dispatch, idempotency keys, `outcome_unknown`, non-copyable completed actions |

Reading of the gap: **runtime topology change is not novel; durable, recoverable, side-effect-safe
topology change is.** Every system above either treats topology as a compile-time artifact
(LangGraph, AutoGen, Airflow, Dagster), or makes in-flight change a human-authored code-versioning
problem (Temporal), or performs runtime mutation without addressing durability, crash recovery or
committed external effects (ATM). The defensible contribution is the second half of the proposal's
own title — governed — and specifically: what happens to a topology change when the process dies
mid-commit, and what happens to a node that already charged a customer.

That also sets the bar. The contribution is not "an Agent can change its graph." It is "a topology
change is a durable, versioned, idempotent, recoverable transition that cannot erase a committed
effect." Everything in the plan that does not serve that sentence is engineering integration.

## 5. Recommended design change: mutation as a governed fork

The proposal assumes mutation edits the active run's topology in place, and then spends §10, §15 and
§16 on the consequences. The alternative is to make a committed mutation produce a **child run at a
new graph version**, exactly as selective replay already produces a replay child.

```text
run A  (graph v1)   ── executes ──▶ terminal, evidence immutable
   │
   │  mutation M1 proposed, validated, dry-run, accepted
   ▼
run B  (graph v2, parent=A, mutation=M1)
   └─ inherits reusable completed steps from A via reuse_completed_step
   └─ executes only what v2 adds or invalidates
```

What this buys, against the proposal's own success criteria:

| Criterion | In-place mutation | Fork |
| --- | --- | --- |
| 4. Immutable new graph version | new table + CAS + transactional commit | the child run's identity hash *is* the version |
| 5. Historical execution preserved | must separate "historical record" from "current topology" in one run | free: run A is untouched and terminal |
| 6. Deterministic invalidation and replay | new mechanism | `WorkflowDag.descendants()` + `reuse_completed_step`, both already tested |
| 7. Restart resumes at the authoritative version | needs an atomic commit boundary and a recovery rule | `create_or_get_execution` already resolves create-or-get idempotently |
| 8. Duplicate application impossible | needs a transactional one-mutation-one-transition invariant | a uniqueness constraint on `(parent_run_id, mutation_id) → child run_id` |
| 9. Side effects cannot be erased | needs the "historical anchor" rule from §15 | already enforced: `workflow_store.py:888` refuses to copy any step with an external action |

The §16 crash matrix mostly evaporates. There is no partial graph transition, because there is no
graph transition — there is a row that either exists or does not, created through a code path that
already survives concurrent creation and restart.

The cost is honest and should be stated: a fork cannot change the topology of a run that is still
executing. The proposal asks for mutation "during an active workflow." The bridge is a clean stop
at a node boundary — the executor finalizes the parent run with a terminal status carrying a
`mutation_requested` reason, and the mutation is committed as the child. From the workflow's point
of view the plan changed mid-flight; from the store's point of view every run is still append-only
and immutable in its identity. Note that the current graph executor runs an entire DAG inside one
synchronous `run()` call holding the lease, so a genuine mid-run pause would have to be built
regardless.

If the research goal specifically requires proving in-place transition semantics, that is a
defensible choice — but it should be a stated research objective, not an incidental design default,
because it costs a new durable entity, a new CAS, a new recovery rule and dual-backend conformance.

## 6. Answers to the feasibility questions

### Architecture

**Does it fit?** The validation, dry-run and evidence half fits naturally — it is the same shape as
`quarantine_resolution` (dry-run plan, stale-plan rejection, apply, evidence) and as the typed
Planner-decision contract. The execution half does not fit yet, for the reason in §3.

**WorkflowStore or a separate GraphStore?** Neither, initially. Put the graph in the run's identity
and in `workflow_events`, and keep the mutation engine as a pure in-memory module
(`runtime_service/graph/`) with no persistence of its own. Reasons: a new `WorkflowStore` method
must be implemented twice (SQLite and PostgreSQL), pass `tests/conformance/test_workflow_store_contract.py`
and the execution-plane contract, and — decisively — new execution-plane tables require bumping
`POSTGRES_SCHEMA_VERSION` (`runtime_service/postgres_schema.py:14`), which makes every existing
deployment fail bootstrap with `PostgreSQL execution-plane schema version is incompatible`
(`postgres_schema.py:242`). There is no migration framework; the bootstrap is deliberately a single
reviewable install. Deferring persistence until the semantics are settled avoids paying that once
per design revision.

If a durable entity does become necessary, one table (`graph_versions`, keyed by
`(run_id, version)`, holding a canonical snapshot and its hash) is enough. Do not normalize nodes
and edges; the proposal's own instinct here is right.

**Snapshot or normalized?** Snapshot. `runtime_service/canonical.py` already provides the exact
primitives — `canonical_json` (sorted keys, tight separators) and `stable_hash` (SHA-256 over that
encoding) — and the repository already treats byte equality of that encoding as identity equality.
A graph version hash should reuse them rather than introduce a second canonicalization.

### Mutation semantics

**Are the four operations sufficient?** `ADD_NODE`, `ADD_EDGE` and `REMOVE_EDGE` are sufficient and
minimal. Drop `REPLACE_SUBGRAPH` from Phase 1: it is not a primitive but a macro whose validation
surface is the union of the others plus boundary rewiring, and it is the one operation that can
silently orphan completed evidence. Anything it expresses can be expressed as a bounded sequence of
the three primitives, which is also easier to audit.

**A constraint the proposal does not state:** `ADD_NODE` cannot carry code. Today a node is a
`StepDefinition` holding a Python callable. A proposed node can only ever name a **pre-registered
node type** plus a declarative config reference, resolved by the runtime against a registry. This
is worth stating as a first-class invariant, because it makes "no arbitrary code generation"
structurally impossible rather than policy-enforced, and it is the cleanest defence against the
runaway behaviour §19 worries about.

**Explicitly unsupported, and worth naming in the failure codes:** `REMOVE_NODE`, `MOVE_NODE`,
changing an existing node's type or config (a silent cache invalidation disguised as an edit),
changing dependency semantics (all-of versus any-of), introducing a node type not in the registry,
and any operation touching a node with a completed external action.

### Execution

**Mutation touching a RUNNING node?** Reject in Phase 1, and say so in a stable failure code. Under
the fork model the question does not arise, since the parent run is terminal before the fork
commits — which is itself an argument for the fork model.

**Invalidation boundary.** Already solved: `WorkflowDag.descendants()` (`dag.py:78`) computes the
transitive dependent closure, and `release_validation` already uses it for exactly this, including
the cascade when a reuse attempt fails (`runtime.py:445`). The mutation-specific rule is only which
roots to seed it with: every node whose incoming edge set changed, plus every node added, plus
every node whose upstream results changed. Everything downstream follows mechanically.

### Concurrency

**Is graph-version CAS enough?** It is more than enough, and it is not the load-bearing mechanism.
Two workers cannot concurrently mutate one run's topology because they cannot both hold its lease —
every workflow-store mutation already asserts the current lease token, and one thread can have only
one RUNNING run. Keep the CAS as cheap defence in depth and as a clear failure code for a stale
Agent proposal, but do not design around it as the primary safety property; the primary property is
already there.

**Interaction with leases and thread serialization.** Under the fork model, none that is new: the
child run is submitted through the existing API and claims its own lease through `RuntimeManager`.
Under in-place mutation, the graph version must become a fenced, lease-asserted write like every
other, and the commit must be in the same transaction as the event append.

### Recovery

**Smallest atomic boundary.** One transaction that both persists the new authoritative version and
appends its `graph.mutation.committed` event, with the mutation id carried in the row so the write
is idempotent by key. This is the same pattern `_finalize_execution` already uses for
status-transition-plus-event. Under the fork model the boundary is the child run's creation, which
already has these properties.

**Most dangerous crash points**, in order: after validation and before commit (the only window that
can lose a decision — survivable because a lost proposal is re-proposable, never half-applied);
after commit and before replay scheduling (must be recoverable from the persisted version alone,
never from in-memory analysis — so persist the invalidation set or make it a pure function of the
graph version, preferably the latter); and the one the proposal under-weights, **after commit and
during execution of a new node that performs an external write**, where the existing
prepare-before-dispatch ledger and `outcome_unknown` semantics are what actually protect you.

### Side effects

**Is the historical-anchor rule sufficient for Phase 1?** Yes, and the runtime already implements
it in the only place it currently matters. Two additional invariants are needed:

1. A proposal may not add a node whose type is `EXTERNAL_WRITE` into a position whose inputs derive
   from invalidated results — otherwise a replay re-derives arguments for an effect that was already
   dispatched under different ones. Statically checkable from `ToolSpec.effect`.
2. A node with a `PREPARED` or `DISPATCHING` external action is not merely un-mutable, it is
   *unknown*: it must be reconciled to a terminal status before any mutation is validated.
   `has_external_action_requiring_reconciliation` already answers this per run.

### Scope

**Small enough for this repository?** Phases 1–2 are: yes, comfortably, if they add no store method
and no schema. Phases 3–5 are not small, for the §3 reason.

**Separate repository?** No. The value is entirely in the durability and side-effect semantics,
which only exist here. Extracted, it becomes another graph-mutation prototype of the kind §4 already
finds unpersuasive.

**Seams to use.** A new `runtime_service/graph/` package for the pure mutation engine, depending on
`dag.py` and `canonical.py` and nothing else. A new domain under `domains/` registered through the
existing trusted extension seam (`runtime_service/extensions.py`) — note that
`RuntimeExtensionContext` currently exposes `registry`, `workflow_store` and `run_event_sink`, so a
graph-aware domain either composes its own graph services or the context gains one field. Do not
touch `dag.py`'s existing behaviour, `release_validation`, or any `WorkflowStore` signature until
Phase 4 forces it. Add the new module to the `mypy` `files` list in `pyproject.toml` and keep it
inside the substrate boundary enforced by `scripts/check_substrate_contract.py`.

### Research value

**Is there a gap?** Yes, but not where the proposal places it. See §4: the gap is durable,
recoverable, side-effect-safe topology transition, not topology change as such.

**Engineering integration versus genuine runtime semantics.** Genuinely interesting: the atomic
commit boundary and its crash matrix; the rule that decides which historical results survive a
topology change; the interaction between an invalidation boundary and a committed external effect;
and whether a mutation is representable as a fork without losing expressive power. Everything else —
schema validation, cycle detection, reachability, node registries, resource limits — is
well-understood engineering that this repository can do in a week and that no reviewer will find
novel.

**The single most decisive experiment.** Not the one in §21. That scenario is a conditional edge,
and a static graph with a branch node completes it identically, which means the ablation's
configurations B and D would be indistinguishable. Use instead a task where **the node identities
are not enumerable before the run**: evidence discovered at runtime reveals N sub-items, each
needing its own durable node identity, its own retry budget, and — critically — its own external
write. Then kill the worker midway and prove that the recovered run re-derives exactly the same
graph version, re-executes exactly the un-committed nodes, and re-dispatches exactly zero committed
effects. That experiment cannot be passed by dynamic routing, cannot be passed by rebuild-before-run,
and is the one LangGraph, AutoGen, Airflow and ATM do not answer.

If that experiment can be expressed as a static graph with a branch, the capability is not needed
and the honest outcome is to stop.

## 7. Revised phase plan

| Phase | Content | Change from the proposal |
| --- | --- | --- |
| 0 | Value gap, above. Plus: write the §6 experiment as a *scenario specification* and prove on paper that no static graph expresses it. | Tightened; adds the falsification test |
| 1 | Canonical graph identity: serialization, hash, equality, a node-type registry, and a declarative node definition that carries no code. In memory, no persistence, no store method. | Was "static graph versioning"; persistence deferred |
| 2 | Deterministic mutation engine: `ADD_NODE`, `ADD_EDGE`, `REMOVE_EDGE`; proposal/decision types; validation pipeline; `dry_run()`/`apply()` over an in-memory graph. Manually constructed proposals only. | `REPLACE_SUBGRAPH` dropped |
| 3 | Execution-state semantics and invalidation, expressed as a pure function of (graph version, persisted step rows) so it is recomputable after a restart rather than recovered. | Emphasis on recomputability |
| 4 | Fork integration: a committed mutation produces a child run that inherits evidence through the existing `reuse_completed_step` path. First phase that touches durable state. | Replaces in-place mutation |
| 5 | Crash recovery proof against the boundaries in §6, in the style of the existing P5 proof. | Moved before the Agent |
| 6 | Side-effect boundary, in a new domain with real `EXTERNAL_WRITE` nodes. | Needs a new fixture domain |
| 7 | The Planner-driven graph executor — the §3 bridge — scoped and costed explicitly. | New, and the real gate |
| 8 | `PROPOSE_GRAPH_MUTATION` as a typed Planner decision, with a rejected proposal returned as a deterministic observation. | Unchanged, but last and clearly gated |

## 8. On evaluation

Two notes on §22. "Invalid graph acceptance rate → 0" is not measurable against hand-written cases;
it needs a generator. Property-based testing over random operation sequences (no accepted mutation
ever produces a cycle, an unknown node, an unreachable required node, or a graph exceeding limits)
is the right instrument, and it composes with the repository's existing discipline of proving
tests kill deliberately introduced source mutants. Second, keep Agent proposal quality and Runtime
safety in separate suites, as §22 says — and make the safety suite adversarial rather than
representative, since its claim is a universal, not an average.

## 9. What this must not become

Restating the proposal's own out-of-scope list where the codebase makes the boundary structural
rather than aspirational: a proposed node names a registered type and never carries code; a
mutation is a typed object and never a natural-language plan; a committed external effect is a fact
about the past that no topology can reinterpret; and no accepted mutation may make a previously
durable run unreplayable.
