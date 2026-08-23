# Store Semantic Conformance

This suite turns the durable execution plane's storage guarantees into backend-driven executable
scenarios. It defines observable semantics rather than requiring matching schemas, SQL, indexes,
locking syntax, or query plans.

The suite now has two implementations:

- SQLite, the default single-host application backend;
- PostgreSQL, Run/Workflow and governed-Memory semantic backends exercised in CI against
  PostgreSQL 16.

The portability claim is deliberately narrow. Both backends are required to preserve the same Run
ownership, fencing, Thread serialization, checkpoint CAS, external-action recovery, and quarantine
semantics. The Memory contract additionally preserves same-authority Run lease fencing, versioned
scoped records, sealed snapshots, and retry-safe audit mirroring. The default API composition
remains SQLite because this phase does not add an application backend selector.

## Contract layers

### 1. Workflow Store Contract

`tests/conformance/test_workflow_store_contract.py` drives the structural `WorkflowStore` surface.
It covers execution and step identity, attempt-token fencing, dispatch-token fencing,
prepare-before-dispatch, cancellation arbitration, transition/event atomicity, append-only event
ordering, and restart-visible committed writes.

Both `SQLiteWorkflowStore` and `PostgresWorkflowStore` execute these scenarios.

### 2. Run Store Behavioral Contract

`tests/conformance/test_run_store_contract.py` drives the consumer-facing `RunStore` behavior. The
production `RunStore` protocol is intentionally consumer-driven: it contains the operations used by
RuntimeManager, the API/Action consumers, and quarantine resolution rather than exposing database
paths, SQL connections, or test-only fault hooks.

The shared tests cover Run claim and exact lease expiry, stale lease fencing, tenant-qualified Thread
scheduling, revision-qualified checkpoint loads, completion CAS, atomic full-Run/projected-checkpoint
completion, failure/cancellation checkpoint preservation, and restart-visible writes.

Test-only backend adapters remain responsible for resource setup, deterministic clocks, race
synchronization, and equivalent fault injection.

### 3. Composite Execution-Plane Contract

`tests/conformance/test_execution_plane_contract.py` retains semantics that cross Run storage,
Workflow storage, `RuntimeManager`, and external-action recovery. It proves that an ambiguous unsafe
effect is reconciled before same-Thread successor work, that checkpoint drift during recovery enters
an inspectable nonterminal quarantine, and that the narrow operator resolution path can release only
an unchanged eligible quarantine while preserving authoritative evidence.

For PostgreSQL, the Run/checkpoint/workflow/tool/action/event tables needed by these scenarios live in
one PostgreSQL schema so the same cross-ledger transactions remain possible.

### 4. Quarantine Evidence Integrity Contract

`tests/conformance/test_quarantine_evidence_contract.py` targets the evidence fingerprint itself. It
proves that workflow evidence added after dry-run makes an apply plan stale, and that a checkpoint
whose content changes without a revision advance is detected during post-commit verification. These
scenarios use test-only out-of-band evidence writers in both backend adapters; production consumers
do not receive those mutation hooks.

### 5. Memory Store Contract

`tests/conformance/test_memory_store_contract.py` drives both `SQLiteMemoryStore` and
`PostgresMemoryStore` through one scenario layer. It covers scoping, monotonic versioning,
supersession/tombstoning, value-free audit, sealed empty/non-empty snapshots, current/stale/exactly
expired lease fencing, persisted Run identity, concurrent first writes, transaction rollback, and
valid-successor repair of the Memory-audit/public-Run-event handoff.

PostgreSQL-specific component lifecycle and database mechanics remain in
`test_postgres_memory_backend.py`. The complete invariant and mutation mapping is documented in
[`postgresql-memory-store.md`](postgresql-memory-store.md).

## Backend selection

The shared fixture is selected explicitly:

```text
STORE_CONFORMANCE_BACKENDS=sqlite
STORE_CONFORMANCE_BACKENDS=postgres
```

PostgreSQL selection also requires:

```text
TEST_POSTGRES_DSN=postgresql://...
```

If PostgreSQL is requested without a DSN, collection fails as a configuration error. The suite does
not convert a missing PostgreSQL environment into a skip and then claim portability.

GitHub Actions runs the same five shared files once with SQLite and once with PostgreSQL on Python
3.11 and 3.12. PostgreSQL-specific schema and connection mechanics remain in
`tests/conformance/test_postgres_backend.py` and `test_postgres_memory_backend.py`; they are not
mixed into the shared semantic scenarios.

## Invariant matrix

Every invariant below is asserted through the same scenario semantics for SQLite and PostgreSQL.
Backend-specific test IDs differ only by the fixture parameter suffix.

| Invariant | Setup | Operation / race / failure injection | Externally observable result | Executable evidence |
| --- | --- | --- | --- | --- |
| **I1 One live owner per Run attempt** | One queued Run; independent store instances share durable storage and a deterministic test clock. | Two claims race; a second claim is attempted before expiry; the clock advances to the exact boundary. | Exactly one initial owner exists; takeover increments `attempt`, rotates the lease token, and records `lease_expired`. | `test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced`; `test_lease_expiry_uses_the_injected_store_clock_exactly` |
| **I2 Stale attempt can never mutate durable state** | A replacement attempt owns the Run; a Workflow step/action also has a newer attempt/dispatch token. | Old Run/tool/action authority attempts a durable mutation. | Stale writes fail with the existing typed fencing outcomes; the winning output/evidence remains authoritative. | Run-store fencing scenario plus workflow attempt/dispatch fencing scenarios |
| **I3 One running Run per tenant-qualified Thread** | Two queued Runs share tenant and Thread; independent stores race. | Concurrent claim. | Exactly one becomes `running`; the successor remains queued with `attempt == 0`. | `test_i3_one_running_run_per_tenant_qualified_thread` plus the PostgreSQL unique-constraint mechanics proof |
| **I4 Thread serialization is tenant-qualified** | Runs use different Threads in one tenant, or the same Thread name in different tenants. | Concurrent claim. | Both independent tenant-qualified Threads may run. | `test_i4_thread_scope_allows_independent_claims` |
| **I5 Checkpoint commit requires expected revision** | A predecessor creates revision 1 and the successor captures base revision 1. | The adapter injects a revision-2 writer before load/completion. | Load reports expected/observed revision mismatch; stale completion returns `CHECKPOINT_CONFLICT`; revision 2 is preserved. | `test_i5_checkpoint_load_and_completion_require_expected_revision`; `test_postgres_checkpoint_write_rejects_stale_revision` |
| **I6 Run + projected checkpoint + required events are atomic** | A leased Run has typed final state and trace evidence. | Equivalent failures are injected at checkpoint write, `checkpoint.saved`, and `run.completed`. | No partial terminal state survives. A later success stores full Run trace, checkpoint `execution_trace=[]`, one revision advance, truthful projection evidence, and terminal events together. Failure/cancellation preserve the prior checkpoint. | `test_i6_completion_checkpoint_and_required_events_commit_atomically`; `test_failure_and_cancellation_preserve_checkpoint_revision` |
| **I7 Recovery never guesses an ambiguous external effect** | An unsafe effect is durably dispatching when terminal evidence persistence fails; a same-Thread successor waits. | Storage/coordinator objects restart and the expired predecessor is recovered. | Recovery never calls the unsafe provider again; uncertainty is reconciled first; only then may successor work proceed. | `test_i7_reconciliation_precedes_successor_and_never_retries_unsafe_effect` |
| **I8 Semantic conflict fails closed into inspectable quarantine** | Reconciliation-pending work has checkpoint base 1; current checkpoint is externally advanced to 2. | A recreated manager recovers the expired predecessor. | Runtime construction does not proceed; the Run remains nonterminal, lease-free, visibly quarantined, and continues blocking its same-Thread successor. | `test_i8_checkpoint_conflict_is_inspectable_nonterminal_quarantine` |
| **I9 Quarantine release requires an unchanged eligible plan and evidence** | Quarantined predecessor has terminal external evidence and queued same-Thread successors. | Dry-run derives a plan; workflow/checkpoint evidence may drift; apply rederives the plan in one transaction; later legal successor CAS progress occurs; exact replay is retried. | Changed pre-apply evidence makes the plan stale; same-revision post-commit evidence drift is detected; an unchanged apply preserves checkpoint/external evidence, writes one resolution and one Run failure, releases the Thread, tolerates later legal revision progress, and is exact-replay idempotent. | `test_i9_unchanged_eligible_plan_releases_quarantine_preserving_evidence`; both tests in `test_quarantine_evidence_contract.py` |

The shared tests intentionally do not assert a specific SQL statement, index name, identity-sequence
value, PRAGMA, PostgreSQL isolation setting, or lock primitive. Those are implementation mechanics.
They assert durable behavior that consumers rely on.

## Cross-PR durable checkpoint integrity

The combined system property established by the ownership, serialization, state-boundary, and
operator-repair work is:

> For one tenant-qualified Thread, only the current leased Run attempt may atomically advance the
> checkpoint from its captured revision. A successful completion stores the full validated Run
> result and a same-type resumable checkpoint projection together with truthful events. A stale,
> conflicting, failed, cancelled, partially committed, or quarantined attempt cannot overwrite or
> ambiguously reinterpret the authoritative checkpoint; operator repair may release quarantine only
> while its evidence-bound plan remains unchanged.

The second backend is accepted only if it preserves this system property without weakening the
shared assertions.

| Property slice | SQLite enforcement | PostgreSQL enforcement | Executable evidence |
| --- | --- | --- | --- |
| Current writer only | lease-token/deadline predicates inside SQLite transactions | lease-token/deadline predicates plus PostgreSQL row locking/conditional updates | I1, I2, I7 |
| One ordered Thread writer | tenant-qualified running-Run uniqueness and claim arbitration | partial unique `(tenant_id, thread_id) WHERE status='running'` plus claim arbitration | I3, I4 |
| Correct predecessor | captured `checkpoint_base_revision` and revision CAS | captured `checkpoint_base_revision` and revision-qualified upsert | I5, I6 |
| Full result versus resumable projection | `project_thread_checkpoint_state()` inside terminal transaction | same typed projection inside PostgreSQL terminal transaction | I6; checkpoint-projection tests |
| One atomic observable completion | one SQLite transaction covers Run/checkpoint/events/lease clear | one PostgreSQL transaction covers the same durable effects | I6 injected failures and M07/M08 source mutants |
| Truthful projection evidence | persisted Run count versus projected checkpoint count | same event payload semantics | I6; state-boundary characterization |
| Recovery interpretation | reconciliation precedence and nonterminal quarantine on drift | same shared recovery/quarantine outcomes | I7, I8 |
| Controlled repair | plan rederivation, evidence checks, atomic resolution/failure | same logical evidence rederivation within one PostgreSQL schema/transaction domain | I9 plus quarantine evidence integrity contract |

Checkpoint projection does not change workflow identity: `dynamic_loop.py` excludes
`execution_trace` from the dynamic workflow input hash. Therefore an old full-trace checkpoint and a
new projected checkpoint with the same semantic state do not create a false workflow identity
mismatch.

Upgrade behavior remains lazy and failure-safe. An old non-empty checkpoint trace may be loaded by
the first post-upgrade execution; the first successful completion advances the revision and projects
that trace out of the resumable checkpoint. Failure, cancellation, conflict, or quarantine does not
silently rewrite the checkpoint.

## Additional Workflow Store scenarios

The workflow contract also covers:

- typed execution identity outcomes and mismatch detection;
- step identity/caching mismatch rules without spurious events;
- ordered cursor reads and reopen visibility;
- prepare-before-dispatch and dispatch-token fencing;
- cancellation winning before first external dispatch;
- workflow transition/event atomicity;
- external-action, parent-step, and terminal-event atomicity.

These scenarios run unchanged against both workflow-store implementations.

## Deterministic execution rules

- Lease scenarios use an injected store clock; they do not sleep for expiry.
- Claim races use explicit barriers and bounded thread joins/results.
- Recovery recreates store/coordinator/manager objects against the same durable backend.
- Composite synchronization uses explicit events rather than wall-clock assumptions.
- Assertions target typed outcomes, status, attempt, revision, durable event order/payload, provider
  call count, and sanitized error codes.
- The PostgreSQL production-clock mechanics test separately proves that a non-injected
  `PostgresRunStore` derives lease time from the database.

## Backend adapter boundary

`tests/conformance/backends.py` contains `StoreConformanceBackend` plus SQLite and PostgreSQL
implementations. This is a **test-resource** protocol, separate from the production `RunStore`
protocol.

Adapters may use implementation knowledge only to create equivalent conditions such as:

- a checkpoint revision change from an out-of-band writer;
- a same-revision checkpoint-content change for evidence-integrity testing;
- an extra workflow event after a quarantine plan is derived;
- a failure at checkpoint persistence;
- a failure at a Run or Workflow event boundary.

The shared scenario cannot inspect backend SQL to decide whether it passed.

The PostgreSQL adapter creates an isolated validated schema per test and drops only that generated
schema during cleanup. SQLite continues to use isolated temporary database files.

## PostgreSQL mechanics kept outside the shared contract

`tests/conformance/test_postgres_backend.py` verifies implementation-specific facts that should not
be generalized into cross-backend assertions:

- schema bootstrap is idempotent;
- incompatible and unversioned execution-plane schemas fail closed;
- invalid schema identifiers are rejected;
- independent stores use independent PostgreSQL server connections and no Python `_lock`;
- production lease time is database-derived;
- deterministic exact-expiry testing remains possible with the injected clock;
- explicit transaction failure rolls back and the context closes its connection;
- committed state survives reopen;
- expected unique and foreign-key constraints are enforced;
- a stale checkpoint revision cannot satisfy the PostgreSQL upsert CAS predicate;
- generated test schemas do not leak data.

## Mutation gate

`tests/conformance/postgres_mutation_proof.py` and `postgres_memory_mutation_proof.py` make the
portability evidence mutation-sensitive.
After the PostgreSQL 3.11 and 3.12 conformance jobs pass, a PostgreSQL 16 / Python 3.12 CI job
applies twelve temporary source mutations and runs the targeted semantic assertion for each. Every
mutation is restored byte-for-byte, and CI finishes the job with `git diff --exit-code`.

The mutation set is sampled rather than exhaustive: it removes representative lease fencing,
exact-expiry, Thread uniqueness/tenant qualification, checkpoint CAS/projection, transaction
atomicity, unsafe-provider recovery, and quarantine evidence properties. In particular M01 samples
one lease-token predicate; it is not a claim that every lease predicate was independently mutated.

M07 and M08 are real source mutations rather than relabeled baseline tests. M07 commits terminal Run
status before the checkpoint/event transaction; M08 moves `run.completed` outside the transaction.
The existing I6 three-way failure-injection assertion kills both by observing the resulting partial
durable state.

The exact mutant-to-test mappings are documented in
[`postgresql-store-backend.md`](postgresql-store-backend.md) and
[`postgresql-memory-store.md`](postgresql-memory-store.md).

## Application boundary and non-goals

The default service still composes `SQLiteRunStore`, `SQLiteWorkflowStore`, and
`SQLiteMemoryStore`. PostgreSQL is not wired into the application root in this phase. The default
`requirements.txt` does not install Psycopg; PostgreSQL users install `requirements-postgres.txt`,
while `requirements-dev.txt` includes it for conformance testing.

This is intentional. `PostgresMemoryStore` now preserves governed Memory's same-database
Run/lease transaction when explicitly used with the PostgreSQL Run store. Combining PostgreSQL
Run/Workflow storage with `SQLiteMemoryStore` would still split that authority and remains
prohibited; coherent application selection is separate work.

Accordingly, this portability phase does **not** add:

- an application backend selector or `RUNTIME_DATABASE_URL`;
- connection pooling or a migration framework;
- a distributed queue/wake mechanism;
- multi-replica or HA deployment claims;
- JSONB normalization or checkpoint-schema changes;
- Redis or another new coordination backend.

The demonstrated result is storage-semantic portability for Run/Workflow and governed Memory, not
a claim that the complete application has already migrated from SQLite to PostgreSQL.
