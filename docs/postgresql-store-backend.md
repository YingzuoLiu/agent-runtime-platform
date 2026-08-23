# PostgreSQL Store Backend

This repository includes a PostgreSQL execution-plane backend for the durable Run and Workflow
storage semantics. It exists to prove that the storage contract is not accidentally tied to
SQLite syntax or process-local locking.

The implementation is intentionally narrower than a production database migration. The default
application composition remains SQLite, and PostgreSQL is exercised through the store semantic
conformance suite and PostgreSQL-specific mechanics tests.

## Implemented surface

The PostgreSQL backend provides:

- `PostgresRunStore` for Run lifecycle, lease ownership, Run events, Thread checkpoint CAS, and
  quarantine resolution;
- `PostgresWorkflowStore` for workflow executions, tool calls, external actions, and workflow
  events;
- one execution-plane schema containing the Run, checkpoint, workflow, tool, action, and event
  tables required by the shared recovery and quarantine transactions;
- a consumer-driven structural `RunStore` protocol so Runtime consumers do not depend on
  `SQLiteRunStore` directly;
- backend-neutral store error classification for contention, integrity, and retryable store errors;
- `PostgresConformanceBackend` so the same I1-I9 shared scenarios execute against SQLite and
  PostgreSQL.

`WorkflowStore` remains the existing structural workflow contract. The PostgreSQL implementation
uses explicit PostgreSQL SQL rather than a generic SQL dialect layer or ORM.

## Transaction and ownership model

### Database time is lease authority

Production PostgreSQL leases derive time from the database inside the transaction. The Run store
uses PostgreSQL `transaction_timestamp()` as the transaction-stable authority for lease checks and
lease writes.

The optional injected millisecond clock exists only for deterministic conformance tests. It must
not be treated as the production authority.

### Claim arbitration

Queued or expired work is selected with PostgreSQL row locking and `SKIP LOCKED`. Claim remains a
conditional durable transition rather than a process-local mutex.

A partial unique index enforces at most one `running` Run for one tenant-qualified Thread:

```text
(tenant_id, thread_id) WHERE status = 'running'
```

This constraint is a second independent guard in addition to claim ordering and conditional update
predicates.

### Fencing

Attempt-specific lease tokens remain part of durable mutation predicates. A stale attempt cannot
append attempt-owned events, renew authority, or commit terminal Run state after another attempt
has taken ownership.

Workflow tool attempts and external-action dispatch attempts preserve their existing attempt-token
and dispatch-token fencing semantics.

### Checkpoint CAS

A successful Run captures the Thread checkpoint base revision when it is claimed. Completion may
advance the checkpoint only from that expected revision.

PostgreSQL performs the checkpoint write with a revision-qualified `INSERT ... ON CONFLICT ... DO
UPDATE ... WHERE ... revision = expected`. If the expected revision no longer matches, the
completion does not silently overwrite the authoritative checkpoint.

### Atomic completion

The successful completion transaction keeps these writes in one PostgreSQL transaction:

```text
full completed Run state
+ projected Thread checkpoint
+ checkpoint revision advance
+ checkpoint.saved
+ run.completed
+ lease clearing
```

The Run keeps its full final `execution_trace`; the resumable Thread checkpoint stores the same
typed state with `execution_trace=[]`.

Injected conformance failures at the checkpoint write, `checkpoint.saved`, and `run.completed`
boundaries must roll the entire transaction back.

### Recovery and quarantine

The Run/checkpoint/workflow/tool/action/event tables used by external-action recovery and operator
quarantine resolution live in the same PostgreSQL schema. This preserves the same-transaction
cross-ledger evidence checks required by I7-I9.

Read-only multi-statement evidence snapshots use a local repeatable-read transaction where a
consistent snapshot is required. The backend does not globally raise isolation to SERIALIZABLE.

## Schema bootstrap

`runtime_service/postgres_schema.py` owns one explicit execution-plane schema version. It is not a
migration framework.

Bootstrap behavior is fail-closed:

- schema names must pass the repository's strict identifier validation;
- a new schema receives the v1 metadata row and execution-plane tables/indexes;
- repeated v1 initialization is idempotent;
- an incompatible metadata version is rejected;
- an unversioned schema that already contains execution-plane tables is rejected rather than
  silently adopted;
- expected v1 columns are probed after initialization.

The storage representation intentionally keeps the current `*_json` values as text. This phase does
not introduce JSONB normalization or change canonical hashing inputs.

## Connection discipline

The backend uses Psycopg 3. Connections are opened in autocommit mode outside explicit transaction
blocks, and mutations use `connection.transaction()` for commit/rollback boundaries. This avoids
leaving ordinary reads accidentally idle in transaction while keeping transaction ownership
explicit.

There is no Python process lock in the PostgreSQL stores. PostgreSQL row locks, constraints,
conditional writes, and transactions provide the durable arbitration.

## Conformance and CI

The shared suite is selected with:

```text
STORE_CONFORMANCE_BACKENDS=sqlite
STORE_CONFORMANCE_BACKENDS=postgres
```

PostgreSQL selection additionally requires `TEST_POSTGRES_DSN`. Missing PostgreSQL configuration is
a test configuration error, not a skip.

GitHub Actions starts PostgreSQL 16 and executes the same shared conformance files on Python 3.11
and 3.12:

```text
tests/conformance/test_run_store_contract.py
tests/conformance/test_workflow_store_contract.py
tests/conformance/test_execution_plane_contract.py
tests/conformance/test_quarantine_evidence_contract.py
```

A separate PostgreSQL mechanics file verifies schema bootstrap/version rejection, independent
server connections, server-time lease authority, exact injected-clock expiry, transaction
rollback/connection close behavior, reopen durability, expected database constraints, stale
checkpoint-CAS rejection, and test schema isolation.

The portability claim is behavioral: both backends must produce the same typed outcomes, durable
state transitions, attempts, revisions, event ordering, recovery behavior, and quarantine
semantics. It is not a claim that they share SQL, indexes, lock syntax, or query plans.

### Mutation and counterfactual proof

A separate PostgreSQL 16 CI job runs `tests/conformance/postgres_mutation_proof.py` after both
PostgreSQL conformance matrix jobs pass. The runner changes source only in the ephemeral Actions
working tree, executes the targeted semantic assertion, and restores the exact original bytes after
each mutant. CI then requires `git diff --exit-code` so a mutation cannot leak into the submitted
source.

The proof covers these counterfactuals:

| # | Counterfactual | Killing/proof test |
| --- | --- | --- |
| M01 | Remove the Run lease-token predicate | `test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced` |
| M02 | Change exact lease expiry from `<=` to `<` | `test_lease_expiry_uses_the_injected_store_clock_exactly` |
| M03 | Remove one-running-Run-per-Thread uniqueness | `test_postgres_expected_unique_and_fk_constraints_are_enforced` |
| M04 | Remove tenant qualification from running-Thread uniqueness | `test_i4_thread_scope_allows_independent_claims` |
| M05 | Remove the checkpoint revision CAS predicate | `test_postgres_checkpoint_write_rejects_stale_revision` |
| M06 | Persist the full Run trace into the Thread checkpoint | `test_i6_completion_checkpoint_and_required_events_commit_atomically` |
| M07 | Split Run/checkpoint/event completion into separately observable transactions | I6 checkpoint-write failure injection |
| M08 | Append the terminal event outside the completion transaction | I6 terminal-event failure injection |
| M09 | Retry an unsafe provider after an ambiguous dispatch | `test_i7_reconciliation_precedes_successor_and_never_retries_unsafe_effect` |
| M10 | Ignore the re-derived quarantine plan identity | `test_i9_workflow_evidence_change_after_plan_makes_plan_stale` |
| M11 | Accept same-revision checkpoint evidence drift | `test_i9_same_revision_checkpoint_evidence_drift_is_detected_after_commit` |
| M12 | Reject legal later successor checkpoint progress | `test_i9_unchanged_eligible_plan_releases_quarantine_preserving_evidence` |

M07 and M08 use the existing targeted transaction-failure injection rather than a committed
source-text mutation. They prove the same externally observable counterfactual: a failure at those
boundaries cannot leave a partial terminal Run, checkpoint, or event sequence.

## Application composition boundary

The default API composition still constructs:

```text
SQLiteRunStore
SQLiteWorkflowStore
SQLiteMemoryStore
```

There is deliberately no application backend selector or `RUNTIME_DATABASE_URL` switch in this
phase.

A PostgreSQL Run/Workflow backend must **not** be combined at the application composition root with
`SQLiteMemoryStore`. Governed memory validates current Run/lease authority in the same database
transaction as memory mutation. Splitting those stores across PostgreSQL and SQLite would weaken
that fencing property.

A future full PostgreSQL application composition therefore requires a PostgreSQL memory backend, or
another design that preserves the same atomic authority check, before switching the default
service composition.

## Explicit non-goals

This phase does not add:

- a PostgreSQL Memory Store;
- a runtime backend-selection environment variable;
- connection pooling;
- multi-replica Runtime deployment;
- a distributed queue or wake-up channel;
- HA/failover operational claims;
- a schema migration framework;
- JSONB normalization;
- Redis or another coordination service.

Independent PostgreSQL connections and database-level arbitration are necessary portability
proofs, but they are not by themselves a horizontal-scaling or production-operations claim.
