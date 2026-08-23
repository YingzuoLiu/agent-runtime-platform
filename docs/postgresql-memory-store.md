# PostgreSQL Memory semantic backend

## Scope and claim

`PostgresMemoryStore` preserves the governed Memory behavior already defined by
`SQLiteMemoryStore`, and both implementations run the same executable Memory contract. This closes
the storage-semantic gap for Memory without changing the application's default SQLite composition.

The accepted Run/Workflow PostgreSQL backend already provides database-authoritative leases and
fencing. The remaining gap was that governed Memory validated those leases only inside the SQLite
database that also held the Memory mutation. Combining PostgreSQL Run ownership with SQLite Memory
would split rule `X1` across two authorities:

```text
X1: validate active Run identity and lease/fence
    + commit the governed Memory mutation or snapshot
    in one database transaction and schema authority
```

This phase adds the missing PostgreSQL implementation and evidence. It does not wire PostgreSQL
into `create_app()`; coherent application selection and migration/startup authority remain a
separate phase.

## Deterministic authority boundary

The LLM and domain parser may propose an allowlisted `MemoryWrite`. They do not decide ownership,
version order, snapshot reuse, or audit mirroring. Those transitions remain deterministic:

| Operation | PostgreSQL enforcement | Authoritative postcondition |
| --- | --- | --- |
| Run-owned upsert | Lock persisted Run, validate tenant/domain/subject and current unexpired lease, acquire scoped transaction lock, mutate records and audit in one transaction | One active scoped key, monotonic next version, complete audit or no mutation |
| Run-owned snapshot | Lock persisted Run and validate the same identity/lease before reading or inserting the snapshot | Exactly one immutable snapshot row for the Run, including an empty snapshot |
| Administrative upsert | Separate explicit API; no implied Run lease | Versioned record and mutation audit committed together |
| Forget | Resolve and transaction-lock the logical scoped key, redact/tombstone every non-deleted version, append one audit event | Future active reads return no value; sealed Run snapshots remain unchanged |
| Public Run-event mirror | Append through the current Run attempt's fenced event API | A committed Memory audit id is mirrored at most once; a valid successor can repair an acknowledgement gap |

Lease expiry uses `transaction_timestamp()` in production, the same PostgreSQL transaction-time
authority as `PostgresRunStore`. Only conformance tests inject a manual clock, so exact-expiry tests
do not depend on host clocks or sleeps.

## Concurrency and version order

A partial unique index independently enforces one `active` row for:

```text
(tenant_id, subject_id, domain_id, kind, memory_key)
```

That index alone cannot serialize two concurrent first writes because no row exists to lock.
`PostgresMemoryStore` therefore acquires a transaction-scoped advisory lock derived from the schema
and complete logical key before reading the active row or choosing `MAX(version) + 1`. Independent
connections then observe a deterministic serial order: versions are consecutive, supersession and
creation audit events agree with that order, and one active version remains. The database unique
index is retained as a separate backstop rather than relying only on application locking.

## Versioned schema component

The accepted PostgreSQL Run/Workflow schema remains `execution-plane = 1`. Memory is installed as
an explicit second component:

```text
runtime_store_schema
  execution-plane = 1
  memory          = 1
```

`initialize_postgres_memory_schema()` first validates/installs the accepted execution-plane v1,
then takes a schema-scoped advisory transaction lock and installs or validates:

- `memory_records`;
- `memory_events`;
- `run_memory_snapshots` and its foreign key to `runs`;
- the active-key, subject-read, and audit-source indexes.

Clean bootstrap, repeated initialization, adding the Memory component to an accepted
execution-plane v1 schema, incompatible component rejection, unversioned-table rejection, shape
validation, and reopen durability are executable PostgreSQL mechanics tests. This is a reviewable
component installation, not a general migration framework or a silent redefinition of schema v1.

## Shared semantic contract

`tests/conformance/test_memory_store_contract.py` runs unchanged through the SQLite and PostgreSQL
backend adapters. It covers:

- tenant/subject/domain/key scoping, canonical unchanged writes, monotonic versions, supersession,
  and value-free audit payloads;
- cross-subject hiding and tombstoning/value redaction of every logical-key version;
- sealed non-empty and explicitly empty snapshots;
- wrong tenant, subject, or domain rejection;
- current, stale, exactly expired, replaced, missing, and empty lease-token behavior;
- valid-successor repair of a committed Memory-audit/public-Run-event gap without duplication;
- barrier-coordinated concurrent writes from independent store connections;
- rollback of record, supersession, and audit changes after an injected event failure;
- expired-record and allowlisted-key retrieval behavior;
- JSON serialization failure before any durable write.

PostgreSQL-specific tests in `test_postgres_memory_backend.py` exercise the component lifecycle,
server-time expiry, database uniqueness, and reopen behavior. Selecting PostgreSQL without
`TEST_POSTGRES_DSN` is a pytest collection error. The PostgreSQL CI job explicitly selects the real
backend, so a green job cannot silently fall back to SQLite or skip required Memory cases.

## Mutation gate

`postgres_memory_mutation_proof.py` temporarily applies five representative source mutations and
requires the targeted semantic assertion to fail with pytest exit code 1:

| Mutant | Removed property | Target evidence |
| --- | --- | --- |
| MM01 | Governed Memory lease-token predicate | stale/replaced attempt rejection |
| MM02 | Persisted Run subject identity check | wrong-subject snapshot/mutation rejection |
| MM03 | Active-key unique index | direct database backstop proof |
| MM04 | Recompute and overwrite an existing sealed snapshot | non-empty/empty sealing contract |
| MM05 | Memory mutation transaction | injected supersession/event rollback contract |

The runner restores every touched source file byte-for-byte. CI follows it with
`git diff --exit-code`.

## Cross-PR system-property scan

| Property | Entry points | Enforcement locations | Durable evidence | Executable tests | Operational signal | Negative space |
| --- | --- | --- | --- | --- | --- | --- |
| Stale owner cannot create a snapshot or mutate Memory | `GovernedMemory.retrieve`, `GovernedMemory.remember` | context token requirement; PostgreSQL Run row identity/lease lock and same transaction; SQLite `BEGIN IMMEDIATE` lease predicate | Run lease fields, Memory record/event or snapshot row | shared current/stale/exact-expiry/replacement/identity cases; MM01/MM02 | typed `RunLeaseLostError` or identity failure with Run id | raw administrative `upsert` is intentionally separate and is not used by Runtime execution |
| One logical key has ordered versions and one active row | administrative or Run-owned upsert | scoped transaction lock, active row lock, partial unique index, unique scoped version | record statuses/versions plus ordered audit ids | shared concurrent/version/rollback cases; PostgreSQL unique mechanics; MM03/MM05 | transaction error fails closed; no partial audit | direct database writes are outside the public/runtime API and schema constraints still reject duplicate active rows |
| First Run snapshot is immutable | governed or explicit test snapshot API | persisted Run identity; one `run_id` primary key; existing-row reuse | `run_memory_snapshots` including `[]` | shared sealed non-empty/empty/identity cases; MM04 | identity mismatch is typed and inspectable | forgetting and later writes deliberately do not rewrite historical snapshots |
| Audit/public-event handoff is retry-safe | `GovernedMemory.remember` | Memory audit commits with mutation; mirror identifies `(event_type, audit_event_id)` and appends through fenced Run attempt | `memory_events` and `run_events` | shared successor-repair case | stale mirror attempt fails with lease loss; successor rereads audit | the two ledgers are not one atomic transaction, so repair is required and explicitly tested |
| Application has no split durable authority | `create_app`, state-boundary eval harness | composition remains three SQLite stores | application state and existing API tests | full SQLite/API regression plus source scan | application continues to report/use local SQLite behavior | no PostgreSQL selector or mixed composition exists in this phase |

The scan found no additional production mutation entry point. Travel runtime reaches Memory only
through `GovernedMemory`; the HTTP surface exposes subject-scoped list and forget operations, not a
raw Run-bypassing write. The state-boundary evaluation harness remains an explicit SQLite test
composition.

## Operational impact, rollback, and limits

- Default installs and imports still do not require Psycopg. `PostgresMemoryStore` is a lazy package
  export, and the default application still constructs SQLite stores.
- PostgreSQL users install `requirements-postgres.txt` and explicitly construct all stores against
  the same DSN/schema in tests or future composition work.
- This phase creates an additive Memory component in an explicitly selected PostgreSQL schema. It
  does not migrate existing SQLite data.
- Before application wiring, rollback is removal of this unused component/code path. Where test
  schemas are disposable, the isolated schema can be dropped. No production-data downgrade path is
  claimed because production composition is not enabled here.
- No connection pooling, backend selector, multi-process scheduling proof, distributed queue,
  cloud deployment, HA, Redis, or new Memory/checkpoint semantics are included.

## Verification

The required PostgreSQL path is:

```bash
STORE_CONFORMANCE_BACKENDS=postgres \
TEST_POSTGRES_DSN=postgresql://... \
pytest -q \
  tests/conformance/test_memory_store_contract.py \
  tests/conformance/test_postgres_memory_backend.py

TEST_POSTGRES_DSN=postgresql://... \
python tests/conformance/postgres_memory_mutation_proof.py
```

The default full suite continues to use SQLite and preserves the application behavior. CI runs the
shared Memory contract for SQLite and PostgreSQL on Python 3.11 and 3.12, then PostgreSQL mechanics
and both mutation runners.
