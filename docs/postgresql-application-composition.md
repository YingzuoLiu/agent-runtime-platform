# PostgreSQL application composition and schema authority

## Problem, existing mechanism, and value gap

The Run/Workflow and governed Memory stores already preserve the same tested semantics on SQLite
and PostgreSQL. That does not by itself make PostgreSQL usable by the API: `create_app()` previously
constructed three SQLite stores directly, and each PostgreSQL store could initialize schema from a
worker constructor.

The remaining system problem was one coherent durable authority:

```text
one backend selection
→ one Run/Workflow/Memory authority
→ one designated schema bootstrap
→ read-only worker compatibility validation
→ bounded short-lived connections
```

`runtime_service/storage.py` is needed only as that composition root. It does not create a generic
database abstraction or SQL dialect. The existing `RunStore`, `WorkflowStore`, governed
`MemoryStore`, and subject-scoped `MemoryAdminStore` protocols remain the consumer boundaries.

## Deterministic authority

No model decides storage, migration, compatibility, credentials, or connection policy. These are
deterministic startup decisions:

| Decision | Enforcement | Failure behavior |
| --- | --- | --- |
| SQLite or PostgreSQL | one `RuntimeStorageConfig` | unknown backend rejected |
| complete authority | one bundle constructs Run, Workflow, and Memory together | SQLite path plus PostgreSQL settings rejected |
| schema ownership | explicit bootstrap command | workers never create or migrate schema |
| compatibility | read-only metadata and shape validation before manager start | missing/incompatible components fail closed |
| credential exposure | DSN read from environment and excluded from metadata/errors | readiness and CLI output never include DSN |
| connection budget | short-lived per-operation connections with finite connect/statement/lock budgets | operations fail within configured bounds |
| external provider boundary | PostgreSQL composition requires injected or HTTP provider | no implicit provider-side SQLite file |

## Configuration

SQLite remains the default:

```bash
export RUNTIME_STORE_BACKEND=sqlite
export RUNTIME_DB_PATH=runtime_data/runtime.db
```

PostgreSQL uses one DSN and schema for all three stores:

```bash
export RUNTIME_STORE_BACKEND=postgres
export RUNTIME_POSTGRES_DSN='postgresql://runtime:secret@db.example/runtime'
export RUNTIME_POSTGRES_SCHEMA=agent_runtime
export RUNTIME_POSTGRES_CONNECT_TIMEOUT_SECONDS=5
export RUNTIME_POSTGRES_STATEMENT_TIMEOUT_SECONDS=30
export RUNTIME_POSTGRES_LOCK_TIMEOUT_SECONDS=5
export RUNTIME_POSTGRES_LEASE_OPERATION_TIMEOUT_SECONDS=1
```

The DSN must come from the process environment or its deployment secret injection. The bootstrap
command intentionally has no `--dsn` argument so credentials are not encouraged into shell history.

The following combinations fail before work begins:

- an unknown backend;
- PostgreSQL without `RUNTIME_POSTGRES_DSN`;
- PostgreSQL plus `RUNTIME_DB_PATH` or an explicit `database_path`;
- SQLite plus any PostgreSQL DSN, schema, or timeout configuration;
- an invalid PostgreSQL schema identifier;
- non-positive or greater-than-300-second PostgreSQL timeout budgets.

## Bootstrap and startup

Install the optional driver before selecting PostgreSQL:

```bash
pip install -r requirements.txt -r requirements-postgres.txt
```

Preview the catalog without mutation:

```bash
python -m runtime_service.postgres_bootstrap --dry-run
```

Apply the bounded `execution-plane = 1` and `memory = 1` components through one designated release
task, then reread their authoritative metadata and table shape:

```bash
python -m runtime_service.postgres_bootstrap --apply
```

Repeated apply is idempotent. Incompatible versions and unversioned pre-existing component tables
remain fail-closed. Application workers do not call either initializer: `create_app()` validates
both version rows and expected table columns in a read-only transaction, constructs all stores with
`initialize=False`, and starts `RuntimeManager` only after validation succeeds.

For PostgreSQL composition, configure `RUNTIME_TRAVEL_ACTION_PROVIDER_URL` and its provider identity
or inject a provider in application code. The bundled `SQLiteTripHoldProvider` remains a local-demo
test double and is never selected implicitly by the PostgreSQL composition.

## Readiness and connection lifecycle

`GET /ready` pings all three selected stores and reports only safe metadata:

```json
{
  "status": "ready",
  "storage": {
    "backend": "postgres",
    "schema": "agent_runtime",
    "schema_versions": {"execution-plane": 1, "memory": 1},
    "connection_policy": {
      "mode": "short-lived-per-operation",
      "connect_timeout_seconds": 5.0,
      "statement_timeout_seconds": 30.0,
      "lock_timeout_seconds": 5.0,
      "lease_operation_timeout_seconds": 1.0
    }
  }
}
```

The DSN is never stored in application state or returned by readiness. This phase deliberately
keeps the already-proven open/use/close lifecycle instead of introducing a pool without a measured
need. Autocommit outside explicit transactions prevents ordinary reads from becoming idle in
transaction; PostgreSQL application tests verify no such session remains after restart/admin flows.

## Application proof and Cross-PR scan

`test_postgres_application_composition.py` runs against a real isolated PostgreSQL schema and
proves:

- dry-run is non-mutating, apply rereads postconditions, and repeat dry-run reports no change;
- startup rejects an uninitialized or incompatible authority instead of mutating it;
- Run, Workflow, and Memory stores share exactly one schema;
- readiness reports backend, versions, and connection policy without DSN leakage;
- the composed API processes a governed-Memory Run, restarts, recovers an abandoned Run, retrieves
  the stored preference, lists and forgets Memory, and creates no SQLite database file;
- the PostgreSQL composition cannot silently select the provider-side SQLite test double;
- no session remains idle in transaction after the proof.

The global property scan is:

| Property | Entry point | Deterministic enforcement | Evidence |
| --- | --- | --- | --- |
| One durable authority | `create_app()` | one resolved config and one three-store bundle | mixed-config unit tests; concrete store/schema assertions |
| Compatible schema before work | lifespan startup | read-only version/shape validator before `RuntimeManager.start()` | missing/incompatible tests; bootstrap command proof |
| Lease-gated Memory shares Run authority | governed runtime | same PostgreSQL DSN/schema constructed once | existing shared Memory contract plus composed API flow |
| Admin Memory does not force SQLite | `/memories` routes | backend-neutral `MemoryAdminStore` | PostgreSQL list/forget integration proof |
| Recovery does not cross backends | manager startup | PostgreSQL Run/Workflow/Memory bundle | abandoned-attempt restart recovery proof |

## Limits and rollback

- There is no online SQLite-to-PostgreSQL data migration. Select PostgreSQL only for a prepared
  schema and an intentionally empty or separately migrated environment.
- There is no connection pool, distributed queue, independent multi-process scheduling claim,
  cloud deployment, autoscaling, or HA claim in this phase.
- Rollback before PostgreSQL carries required data is configuration rollback to the unchanged
  SQLite default. Once PostgreSQL becomes authoritative, rollback requires a separately reviewed
  data/schema compatibility plan; this command does not copy data back to SQLite.
