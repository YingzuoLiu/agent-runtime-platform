from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import psycopg
from psycopg import sql
from psycopg.connection import Connection
from psycopg.rows import dict_row


POSTGRES_SCHEMA_VERSION = 1
POSTGRES_MEMORY_SCHEMA_VERSION = 1
_SCHEMA_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_CORE_TABLES = (
    "runs",
    "run_events",
    "thread_states",
    "workflow_executions",
    "tool_calls",
    "external_actions",
    "workflow_events",
)
_MEMORY_TABLES = (
    "memory_records",
    "memory_events",
    "run_memory_snapshots",
)


class PostgresSchemaError(RuntimeError):
    """Raised when an execution-plane schema cannot be used safely."""


@dataclass(frozen=True, slots=True)
class PostgresApplicationSchemaStatus:
    """Value-free catalog view used by dry-run and startup validation."""

    schema: str
    schema_exists: bool
    metadata_exists: bool
    components: dict[str, int]

    @property
    def compatible(self) -> bool:
        return (
            self.components.get("execution-plane") == POSTGRES_SCHEMA_VERSION
            and self.components.get("memory") == POSTGRES_MEMORY_SCHEMA_VERSION
        )


def validate_postgres_schema_name(schema: str) -> str:
    """Accept only generated/reviewed simple PostgreSQL schema identifiers.

    Schema names cannot be passed as query parameters. Keeping the accepted
    alphabet narrow makes test-schema lifecycle explicit; SQL composition
    still uses ``Identifier`` so no caller-controlled string is interpolated.
    """

    if not _SCHEMA_NAME.fullmatch(schema):
        raise ValueError("PostgreSQL schema must match ^[a-z][a-z0-9_]{0,62}$")
    return schema


def open_postgres_connection(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> Connection[dict[str, object]]:
    """Open one explicit autocommit connection scoped to ``schema``.

    Store methods use ``connection.transaction()`` for write/read snapshots.
    Autocommit outside those contexts prevents a returned connection from
    being left idle in transaction. No DSN is included in raised messages.
    """

    validate_postgres_schema_name(schema)
    if idle_in_transaction_session_timeout_seconds <= 0:
        raise ValueError(
            "idle_in_transaction_session_timeout_seconds must be positive"
        )
    timeout = max(1, int(connect_timeout_seconds + 0.999))
    try:
        connection = psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=timeout,
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        if statement_timeout_seconds is not None:
            milliseconds = max(1, int(statement_timeout_seconds * 1000))
            connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{milliseconds}ms",),
            )
        if lock_timeout_seconds is not None:
            milliseconds = max(1, int(lock_timeout_seconds * 1000))
            connection.execute(
                "SELECT set_config('lock_timeout', %s, false)",
                (f"{milliseconds}ms",),
            )
        idle_milliseconds = max(
            1,
            int(idle_in_transaction_session_timeout_seconds * 1000),
        )
        connection.execute(
            "SELECT set_config('idle_in_transaction_session_timeout', %s, false)",
            (f"{idle_milliseconds}ms",),
        )
        return connection
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL connection setup failed") from exc


@contextmanager
def postgres_connection(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> Iterator[Connection[dict[str, object]]]:
    connection = open_postgres_connection(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    )
    try:
        yield connection
    finally:
        connection.close()


def initialize_postgres_schema(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> None:
    """Create or validate the explicit v1 PostgreSQL execution-plane schema.

    The first version intentionally uses one reviewable bootstrap instead of a
    migration framework. Run, checkpoint, Workflow, Tool, Action, and event
    tables share one PostgreSQL schema so recovery and operator repair can read
    and mutate their evidence in a single database transaction.
    """

    validate_postgres_schema_name(schema)
    timeout = max(1, int(connect_timeout_seconds + 0.999))
    try:
        bootstrap = psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=timeout,
        )
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL bootstrap connection failed") from exc
    try:
        if statement_timeout_seconds is not None:
            milliseconds = max(1, int(statement_timeout_seconds * 1000))
            bootstrap.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{milliseconds}ms",),
            )
        if lock_timeout_seconds is not None:
            milliseconds = max(1, int(lock_timeout_seconds * 1000))
            bootstrap.execute(
                "SELECT set_config('lock_timeout', %s, false)",
                (f"{milliseconds}ms",),
            )
        bootstrap.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL schema bootstrap failed") from exc
    finally:
        bootstrap.close()

    with postgres_connection(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    ) as connection:
        try:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"agent-runtime-postgres-schema:{schema}",),
                )
                existing_rows = connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                        AND table_name = ANY(%s)
                    """,
                    (list(_CORE_TABLES),),
                ).fetchall()
                existing_core_tables = {str(row["table_name"]) for row in existing_rows}

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runtime_store_schema (
                        component TEXT PRIMARY KEY,
                        version INTEGER NOT NULL
                    )
                    """
                )
                metadata = connection.execute(
                    "SELECT version FROM runtime_store_schema WHERE component = %s",
                    ("execution-plane",),
                ).fetchone()
                if metadata is None and existing_core_tables:
                    raise PostgresSchemaError(
                        "PostgreSQL execution-plane schema is unversioned and cannot be adopted"
                    )
                if metadata is not None and int(metadata["version"]) != POSTGRES_SCHEMA_VERSION:
                    raise PostgresSchemaError(
                        "PostgreSQL execution-plane schema version is incompatible"
                    )

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        tenant_id TEXT NOT NULL,
                        execution_authority_json TEXT,
                        thread_id TEXT NOT NULL,
                        agent_id TEXT NOT NULL,
                        agent_version TEXT NOT NULL,
                        domain_id TEXT NOT NULL DEFAULT 'travel',
                        schema_version TEXT NOT NULL DEFAULT '1',
                        status TEXT NOT NULL,
                        input_message TEXT NOT NULL,
                        input_json TEXT,
                        state_json TEXT,
                        output_message TEXT,
                        validation_errors_json TEXT NOT NULL,
                        error_code TEXT,
                        error TEXT,
                        attempt INTEGER NOT NULL,
                        cancel_requested BOOLEAN NOT NULL,
                        client_request_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        lease_owner_id TEXT,
                        lease_token TEXT,
                        lease_heartbeat_at BIGINT,
                        lease_expires_at BIGINT,
                        checkpoint_base_revision INTEGER
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS run_events (
                        event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                        sequence INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        UNIQUE(run_id, sequence)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS thread_states (
                        tenant_id TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        domain_id TEXT NOT NULL DEFAULT 'travel',
                        schema_version TEXT NOT NULL DEFAULT '1',
                        state_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY(tenant_id, thread_id),
                        CHECK (revision >= 1)
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_tenant_thread_id "
                    "ON runs(tenant_id, thread_id)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_claimable "
                    "ON runs(status, lease_expires_at, created_at)"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_running_per_thread "
                    "ON runs(tenant_id, thread_id) WHERE status = 'running'"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_tenant_client_request_id "
                    "ON runs(tenant_id, client_request_id) "
                    "WHERE client_request_id IS NOT NULL"
                )

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workflow_executions (
                        run_id TEXT PRIMARY KEY,
                        workflow_type TEXT NOT NULL,
                        input_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        result_json TEXT,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        completed_at TEXT
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tool_calls (
                        call_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES workflow_executions(run_id) ON DELETE CASCADE,
                        step_id TEXT NOT NULL,
                        tool_name TEXT NOT NULL,
                        input_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        attempt_token TEXT,
                        result_json TEXT,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(run_id, step_id)
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tool_calls_run_id ON tool_calls(run_id)"
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS external_actions (
                        action_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        workflow_type TEXT NOT NULL,
                        tool_name TEXT NOT NULL,
                        provider_name TEXT NOT NULL,
                        provider_identity TEXT NOT NULL,
                        input_hash TEXT NOT NULL,
                        arguments_json TEXT NOT NULL,
                        retry_mode TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL,
                        dispatch_count INTEGER NOT NULL DEFAULT 0,
                        dispatch_token TEXT,
                        provider_reference TEXT,
                        result_json TEXT,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        dispatched_at TEXT,
                        finalized_at TEXT,
                        UNIQUE(run_id, step_id),
                        FOREIGN KEY(run_id, step_id)
                            REFERENCES tool_calls(run_id, step_id) ON DELETE CASCADE
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_external_actions_run_id "
                    "ON external_actions(run_id)"
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workflow_events (
                        event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES workflow_executions(run_id) ON DELETE CASCADE,
                        sequence INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        UNIQUE(run_id, sequence)
                    )
                    """
                )

                if metadata is None:
                    connection.execute(
                        """
                        INSERT INTO runtime_store_schema(component, version)
                        VALUES (%s, %s)
                        ON CONFLICT (component) DO NOTHING
                        """,
                        ("execution-plane", POSTGRES_SCHEMA_VERSION),
                    )
                    metadata = connection.execute(
                        "SELECT version FROM runtime_store_schema WHERE component = %s",
                        ("execution-plane",),
                    ).fetchone()
                    if metadata is None or int(metadata["version"]) != POSTGRES_SCHEMA_VERSION:
                        raise PostgresSchemaError(
                            "PostgreSQL execution-plane schema initialization raced incompatibly"
                        )

                _assert_v1_schema_shape(connection)
        except PostgresSchemaError:
            raise
        except psycopg.Error as exc:
            raise PostgresSchemaError(
                "PostgreSQL execution-plane schema initialization failed"
            ) from exc


def initialize_postgres_memory_schema(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> None:
    """Install or validate the versioned Memory component beside schema v1.

    Memory is an explicit component rather than a silent redefinition of the
    accepted Run/Workflow execution-plane v1. The component still shares the
    same PostgreSQL schema and ``runs`` authority so a governed Memory write
    can validate its lease and commit under one database transaction.
    """

    validate_postgres_schema_name(schema)
    initialize_postgres_schema(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    )
    with postgres_connection(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    ) as connection:
        try:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"agent-runtime-postgres-memory-schema:{schema}",),
                )
                existing_rows = connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                        AND table_name = ANY(%s)
                    """,
                    (list(_MEMORY_TABLES),),
                ).fetchall()
                existing_memory_tables = {
                    str(row["table_name"]) for row in existing_rows
                }
                metadata = connection.execute(
                    "SELECT version FROM runtime_store_schema WHERE component = %s",
                    ("memory",),
                ).fetchone()
                if metadata is None and existing_memory_tables:
                    raise PostgresSchemaError(
                        "PostgreSQL Memory schema is unversioned and cannot be adopted"
                    )
                if (
                    metadata is not None
                    and int(metadata["version"]) != POSTGRES_MEMORY_SCHEMA_VERSION
                ):
                    raise PostgresSchemaError(
                        "PostgreSQL Memory schema version is incompatible"
                    )

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS memory_records (
                        memory_id TEXT PRIMARY KEY,
                        tenant_id TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        domain_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        memory_key TEXT NOT NULL,
                        value_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        source_run_id TEXT,
                        source_thread_id TEXT,
                        confidence DOUBLE PRECISION NOT NULL,
                        version INTEGER NOT NULL CHECK (version >= 1),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        expires_at TEXT,
                        UNIQUE (
                            tenant_id, subject_id, domain_id, kind, memory_key, version
                        )
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS memory_events (
                        event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        tenant_id TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        memory_id TEXT,
                        actor_subject_id TEXT NOT NULL,
                        source_run_id TEXT,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS run_memory_snapshots (
                        run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
                        tenant_id TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        domain_id TEXT NOT NULL,
                        memories_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_records_active_key
                    ON memory_records(tenant_id, subject_id, domain_id, kind, memory_key)
                    WHERE status = 'active'
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_memory_records_subject
                    ON memory_records(tenant_id, subject_id, domain_id, status)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_memory_events_subject
                    ON memory_events(tenant_id, subject_id, event_id)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_memory_events_source_run
                    ON memory_events(source_run_id, event_id)
                    WHERE source_run_id IS NOT NULL
                    """
                )

                if metadata is None:
                    connection.execute(
                        """
                        INSERT INTO runtime_store_schema(component, version)
                        VALUES (%s, %s)
                        ON CONFLICT (component) DO NOTHING
                        """,
                        ("memory", POSTGRES_MEMORY_SCHEMA_VERSION),
                    )
                    metadata = connection.execute(
                        "SELECT version FROM runtime_store_schema WHERE component = %s",
                        ("memory",),
                    ).fetchone()
                    if (
                        metadata is None
                        or int(metadata["version"]) != POSTGRES_MEMORY_SCHEMA_VERSION
                    ):
                        raise PostgresSchemaError(
                            "PostgreSQL Memory schema initialization raced incompatibly"
                        )

                _assert_memory_v1_schema_shape(connection)
        except PostgresSchemaError:
            raise
        except psycopg.Error as exc:
            raise PostgresSchemaError(
                "PostgreSQL Memory schema initialization failed"
            ) from exc


def inspect_postgres_application_schema(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
) -> PostgresApplicationSchemaStatus:
    """Inspect catalog metadata without creating or modifying schema objects."""

    validate_postgres_schema_name(schema)
    timeout = max(1, int(connect_timeout_seconds + 0.999))
    try:
        connection = psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=timeout,
        )
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL schema inspection connection failed") from exc
    try:
        schema_row = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s) "
            "AS present",
            (schema,),
        ).fetchone()
        schema_exists = bool(schema_row and schema_row["present"])
        if not schema_exists:
            return PostgresApplicationSchemaStatus(
                schema=schema,
                schema_exists=False,
                metadata_exists=False,
                components={},
            )

        metadata_row = connection.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = 'runtime_store_schema'
            ) AS present
            """,
            (schema,),
        ).fetchone()
        metadata_exists = bool(metadata_row and metadata_row["present"])
        if not metadata_exists:
            return PostgresApplicationSchemaStatus(
                schema=schema,
                schema_exists=True,
                metadata_exists=False,
                components={},
            )

        rows = connection.execute(
            sql.SQL("SELECT component, version FROM {}.runtime_store_schema").format(
                sql.Identifier(schema)
            )
        ).fetchall()
        return PostgresApplicationSchemaStatus(
            schema=schema,
            schema_exists=True,
            metadata_exists=True,
            components={str(row["component"]): int(row["version"]) for row in rows},
        )
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL schema inspection failed") from exc
    finally:
        connection.close()


def validate_postgres_application_schema(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> dict[str, int]:
    """Validate the complete application schema without mutating it."""

    status = inspect_postgres_application_schema(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
    )
    if not status.schema_exists or not status.metadata_exists:
        raise PostgresSchemaError(
            "PostgreSQL application schema is not initialized; run the bootstrap command"
        )

    with postgres_connection(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    ) as connection:
        try:
            with connection.transaction():
                connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                current = connection.execute(
                    "SELECT current_schema() AS schema"
                ).fetchone()
                if current is None or current["schema"] != schema:
                    raise PostgresSchemaError(
                        "PostgreSQL application schema is not initialized; "
                        "run the bootstrap command"
                    )
                rows = connection.execute(
                    "SELECT component, version FROM runtime_store_schema"
                ).fetchall()
                components = {
                    str(row["component"]): int(row["version"]) for row in rows
                }
                if components.get("execution-plane") != POSTGRES_SCHEMA_VERSION:
                    raise PostgresSchemaError(
                        "PostgreSQL execution-plane schema version is incompatible"
                    )
                if components.get("memory") != POSTGRES_MEMORY_SCHEMA_VERSION:
                    raise PostgresSchemaError(
                        "PostgreSQL Memory schema version is incompatible"
                    )
                _assert_v1_schema_shape(connection)
                _assert_memory_v1_schema_shape(connection)
        except PostgresSchemaError:
            raise
        except psycopg.Error as exc:
            raise PostgresSchemaError(
                "PostgreSQL application schema validation failed"
            ) from exc
    return {
        "execution-plane": int(components["execution-plane"]),
        "memory": int(components["memory"]),
    }


def bootstrap_postgres_application_schema(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
    idle_in_transaction_session_timeout_seconds: float = 5.0,
) -> dict[str, int]:
    """Apply the bounded v1 components and reread their authoritative shape."""

    initialize_postgres_memory_schema(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    )
    return validate_postgres_application_schema(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
    )


def _assert_v1_schema_shape(connection: Connection[dict[str, object]]) -> None:
    """Make CREATE IF NOT EXISTS fail closed for incompatible pre-existing tables."""

    probes = {
        "runs": (
            "run_id, tenant_id, execution_authority_json, thread_id, agent_id, "
            "agent_version, domain_id, schema_version, status, input_message, "
            "input_json, state_json, output_message, validation_errors_json, "
            "error_code, error, attempt, cancel_requested, client_request_id, "
            "created_at, updated_at, started_at, completed_at, lease_owner_id, "
            "lease_token, lease_heartbeat_at, lease_expires_at, checkpoint_base_revision"
        ),
        "run_events": "event_id, run_id, sequence, event_type, payload_json, created_at",
        "thread_states": (
            "tenant_id, thread_id, domain_id, schema_version, state_json, updated_at, revision"
        ),
        "workflow_executions": (
            "run_id, workflow_type, input_hash, status, result_json, error_code, "
            "created_at, updated_at, completed_at"
        ),
        "tool_calls": (
            "call_id, run_id, step_id, tool_name, input_hash, status, attempt_count, "
            "attempt_token, result_json, error_code, created_at, updated_at"
        ),
        "external_actions": (
            "action_id, run_id, step_id, tenant_id, subject_id, workflow_type, "
            "tool_name, provider_name, provider_identity, input_hash, arguments_json, "
            "retry_mode, idempotency_key, status, dispatch_count, dispatch_token, "
            "provider_reference, result_json, error_code, created_at, updated_at, "
            "dispatched_at, finalized_at"
        ),
        "workflow_events": "event_id, run_id, sequence, event_type, payload_json, created_at",
    }
    try:
        for table, columns in probes.items():
            connection.execute(
                sql.SQL("SELECT {} FROM {} WHERE FALSE").format(
                    sql.SQL(columns),
                    sql.Identifier(table),
                )
            )
    except psycopg.Error as exc:
        raise PostgresSchemaError(
            "PostgreSQL execution-plane schema shape is incompatible with v1"
        ) from exc


def _assert_memory_v1_schema_shape(
    connection: Connection[dict[str, object]],
) -> None:
    probes = {
        "memory_records": (
            "memory_id, tenant_id, subject_id, domain_id, kind, memory_key, "
            "value_json, status, source_run_id, source_thread_id, confidence, "
            "version, created_at, updated_at, expires_at"
        ),
        "memory_events": (
            "event_id, tenant_id, subject_id, event_type, memory_id, "
            "actor_subject_id, source_run_id, payload_json, created_at"
        ),
        "run_memory_snapshots": (
            "run_id, tenant_id, subject_id, domain_id, memories_json, created_at"
        ),
    }
    try:
        for table, columns in probes.items():
            connection.execute(
                sql.SQL("SELECT {} FROM {} WHERE FALSE").format(
                    sql.SQL(columns),
                    sql.Identifier(table),
                )
            )
    except psycopg.Error as exc:
        raise PostgresSchemaError(
            "PostgreSQL Memory schema shape is incompatible with v1"
        ) from exc
