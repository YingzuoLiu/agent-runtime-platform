from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.connection import Connection


POSTGRES_SCHEMA_VERSION = 1
_SCHEMA_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class PostgresSchemaError(RuntimeError):
    """Raised when an execution-plane schema cannot be used safely."""


def validate_postgres_schema_name(schema: str) -> str:
    """Accept only generated/reviewed simple PostgreSQL schema identifiers.

    Schema names cannot be passed as query parameters. Keeping the accepted
    alphabet narrow makes test-schema lifecycle explicit; SQL composition
    still uses ``Identifier`` so no caller-controlled string is interpolated.
    """

    if not _SCHEMA_NAME.fullmatch(schema):
        raise ValueError(
            "PostgreSQL schema must match ^[a-z][a-z0-9_]{0,62}$"
        )
    return schema


def open_postgres_connection(
    dsn: str,
    *,
    schema: str,
    connect_timeout_seconds: float = 30,
    statement_timeout_seconds: float | None = None,
    lock_timeout_seconds: float | None = None,
) -> Connection[dict[str, object]]:
    """Open one explicit autocommit connection scoped to ``schema``.

    Store methods use ``connection.transaction()`` for write/read snapshots.
    Autocommit outside those contexts prevents a returned connection from
    being left idle in transaction. No DSN is included in raised messages.
    """

    validate_postgres_schema_name(schema)
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
) -> Iterator[Connection[dict[str, object]]]:
    connection = open_postgres_connection(
        dsn,
        schema=schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
    )
    try:
        yield connection
    finally:
        connection.close()


def initialize_postgres_schema(dsn: str, *, schema: str) -> None:
    """Create or validate the explicit v1 PostgreSQL execution-plane schema.

    The first version intentionally uses one reviewable bootstrap instead of a
    migration framework. Run, checkpoint, Workflow, Tool, Action, and event
    tables share one PostgreSQL schema so recovery and operator repair can read
    and mutate their evidence in a single database transaction.
    """

    validate_postgres_schema_name(schema)
    # The schema itself has to exist before search_path can target it.
    try:
        bootstrap = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
    except psycopg.Error as exc:
        raise PostgresSchemaError("PostgreSQL bootstrap connection failed") from exc
    try:
        bootstrap.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
    finally:
        bootstrap.close()

    with postgres_connection(dsn, schema=schema) as connection:
        try:
            with connection.transaction():
                # Serialize bootstrap for the same schema without introducing a
                # process lock. hashtextextended is server-local and used only
                # for short DDL initialization, never for runtime ownership.
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"agent-runtime-postgres-schema:{schema}",),
                )
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

                # A v1 metadata marker is written only after every table/index
                # statement above succeeds in this transaction. Reopening an
                # incompatible manually-created table therefore fails before a
                # schema can be marked compatible.
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
