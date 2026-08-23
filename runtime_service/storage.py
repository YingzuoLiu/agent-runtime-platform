from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast

from pydantic import SecretStr

from .memory import RuntimeMemoryStore, SQLiteMemoryStore
from .run_store import RunStore
from .store import SQLiteRunStore
from .workflow_store import SQLiteWorkflowStore, WorkflowStore


RuntimeStoreBackend = Literal["sqlite", "postgres"]


class RuntimeStorageConfigurationError(ValueError):
    """Raised before startup when durable authority configuration is incoherent."""


@dataclass(frozen=True, slots=True)
class RuntimeStorageConfig:
    """One complete durable authority selection with secret-safe representation."""

    backend: RuntimeStoreBackend
    sqlite_path: Path | None = None
    postgres_dsn: SecretStr | None = field(default=None, repr=False)
    postgres_schema: str | None = None
    connect_timeout_seconds: float | None = None
    statement_timeout_seconds: float | None = None
    lock_timeout_seconds: float | None = None
    idle_in_transaction_session_timeout_seconds: float | None = None
    lease_operation_timeout_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStorageMetadata:
    backend: RuntimeStoreBackend
    schema: str | None
    schema_versions: dict[str, int]
    connection_policy: dict[str, float | str]

    def public_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "schema": self.schema,
            "schema_versions": dict(self.schema_versions),
            "connection_policy": dict(self.connection_policy),
        }


@dataclass(frozen=True, slots=True)
class RuntimeStoreBundle:
    run_store: RunStore
    memory_store: RuntimeMemoryStore
    workflow_store: WorkflowStore
    metadata: RuntimeStorageMetadata


def _environment_value(environment: Mapping[str, str], name: str) -> str | None:
    value = environment.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _resolved_seconds(
    explicit: float | None,
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    if explicit is None:
        raw = _environment_value(environment, name)
        if raw is None:
            value = default
        else:
            try:
                value = float(raw)
            except ValueError as exc:
                raise RuntimeStorageConfigurationError(
                    f"{name} must be a number of seconds"
                ) from exc
    else:
        value = explicit
    if not 0 < value <= 300:
        raise RuntimeStorageConfigurationError(
            f"{name} must be greater than 0 and at most 300 seconds"
        )
    return value


def resolve_runtime_storage_config(
    *,
    backend: str | None = None,
    database_path: str | Path | None = None,
    postgres_dsn: str | None = None,
    postgres_schema: str | None = None,
    postgres_connect_timeout_seconds: float | None = None,
    postgres_statement_timeout_seconds: float | None = None,
    postgres_lock_timeout_seconds: float | None = None,
    postgres_idle_in_transaction_session_timeout_seconds: float | None = None,
    postgres_lease_operation_timeout_seconds: float | None = None,
    environment: Mapping[str, str] | None = None,
) -> RuntimeStorageConfig:
    """Resolve one backend and reject every mixed durable configuration."""

    values = os.environ if environment is None else environment
    selected = (backend or _environment_value(values, "RUNTIME_STORE_BACKEND") or "sqlite")
    selected = selected.strip().lower()
    if selected not in {"sqlite", "postgres"}:
        raise RuntimeStorageConfigurationError(
            "RUNTIME_STORE_BACKEND must be 'sqlite' or 'postgres'"
        )

    environment_database_path = _environment_value(values, "RUNTIME_DB_PATH")
    environment_postgres_dsn = _environment_value(values, "RUNTIME_POSTGRES_DSN")
    environment_postgres_schema = _environment_value(
        values, "RUNTIME_POSTGRES_SCHEMA"
    )
    postgres_environment_names = (
        "RUNTIME_POSTGRES_CONNECT_TIMEOUT_SECONDS",
        "RUNTIME_POSTGRES_STATEMENT_TIMEOUT_SECONDS",
        "RUNTIME_POSTGRES_LOCK_TIMEOUT_SECONDS",
        "RUNTIME_POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_SECONDS",
        "RUNTIME_POSTGRES_LEASE_OPERATION_TIMEOUT_SECONDS",
    )
    has_postgres_timeout_environment = any(
        _environment_value(values, name) is not None
        for name in postgres_environment_names
    )
    has_explicit_postgres_timeout = any(
        value is not None
        for value in (
            postgres_connect_timeout_seconds,
            postgres_statement_timeout_seconds,
            postgres_lock_timeout_seconds,
            postgres_idle_in_transaction_session_timeout_seconds,
            postgres_lease_operation_timeout_seconds,
        )
    )

    if selected == "sqlite":
        if (
            postgres_dsn is not None
            or postgres_schema is not None
            or environment_postgres_dsn is not None
            or environment_postgres_schema is not None
            or has_explicit_postgres_timeout
            or has_postgres_timeout_environment
        ):
            raise RuntimeStorageConfigurationError(
                "SQLite storage cannot be combined with PostgreSQL configuration"
            )
        path_value = database_path or environment_database_path or "runtime_data/runtime.db"
        return RuntimeStorageConfig(
            backend="sqlite",
            sqlite_path=Path(path_value),
        )

    if database_path is not None or environment_database_path is not None:
        raise RuntimeStorageConfigurationError(
            "PostgreSQL storage cannot be combined with SQLite database-path configuration"
        )
    resolved_dsn = postgres_dsn or environment_postgres_dsn
    if resolved_dsn is None or not resolved_dsn.strip():
        raise RuntimeStorageConfigurationError(
            "RUNTIME_POSTGRES_DSN is required when PostgreSQL storage is selected"
        )
    resolved_schema = postgres_schema or environment_postgres_schema or "agent_runtime"
    from .postgres_schema import validate_postgres_schema_name

    validate_postgres_schema_name(resolved_schema)
    connect_timeout_seconds = _resolved_seconds(
        postgres_connect_timeout_seconds,
        values,
        "RUNTIME_POSTGRES_CONNECT_TIMEOUT_SECONDS",
        5.0,
    )
    statement_timeout_seconds = _resolved_seconds(
        postgres_statement_timeout_seconds,
        values,
        "RUNTIME_POSTGRES_STATEMENT_TIMEOUT_SECONDS",
        30.0,
    )
    lock_timeout_seconds = _resolved_seconds(
        postgres_lock_timeout_seconds,
        values,
        "RUNTIME_POSTGRES_LOCK_TIMEOUT_SECONDS",
        5.0,
    )
    idle_in_transaction_session_timeout_seconds = _resolved_seconds(
        postgres_idle_in_transaction_session_timeout_seconds,
        values,
        "RUNTIME_POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_SECONDS",
        5.0,
    )
    lease_operation_timeout_seconds = _resolved_seconds(
        postgres_lease_operation_timeout_seconds,
        values,
        "RUNTIME_POSTGRES_LEASE_OPERATION_TIMEOUT_SECONDS",
        1.0,
    )
    if (
        idle_in_transaction_session_timeout_seconds
        <= lease_operation_timeout_seconds
    ):
        raise RuntimeStorageConfigurationError(
            "RUNTIME_POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_SECONDS "
            "must be greater than "
            "RUNTIME_POSTGRES_LEASE_OPERATION_TIMEOUT_SECONDS"
        )
    return RuntimeStorageConfig(
        backend=cast(RuntimeStoreBackend, selected),
        postgres_dsn=SecretStr(resolved_dsn),
        postgres_schema=resolved_schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            idle_in_transaction_session_timeout_seconds
        ),
        lease_operation_timeout_seconds=lease_operation_timeout_seconds,
    )


def build_runtime_store_bundle(config: RuntimeStorageConfig) -> RuntimeStoreBundle:
    """Construct all stores from one validated authority selection."""

    if config.backend == "sqlite":
        if config.sqlite_path is None:
            raise RuntimeStorageConfigurationError("SQLite database path is missing")
        return RuntimeStoreBundle(
            run_store=SQLiteRunStore(config.sqlite_path),
            memory_store=SQLiteMemoryStore(config.sqlite_path),
            workflow_store=SQLiteWorkflowStore(config.sqlite_path),
            metadata=RuntimeStorageMetadata(
                backend="sqlite",
                schema=None,
                schema_versions={},
                connection_policy={"mode": "short-lived-per-operation"},
            ),
        )

    if config.postgres_dsn is None or config.postgres_schema is None:
        raise RuntimeStorageConfigurationError("PostgreSQL storage configuration is incomplete")
    assert config.connect_timeout_seconds is not None
    assert config.statement_timeout_seconds is not None
    assert config.lock_timeout_seconds is not None
    assert config.idle_in_transaction_session_timeout_seconds is not None
    assert config.lease_operation_timeout_seconds is not None

    from .postgres_memory_store import PostgresMemoryStore
    from .postgres_schema import validate_postgres_application_schema
    from .postgres_store import PostgresRunStore
    from .postgres_workflow_store import PostgresWorkflowStore

    dsn = config.postgres_dsn.get_secret_value()
    versions = validate_postgres_application_schema(
        dsn,
        schema=config.postgres_schema,
        connect_timeout_seconds=config.connect_timeout_seconds,
        statement_timeout_seconds=config.statement_timeout_seconds,
        lock_timeout_seconds=config.lock_timeout_seconds,
        idle_in_transaction_session_timeout_seconds=(
            config.idle_in_transaction_session_timeout_seconds
        ),
    )
    return RuntimeStoreBundle(
        run_store=PostgresRunStore(
            dsn,
            schema=config.postgres_schema,
            lease_operation_timeout_seconds=config.lease_operation_timeout_seconds,
            connect_timeout_seconds=config.connect_timeout_seconds,
            statement_timeout_seconds=config.statement_timeout_seconds,
            lock_timeout_seconds=config.lock_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                config.idle_in_transaction_session_timeout_seconds
            ),
            initialize=False,
        ),
        memory_store=PostgresMemoryStore(
            dsn,
            schema=config.postgres_schema,
            connect_timeout_seconds=config.connect_timeout_seconds,
            statement_timeout_seconds=config.statement_timeout_seconds,
            lock_timeout_seconds=config.lock_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                config.idle_in_transaction_session_timeout_seconds
            ),
            initialize=False,
        ),
        workflow_store=PostgresWorkflowStore(
            dsn,
            schema=config.postgres_schema,
            connect_timeout_seconds=config.connect_timeout_seconds,
            statement_timeout_seconds=config.statement_timeout_seconds,
            lock_timeout_seconds=config.lock_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                config.idle_in_transaction_session_timeout_seconds
            ),
            initialize=False,
        ),
        metadata=RuntimeStorageMetadata(
            backend="postgres",
            schema=config.postgres_schema,
            schema_versions=versions,
            connection_policy={
                "mode": "short-lived-per-operation",
                "connect_timeout_seconds": config.connect_timeout_seconds,
                "statement_timeout_seconds": config.statement_timeout_seconds,
                "lock_timeout_seconds": config.lock_timeout_seconds,
                "idle_in_transaction_session_timeout_seconds": (
                    config.idle_in_transaction_session_timeout_seconds
                ),
                "lease_operation_timeout_seconds": (
                    config.lease_operation_timeout_seconds
                ),
            },
        ),
    )
