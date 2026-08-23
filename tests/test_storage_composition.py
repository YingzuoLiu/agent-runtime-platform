from __future__ import annotations

from pathlib import Path

import pytest

from runtime_service.memory import SQLiteMemoryStore
from runtime_service.storage import (
    RuntimeStorageConfigurationError,
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import SQLiteWorkflowStore


def test_default_storage_is_one_sqlite_authority(tmp_path: Path) -> None:
    database_path = tmp_path / "runtime.db"
    config = resolve_runtime_storage_config(
        database_path=database_path,
        environment={},
    )
    bundle = build_runtime_store_bundle(config)

    assert config.backend == "sqlite"
    assert config.sqlite_path == database_path
    assert isinstance(bundle.run_store, SQLiteRunStore)
    assert isinstance(bundle.memory_store, SQLiteMemoryStore)
    assert isinstance(bundle.workflow_store, SQLiteWorkflowStore)
    assert bundle.run_store.database_path == str(database_path)
    assert bundle.memory_store.database_path == str(database_path)
    assert bundle.workflow_store.database_path == str(database_path)
    assert bundle.metadata.public_dict() == {
        "backend": "sqlite",
        "schema": None,
        "schema_versions": {},
        "connection_policy": {"mode": "short-lived-per-operation"},
    }


def test_postgres_configuration_is_secret_safe_and_bounded() -> None:
    secret_dsn = "postgresql://runtime:do-not-print@example.invalid/runtime"
    config = resolve_runtime_storage_config(
        environment={
            "RUNTIME_STORE_BACKEND": "postgres",
            "RUNTIME_POSTGRES_DSN": secret_dsn,
            "RUNTIME_POSTGRES_SCHEMA": "runtime_prod",
            "RUNTIME_POSTGRES_CONNECT_TIMEOUT_SECONDS": "3",
            "RUNTIME_POSTGRES_STATEMENT_TIMEOUT_SECONDS": "20",
            "RUNTIME_POSTGRES_LOCK_TIMEOUT_SECONDS": "4",
            "RUNTIME_POSTGRES_LEASE_OPERATION_TIMEOUT_SECONDS": "0.5",
        }
    )

    assert config.backend == "postgres"
    assert config.postgres_dsn == secret_dsn
    assert config.postgres_schema == "runtime_prod"
    assert config.connect_timeout_seconds == 3
    assert config.statement_timeout_seconds == 20
    assert config.lock_timeout_seconds == 4
    assert config.lease_operation_timeout_seconds == 0.5
    assert secret_dsn not in repr(config)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"backend": "mysql"},
        {"backend": "sqlite", "postgres_dsn": "postgresql://secret"},
        {
            "backend": "postgres",
            "postgres_dsn": "postgresql://secret",
            "database_path": "runtime.db",
        },
        {"backend": "postgres"},
        {
            "backend": "postgres",
            "postgres_dsn": "postgresql://secret",
            "postgres_schema": "INVALID-SCHEMA",
        },
        {
            "backend": "postgres",
            "postgres_dsn": "postgresql://secret",
            "postgres_connect_timeout_seconds": 0,
        },
    ],
)
def test_invalid_or_mixed_storage_configuration_fails_closed(kwargs) -> None:
    with pytest.raises((RuntimeStorageConfigurationError, ValueError)):
        resolve_runtime_storage_config(environment={}, **kwargs)


def test_environment_cannot_select_sqlite_with_postgres_settings() -> None:
    with pytest.raises(
        RuntimeStorageConfigurationError,
        match="cannot be combined with PostgreSQL configuration",
    ):
        resolve_runtime_storage_config(
            environment={
                "RUNTIME_STORE_BACKEND": "sqlite",
                "RUNTIME_POSTGRES_SCHEMA": "agent_runtime",
            }
        )


def test_environment_cannot_select_postgres_with_sqlite_path() -> None:
    with pytest.raises(
        RuntimeStorageConfigurationError,
        match="cannot be combined with SQLite database-path configuration",
    ):
        resolve_runtime_storage_config(
            environment={
                "RUNTIME_STORE_BACKEND": "postgres",
                "RUNTIME_POSTGRES_DSN": "postgresql://secret",
                "RUNTIME_DB_PATH": "runtime.db",
            }
        )
