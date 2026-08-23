from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import psycopg
from psycopg import sql

from agent.contracts import BaseRuntimeState
from runtime_service.postgres_schema import (
    open_postgres_connection,
    validate_postgres_schema_name,
)
from runtime_service.postgres_store import PostgresRunStore
from runtime_service.postgres_workflow_store import PostgresWorkflowStore
from runtime_service.registry import AgentRegistry, build_default_registry
from runtime_service.run_store import RunStore
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStore


class InjectedConformanceFailure(RuntimeError):
    """Stable failure used to prove transactional rollback at event boundaries."""


class _FailingCheckpointConnection:
    """Test-only SQLite connection proxy that fails at the checkpoint write boundary."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._connection.__exit__(exc_type, exc_value, traceback)

    def execute(self, sql_text: str, parameters=()):
        if "INSERT INTO thread_states" in sql_text:
            raise InjectedConformanceFailure("injected checkpoint write failure")
        return self._connection.execute(sql_text, parameters)

    def __getattr__(self, name: str):
        return getattr(self._connection, name)


class AdjustableStoreClock(Protocol):
    def __call__(self) -> int: ...

    def advance(self, delta_ms: int) -> int: ...


class StoreConformanceBackend(Protocol):
    """Test-only resource/fault seam used by backend-independent scenarios."""

    backend_id: str
    clock: AdjustableStoreClock

    def open_workflow_store(self) -> WorkflowStore: ...

    def open_run_store(
        self,
        *,
        state_registry: AgentRegistry | None = None,
        bind_default_registry: bool = True,
    ) -> RunStore: ...

    def fail_workflow_event(
        self,
        store: WorkflowStore,
        event_type: str,
    ) -> AbstractContextManager[None]: ...

    def fail_run_event(
        self,
        store: RunStore,
        event_type: str,
    ) -> AbstractContextManager[None]: ...

    def fail_checkpoint_write(
        self,
        store: RunStore,
    ) -> AbstractContextManager[None]: ...

    def replace_checkpoint_out_of_band(
        self,
        *,
        tenant_id: str,
        thread_id: str,
        state: BaseRuntimeState,
        expected_revision: int,
    ) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class SQLiteConformanceBackend:
    """Bind the shared semantic scenarios to the SQLite reference stores."""

    database_path: Path
    clock: AdjustableStoreClock
    backend_id: str = "sqlite"

    def open_workflow_store(self) -> WorkflowStore:
        return SQLiteWorkflowStore(self.database_path)

    def open_sqlite_workflow_store(self) -> SQLiteWorkflowStore:
        return SQLiteWorkflowStore(self.database_path)

    def open_run_store(
        self,
        *,
        state_registry: AgentRegistry | None = None,
        bind_default_registry: bool = True,
    ) -> SQLiteRunStore:
        registry = (
            state_registry
            if state_registry is not None or not bind_default_registry
            else build_default_registry()
        )
        return SQLiteRunStore(
            self.database_path,
            state_registry=registry,
            lease_clock_ms=self.clock,
        )

    @contextmanager
    def fail_workflow_event(
        self,
        store: WorkflowStore,
        event_type: str,
    ) -> Iterator[None]:
        if not isinstance(store, SQLiteWorkflowStore):
            raise TypeError("SQLite event injection requires SQLiteWorkflowStore")
        original = store._append_event_with_connection

        def injected(
            connection: sqlite3.Connection,
            run_id: str,
            actual_event_type: str,
            payload: dict[str, Any] | None = None,
        ):
            if actual_event_type == event_type:
                raise InjectedConformanceFailure(
                    f"injected workflow event failure: {event_type}"
                )
            return original(connection, run_id, actual_event_type, payload)

        store._append_event_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._append_event_with_connection = original  # type: ignore[method-assign]

    @contextmanager
    def fail_run_event(
        self,
        store: RunStore,
        event_type: str,
    ) -> Iterator[None]:
        if not isinstance(store, SQLiteRunStore):
            raise TypeError("SQLite event injection requires SQLiteRunStore")
        original = store._append_event_with_connection

        def injected(
            connection: sqlite3.Connection,
            run_id: str,
            actual_event_type: str,
            payload: dict[str, Any] | None = None,
        ):
            if actual_event_type == event_type:
                raise InjectedConformanceFailure(f"injected run event failure: {event_type}")
            return original(connection, run_id, actual_event_type, payload)

        store._append_event_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._append_event_with_connection = original  # type: ignore[method-assign]

    @contextmanager
    def fail_checkpoint_write(self, store: RunStore) -> Iterator[None]:
        if not isinstance(store, SQLiteRunStore):
            raise TypeError("SQLite checkpoint injection requires SQLiteRunStore")
        original = store._lease_connect

        def injected():
            return _FailingCheckpointConnection(original())

        store._lease_connect = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._lease_connect = original  # type: ignore[method-assign]

    def replace_checkpoint_out_of_band(
        self,
        *,
        tenant_id: str,
        thread_id: str,
        state: BaseRuntimeState,
        expected_revision: int,
    ) -> None:
        with closing(sqlite3.connect(self.database_path, timeout=30)) as connection, connection:
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            cursor = connection.execute(
                """
                UPDATE thread_states
                SET state_json = ?, revision = revision + 1
                WHERE tenant_id = ? AND thread_id = ? AND revision = ?
                """,
                (
                    state.model_dump_json(),
                    tenant_id,
                    thread_id,
                    expected_revision,
                ),
            )
            rowcount = cursor.rowcount
        if rowcount != 1:
            raise AssertionError("Checkpoint drift injection did not match the expected revision")

    def close(self) -> None:
        return None


@dataclass(frozen=True)
class PostgresConformanceBackend:
    """Bind the same scenarios to an isolated PostgreSQL schema."""

    dsn: str
    schema: str
    clock: AdjustableStoreClock
    backend_id: str = "postgres"

    def __post_init__(self) -> None:
        validate_postgres_schema_name(self.schema)

    def open_workflow_store(self) -> WorkflowStore:
        return PostgresWorkflowStore(
            self.dsn,
            schema=self.schema,
            lease_clock_ms=self.clock,
        )

    def open_postgres_workflow_store(self) -> PostgresWorkflowStore:
        return PostgresWorkflowStore(
            self.dsn,
            schema=self.schema,
            lease_clock_ms=self.clock,
        )

    def open_run_store(
        self,
        *,
        state_registry: AgentRegistry | None = None,
        bind_default_registry: bool = True,
    ) -> PostgresRunStore:
        registry = (
            state_registry
            if state_registry is not None or not bind_default_registry
            else build_default_registry()
        )
        return PostgresRunStore(
            self.dsn,
            schema=self.schema,
            state_registry=registry,
            lease_clock_ms=self.clock,
        )

    def open_postgres_run_store(
        self,
        *,
        state_registry: AgentRegistry | None = None,
        bind_default_registry: bool = True,
    ) -> PostgresRunStore:
        store = self.open_run_store(
            state_registry=state_registry,
            bind_default_registry=bind_default_registry,
        )
        assert isinstance(store, PostgresRunStore)
        return store

    @contextmanager
    def fail_workflow_event(
        self,
        store: WorkflowStore,
        event_type: str,
    ) -> Iterator[None]:
        if not isinstance(store, PostgresWorkflowStore):
            raise TypeError("PostgreSQL event injection requires PostgresWorkflowStore")
        original = store._append_event_with_connection

        def injected(
            connection,
            run_id: str,
            actual_event_type: str,
            payload: dict[str, Any] | None = None,
        ):
            if actual_event_type == event_type:
                raise InjectedConformanceFailure(
                    f"injected workflow event failure: {event_type}"
                )
            return original(connection, run_id, actual_event_type, payload)

        store._append_event_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._append_event_with_connection = original  # type: ignore[method-assign]

    @contextmanager
    def fail_run_event(
        self,
        store: RunStore,
        event_type: str,
    ) -> Iterator[None]:
        if not isinstance(store, PostgresRunStore):
            raise TypeError("PostgreSQL event injection requires PostgresRunStore")
        original = store._append_event_with_connection

        def injected(
            connection,
            run_id: str,
            actual_event_type: str,
            payload: dict[str, Any] | None = None,
        ):
            if actual_event_type == event_type:
                raise InjectedConformanceFailure(f"injected run event failure: {event_type}")
            return original(connection, run_id, actual_event_type, payload)

        store._append_event_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._append_event_with_connection = original  # type: ignore[method-assign]

    @contextmanager
    def fail_checkpoint_write(self, store: RunStore) -> Iterator[None]:
        if not isinstance(store, PostgresRunStore):
            raise TypeError("PostgreSQL checkpoint injection requires PostgresRunStore")
        original = store._write_checkpoint_with_connection

        def injected(*_args, **_kwargs):
            raise InjectedConformanceFailure("injected checkpoint write failure")

        store._write_checkpoint_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._write_checkpoint_with_connection = original  # type: ignore[method-assign]

    def replace_checkpoint_out_of_band(
        self,
        *,
        tenant_id: str,
        thread_id: str,
        state: BaseRuntimeState,
        expected_revision: int,
    ) -> None:
        connection = open_postgres_connection(self.dsn, schema=self.schema)
        try:
            with connection.transaction():
                row = connection.execute(
                    """
                    UPDATE thread_states
                    SET state_json = %s, revision = revision + 1
                    WHERE tenant_id = %s AND thread_id = %s AND revision = %s
                    RETURNING revision
                    """,
                    (
                        state.model_dump_json(),
                        tenant_id,
                        thread_id,
                        expected_revision,
                    ),
                ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise AssertionError("Checkpoint drift injection did not match the expected revision")

    def close(self) -> None:
        validate_postgres_schema_name(self.schema)
        connection = psycopg.connect(self.dsn, autocommit=True)
        try:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(self.schema)
                )
            )
        finally:
            connection.close()
