from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent.contracts import BaseRuntimeState
from runtime_service.registry import AgentRegistry, build_default_registry
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStore


class InjectedConformanceFailure(RuntimeError):
    """Stable failure used to prove transactional rollback at event boundaries."""


class ManualStoreClock:
    """Thread-safe store clock advanced explicitly by conformance scenarios."""

    def __init__(self, now_ms: int | None = None) -> None:
        self._now_ms = int(time.time() * 1_000) if now_ms is None else now_ms
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            return self._now_ms

    def advance(self, delta_ms: int) -> int:
        if delta_ms < 0:
            raise ValueError("Manual store clock cannot move backwards")
        with self._lock:
            self._now_ms += delta_ms
            return self._now_ms


@dataclass(frozen=True)
class SQLiteConformanceBackend:
    """Test-only adapter that binds generic scenarios to the SQLite reference stores.

    This is intentionally not a production store abstraction. A future backend adds
    another test adapter and pytest parameter; RuntimeManager remains typed to
    SQLiteRunStore until a real second implementation justifies a production seam.
    """

    database_path: Path
    clock: ManualStoreClock
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
        store: SQLiteRunStore,
        event_type: str,
    ) -> Iterator[None]:
        original = store._append_event_with_connection

        def injected(
            connection: sqlite3.Connection,
            run_id: str,
            actual_event_type: str,
            payload: dict[str, Any] | None = None,
        ):
            if actual_event_type == event_type:
                raise InjectedConformanceFailure(
                    f"injected run event failure: {event_type}"
                )
            return original(connection, run_id, actual_event_type, payload)

        store._append_event_with_connection = injected  # type: ignore[method-assign]
        try:
            yield
        finally:
            store._append_event_with_connection = original  # type: ignore[method-assign]

    def replace_checkpoint_out_of_band(
        self,
        *,
        tenant_id: str,
        thread_id: str,
        state: BaseRuntimeState,
        expected_revision: int,
    ) -> None:
        """Inject the mixed-writer/corruption condition a backend must detect.

        The executable scenarios call this semantic hook and never depend on
        SQLite schema details. Each future backend adapter must implement an
        equivalent fault injection for the same externally observed contract.
        """

        with sqlite3.connect(self.database_path) as connection:
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
        if cursor.rowcount != 1:
            raise AssertionError(
                "Checkpoint drift injection did not match the expected revision"
            )
