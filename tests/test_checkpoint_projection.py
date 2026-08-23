from __future__ import annotations

import sqlite3
import time
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionContext,
    RuntimeResponse,
    TraceEvent,
)
from runtime_service import (
    AgentRegistry,
    RunCreateRequest,
    RunRecord,
    RunStatus,
    RuntimeManager,
    SQLiteRunStore,
    TenantContext,
)


TENANT_CONTEXT = TenantContext(
    tenant_id="checkpoint-projection-tenant",
    subject_id="checkpoint-projection-tester",
)
FIXED_TIME = "2026-08-23T00:00:00+00:00"


class ProjectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    marker: str


class ProjectionState(BaseRuntimeState):
    domain_id: ClassVar[str] = "checkpoint-projection"
    schema_version: ClassVar[str] = "1"

    marker: str | None = None
    nested: dict[str, list[str]] = Field(default_factory=lambda: {"markers": []})


class RecordingProjectionRuntime:
    def __init__(self) -> None:
        self.loaded_trace_markers: list[list[str]] = []

    def initial_state(self, thread_id: str) -> ProjectionState:
        return ProjectionState(thread_id=thread_id)

    def execute(
        self,
        state: ProjectionState,
        runtime_input: ProjectionInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ProjectionState]:
        del context
        self.loaded_trace_markers.append(
            [str(event.payload["marker"]) for event in state.execution_trace]
        )
        result = state.model_copy(deep=True)
        result.marker = runtime_input.marker
        result.nested["markers"].append(runtime_input.marker)
        result.execution_trace.append(
            TraceEvent(
                event="projection.marker",
                reason="current Run marker",
                payload={"marker": runtime_input.marker},
                timestamp=FIXED_TIME,
            )
        )
        return RuntimeResponse[ProjectionState](
            message=f"completed {runtime_input.marker}",
            state=result,
            validation_errors=[],
        )


def _build_manager(database_path):
    runtime = RecordingProjectionRuntime()
    registry = AgentRegistry()
    registry.register(
        "checkpoint-projection-agent",
        "1.0.0",
        lambda: runtime,
        description="Checkpoint projection contract test",
        input_model=ProjectionInput,
        state_model=ProjectionState,
    )
    store = SQLiteRunStore(database_path)
    manager = RuntimeManager(
        store,
        registry,
        poll_interval_seconds=0.01,
    )
    return manager, store, registry, runtime


def _submit_and_wait(
    manager: RuntimeManager,
    store: SQLiteRunStore,
    *,
    thread_id: str,
    marker: str,
) -> RunRecord:
    submitted = manager.submit(
        RunCreateRequest(
            thread_id=thread_id,
            agent_id="checkpoint-projection-agent",
            agent_version="1.0.0",
            input={"marker": marker},
        ),
        tenant_context=TENANT_CONTEXT,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        persisted = store.get_run_internal(submitted.run_id)
        if persisted is not None and persisted.status.is_terminal:
            assert persisted.status == RunStatus.COMPLETED
            return persisted
        time.sleep(0.01)
    raise AssertionError(f"Run did not finish: {submitted.run_id}")


def _trace_markers(state: BaseRuntimeState) -> list[str]:
    return [str(event.payload["marker"]) for event in state.execution_trace]


def test_completed_runs_keep_full_trace_while_successors_load_projected_checkpoint(
    tmp_path,
) -> None:
    database_path = tmp_path / "runtime.db"
    manager, store, registry, runtime = _build_manager(database_path)
    manager.start()
    try:
        first = _submit_and_wait(
            manager,
            store,
            thread_id="two-turn-thread",
            marker="run-one",
        )
        first_snapshot = store.load_thread_state_snapshot(
            "two-turn-thread",
            tenant_id=TENANT_CONTEXT.tenant_id,
            domain_id=ProjectionState.domain_id,
            schema_version=ProjectionState.schema_version,
        )
        assert isinstance(first.state, ProjectionState)
        assert _trace_markers(first.state) == ["run-one"]
        assert isinstance(first_snapshot.state, ProjectionState)
        assert first_snapshot.state.execution_trace == []
        assert first_snapshot.revision == 1

        first_checkpoint_event = next(
            event
            for event in store.list_events(first.run_id)
            if event.event_type == "checkpoint.saved"
        )
        assert first_checkpoint_event.payload == {
            "thread_id": "two-turn-thread",
            "trace_events": 0,
            "run_trace_events": 1,
            "projection": "execution_trace_reset",
            "base_revision": 0,
            "revision": 1,
        }

        second = _submit_and_wait(
            manager,
            store,
            thread_id="two-turn-thread",
            marker="run-two",
        )
        third = _submit_and_wait(
            manager,
            store,
            thread_id="two-turn-thread",
            marker="run-three",
        )
    finally:
        manager.stop()

    assert runtime.loaded_trace_markers == [[], [], []]
    assert isinstance(second.state, ProjectionState)
    assert _trace_markers(second.state) == ["run-two"]
    assert isinstance(third.state, ProjectionState)
    assert _trace_markers(third.state) == ["run-three"]
    final_snapshot = store.load_thread_state_snapshot(
        "two-turn-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
        domain_id=ProjectionState.domain_id,
        schema_version=ProjectionState.schema_version,
    )
    assert isinstance(final_snapshot.state, ProjectionState)
    assert final_snapshot.state.execution_trace == []
    assert final_snapshot.state.nested == {
        "markers": ["run-one", "run-two", "run-three"]
    }
    assert final_snapshot.revision == 3
    assert third.state.model_dump(exclude={"execution_trace"}) == final_snapshot.state.model_dump(
        exclude={"execution_trace"}
    )

    reopened = SQLiteRunStore(database_path, state_registry=registry)
    durable_first = reopened.get_run_internal(first.run_id)
    assert durable_first is not None
    assert isinstance(durable_first.state, ProjectionState)
    assert _trace_markers(durable_first.state) == ["run-one"]


def test_legacy_trace_is_loaded_once_then_compacted_after_successful_completion(
    tmp_path,
) -> None:
    database_path = tmp_path / "runtime.db"
    manager, store, _registry, runtime = _build_manager(database_path)
    legacy_state = ProjectionState(
        thread_id="legacy-thread",
        marker="legacy",
        nested={"markers": ["legacy"]},
        execution_trace=[
            TraceEvent(
                event="projection.marker",
                reason="legacy checkpoint marker",
                payload={"marker": "legacy-prefix"},
                timestamp=FIXED_TIME,
            )
        ],
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO thread_states (
                tenant_id, thread_id, domain_id, schema_version,
                state_json, updated_at, revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TENANT_CONTEXT.tenant_id,
                legacy_state.thread_id,
                legacy_state.domain_id,
                legacy_state.schema_version,
                legacy_state.model_dump_json(),
                FIXED_TIME,
                7,
            ),
        )

    manager.start()
    try:
        compacting_run = _submit_and_wait(
            manager,
            store,
            thread_id="legacy-thread",
            marker="compacting-run",
        )
        compacted = store.load_thread_state_snapshot(
            "legacy-thread",
            tenant_id=TENANT_CONTEXT.tenant_id,
            domain_id=ProjectionState.domain_id,
            schema_version=ProjectionState.schema_version,
        )
        assert isinstance(compacting_run.state, ProjectionState)
        assert _trace_markers(compacting_run.state) == [
            "legacy-prefix",
            "compacting-run",
        ]
        assert isinstance(compacted.state, ProjectionState)
        assert compacted.state.execution_trace == []
        assert compacted.revision == 8

        successor = _submit_and_wait(
            manager,
            store,
            thread_id="legacy-thread",
            marker="successor",
        )
    finally:
        manager.stop()

    assert runtime.loaded_trace_markers == [["legacy-prefix"], []]
    assert isinstance(successor.state, ProjectionState)
    assert _trace_markers(successor.state) == ["successor"]
    final_snapshot = store.load_thread_state_snapshot(
        "legacy-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
        domain_id=ProjectionState.domain_id,
        schema_version=ProjectionState.schema_version,
    )
    assert isinstance(final_snapshot.state, ProjectionState)
    assert final_snapshot.state.execution_trace == []
    assert final_snapshot.revision == 9
