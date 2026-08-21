from __future__ import annotations

import time
from collections.abc import Callable
from typing import ClassVar

import pytest
from pydantic import BaseModel, ConfigDict, field_validator

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionContext,
    RuntimeResponse,
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
    tenant_id="state-contract-tenant",
    subject_id="state-contract-tester",
)


class ContractInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    marker: str


class ContractState(BaseRuntimeState):
    domain_id: ClassVar[str] = "state-contract"
    schema_version: ClassVar[str] = "1"

    marker: str | None = None

    @field_validator("marker")
    @classmethod
    def normalize_marker(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None


class ForeignState(BaseRuntimeState):
    domain_id: ClassVar[str] = "foreign-state"
    schema_version: ClassVar[str] = "1"

    foreign_marker: str = "unexpected"


class ValidContractRuntime:
    def initial_state(self, thread_id: str) -> ContractState:
        return ContractState(thread_id=thread_id)

    def execute(
        self,
        state: ContractState,
        runtime_input: ContractInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ContractState]:
        del context
        return RuntimeResponse[ContractState](
            message="contract state persisted",
            state=state.model_copy(update={"marker": runtime_input.marker}),
            validation_errors=[],
        )


class WrongInitialStateRuntime(ValidContractRuntime):
    def initial_state(self, thread_id: str) -> ForeignState:
        return ForeignState(thread_id=thread_id)


class WrongResultStateRuntime(ValidContractRuntime):
    def execute(
        self,
        state: ContractState,
        runtime_input: ContractInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ForeignState]:
        del state, runtime_input, context
        return RuntimeResponse[ForeignState](
            message="wrong state",
            state=ForeignState(thread_id="state-contract-thread"),
            validation_errors=[],
        )


class WrongResultThreadRuntime(ValidContractRuntime):
    def execute(
        self,
        state: ContractState,
        runtime_input: ContractInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ContractState]:
        del state, runtime_input, context
        return RuntimeResponse[ContractState](
            message="wrong thread",
            state=ContractState(thread_id="different-thread", marker="unsafe"),
            validation_errors=[],
        )


class InvalidConstructedResultRuntime(ValidContractRuntime):
    def execute(
        self,
        state: ContractState,
        runtime_input: ContractInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ContractState]:
        del state, runtime_input, context
        invalid_state = ContractState.model_construct(
            thread_id="state-contract-thread",
            marker=42,
        )
        return RuntimeResponse[ContractState].model_construct(
            message="invalid constructed state",
            state=invalid_state,
            validation_errors=[],
        )


class CapturingResultRuntime(ValidContractRuntime):
    def __init__(self, returned_states: list[ContractState]) -> None:
        self.returned_states = returned_states

    def execute(
        self,
        state: ContractState,
        runtime_input: ContractInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ContractState]:
        del state
        raw_state = ContractState(
            thread_id=context.thread_id,
            marker=runtime_input.marker,
        )
        raw_state = raw_state.model_copy(update={"marker": f" {runtime_input.marker} "})
        self.returned_states.append(raw_state)
        return RuntimeResponse[ContractState](
            message="captured result state",
            state=raw_state,
            validation_errors=[],
        )


def build_manager(
    database_path,
    runtime_factory: Callable[[], object],
) -> tuple[RuntimeManager, SQLiteRunStore]:
    store = SQLiteRunStore(database_path)
    registry = AgentRegistry()
    registry.register(
        "state-contract-agent",
        "1.0.0",
        runtime_factory,
        description="Runtime state boundary contract test",
        input_model=ContractInput,
        state_model=ContractState,
    )
    return RuntimeManager(store, registry), store


def submit_and_wait(manager: RuntimeManager, *, marker: str = "expected"):
    submitted = manager.submit(
        RunCreateRequest(
            thread_id="state-contract-thread",
            agent_id="state-contract-agent",
            agent_version="1.0.0",
            input={"marker": marker},
        ),
        tenant_context=TENANT_CONTEXT,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        run = manager.get_run(
            submitted.run_id,
            tenant_context=TENANT_CONTEXT,
        )
        if run is not None and run.status.is_terminal:
            return run
        time.sleep(0.02)
    raise AssertionError(f"Run did not finish: {submitted.run_id}")


def assert_no_checkpoint(store: SQLiteRunStore, run_id: str) -> None:
    event_types = [event.event_type for event in store.list_events(run_id)]
    assert "checkpoint.saved" not in event_types
    assert "run.completed" not in event_types


def test_registered_state_model_round_trips_through_sqlite(tmp_path):
    manager, store = build_manager(tmp_path / "runtime.db", ValidContractRuntime)
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.COMPLETED
    assert isinstance(result.state, ContractState)
    assert result.state.marker == "expected"
    persisted = store.load_thread_state(
        "state-contract-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
        domain_id=ContractState.domain_id,
        schema_version=ContractState.schema_version,
    )
    assert isinstance(persisted, ContractState)
    assert persisted.marker == "expected"


def test_wrong_initial_state_fails_before_checkpoint_persistence(tmp_path):
    manager, store = build_manager(tmp_path / "runtime.db", WrongInitialStateRuntime)
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.FAILED
    assert result.error_code == "runtime_execution_failed"
    assert "initial_state state does not match registered schema state-contract:1" in (
        result.error or ""
    )
    assert result.state is None
    assert_no_checkpoint(store, result.run_id)
    assert store.load_thread_state(
        "state-contract-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
    ) is None


def test_wrong_result_state_fails_before_checkpoint_persistence(tmp_path):
    manager, store = build_manager(tmp_path / "runtime.db", WrongResultStateRuntime)
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.FAILED
    assert "execute state does not match registered schema state-contract:1" in (
        result.error or ""
    )
    assert result.state is None
    assert_no_checkpoint(store, result.run_id)
    assert store.load_thread_state(
        "state-contract-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
    ) is None


def test_wrong_result_thread_fails_before_checkpoint_persistence(tmp_path):
    manager, store = build_manager(tmp_path / "runtime.db", WrongResultThreadRuntime)
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.FAILED
    assert "execute state.thread_id must match run.thread_id" in (result.error or "")
    assert result.state is None
    assert_no_checkpoint(store, result.run_id)
    assert store.load_thread_state(
        "state-contract-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
    ) is None


def test_same_model_instance_is_revalidated_before_checkpoint_persistence(tmp_path):
    manager, store = build_manager(
        tmp_path / "runtime.db",
        InvalidConstructedResultRuntime,
    )
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.FAILED
    assert "execute state does not match registered schema state-contract:1" in (
        result.error or ""
    )
    assert result.state is None
    assert_no_checkpoint(store, result.run_id)
    assert store.load_thread_state(
        "state-contract-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
    ) is None


def test_cancel_race_persists_the_revalidated_result_state(tmp_path, monkeypatch):
    returned_states: list[ContractState] = []
    manager, store = build_manager(
        tmp_path / "runtime.db",
        lambda: CapturingResultRuntime(returned_states),
    )
    original_complete = store.commit_completed_run

    def cancel_before_completion(run, *, lease_token: str):
        store.request_cancel_atomically(run.run_id, tenant_id=run.tenant_id)
        return original_complete(run, lease_token=lease_token)

    cancelled_states: list[ContractState] = []
    original_cancel = store.commit_cancelled_run

    def capture_cancelled_state(run, *, reason: str, lease_token: str):
        assert isinstance(run.state, ContractState)
        cancelled_states.append(run.state)
        return original_cancel(run, reason=reason, lease_token=lease_token)

    monkeypatch.setattr(store, "commit_completed_run", cancel_before_completion)
    monkeypatch.setattr(store, "commit_cancelled_run", capture_cancelled_state)
    manager.start()
    try:
        result = submit_and_wait(manager)
    finally:
        manager.stop()

    assert result.status == RunStatus.CANCELLED
    assert len(returned_states) == 1
    assert len(cancelled_states) == 1
    assert returned_states[0].marker == " expected "
    assert cancelled_states[0].marker == "expected"
    assert isinstance(result.state, ContractState)
    assert result.state.marker == "expected"


def test_recoverable_run_schema_mismatch_fails_before_workers_start(tmp_path):
    manager, store = build_manager(tmp_path / "runtime.db", ValidContractRuntime)
    run = RunRecord(
        run_id="run_state_contract_schema_mismatch",
        tenant_id=TENANT_CONTEXT.tenant_id,
        thread_id="state-contract-schema-mismatch",
        agent_id="state-contract-agent",
        agent_version="1.0.0",
        domain_id="wrong-domain",
        schema_version="1",
        status=RunStatus.QUEUED,
        input={"marker": "expected"},
    )
    store.create_run(run)

    with pytest.raises(
        RuntimeError,
        match=(
            "recoverable run run_state_contract_schema_mismatch schema "
            "wrong-domain:1 does not match its registered Agent version"
        ),
    ):
        manager.start()

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.QUEUED
    assert store.list_events(run.run_id) == []
