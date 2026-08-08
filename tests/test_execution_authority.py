from __future__ import annotations

import time

from agent.contracts import RuntimeExecutionContext, RuntimeResponse
from domains.travel.runtime import TravelMessageInput
from domains.travel.state import AgentState
from runtime_service import (
    AgentRegistry,
    RunCreateRequest,
    RuntimeManager,
    SQLiteRunStore,
    TenantContext,
)


class AuthorityCapturingRuntime:
    def __init__(self, captured: list[RuntimeExecutionContext]) -> None:
        self.captured = captured

    def initial_state(self, thread_id: str) -> AgentState:
        return AgentState(thread_id=thread_id)

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        self.captured.append(context)
        return RuntimeResponse[AgentState](
            message=runtime_input.user_message,
            state=state,
            validation_errors=[],
        )


def capturing_registry(captured: list[RuntimeExecutionContext]) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "authority-capture",
        "1.0.0",
        lambda: AuthorityCapturingRuntime(captured),
        description="Test-only authority capture runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def wait_for_completion(manager: RuntimeManager, run_id: str) -> None:
    deadline = time.monotonic() + 3
    context = TenantContext(tenant_id="tenant-a", subject_id="reader")
    while time.monotonic() < deadline:
        run = manager.get_run(run_id, tenant_context=context)
        if run is not None and run.status.is_terminal:
            return
        time.sleep(0.02)
    raise AssertionError("run did not complete")


def test_restarted_manager_uses_persisted_authority_snapshot(tmp_path):
    database_path = tmp_path / "runtime.db"
    captured: list[RuntimeExecutionContext] = []
    registry = capturing_registry(captured)
    first_manager = RuntimeManager(SQLiteRunStore(database_path), registry)
    queued = first_manager.submit(
        RunCreateRequest(
            thread_id="authority-restart",
            agent_id="authority-capture",
            agent_version="1.0.0",
            input={"user_message": "capture authority"},
        ),
        tenant_context=TenantContext(
            tenant_id="tenant-a",
            subject_id="original-subject",
            permissions=("tools:execute", "runs:create"),
        ),
    )

    restarted = RuntimeManager(SQLiteRunStore(database_path), registry)
    restarted.start()
    try:
        wait_for_completion(restarted, queued.run_id)
    finally:
        restarted.stop()

    assert len(captured) == 1
    authority = captured[0].authority
    assert authority.tenant_id == "tenant-a"
    assert authority.subject_id == "original-subject"
    assert authority.permissions == ("runs:create", "tools:execute")
    assert captured[0].recovered_after_restart is True


def test_idempotent_resubmit_does_not_replace_original_authority(tmp_path):
    captured: list[RuntimeExecutionContext] = []
    manager = RuntimeManager(
        SQLiteRunStore(tmp_path / "runtime.db"),
        capturing_registry(captured),
    )
    request = RunCreateRequest(
        thread_id="authority-idempotent",
        agent_id="authority-capture",
        agent_version="1.0.0",
        input={"user_message": "capture authority"},
        client_request_id="same-request",
    )
    first = manager.submit(
        request,
        tenant_context=TenantContext(
            tenant_id="tenant-a",
            subject_id="first-subject",
            permissions=("tools:execute",),
        ),
    )
    duplicate = manager.submit(
        request,
        tenant_context=TenantContext(
            tenant_id="tenant-a",
            subject_id="second-subject",
            permissions=(),
        ),
    )

    assert duplicate.run_id == first.run_id
    stored = manager.store.get_run_internal(first.run_id)
    assert stored is not None and stored.execution_authority is not None
    assert stored.execution_authority.subject_id == "first-subject"
    assert stored.execution_authority.permissions == ("tools:execute",)
