from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import (
    RuntimeExecutionAuthority,
    RuntimeExecutionContext,
    RuntimeExecutionError,
)
from runtime_service.dynamic_loop import (
    DynamicLoopOutcome,
    DynamicToolLoop,
    FinishEvaluation,
)
from runtime_service.external_actions import (
    AmbiguousExternalActionError,
    DefinitiveExternalActionError,
    ExternalActionDispatcher,
    ExternalActionProviderRegistry,
    ExternalActionProviderResult,
    ExternalActionReconciliationPendingError,
    ExternalActionRequest,
)
from runtime_service.models import RunRecord, RunStatus
from runtime_service.planner import CallToolDecision, FinishDecision, PlannerContext
from runtime_service.sandbox import (
    ToolEffect,
    ToolExecutionResult,
    ToolExecutionStatus,
    ToolPolicy,
    ToolRegistry,
    ToolRetryMode,
    ToolSpec,
)
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import (
    ExternalActionStatus,
    SQLiteWorkflowStore,
    ToolCallStatus,
    WorkflowStatus,
)


WORKFLOW_TYPE = "generic-external-action:1.0.0"
RUNTIME_INPUT = {"request": "hold the selected item"}
RUNTIME_STATE = {"selection": "item-42", "status": "new"}


class HoldInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1)
    quantity: int = Field(default=1, ge=1, le=10)


class HoldResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hold_id: str
    status: Literal["held"]
    provider_reference: str


class EchoInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: int


class ChangedHoldInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replacement_item_id: str


class ChangedHoldResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["held"]
    provider_reference: str
    replacement_receipt: str


class ScriptedPlanner:
    def __init__(self, *decisions: Any) -> None:
        self.decisions = list(decisions)
        self.contexts: list[PlannerContext] = []

    def decide(self, context: PlannerContext) -> Any:
        self.contexts.append(context)
        if not self.decisions:
            raise AssertionError("Planner was called more times than expected")
        decision = self.decisions.pop(0)
        if isinstance(decision, BaseException):
            raise decision
        return decision


class ProcessCrash(BaseException):
    pass


class RecordingSandbox:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, tool_name: str, arguments: dict[str, Any]) -> ToolExecutionResult:
        self.calls.append((tool_name, arguments))
        if self.result is None:
            raise AssertionError("External-write tool reached the subprocess sandbox")
        return ToolExecutionResult(
            execution_id="exec-read-only",
            tool_name=tool_name,
            status=ToolExecutionStatus.COMPLETED,
            result=self.result,
            duration_ms=1,
            exit_code=0,
        )


class SuccessProvider:
    supports_idempotency = True

    def __init__(
        self,
        workflow_store: SQLiteWorkflowStore | None = None,
        *,
        provider_identity: str = "synthetic-hold-account-v1",
    ) -> None:
        self.workflow_store = workflow_store
        self.provider_identity = provider_identity
        self.requests: list[ExternalActionRequest] = []

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        if self.workflow_store is not None:
            action = self.workflow_store.get_external_action(
                request.run_id,
                request.step_id,
            )
            step = self.workflow_store.get_step(request.run_id, request.step_id)
            assert action is not None
            assert action.status == ExternalActionStatus.DISPATCHING
            assert action.dispatch_count == 1
            assert step is not None and step.status == ToolCallStatus.RUNNING
        return ExternalActionProviderResult(
            provider_reference=f"hold:{request.arguments['item_id']}",
            result={"hold_id": "hold-42", "status": "held"},
        )


class CommitThenAmbiguousProvider:
    supports_idempotency = True
    provider_identity = "synthetic-hold-account-v1"

    def __init__(self) -> None:
        self.requests: list[ExternalActionRequest] = []
        self.effects: dict[str, ExternalActionProviderResult] = {}

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        existing = self.effects.get(request.idempotency_key)
        if existing is not None:
            return existing
        result = ExternalActionProviderResult(
            provider_reference="hold:committed-once",
            result={"hold_id": "hold-once", "status": "held"},
        )
        self.effects[request.idempotency_key] = result
        raise AmbiguousExternalActionError("connection dropped after commit")


class CrashProvider:
    supports_idempotency = False
    provider_identity = "synthetic-hold-account-v1"

    def __init__(self) -> None:
        self.requests: list[ExternalActionRequest] = []

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        raise ProcessCrash()


class IdempotentCrashProvider(CrashProvider):
    supports_idempotency = True


class NeverCalledProvider:
    supports_idempotency = True

    def __init__(
        self,
        *,
        provider_identity: str = "synthetic-hold-account-v1",
    ) -> None:
        self.provider_identity = provider_identity
        self.requests: list[ExternalActionRequest] = []

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        raise AssertionError("Recovered terminal action was dispatched again")


class NonIdempotentProvider(SuccessProvider):
    supports_idempotency = False


class LeakyResultProvider(SuccessProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        return ExternalActionProviderResult(
            provider_reference="hold:untrusted-extra",
            result={
                "hold_id": "hold-leaky",
                "status": "held",
                "raw_authorization": "secret-token-from-provider",
                "idempotency_key": request.idempotency_key,
            },
        )


class IdempotencyKeyEchoProvider(SuccessProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        return ExternalActionProviderResult(
            provider_reference=f"hold_{request.idempotency_key}",
            result={"hold_id": "hold-key-echo", "status": "held"},
        )


class DefinitiveFailureProvider(SuccessProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        raise DefinitiveExternalActionError("upstream rejected secret-token")


def call_hold() -> CallToolDecision:
    return CallToolDecision(
        tool_name="create_hold",
        arguments={"item_id": "item-42"},
        reason="Create the requested durable hold.",
    )


def finish() -> FinishDecision:
    return FinishDecision(
        message="Hold created.",
        output={"status": "ready"},
        reason="The hold result is sufficient.",
    )


def finish_evaluator(
    decision: FinishDecision,
    _observations: list[Any],
) -> FinishEvaluation:
    return FinishEvaluation(
        outcome=DynamicLoopOutcome.FINISHED,
        message=decision.message,
        output=decision.output,
    )


def execution_context(
    run_id: str,
    *,
    permissions: tuple[str, ...] = (
        "external-actions:execute",
        "tools:execute",
    ),
    recovered_after_restart: bool = False,
) -> RuntimeExecutionContext:
    return RuntimeExecutionContext(
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        recovered_after_restart=recovered_after_restart,
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-generic",
            subject_id="subject-generic",
            permissions=permissions,
        ),
    )


def initialize_stores(
    database_path: Path,
    context: RuntimeExecutionContext,
) -> tuple[SQLiteRunStore, SQLiteWorkflowStore]:
    run_store = SQLiteRunStore(database_path)
    run_store.create_run(
        RunRecord(
            run_id=context.run_id,
            tenant_id=context.authority.tenant_id,
            thread_id=context.thread_id,
            agent_id="generic-agent",
            agent_version="1.0.0",
            domain_id="generic",
            schema_version="1",
            status=RunStatus.RUNNING,
            input=RUNTIME_INPUT,
            execution_authority=context.authority,
            attempt=1,
        )
    )
    return run_store, SQLiteWorkflowStore(database_path)


def external_registry(
    retry_mode: ToolRetryMode,
    *,
    runtime_input_gate=None,
) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="create_hold",
            description="Create a synthetic external hold.",
            input_model=HoldInput,
            output_model=HoldResult,
            policy=ToolPolicy(),
            effect=ToolEffect.EXTERNAL_WRITE,
            retry_mode=retry_mode,
            provider_name="synthetic-hold",
            runtime_input_gate=runtime_input_gate,
        )
    )
    return registry


def dispatcher_for(provider: Any) -> ExternalActionDispatcher:
    providers = ExternalActionProviderRegistry()
    providers.register("synthetic-hold", provider)
    return ExternalActionDispatcher(providers)


def build_external_loop(
    *,
    planner: ScriptedPlanner,
    workflow_store: SQLiteWorkflowStore,
    run_store: SQLiteRunStore,
    sandbox: RecordingSandbox,
    retry_mode: ToolRetryMode,
    dispatcher: ExternalActionDispatcher | None,
    runtime_input_gate=None,
) -> DynamicToolLoop:
    return DynamicToolLoop(
        planner=planner,
        tool_registry=external_registry(
            retry_mode,
            runtime_input_gate=runtime_input_gate,
        ),
        tool_sandbox=sandbox,  # type: ignore[arg-type]
        workflow_store=workflow_store,
        run_event_sink=run_store,
        workflow_type=WORKFLOW_TYPE,
        external_action_dispatcher=dispatcher,
    )


def execute_loop(
    loop: DynamicToolLoop,
    context: RuntimeExecutionContext,
):
    return loop.execute(
        runtime_input=RUNTIME_INPUT,
        state=RUNTIME_STATE,
        context=context,
        finish_evaluator=finish_evaluator,
    )


def test_external_action_is_prepared_before_provider_and_finalized_atomically(
    tmp_path: Path,
):
    context = execution_context("run-external-success")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider(workflow_store)
    sandbox = RecordingSandbox()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold(), finish()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=sandbox,
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    result = execute_loop(loop, context)

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert len(provider.requests) == 1
    assert sandbox.calls == []
    assert result.observations[0].result["provider_reference"] == "hold:item-42"
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED
    assert action.dispatch_count == 1
    assert action.arguments_json == '{"item_id":"item-42","quantity":1}'
    assert step is not None and step.status == ToolCallStatus.COMPLETED
    assert action.result_json == step.result_json
    assert [
        event.event_type
        for event in run_store.list_events(context.run_id)
        if event.event_type.startswith("external_action.")
    ] == [
        "external_action.prepared",
        "external_action.dispatch_started",
        "external_action.succeeded",
    ]


def test_provider_commit_then_ambiguous_retries_once_with_same_server_key(
    tmp_path: Path,
):
    context = execution_context("run-external-idempotent-retry")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = CommitThenAmbiguousProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold(), finish()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    result = execute_loop(loop, context)

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert len(provider.requests) == 2
    assert len(provider.effects) == 1
    assert provider.requests[0].idempotency_key == provider.requests[1].idempotency_key
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED
    assert action.dispatch_count == 2


def test_provider_result_extras_are_never_persisted_or_exposed(
    tmp_path: Path,
):
    context = execution_context("run-external-result-extra-rejected")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = LeakyResultProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_outcome_unknown"
    assert len(provider.requests) == 2
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert action.result_json is None
    assert step is not None and step.result_json is None
    public_evidence = [
        event.model_dump(mode="json")
        for event in (
            workflow_store.list_events(context.run_id)
            + run_store.list_events(context.run_id)
        )
    ]
    encoded_evidence = str(public_evidence)
    assert "secret-token-from-provider" not in encoded_evidence
    assert provider.requests[0].idempotency_key not in encoded_evidence


def test_schema_allowed_reference_cannot_echo_runtime_idempotency_key(
    tmp_path: Path,
):
    context = execution_context("run-external-idempotency-key-echo")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = IdempotencyKeyEchoProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_outcome_unknown"
    assert len(provider.requests) == 2
    idempotency_key = provider.requests[0].idempotency_key
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert action.result_json is None
    assert step is not None and step.result_json is None
    public_evidence = str(
        [
            event.model_dump(mode="json")
            for event in (
                workflow_store.list_events(context.run_id)
                + run_store.list_events(context.run_id)
            )
        ]
    )
    assert idempotency_key not in public_evidence


def test_definitive_failure_keeps_stable_code_when_run_evidence_mirror_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-definitive-evidence-failure")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = DefinitiveFailureProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )
    original_append = run_store.append_event

    def fail_failure_mirror(run_id, event_type, payload=None):
        if event_type == "external_action.failed":
            raise sqlite3.OperationalError("injected run evidence failure")
        return original_append(run_id, event_type, payload)

    monkeypatch.setattr(run_store, "append_event", fail_failure_mirror)

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_failed"
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.FAILED
    assert step is not None and step.error_code == "external_action_failed"


def test_ambiguous_provider_with_local_retry_write_failure_stays_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-ambiguous-retry-write-failure")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = CommitThenAmbiguousProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    def fail_retry_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected retry claim failure")

    monkeypatch.setattr(
        workflow_store,
        "retry_external_action_dispatch",
        fail_retry_write,
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_outcome_unknown"
    assert len(provider.requests) == 1
    assert len(provider.effects) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN


def test_provider_success_with_terminal_write_failure_becomes_outcome_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-success-write-failure")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    def fail_success_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected terminal write failure")

    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_succeeded",
        fail_success_write,
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_outcome_unknown"
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert step is not None and step.status == ToolCallStatus.FAILED


def test_success_and_unknown_terminal_writes_fail_precommit_stay_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-terminal-writes-fail-precommit")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(provider),
    )

    def fail_terminal_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected pre-commit terminal write failure")

    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_succeeded",
        fail_terminal_write,
    )
    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_outcome_unknown",
        fail_terminal_write,
    )

    with pytest.raises(ExternalActionReconciliationPendingError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_reconciliation_pending"
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    execution = workflow_store.get_execution(context.run_id)
    assert action is not None and action.status == ExternalActionStatus.DISPATCHING
    assert action.dispatch_count == 1
    assert step is not None and step.status == ToolCallStatus.RUNNING
    assert execution is not None and execution.status == WorkflowStatus.RUNNING

    workflow_event_types = [
        event.event_type for event in workflow_store.list_events(context.run_id)
    ]
    run_event_types = [event.event_type for event in run_store.list_events(context.run_id)]
    terminal_event_types = {
        "external_action.succeeded",
        "external_action.failed",
        "external_action.outcome_unknown",
        "step.completed",
        "step.failed",
        "tool.result",
        "loop.outcome",
        "workflow.ready",
        "workflow.blocked",
        "workflow.failed",
    }
    assert terminal_event_types.isdisjoint(workflow_event_types)
    assert terminal_event_types.isdisjoint(run_event_types)
    assert [
        event_type
        for event_type in workflow_event_types
        if event_type.startswith("external_action.")
    ] == ["external_action.prepared", "external_action.dispatch_started"]
    assert [
        event_type for event_type in workflow_event_types if event_type.startswith("step.")
    ] == ["step.claimed"]

    recovered_provider = NeverCalledProvider()
    recovered_workflow_store = SQLiteWorkflowStore(database_path)
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=recovered_workflow_store,
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(recovered_provider),
    )

    with pytest.raises(RuntimeExecutionError) as recovered:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert recovered.value.code == "external_action_outcome_unknown"
    assert recovered_provider.requests == []
    recovered_action = recovered_workflow_store.get_external_action(
        context.run_id,
        "call-0001",
    )
    recovered_step = recovered_workflow_store.get_step(context.run_id, "call-0001")
    recovered_execution = recovered_workflow_store.get_execution(context.run_id)
    assert (
        recovered_action is not None
        and recovered_action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    )
    assert recovered_step is not None and recovered_step.status == ToolCallStatus.FAILED
    assert (
        recovered_execution is not None
        and recovered_execution.status == WorkflowStatus.FAILED
    )
    recovered_event_types = [
        event.event_type
        for event in recovered_workflow_store.list_events(context.run_id)
    ]
    assert recovered_event_types.count("external_action.outcome_unknown") == 1
    assert recovered_event_types.count("step.failed") == 1
    assert recovered_event_types.count("workflow.failed") == 1


def test_recovery_prepare_failure_cannot_terminalize_dispatching_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-recovery-prepare-failure")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = CrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(crashing_provider),
    )

    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)

    recovered_workflow_store = SQLiteWorkflowStore(database_path)
    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=recovered_workflow_store,
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(recovered_provider),
    )

    def fail_recovery_prepare(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected recovery preparation failure")

    monkeypatch.setattr(
        recovered_workflow_store,
        "prepare_external_action",
        fail_recovery_prepare,
    )

    with pytest.raises(ExternalActionReconciliationPendingError):
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert recovered_provider.requests == []
    pending_action = recovered_workflow_store.get_external_action(
        context.run_id,
        "call-0001",
    )
    pending_step = recovered_workflow_store.get_step(context.run_id, "call-0001")
    pending_execution = recovered_workflow_store.get_execution(context.run_id)
    assert (
        pending_action is not None
        and pending_action.status == ExternalActionStatus.DISPATCHING
    )
    assert pending_step is not None and pending_step.status == ToolCallStatus.RUNNING
    assert (
        pending_execution is not None
        and pending_execution.status == WorkflowStatus.RUNNING
    )
    pending_event_types = [
        event.event_type
        for event in recovered_workflow_store.list_events(context.run_id)
    ]
    assert "external_action.outcome_unknown" not in pending_event_types
    assert "step.failed" not in pending_event_types
    assert "loop.outcome" not in pending_event_types
    assert "workflow.failed" not in pending_event_types

    final_workflow_store = SQLiteWorkflowStore(database_path)
    final_provider = NeverCalledProvider()
    final_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=final_workflow_store,
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(final_provider),
    )
    with pytest.raises(RuntimeExecutionError) as final:
        execute_loop(
            final_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert final.value.code == "external_action_outcome_unknown"
    assert final_provider.requests == []
    final_action = final_workflow_store.get_external_action(
        context.run_id,
        "call-0001",
    )
    final_step = final_workflow_store.get_step(context.run_id, "call-0001")
    final_execution = final_workflow_store.get_execution(context.run_id)
    assert final_action is not None
    assert final_action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert final_step is not None and final_step.status == ToolCallStatus.FAILED
    assert final_execution is not None
    assert final_execution.status == WorkflowStatus.FAILED


@pytest.mark.parametrize("missing_token", ["dispatch", "attempt"])
def test_recovery_missing_fence_token_remains_reconciliation_pending(
    tmp_path: Path,
    missing_token: str,
):
    context = execution_context(f"run-external-missing-{missing_token}-token")
    database_path = tmp_path / f"{missing_token}.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(CrashProvider()),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)

    with sqlite3.connect(database_path) as connection:
        if missing_token == "dispatch":
            connection.execute(
                """
                UPDATE external_actions SET dispatch_token = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (context.run_id, "call-0001"),
            )
        else:
            connection.execute(
                """
                UPDATE tool_calls SET attempt_token = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (context.run_id, "call-0001"),
            )

    recovered_workflow_store = SQLiteWorkflowStore(database_path)
    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=recovered_workflow_store,
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(recovered_provider),
    )
    with pytest.raises(ExternalActionReconciliationPendingError):
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert recovered_provider.requests == []
    action = recovered_workflow_store.get_external_action(
        context.run_id,
        "call-0001",
    )
    execution = recovered_workflow_store.get_execution(context.run_id)
    assert action is not None and action.status == ExternalActionStatus.DISPATCHING
    assert execution is not None and execution.status == WorkflowStatus.RUNNING
    event_types = [
        event.event_type
        for event in recovered_workflow_store.list_events(context.run_id)
    ]
    assert "loop.outcome" not in event_types
    assert "workflow.failed" not in event_types


def test_unknown_terminal_write_commit_then_raise_is_read_back_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-unknown-post-commit-exception")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )
    original_unknown_finalize = workflow_store.finalize_external_action_outcome_unknown

    def fail_success_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected pre-commit success write failure")

    def commit_unknown_then_raise(*args, **kwargs):
        original_unknown_finalize(*args, **kwargs)
        raise sqlite3.OperationalError("injected exception after unknown commit")

    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_succeeded",
        fail_success_write,
    )
    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_outcome_unknown",
        commit_unknown_then_raise,
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_outcome_unknown"
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    execution = workflow_store.get_execution(context.run_id)
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert step is not None and step.status == ToolCallStatus.FAILED
    assert step.error_code == "external_action_outcome_unknown"
    assert execution is not None and execution.status == WorkflowStatus.FAILED
    assert execution.error_code == "external_action_outcome_unknown"

    terminal_event_types = {
        "external_action.succeeded",
        "external_action.failed",
        "external_action.outcome_unknown",
        "step.completed",
        "step.failed",
        "tool.result",
        "loop.outcome",
        "workflow.ready",
        "workflow.blocked",
        "workflow.failed",
    }
    expected_terminal_sequence = [
        "external_action.outcome_unknown",
        "step.failed",
        "tool.result",
        "loop.outcome",
        "workflow.failed",
    ]
    expected_run_terminal_sequence = [
        "external_action.outcome_unknown",
        "tool.result",
        "loop.outcome",
    ]
    workflow_terminal_sequence = [
        event.event_type
        for event in workflow_store.list_events(context.run_id)
        if event.event_type in terminal_event_types
    ]
    run_terminal_sequence = [
        event.event_type
        for event in run_store.list_events(context.run_id)
        if event.event_type in terminal_event_types
    ]
    assert workflow_terminal_sequence == expected_terminal_sequence
    assert run_terminal_sequence == expected_run_terminal_sequence


def test_exception_after_success_commit_is_resolved_without_provider_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-success-post-commit-exception")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold(), finish()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )
    original_finalize = workflow_store.finalize_external_action_succeeded

    def commit_then_raise(*args, **kwargs):
        original_finalize(*args, **kwargs)
        raise sqlite3.OperationalError("injected exception after commit")

    monkeypatch.setattr(
        workflow_store,
        "finalize_external_action_succeeded",
        commit_then_raise,
    )

    result = execute_loop(loop, context)

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


def test_success_with_unrepairable_run_evidence_gap_is_not_generic_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-success-evidence-failure")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )
    original_append = run_store.append_event

    def fail_success_mirror(run_id, event_type, payload=None):
        if event_type == "external_action.succeeded":
            raise sqlite3.OperationalError("injected run evidence failure")
        return original_append(run_id, event_type, payload)

    monkeypatch.setattr(run_store, "append_event", fail_success_mirror)

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_evidence_incomplete"
    assert len(provider.requests) == 1
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


def test_unsafe_dispatch_recovery_never_calls_provider_and_fails_unknown(
    tmp_path: Path,
):
    context = execution_context("run-external-unsafe-recovery")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = CrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(crashing_provider),
    )

    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)
    assert len(crashing_provider.requests) == 1

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(recovered_provider),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    step = workflow_store.get_step(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert action.dispatch_count == 1
    assert step is not None and step.status == ToolCallStatus.FAILED
    assert step.error_code == "external_action_outcome_unknown"


def test_recovery_state_drift_cannot_bypass_dispatched_action_reconciliation(
    tmp_path: Path,
):
    context = execution_context("run-external-recovery-state-drift")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = IdempotentCrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(crashing_provider),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        recovered_loop.execute(
            runtime_input=RUNTIME_INPUT,
            state={**RUNTIME_STATE, "selection": "different-item"},
            context=context.model_copy(update={"recovered_after_restart": True}),
            finish_evaluator=finish_evaluator,
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN


@pytest.mark.parametrize("configuration", ["missing", "idempotency_drift"])
def test_recovery_provider_drift_fails_dispatched_action_as_unknown(
    tmp_path: Path,
    configuration: str,
):
    context = execution_context(f"run-external-provider-drift-{configuration}")
    database_path = tmp_path / f"{configuration}.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = IdempotentCrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(crashing_provider),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)

    recovered_provider = NonIdempotentProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=(
            None
            if configuration == "missing"
            else dispatcher_for(recovered_provider)
        ),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN


def test_recovery_provider_identity_drift_never_dispatches_to_new_account(
    tmp_path: Path,
):
    context = execution_context("run-external-provider-identity-drift")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = IdempotentCrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(crashing_provider),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)

    replacement_provider = NeverCalledProvider(
        provider_identity="synthetic-hold-account-v2"
    )
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(replacement_provider),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert replacement_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert action.provider_identity == "synthetic-hold-account-v1"


def _persist_success_then_crash(
    *,
    context: RuntimeExecutionContext,
    run_store: SQLiteRunStore,
    workflow_store: SQLiteWorkflowStore,
) -> SuccessProvider:
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold(), ProcessCrash()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(loop, context)
    return provider


def test_succeeded_external_action_restores_as_cached_without_provider_call(
    tmp_path: Path,
):
    context = execution_context("run-external-success-restore")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    first_provider = _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    assert len(first_provider.requests) == 1

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    result = execute_loop(
        recovered_loop,
        context.model_copy(update={"recovered_after_restart": True}),
    )

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert result.observations[0].cached is True
    assert recovered_provider.requests == []
    execution = workflow_store.get_execution(context.run_id)
    assert execution is not None and execution.status == WorkflowStatus.READY


def test_cached_success_evidence_failure_is_not_generic_or_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-cached-success-evidence-failure")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )

    def fail_tool_result(*_args, **_kwargs):
        raise sqlite3.OperationalError("injected cached tool evidence failure")

    monkeypatch.setattr(recovered_loop, "_record_tool_success", fail_tool_result)

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


def test_initial_recovery_mirror_failure_preserves_terminal_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-initial-mirror-failure")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "DELETE FROM run_events WHERE run_id = ? AND event_type = ?",
            (context.run_id, "external_action.succeeded"),
        )
    recovered_provider = NeverCalledProvider()
    recovered_run_store = SQLiteRunStore(database_path)
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=recovered_run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    original_append = recovered_run_store.append_event

    def fail_success_mirror(run_id, event_type, payload=None):
        if event_type == "external_action.succeeded":
            raise sqlite3.OperationalError("injected recovery mirror failure")
        return original_append(run_id, event_type, payload)

    monkeypatch.setattr(recovered_run_store, "append_event", fail_success_mirror)

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


@pytest.mark.parametrize("drift", ["tool_removed", "input_schema"])
def test_terminal_success_restore_drift_preserves_action_semantics(
    tmp_path: Path,
    drift: str,
):
    context = execution_context(f"run-external-success-restore-{drift}")
    database_path = tmp_path / f"{drift}.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    drifted_registry = ToolRegistry()
    if drift == "input_schema":
        drifted_registry.register(
            ToolSpec(
                name="create_hold",
                description="Drifted external hold schema.",
                input_model=ChangedHoldInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
                retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
                provider_name="synthetic-hold",
                output_model=HoldResult,
            )
        )
    recovered_loop.tool_registry = drifted_registry

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


@pytest.mark.parametrize("drift", ["output_schema", "corrupt_result_json"])
def test_terminal_success_result_drift_preserves_action_semantics(
    tmp_path: Path,
    drift: str,
):
    context = execution_context(f"run-external-success-result-{drift}")
    database_path = tmp_path / f"{drift}.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    if drift == "corrupt_result_json":
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "UPDATE tool_calls SET result_json = ? WHERE run_id = ? AND step_id = ?",
                ("not-json", context.run_id, "call-0001"),
            )

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    if drift == "output_schema":
        drifted_registry = ToolRegistry()
        drifted_registry.register(
            ToolSpec(
                name="create_hold",
                description="Drifted external hold output schema.",
                input_model=HoldInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
                retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
                provider_name="synthetic-hold",
                output_model=ChangedHoldResult,
            )
        )
        recovered_loop.tool_registry = drifted_registry

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


def test_restore_never_republishes_a_persisted_idempotency_key_echo(
    tmp_path: Path,
):
    context = execution_context("run-external-persisted-key-echo")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None
    echoed_reference = f"hold_{action.idempotency_key}"
    echoed_result = DynamicToolLoop._canonical_json(
        {
            "hold_id": "hold-key-echo",
            "status": "held",
            "provider_reference": echoed_reference,
        }
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            UPDATE external_actions
            SET provider_reference = ?, result_json = ?
            WHERE run_id = ? AND step_id = ?
            """,
            (echoed_reference, echoed_result, context.run_id, "call-0001"),
        )
        connection.execute(
            "UPDATE tool_calls SET result_json = ? WHERE run_id = ? AND step_id = ?",
            (echoed_result, context.run_id, "call-0001"),
        )

    recovered_provider = NeverCalledProvider()
    recovered_run_store = SQLiteRunStore(database_path)
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=recovered_run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []
    persisted_action = workflow_store.get_external_action(
        context.run_id,
        "call-0001",
    )
    assert (
        persisted_action is not None
        and persisted_action.status == ExternalActionStatus.SUCCEEDED
    )
    public_evidence = str(
        [event.model_dump(mode="json") for event in recovered_run_store.list_events(context.run_id)]
    )
    assert action.idempotency_key not in public_evidence


def test_terminal_failure_restore_spec_drift_preserves_definitive_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-failure-restore-spec-drift")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    provider = DefinitiveFailureProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    def crash_after_terminal_failure(**_kwargs):
        raise ProcessCrash()

    monkeypatch.setattr(
        first_loop,
        "_raise_external_terminal_failure",
        crash_after_terminal_failure,
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.FAILED

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    recovered_loop.tool_registry = ToolRegistry()

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_failed"
    assert len(provider.requests) == 1
    assert recovered_provider.requests == []


def test_terminal_action_does_not_mask_a_later_planner_failure(tmp_path: Path):
    context = execution_context("run-external-terminal-then-planner-failure")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(RuntimeError("injected later planner failure")),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "planner_provider_failed"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.SUCCEEDED


def test_restore_rejects_external_action_binding_tamper_before_provider_call(
    tmp_path: Path,
):
    context = execution_context("run-external-binding-tamper")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE external_actions SET tenant_id = ? WHERE run_id = ?",
            ("tenant-attacker", context.run_id),
        )

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_evidence_incomplete"
    assert recovered_provider.requests == []


def test_dispatching_binding_tamper_stays_outcome_unknown_during_cancellation(
    tmp_path: Path,
):
    context = execution_context("run-external-dispatch-binding-tamper")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    crashing_provider = CrashProvider()
    first_loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(crashing_provider),
    )
    with pytest.raises(ProcessCrash):
        execute_loop(first_loop, context)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE external_actions SET tenant_id = ? WHERE run_id = ?",
            ("tenant-attacker", context.run_id),
        )
    run_store.request_cancel_atomically(
        context.run_id,
        tenant_id=context.authority.tenant_id,
    )

    recovered_provider = NeverCalledProvider()
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=SQLiteRunStore(database_path),
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.UNSAFE,
        dispatcher=dispatcher_for(recovered_provider),
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(
            recovered_loop,
            context.model_copy(update={"recovered_after_restart": True}),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert recovered_provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("permission", "external_action_permission_denied"),
        ("provider", "external_action_not_configured"),
        ("idempotency", "external_action_idempotency_unsupported"),
    ],
)
def test_external_action_preflight_denials_create_no_step_or_action_rows(
    tmp_path: Path,
    case: str,
    expected_code: str,
):
    permissions = (
        ("tools:execute",)
        if case == "permission"
        else ("external-actions:execute", "tools:execute")
    )
    context = execution_context(f"run-external-preflight-{case}", permissions=permissions)
    run_store, workflow_store = initialize_stores(
        tmp_path / f"{case}.db",
        context,
    )
    provider: SuccessProvider = (
        NonIdempotentProvider() if case == "idempotency" else SuccessProvider()
    )
    dispatcher = None if case == "provider" else dispatcher_for(provider)
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher,
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == expected_code
    assert workflow_store.list_steps(context.run_id) == []
    assert workflow_store.list_external_actions(context.run_id) == []
    assert provider.requests == []


def test_external_action_runtime_input_gate_denies_unrequested_write_before_claim(
    tmp_path: Path,
):
    context = execution_context("run-external-not-explicitly-requested")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
        runtime_input_gate=lambda runtime_input: (
            runtime_input.get("requested_action") == "create_hold"
        ),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "external_action_not_requested"
    assert provider.requests == []
    assert workflow_store.list_steps(context.run_id) == []
    assert workflow_store.list_external_actions(context.run_id) == []


def test_recovery_repairs_missing_external_action_run_event_mirrors(
    tmp_path: Path,
):
    context = execution_context("run-external-mirror-repair")
    database_path = tmp_path / "runtime.db"
    run_store, workflow_store = initialize_stores(database_path, context)
    _persist_success_then_crash(
        context=context,
        run_store=run_store,
        workflow_store=workflow_store,
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "DELETE FROM run_events WHERE run_id = ? AND event_type LIKE ?",
            (context.run_id, "external_action.%"),
        )
    assert not any(
        event.event_type.startswith("external_action.")
        for event in run_store.list_events(context.run_id)
    )

    recovered_provider = NeverCalledProvider()
    recovered_run_store = SQLiteRunStore(database_path)
    recovered_loop = build_external_loop(
        planner=ScriptedPlanner(finish()),
        workflow_store=SQLiteWorkflowStore(database_path),
        run_store=recovered_run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(recovered_provider),
    )
    execute_loop(
        recovered_loop,
        context.model_copy(update={"recovered_after_restart": True}),
    )

    workflow_evidence = {
        (event.event_type, event.payload["evidence_id"])
        for event in workflow_store.list_events(context.run_id)
        if event.event_type.startswith("external_action.")
    }
    run_evidence = {
        (event.event_type, event.payload["evidence_id"])
        for event in recovered_run_store.list_events(context.run_id)
        if event.event_type.startswith("external_action.")
    }
    assert run_evidence == workflow_evidence
    assert recovered_provider.requests == []


def test_cancelled_run_stops_after_prepare_without_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    context = execution_context("run-external-cancel-before-dispatch")
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    provider = SuccessProvider()
    original_begin = workflow_store.begin_external_action_dispatch

    def cancel_then_begin(
        run_id: str,
        step_id: str,
        *,
        tool_attempt_token: str,
    ):
        run_store.request_cancel_atomically(
            run_id,
            tenant_id=context.authority.tenant_id,
        )
        return original_begin(
            run_id,
            step_id,
            tool_attempt_token=tool_attempt_token,
        )

    monkeypatch.setattr(
        workflow_store,
        "begin_external_action_dispatch",
        cancel_then_begin,
    )
    loop = build_external_loop(
        planner=ScriptedPlanner(call_hold()),
        workflow_store=workflow_store,
        run_store=run_store,
        sandbox=RecordingSandbox(),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        dispatcher=dispatcher_for(provider),
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        execute_loop(loop, context)

    assert raised.value.code == "run_cancel_requested"
    assert provider.requests == []
    action = workflow_store.get_external_action(context.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.PREPARED
    assert action.dispatch_count == 0


def test_read_only_loop_keeps_original_policy_payload_and_sandbox_path(
    tmp_path: Path,
):
    context = execution_context(
        "run-read-only-compatibility",
        permissions=("tools:execute",),
    )
    run_store, workflow_store = initialize_stores(tmp_path / "runtime.db", context)
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="echo",
            description="Return a value.",
            input_model=EchoInput,
            policy=ToolPolicy(),
            handler_entrypoint="tests.sandbox_handlers:echo_payload",
        )
    )
    sandbox = RecordingSandbox({"value": 7})
    loop = DynamicToolLoop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="echo",
                arguments={"value": 7},
                reason="Read the value.",
            ),
            finish(),
        ),
        tool_registry=registry,
        tool_sandbox=sandbox,  # type: ignore[arg-type]
        workflow_store=workflow_store,
        run_event_sink=run_store,
        workflow_type="generic-read-only:1.0.0",
    )

    result = execute_loop(loop, context)

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert sandbox.calls == [("echo", {"value": 7})]
    policy_event = next(
        event
        for event in workflow_store.list_events(context.run_id)
        if event.event_type == "policy.decision"
    )
    assert policy_event.payload == {
        "evidence_id": "policy:call-0001",
        "step_id": "call-0001",
        "tool_name": "echo",
        "outcome": "allowed",
        "error_code": None,
    }
    assert workflow_store.list_external_actions(context.run_id) == []
