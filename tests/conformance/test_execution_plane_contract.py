from __future__ import annotations

import threading

import pytest
from pydantic import BaseModel, ConfigDict

from agent.contracts import RuntimeExecutionContext, RuntimeExecutionError
from domains.travel.runtime import TravelMessageInput
from domains.travel.state import AgentState
from runtime_service.evidence import EvidenceProjector
from runtime_service.external_action_coordinator import ExternalActionCoordinator
from runtime_service.external_actions import (
    AmbiguousExternalActionError,
    ExternalActionDispatcher,
    ExternalActionProviderRegistry,
    ExternalActionReconciliationPendingError,
)
from runtime_service.manager import RuntimeManager
from runtime_service.models import RunCommitOutcome, RunStatus
from runtime_service.registry import AgentRegistry
from runtime_service.sandbox import ToolEffect, ToolPolicy, ToolRetryMode, ToolSpec
from runtime_service.store import THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
from runtime_service.workflow_store import ExternalActionStatus, ToolCallStatus, WorkflowStore

from .backends import InjectedConformanceFailure, SQLiteConformanceBackend
from .scenarios import (
    PROVIDER_IDENTITY,
    PROVIDER_NAME,
    TENANT_ID,
    TOOL_NAME,
    WORKFLOW_TYPE,
    claim_run,
    complete_with_budget,
    create_queued_run,
)


class ConformanceActionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: int


class ConformanceActionOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    created: bool
    provider_reference: str


class AmbiguousProvider:
    supports_idempotency = False
    provider_identity = PROVIDER_IDENTITY

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _request):
        self.calls += 1
        raise AmbiguousExternalActionError()


class NeverCalledProvider:
    supports_idempotency = False
    provider_identity = PROVIDER_IDENTITY

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _request):
        self.calls += 1
        raise AssertionError("Unsafe recovery must not call the provider")


def unsafe_spec() -> ToolSpec:
    return ToolSpec(
        name=TOOL_NAME,
        description="Conformance-only external write",
        input_model=ConformanceActionInput,
        output_model=ConformanceActionOutput,
        policy=ToolPolicy(),
        effect=ToolEffect.EXTERNAL_WRITE,
        retry_mode=ToolRetryMode.UNSAFE,
        provider_name=PROVIDER_NAME,
    )


def execution_context(claim, *, recovered: bool) -> RuntimeExecutionContext:
    authority = claim.run.execution_authority
    assert authority is not None
    return RuntimeExecutionContext(
        run_id=claim.run.run_id,
        thread_id=claim.run.thread_id,
        recovered_after_restart=recovered,
        authority=authority,
        lease_token=claim.lease_token,
    )


def build_coordinator(*, workflow_store: WorkflowStore, run_store, provider):
    providers = ExternalActionProviderRegistry()
    providers.register(PROVIDER_NAME, provider)

    def fail(_context, code: str, message: str):
        raise RuntimeExecutionError(code, message)

    return ExternalActionCoordinator(
        workflow_store=workflow_store,
        dispatcher=ExternalActionDispatcher(providers),
        workflow_type=WORKFLOW_TYPE,
        evidence_projector=EvidenceProjector(
            workflow_store=workflow_store,
            run_event_sink=run_store,
        ),
        fail=fail,
        failure_messages=ExternalActionCoordinator.DEFAULT_FAILURE_MESSAGES,
    )


def leave_unsafe_dispatch_pending(
    *,
    run_store,
    workflow_store: WorkflowStore,
    claim,
) -> AmbiguousProvider:
    workflow_store.create_or_get_execution(
        claim.run.run_id,
        WORKFLOW_TYPE,
        "conformance-input-hash",
        lease_token=claim.lease_token,
    )
    workflow_store.mark_running(claim.run.run_id, lease_token=claim.lease_token)
    provider = AmbiguousProvider()
    coordinator = build_coordinator(
        workflow_store=workflow_store,
        run_store=run_store,
        provider=provider,
    )
    original = workflow_store.finalize_external_action_outcome_unknown

    def fail_terminal_write(*_args, **_kwargs):
        raise InjectedConformanceFailure("injected terminal action write failure")

    workflow_store.finalize_external_action_outcome_unknown = fail_terminal_write  # type: ignore[method-assign]
    try:
        with pytest.raises(ExternalActionReconciliationPendingError):
            coordinator.execute(
                context=execution_context(claim, recovered=False),
                tool_name=TOOL_NAME,
                spec=unsafe_spec(),
                step_id="call-0001",
                normalized_arguments={"value": 1},
            )
    finally:
        workflow_store.finalize_external_action_outcome_unknown = original  # type: ignore[method-assign]

    assert provider.calls == 1
    action = workflow_store.get_external_action(claim.run.run_id, "call-0001")
    assert action is not None
    assert action.status == ExternalActionStatus.DISPATCHING
    assert action.dispatch_count == 1
    assert (
        run_store.commit_reconciliation_pending(
            claim.run.run_id,
            tenant_id=claim.run.tenant_id,
            lease_token=claim.lease_token,
            error_code=ExternalActionReconciliationPendingError.CODE,
            error="ExternalActionReconciliationPendingError: recovery required",
        )
        == RunCommitOutcome.COMMITTED
    )
    return provider


def test_i7_reconciliation_precedes_successor_and_never_retries_unsafe_effect(
    store_backend: SQLiteConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    workflow_store = store_backend.open_workflow_store()
    create_queued_run(
        run_store,
        "run-ambiguous-predecessor",
        thread_id="ambiguous-thread",
        order=1,
    )
    predecessor = claim_run(run_store, "ambiguous-first-owner")
    assert predecessor is not None
    first_provider = leave_unsafe_dispatch_pending(
        run_store=run_store,
        workflow_store=workflow_store,
        claim=predecessor,
    )
    create_queued_run(
        run_store,
        "run-after-ambiguous-effect",
        thread_id="ambiguous-thread",
        order=2,
    )
    assert predecessor.run.lease_expires_at is not None
    store_backend.clock.advance(
        predecessor.run.lease_expires_at - store_backend.clock()
    )

    recovered_run_store = store_backend.open_run_store()
    recovered_workflow_store = store_backend.open_workflow_store()
    recovered = claim_run(recovered_run_store, "ambiguous-recovery-owner")
    assert recovered is not None
    assert recovered.run.run_id == predecessor.run.run_id
    queued_successor = recovered_run_store.get_run_internal("run-after-ambiguous-effect")
    assert queued_successor is not None
    assert queued_successor.status == RunStatus.QUEUED
    assert queued_successor.attempt == 0

    recovery_provider = NeverCalledProvider()
    recovered_coordinator = build_coordinator(
        workflow_store=recovered_workflow_store,
        run_store=recovered_run_store,
        provider=recovery_provider,
    )
    with pytest.raises(RuntimeExecutionError) as raised:
        recovered_coordinator.execute(
            context=execution_context(recovered, recovered=True),
            tool_name=TOOL_NAME,
            spec=unsafe_spec(),
            step_id="call-0001",
            normalized_arguments={"value": 1},
        )
    assert raised.value.code == "external_action_outcome_unknown"
    assert first_provider.calls == 1
    assert recovery_provider.calls == 0

    action = recovered_workflow_store.get_external_action(
        recovered.run.run_id,
        "call-0001",
    )
    step = recovered_workflow_store.get_step(recovered.run.run_id, "call-0001")
    assert action is not None
    assert action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert action.dispatch_count == 1
    assert action.error_code == "external_action_outcome_unknown"
    assert step is not None
    assert step.status == ToolCallStatus.FAILED
    assert step.error_code == "external_action_outcome_unknown"

    assert (
        recovered_run_store.commit_failed_run(
            recovered.run,
            lease_token=recovered.lease_token,
            error_code=raised.value.code,
            error="RuntimeExecutionError: ambiguous external effect",
        )
        == RunCommitOutcome.COMMITTED
    )
    successor = claim_run(recovered_run_store, "successor-owner")
    assert successor is not None
    assert successor.run.run_id == "run-after-ambiguous-effect"

    durable_run_store = store_backend.open_run_store()
    durable_workflow_store = store_backend.open_workflow_store()
    predecessor_run = durable_run_store.get_run_internal(predecessor.run.run_id)
    durable_action = durable_workflow_store.get_external_action(
        predecessor.run.run_id,
        "call-0001",
    )
    assert predecessor_run is not None
    assert predecessor_run.status == RunStatus.FAILED
    assert predecessor_run.error_code == "external_action_outcome_unknown"
    assert durable_action is not None
    assert durable_action.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert durable_action.dispatch_count == 1


def test_i8_checkpoint_conflict_is_inspectable_nonterminal_quarantine(
    store_backend: SQLiteConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    workflow_store = store_backend.open_workflow_store()
    thread_id = "quarantine-thread"
    create_queued_run(run_store, "run-quarantine-seed", thread_id=thread_id, order=1)
    seed = claim_run(run_store, "quarantine-seed-owner")
    assert seed is not None
    assert complete_with_budget(run_store, seed, budget=6_000) == RunCommitOutcome.COMMITTED

    create_queued_run(
        run_store,
        "run-quarantine-predecessor",
        thread_id=thread_id,
        order=2,
    )
    predecessor = claim_run(run_store, "quarantine-first-owner")
    assert predecessor is not None
    assert predecessor.run.checkpoint_base_revision == 1
    leave_unsafe_dispatch_pending(
        run_store=run_store,
        workflow_store=workflow_store,
        claim=predecessor,
    )
    create_queued_run(
        run_store,
        "run-quarantine-successor",
        thread_id=thread_id,
        order=3,
    )
    store_backend.replace_checkpoint_out_of_band(
        tenant_id=TENANT_ID,
        thread_id=thread_id,
        state=AgentState(
            thread_id=thread_id,
            destination="Tokyo",
            budget=7_777,
        ),
        expected_revision=1,
    )
    assert predecessor.run.lease_expires_at is not None
    store_backend.clock.advance(
        predecessor.run.lease_expires_at - store_backend.clock()
    )

    constructed = threading.Event()
    registry = AgentRegistry()

    def forbidden_runtime_factory():
        constructed.set()
        raise AssertionError("Quarantine must precede Runtime construction")

    registry.register(
        "travel-agent",
        "0.3.0",
        forbidden_runtime_factory,
        description="Conformance quarantine sentinel",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    recovery_store = store_backend.open_run_store(bind_default_registry=False)
    recovery_workflow_store = store_backend.open_workflow_store()
    quarantined = threading.Event()
    original_quarantine = recovery_store.quarantine_checkpoint_conflict_for_reconciliation

    def observe_quarantine(run, *, lease_token: str, phase: str):
        outcome = original_quarantine(
            run,
            lease_token=lease_token,
            phase=phase,
        )
        quarantined.set()
        return outcome

    recovery_store.quarantine_checkpoint_conflict_for_reconciliation = (  # type: ignore[method-assign]
        observe_quarantine
    )
    manager = RuntimeManager(
        recovery_store,
        registry,
        recovery_reconciliation_required=(
            recovery_workflow_store.has_external_action_requiring_reconciliation
        ),
        owner_id="quarantine-recovery-manager",
        lease_duration_seconds=10,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    manager.start()
    try:
        assert quarantined.wait(timeout=5)
    finally:
        manager.stop()

    assert not constructed.is_set()
    persisted = recovery_store.get_run_internal(predecessor.run.run_id)
    successor = recovery_store.get_run_internal("run-quarantine-successor")
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert persisted.error_code == THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
    assert persisted.completed_at is None
    assert persisted.lease_token is None
    assert successor is not None
    assert successor.status == RunStatus.QUEUED
    assert successor.attempt == 0
    event_types = [
        event.event_type
        for event in recovery_store.list_events(predecessor.run.run_id)
    ]
    assert event_types[-3:] == ["run.recovered", "run.started", "checkpoint.conflict"]
    assert "run.failed" not in event_types
    conflict = recovery_store.list_events(predecessor.run.run_id)[-1]
    assert conflict.payload == {
        "phase": "load",
        "expected_revision": 1,
        "observed_revision": 2,
        "disposition": "external_action_reconciliation_quarantined",
    }

    restarted = store_backend.open_run_store()
    assert claim_run(restarted, "must-not-take-over-quarantine") is None
    restarted.request_cancel_atomically(predecessor.run.run_id, tenant_id=TENANT_ID)
    assert claim_run(restarted, "must-not-terminalize-quarantine") is None
    after_cancel = restarted.get_run_internal(predecessor.run.run_id)
    assert after_cancel is not None
    assert after_cancel.status == RunStatus.RUNNING
    assert after_cancel.error_code == THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
    assert after_cancel.cancel_requested

    create_queued_run(
        restarted,
        "run-outside-quarantine",
        thread_id="independent-quarantine-thread",
        order=4,
    )
    independent = claim_run(restarted, "independent-owner")
    assert independent is not None
    assert independent.run.run_id == "run-outside-quarantine"
