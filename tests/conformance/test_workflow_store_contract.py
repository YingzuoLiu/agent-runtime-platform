from __future__ import annotations

import pytest

from runtime_service.models import RunStatus
from runtime_service.workflow_store import (
    ClaimOutcome,
    ExecutionOutcome,
    ExternalActionDispatchOutcome,
    ExternalActionRetryMode,
    ExternalActionStatus,
    StaleAttemptError,
    StaleDispatchError,
    ToolCallStatus,
    WorkflowStatus,
)

from .backends import InjectedConformanceFailure, StoreConformanceBackend
from .scenarios import (
    INPUT_HASH,
    TOOL_NAME,
    WORKFLOW_TYPE,
    claim_run,
    create_managed_workflow,
    create_queued_run,
    prepare_external_action,
)


def test_workflow_execution_identity_outcomes(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_workflow_store()
    created = store.create_or_get_execution("workflow-identity", WORKFLOW_TYPE, INPUT_HASH)
    repeated = store.create_or_get_execution("workflow-identity", WORKFLOW_TYPE, INPUT_HASH)
    wrong_input = store.create_or_get_execution(
        "workflow-identity",
        WORKFLOW_TYPE,
        "sha256:" + "b" * 64,
    )
    wrong_workflow = store.create_or_get_execution(
        "workflow-identity",
        "other-workflow:1.0.0",
        INPUT_HASH,
    )

    assert created.outcome == ExecutionOutcome.CREATED
    assert repeated.outcome == ExecutionOutcome.EXISTING
    assert repeated.execution.created_at == created.execution.created_at
    assert wrong_input.outcome == ExecutionOutcome.INPUT_MISMATCH
    assert wrong_workflow.outcome == ExecutionOutcome.WORKFLOW_TYPE_MISMATCH


def test_workflow_step_identity_checks_do_not_append_events(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_workflow_store()
    store.create_or_get_execution("workflow-step-identity", WORKFLOW_TYPE, INPUT_HASH)
    store.mark_running("workflow-step-identity")
    claim = store.claim_step(
        "workflow-step-identity",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=2,
    )
    assert claim.outcome == ClaimOutcome.CLAIMED
    assert claim.attempt_token is not None
    store.complete_step(
        "workflow-step-identity",
        "step-1",
        claim.attempt_token,
        result_json='{"ok":true}',
    )
    before_identity_checks = store.list_events("workflow-step-identity")
    cached = store.claim_step(
        "workflow-step-identity",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=2,
    )
    wrong_step_input = store.claim_step(
        "workflow-step-identity",
        "step-1",
        "read_evidence",
        "sha256:" + "c" * 64,
        max_attempts=2,
    )
    wrong_step_tool = store.claim_step(
        "workflow-step-identity",
        "step-1",
        "different_tool",
        INPUT_HASH,
        max_attempts=2,
    )

    assert cached.outcome == ClaimOutcome.CACHED
    assert wrong_step_input.outcome == ClaimOutcome.INPUT_MISMATCH
    assert wrong_step_tool.outcome == ClaimOutcome.DEFINITION_MISMATCH
    assert store.list_events("workflow-step-identity") == before_identity_checks


def test_workflow_event_order_cursor_and_restart_visibility(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_workflow_store()
    store.create_or_get_execution("workflow-events", WORKFLOW_TYPE, INPUT_HASH)
    store.mark_running("workflow-events")
    claim = store.claim_step(
        "workflow-events",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=2,
    )
    assert claim.attempt_token is not None
    store.complete_step(
        "workflow-events",
        "step-1",
        claim.attempt_token,
        result_json='{"ok":true}',
    )
    store.finalize_ready("workflow-events", result_json='{"status":"ready"}')

    events = store.list_events("workflow-events")
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert [event.event_type for event in events] == [
        "workflow.started",
        "step.claimed",
        "step.completed",
        "workflow.ready",
    ]
    assert (
        store.list_events(
            "workflow-events",
            after_sequence=events[1].sequence,
        )
        == events[2:]
    )

    reopened = store_backend.open_workflow_store()
    execution = reopened.get_execution("workflow-events")
    step = reopened.get_step("workflow-events", "step-1")
    assert execution is not None and execution.status == WorkflowStatus.READY
    assert step is not None and step.status == ToolCallStatus.COMPLETED
    assert reopened.list_events("workflow-events") == events


def test_i2_stale_tool_attempt_cannot_mutate_durable_state(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_workflow_store()
    store.create_or_get_execution("workflow-attempt-fence", WORKFLOW_TYPE, INPUT_HASH)
    first = store.claim_step(
        "workflow-attempt-fence",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=2,
    )
    assert first.attempt_token is not None
    store.recover_interrupted_step("workflow-attempt-fence", "step-1")
    second = store.claim_step(
        "workflow-attempt-fence",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=2,
    )
    assert second.attempt_token is not None
    assert second.attempt_token != first.attempt_token
    before_events = store.list_events("workflow-attempt-fence")

    with pytest.raises(StaleAttemptError):
        store.complete_step(
            "workflow-attempt-fence",
            "step-1",
            first.attempt_token,
            result_json='{"stale":true}',
        )

    persisted = store.get_step("workflow-attempt-fence", "step-1")
    assert persisted is not None
    assert persisted.status == ToolCallStatus.RUNNING
    assert persisted.attempt_token == second.attempt_token
    assert persisted.result_json is None
    assert store.list_events("workflow-attempt-fence") == before_events


def test_prepare_precedes_dispatch_and_dispatch_token_fences_late_result(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_workflow_store()
    create_queued_run(
        run_store,
        "workflow-dispatch-fence",
        thread_id="workflow-dispatch-fence-thread",
        order=1,
    )
    run_claim = claim_run(run_store, "workflow-dispatch-owner")
    assert run_claim is not None
    store.create_or_get_execution(
        run_claim.run.run_id,
        WORKFLOW_TYPE,
        INPUT_HASH,
        lease_token=run_claim.lease_token,
    )
    store.mark_running(run_claim.run.run_id, lease_token=run_claim.lease_token)
    step_claim = store.claim_step(
        run_claim.run.run_id,
        "call-0001",
        TOOL_NAME,
        INPUT_HASH,
        max_attempts=1,
        lease_token=run_claim.lease_token,
    )
    assert step_claim.attempt_token is not None

    with pytest.raises(KeyError, match="External action not found"):
        store.begin_external_action_dispatch(
            run_claim.run.run_id,
            "call-0001",
            tool_attempt_token=step_claim.attempt_token,
            lease_token=run_claim.lease_token,
        )

    prepared = prepare_external_action(
        store,
        run_claim.lease_token,
        run_id=run_claim.run.run_id,
        tool_attempt_token=step_claim.attempt_token,
        retry_mode=ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
    )
    assert prepared.action is not None
    first = store.begin_external_action_dispatch(
        run_claim.run.run_id,
        "call-0001",
        tool_attempt_token=step_claim.attempt_token,
        lease_token=run_claim.lease_token,
    )
    assert first.dispatch_token is not None
    second = store.retry_external_action_dispatch(
        run_claim.run.run_id,
        "call-0001",
        previous_dispatch_token=first.dispatch_token,
        tool_attempt_token=step_claim.attempt_token,
        lease_token=run_claim.lease_token,
    )
    assert second.outcome == ExternalActionDispatchOutcome.RETRY_CLAIMED
    assert second.dispatch_token is not None
    assert second.dispatch_token != first.dispatch_token

    with pytest.raises(StaleDispatchError):
        store.finalize_external_action_succeeded(
            run_claim.run.run_id,
            "call-0001",
            dispatch_token=first.dispatch_token,
            tool_attempt_token=step_claim.attempt_token,
            result_json='{"created":true}',
            provider_reference="stale-provider-result",
        )

    action = store.get_external_action(run_claim.run.run_id, "call-0001")
    step = store.get_step(run_claim.run.run_id, "call-0001")
    assert action is not None and action.status == ExternalActionStatus.DISPATCHING
    assert action.dispatch_count == 2
    assert step is not None and step.status == ToolCallStatus.RUNNING
    assert step.result_json is None


def test_cancellation_wins_before_first_external_dispatch(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store, store, run_claim, step_claim, action = create_managed_workflow(
        store_backend,
        run_id="workflow-cancel-arbitration",
    )
    run_store.request_cancel_atomically(run_claim.run.run_id, tenant_id=run_claim.run.tenant_id)

    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=step_claim.attempt_token,
        lease_token=run_claim.lease_token,
    )

    assert dispatch.outcome == ExternalActionDispatchOutcome.RUN_CANCELLED
    persisted = store.get_external_action(action.run_id, action.step_id)
    assert persisted is not None
    assert persisted.status == ExternalActionStatus.PREPARED
    assert persisted.dispatch_count == 0
    assert not any(
        event.event_type == "external_action.dispatch_started"
        for event in store.list_events(action.run_id)
    )


def test_workflow_state_transition_and_event_append_are_atomic(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_workflow_store()
    store.create_or_get_execution("workflow-atomic", WORKFLOW_TYPE, INPUT_HASH)
    claim = store.claim_step(
        "workflow-atomic",
        "step-1",
        "read_evidence",
        INPUT_HASH,
        max_attempts=1,
    )
    assert claim.attempt_token is not None

    with store_backend.fail_workflow_event(store, "step.completed"):
        with pytest.raises(InjectedConformanceFailure):
            store.complete_step(
                "workflow-atomic",
                "step-1",
                claim.attempt_token,
                result_json='{"ok":true}',
            )

    reopened = store_backend.open_workflow_store()
    step = reopened.get_step("workflow-atomic", "step-1")
    assert step is not None
    assert step.status == ToolCallStatus.RUNNING
    assert step.result_json is None
    assert [event.event_type for event in reopened.list_events("workflow-atomic")] == [
        "step.claimed"
    ]


def test_external_action_parent_step_and_events_finalize_atomically(
    store_backend: StoreConformanceBackend,
) -> None:
    _, store, run_claim, step_claim, action = create_managed_workflow(
        store_backend,
        run_id="workflow-action-atomic",
    )
    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=step_claim.attempt_token,
        lease_token=run_claim.lease_token,
    )
    assert dispatch.dispatch_token is not None

    with store_backend.fail_workflow_event(store, "step.completed"):
        with pytest.raises(InjectedConformanceFailure):
            store.finalize_external_action_succeeded(
                action.run_id,
                action.step_id,
                dispatch_token=dispatch.dispatch_token,
                tool_attempt_token=step_claim.attempt_token,
                result_json='{"created":true}',
                provider_reference="provider-reference",
            )

    reopened = store_backend.open_workflow_store()
    persisted_action = reopened.get_external_action(action.run_id, action.step_id)
    persisted_step = reopened.get_step(action.run_id, action.step_id)
    assert persisted_action is not None
    assert persisted_action.status == ExternalActionStatus.DISPATCHING
    assert persisted_action.result_json is None
    assert persisted_step is not None
    assert persisted_step.status == ToolCallStatus.RUNNING
    assert persisted_step.result_json is None
    event_types = [event.event_type for event in reopened.list_events(action.run_id)]
    assert "external_action.succeeded" not in event_types
    assert "step.completed" not in event_types

    run = store_backend.open_run_store().get_run_internal(action.run_id)
    assert run is not None and run.status == RunStatus.RUNNING
