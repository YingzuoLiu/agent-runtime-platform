from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from agent.contracts import RuntimeExecutionAuthority
from domains.travel.state import AgentState
from runtime_service.models import RunCommitOutcome, RunRecord, RunStatus
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import (
    ExternalActionPrepareResult,
    ExternalActionRetryMode,
    ExternalActionStatus,
    WorkflowStore,
)

from .backends import SQLiteConformanceBackend


TENANT_ID = "conformance-tenant"
SUBJECT_ID = "conformance-subject"
LEASE_SECONDS = 10
WORKFLOW_TYPE = "conformance-workflow:1.0.0"
TOOL_NAME = "conformance_external_write"
PROVIDER_NAME = "conformance-provider"
PROVIDER_IDENTITY = "conformance-provider-account-v1"
INPUT_HASH = "sha256:" + "a" * 64
ARGUMENTS_JSON = '{"value":1}'


def create_queued_run(
    store: SQLiteRunStore,
    run_id: str,
    *,
    thread_id: str,
    order: int,
    tenant_id: str = TENANT_ID,
    agent_id: str = "travel-agent",
    agent_version: str = "0.3.0",
) -> RunRecord:
    timestamp = f"2026-08-22T00:00:{order:02d}+00:00"
    run = RunRecord(
        run_id=run_id,
        tenant_id=tenant_id,
        thread_id=thread_id,
        agent_id=agent_id,
        agent_version=agent_version,
        domain_id="travel",
        schema_version="1",
        status=RunStatus.QUEUED,
        input={"user_message": f"execute {run_id}"},
        execution_authority=RuntimeExecutionAuthority(
            tenant_id=tenant_id,
            subject_id=SUBJECT_ID,
            permissions=("external-actions:execute", "tools:execute"),
        ),
        created_at=timestamp,
        updated_at=timestamp,
    )
    store.create_run_with_event(run, event_type="run.queued")
    return run


def claim_run(store: SQLiteRunStore, owner_id: str):
    return store.claim_next_run(
        owner_id=owner_id,
        lease_duration_seconds=LEASE_SECONDS,
        reconciliation_pending_code="external_action_reconciliation_pending",
    )


def claim_concurrently(
    first_store: SQLiteRunStore,
    second_store: SQLiteRunStore,
):
    barrier = threading.Barrier(3)

    def claim(store: SQLiteRunStore, owner_id: str):
        barrier.wait(timeout=5)
        return claim_run(store, owner_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(claim, first_store, "conformance-owner-a")
        second = executor.submit(claim, second_store, "conformance-owner-b")
        barrier.wait(timeout=5)
        return first.result(timeout=5), second.result(timeout=5)


def complete_with_budget(
    store: SQLiteRunStore,
    claim,
    *,
    budget: int,
) -> RunCommitOutcome:
    claim.run.state = AgentState(
        thread_id=claim.run.thread_id,
        destination="Tokyo",
        budget=budget,
    )
    claim.run.output_message = f"budget={budget}"
    claim.run.validation_errors = []
    return store.commit_completed_run(
        claim.run,
        lease_token=claim.lease_token,
    )


def create_managed_workflow(
    backend: SQLiteConformanceBackend,
    *,
    run_id: str,
    thread_id: str | None = None,
    retry_mode: ExternalActionRetryMode = ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
):
    run_store = backend.open_run_store()
    workflow_store = backend.open_workflow_store()
    create_queued_run(
        run_store,
        run_id,
        thread_id=thread_id or f"thread-{run_id}",
        order=1,
    )
    run_claim = claim_run(run_store, f"owner-{run_id}")
    assert run_claim is not None and run_claim.run.run_id == run_id
    workflow_store.create_or_get_execution(
        run_id,
        WORKFLOW_TYPE,
        INPUT_HASH,
        lease_token=run_claim.lease_token,
    )
    workflow_store.mark_running(run_id, lease_token=run_claim.lease_token)
    step_claim = workflow_store.claim_step(
        run_id,
        "call-0001",
        TOOL_NAME,
        INPUT_HASH,
        max_attempts=3,
        lease_token=run_claim.lease_token,
    )
    assert step_claim.attempt_token is not None
    prepared = prepare_external_action(
        workflow_store,
        run_claim.lease_token,
        run_id=run_id,
        tool_attempt_token=step_claim.attempt_token,
        retry_mode=retry_mode,
    )
    assert prepared.action is not None
    return run_store, workflow_store, run_claim, step_claim, prepared.action


def prepare_external_action(
    store: WorkflowStore,
    lease_token: str,
    *,
    run_id: str,
    tool_attempt_token: str,
    retry_mode: ExternalActionRetryMode,
    input_hash: str = INPUT_HASH,
    arguments_json: str = ARGUMENTS_JSON,
    idempotency_key: str | None = None,
) -> ExternalActionPrepareResult:
    return store.prepare_external_action(
        run_id=run_id,
        step_id="call-0001",
        tool_attempt_token=tool_attempt_token,
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        workflow_type=WORKFLOW_TYPE,
        tool_name=TOOL_NAME,
        provider_name=PROVIDER_NAME,
        provider_identity=PROVIDER_IDENTITY,
        input_hash=input_hash,
        arguments_json=arguments_json,
        retry_mode=retry_mode,
        idempotency_key=idempotency_key or f"idempotency-{run_id}",
        lease_token=lease_token,
    )


def create_action_quarantine(
    backend: SQLiteConformanceBackend,
    *,
    run_id: str,
    thread_id: str,
    terminal_status: ExternalActionStatus | None = ExternalActionStatus.SUCCEEDED,
):
    run_store = backend.open_run_store()
    workflow_store = backend.open_workflow_store()
    create_queued_run(
        run_store,
        f"{run_id}-seed",
        thread_id=thread_id,
        order=1,
    )
    seed = claim_run(run_store, f"{run_id}-seed-owner")
    assert seed is not None
    assert complete_with_budget(run_store, seed, budget=6_000) == RunCommitOutcome.COMMITTED

    create_queued_run(run_store, run_id, thread_id=thread_id, order=2)
    predecessor = claim_run(run_store, f"{run_id}-first-owner")
    assert predecessor is not None
    workflow_store.create_or_get_execution(
        run_id,
        WORKFLOW_TYPE,
        INPUT_HASH,
        lease_token=predecessor.lease_token,
    )
    workflow_store.mark_running(run_id, lease_token=predecessor.lease_token)
    step = workflow_store.claim_step(
        run_id,
        "call-0001",
        TOOL_NAME,
        INPUT_HASH,
        max_attempts=2,
        lease_token=predecessor.lease_token,
    )
    assert step.attempt_token is not None
    prepared = prepare_external_action(
        workflow_store,
        predecessor.lease_token,
        run_id=run_id,
        tool_attempt_token=step.attempt_token,
        retry_mode=ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
    )
    assert prepared.action is not None
    dispatch = workflow_store.begin_external_action_dispatch(
        run_id,
        "call-0001",
        tool_attempt_token=step.attempt_token,
        lease_token=predecessor.lease_token,
    )
    assert dispatch.dispatch_token is not None
    if terminal_status == ExternalActionStatus.SUCCEEDED:
        workflow_store.finalize_external_action_succeeded(
            run_id,
            "call-0001",
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=step.attempt_token,
            result_json='{"created":true,"provider_reference":"conformance-ref"}',
            provider_reference="conformance-ref",
        )
    elif terminal_status == ExternalActionStatus.FAILED:
        workflow_store.finalize_external_action_failed(
            run_id,
            "call-0001",
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=step.attempt_token,
            error_code="external_action_failed",
        )
    elif terminal_status == ExternalActionStatus.OUTCOME_UNKNOWN:
        workflow_store.finalize_external_action_outcome_unknown(
            run_id,
            "call-0001",
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=step.attempt_token,
            error_code="external_action_outcome_unknown",
        )
    elif terminal_status is not None:
        raise ValueError("terminal_status must be a terminal external action status")

    assert (
        run_store.commit_reconciliation_pending(
            run_id,
            tenant_id=TENANT_ID,
            lease_token=predecessor.lease_token,
            error_code="external_action_reconciliation_pending",
            error="ExternalActionReconciliationPendingError: recovery required",
        )
        == RunCommitOutcome.COMMITTED
    )
    successor_id = f"{run_id}-successor"
    create_queued_run(
        run_store,
        successor_id,
        thread_id=thread_id,
        order=3,
    )
    backend.replace_checkpoint_out_of_band(
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
    backend.clock.advance(predecessor.run.lease_expires_at - backend.clock())
    recovery_store = backend.open_run_store()
    recovered = claim_run(recovery_store, f"{run_id}-recovery-owner")
    assert recovered is not None and recovered.run.run_id == run_id
    assert (
        recovery_store.quarantine_checkpoint_conflict_for_reconciliation(
            recovered.run,
            lease_token=recovered.lease_token,
            phase="load",
        )
        == RunCommitOutcome.COMMITTED
    )
    return recovery_store, workflow_store, successor_id
