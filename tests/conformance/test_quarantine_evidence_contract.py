from __future__ import annotations

import pytest

from domains.travel.state import AgentState
from runtime_service.models import RunStatus
from runtime_service.quarantine import (
    QuarantineResolutionEvidenceIncompleteError,
    QuarantineResolutionKind,
    QuarantineResolutionStalePlanError,
    QuarantineResolutionTarget,
)

from .backends import StoreConformanceBackend
from .scenarios import TENANT_ID, create_action_quarantine


RESOLUTION = QuarantineResolutionKind.TERMINALIZE_FAILED_PRESERVING_CHECKPOINT


def test_i9_workflow_evidence_change_after_plan_makes_plan_stale(
    store_backend: StoreConformanceBackend,
) -> None:
    run_id = "run-quarantine-workflow-drift"
    thread_id = "thread-quarantine-workflow-drift"
    run_store, _workflow_store, successor_id = create_action_quarantine(
        store_backend,
        run_id=run_id,
        thread_id=thread_id,
    )
    target = QuarantineResolutionTarget(run_id=run_id)
    plan = run_store.plan_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
    )
    assert plan.eligible and plan.plan_id is not None
    before_events = run_store.list_events(run_id)

    store_backend.append_workflow_event_out_of_band(
        run_id=run_id,
        event_type="evidence.observed_after_plan",
        payload={"status": "observed"},
    )

    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="conformance-operator",
            operator_credential_id="conformance-credential",
        )

    persisted = store_backend.open_run_store().get_run_internal(run_id)
    successor = store_backend.open_run_store().get_run_internal(successor_id)
    assert persisted is not None and persisted.status == RunStatus.RUNNING
    assert successor is not None and successor.status == RunStatus.QUEUED
    assert run_store.list_events(run_id) == before_events


def test_i9_same_revision_checkpoint_evidence_drift_is_detected_after_commit(
    store_backend: StoreConformanceBackend,
) -> None:
    run_id = "run-quarantine-same-revision-drift"
    thread_id = "thread-quarantine-same-revision-drift"
    run_store, _workflow_store, _successor_id = create_action_quarantine(
        store_backend,
        run_id=run_id,
        thread_id=thread_id,
    )
    target = QuarantineResolutionTarget(run_id=run_id)
    plan = run_store.plan_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
    )
    assert plan.eligible and plan.plan_id is not None
    commit = run_store.apply_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
        expected_plan_id=plan.plan_id,
        operator_subject_id="conformance-operator",
        operator_credential_id="conformance-credential",
    )

    snapshot = run_store.load_thread_state_snapshot(
        thread_id,
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.revision == plan.observed_checkpoint_revision
    store_backend.replace_checkpoint_state_without_revision_out_of_band(
        tenant_id=TENANT_ID,
        thread_id=thread_id,
        expected_revision=snapshot.revision,
        state=AgentState(
            thread_id=thread_id,
            destination="Evidence drift",
            budget=123_456,
        ),
    )

    with pytest.raises(QuarantineResolutionEvidenceIncompleteError):
        run_store.verify_quarantine_resolution(commit)

    persisted = store_backend.open_run_store().get_run_internal(run_id)
    assert persisted is not None and persisted.status == RunStatus.FAILED
