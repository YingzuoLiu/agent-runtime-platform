from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from domains.travel.state import AgentState
from runtime_service.models import RunStatus
from runtime_service.quarantine import (
    QuarantineResolutionEvidenceIncompleteError,
    QuarantineResolutionKind,
    QuarantineResolutionStalePlanError,
    QuarantineResolutionTarget,
)
from runtime_service.store import THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
from runtime_service.workflow_store import ExternalActionStatus
from tests.conformance.backends import (
    InjectedConformanceFailure,
    SQLiteConformanceBackend,
)
from tests.conformance.scenarios import (
    TENANT_ID,
    create_action_quarantine,
)


RESOLUTION = QuarantineResolutionKind.TERMINALIZE_FAILED_PRESERVING_CHECKPOINT


@pytest.fixture
def backend(tmp_path, manual_store_clock) -> SQLiteConformanceBackend:
    return SQLiteConformanceBackend(
        database_path=tmp_path / "quarantine-resolution.db",
        clock=manual_store_clock,
    )


def create_quarantine(
    backend: SQLiteConformanceBackend,
    *,
    suffix: str,
    terminal_status: ExternalActionStatus | None = ExternalActionStatus.SUCCEEDED,
):
    run_id = f"run-quarantine-{suffix}"
    thread_id = f"thread-quarantine-{suffix}"
    run_store, workflow_store, successor_id = create_action_quarantine(
        backend,
        run_id=run_id,
        thread_id=thread_id,
        terminal_status=terminal_status,
    )
    return run_id, thread_id, successor_id, run_store, workflow_store


def plan_for(run_store, run_id: str):
    target = QuarantineResolutionTarget(run_id=run_id)
    plan = run_store.plan_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
    )
    return target, plan


@pytest.mark.parametrize(
    "terminal_status",
    [
        ExternalActionStatus.SUCCEEDED,
        ExternalActionStatus.FAILED,
        ExternalActionStatus.OUTCOME_UNKNOWN,
    ],
)
def test_all_durable_terminal_action_outcomes_are_eligible(
    backend: SQLiteConformanceBackend,
    terminal_status: ExternalActionStatus,
) -> None:
    run_id, _thread_id, _successor_id, run_store, _workflow_store = create_quarantine(
        backend,
        suffix=terminal_status.value,
        terminal_status=terminal_status,
    )

    _target, plan = plan_for(run_store, run_id)

    assert plan.eligible
    assert plan.plan_id is not None
    assert getattr(plan.external_actions, terminal_status.value) == 1
    assert not plan.workflow_reconciliation_required


def test_stale_checkpoint_plan_writes_nothing_and_keeps_thread_blocked(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, thread_id, successor_id, run_store, workflow_store = create_quarantine(
        backend,
        suffix="stale-checkpoint",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    before_events = run_store.list_events(run_id)
    before_workflow = workflow_store.read_run_snapshot(run_id)
    backend.replace_checkpoint_out_of_band(
        tenant_id=TENANT_ID,
        thread_id=thread_id,
        state=AgentState(
            thread_id=thread_id,
            destination="Kyoto",
            budget=8_888,
        ),
        expected_revision=2,
    )

    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="operator",
            operator_credential_id="credential",
        )

    persisted = backend.open_run_store().get_run_internal(run_id)
    successor = backend.open_run_store().get_run_internal(successor_id)
    assert persisted is not None and persisted.status == RunStatus.RUNNING
    assert persisted.error_code == THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
    assert successor is not None and successor.status == RunStatus.QUEUED
    assert run_store.list_events(run_id) == before_events
    assert workflow_store.read_run_snapshot(run_id) == before_workflow
    assert (
        run_store.load_thread_state_snapshot(
            thread_id,
            tenant_id=TENANT_ID,
            domain_id="travel",
            schema_version="1",
        ).revision
        == 3
    )


def test_workflow_evidence_change_after_plan_makes_plan_stale(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, _thread_id, successor_id, run_store, workflow_store = create_quarantine(
        backend,
        suffix="stale-workflow",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    before_run_events = run_store.list_events(run_id)
    with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
        sequence = int(
            connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM workflow_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO workflow_events (
                run_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                sequence,
                "evidence.observed_after_plan",
                json.dumps({"status": "observed"}),
                "2026-08-23T00:00:00+00:00",
            ),
        )

    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="operator",
            operator_credential_id="credential",
        )

    persisted = backend.open_run_store().get_run_internal(run_id)
    successor = backend.open_run_store().get_run_internal(successor_id)
    assert persisted is not None and persisted.status == RunStatus.RUNNING
    assert successor is not None and successor.status == RunStatus.QUEUED
    assert run_store.list_events(run_id) == before_run_events
    assert workflow_store.read_run_snapshot(run_id).events[-1].event_type == (
        "evidence.observed_after_plan"
    )


def test_private_provider_binding_change_after_plan_makes_plan_stale(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, _thread_id, _successor_id, run_store, _workflow_store = (
        create_quarantine(
            backend,
            suffix="stale-private-provider-binding",
        )
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
        connection.execute(
            """
            UPDATE external_actions SET provider_identity = ?
            WHERE run_id = ?
            """,
            ("changed-private-provider-identity", run_id),
        )

    refreshed = run_store.plan_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
    )
    assert refreshed.eligible
    assert refreshed.plan_id != plan.plan_id
    assert "changed-private-provider-identity" not in refreshed.model_dump_json()
    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="operator",
            operator_credential_id="credential",
        )


def test_mismatched_terminal_event_is_ineligible(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, _thread_id, _successor_id, run_store, _workflow_store = (
        create_quarantine(
            backend,
            suffix="mismatched-terminal-event",
        )
    )
    with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
        event = connection.execute(
            """
            SELECT event_id, payload_json FROM workflow_events
            WHERE run_id = ? AND event_type = 'external_action.succeeded'
            """,
            (run_id,),
        ).fetchone()
        assert event is not None
        payload = json.loads(event[1])
        payload["provider_name"] = "mismatched-provider"
        connection.execute(
            "UPDATE workflow_events SET payload_json = ? WHERE event_id = ?",
            (json.dumps(payload), event[0]),
        )

    _target, plan = plan_for(run_store, run_id)
    assert not plan.eligible
    assert plan.plan_id is None
    assert "external_action_evidence_inconsistent" in plan.ineligibility_reasons


@pytest.mark.parametrize("action_state", ["prepared", "dispatching"])
def test_nonterminal_action_is_ineligible_and_never_releases_thread(
    backend: SQLiteConformanceBackend,
    action_state: str,
) -> None:
    run_id, _thread_id, successor_id, run_store, workflow_store = create_quarantine(
        backend,
        suffix=action_state,
        terminal_status=None,
    )
    if action_state == "prepared":
        with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
            connection.execute(
                """
                UPDATE external_actions SET
                    status = 'prepared', dispatch_count = 0,
                    dispatch_token = NULL, dispatched_at = NULL,
                    updated_at = 'returned-to-prepared-for-test'
                WHERE run_id = ?
                """,
                (run_id,),
            )
            connection.execute(
                """
                DELETE FROM workflow_events
                WHERE run_id = ? AND event_type = 'external_action.dispatch_started'
                """,
                (run_id,),
            )
    target, plan = plan_for(run_store, run_id)

    assert not plan.eligible
    assert plan.plan_id is None
    assert getattr(plan.external_actions, action_state) == 1
    assert plan.workflow_reconciliation_required
    assert "external_action_nonterminal" in plan.ineligibility_reasons
    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id="qrp_" + "0" * 64,
            operator_subject_id="operator",
            operator_credential_id="credential",
        )

    action = workflow_store.list_external_actions(run_id)[0]
    successor = backend.open_run_store().get_run_internal(successor_id)
    assert action.status.value == action_state
    assert successor is not None and successor.status == RunStatus.QUEUED
    assert not any(
        event.event_type == "quarantine.resolution_applied"
        for event in run_store.list_events(run_id)
    )


@pytest.mark.parametrize(
    "mutation",
    ["cancel", "lease", "attempt", "status", "quarantine_code"],
)
def test_run_precondition_change_after_plan_fails_closed(
    backend: SQLiteConformanceBackend,
    mutation: str,
) -> None:
    run_id, _thread_id, _successor_id, run_store, _workflow_store = create_quarantine(
        backend,
        suffix=f"run-change-{mutation}",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    if mutation == "cancel":
        run_store.request_cancel_atomically(run_id, tenant_id=TENANT_ID)
    else:
        assignments = {
            "lease": (
                "lease_owner_id = 'other-owner', lease_token = 'other-token', "
                "lease_heartbeat_at = 1, lease_expires_at = 9999999999999"
            ),
            "attempt": "attempt = attempt + 1, updated_at = 'changed-attempt'",
            "status": "status = 'failed', updated_at = 'changed-status'",
            "quarantine_code": (
                "error_code = 'other_quarantine_code', updated_at = 'changed-code'"
            ),
        }
        with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
            connection.execute(
                f"UPDATE runs SET {assignments[mutation]} WHERE run_id = ?",
                (run_id,),
            )

    with pytest.raises(QuarantineResolutionStalePlanError):
        run_store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="operator",
            operator_credential_id="credential",
        )
    assert not any(
        event.event_type == "quarantine.resolution_applied"
        for event in run_store.list_events(run_id)
    )


@pytest.mark.parametrize(
    "failing_event",
    ["quarantine.resolution_applied", "run.failed"],
)
def test_resolution_event_failure_rolls_back_entire_transaction(
    backend: SQLiteConformanceBackend,
    failing_event: str,
) -> None:
    run_id, thread_id, successor_id, run_store, workflow_store = create_quarantine(
        backend,
        suffix=f"rollback-{failing_event.replace('.', '-')}",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    before_events = run_store.list_events(run_id)
    before_checkpoint = run_store.load_thread_state_snapshot(
        thread_id,
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    before_workflow = workflow_store.read_run_snapshot(run_id)

    with backend.fail_run_event(run_store, failing_event):
        with pytest.raises(InjectedConformanceFailure):
            run_store.apply_quarantine_resolution(
                run_id,
                tenant_id=TENANT_ID,
                target=target,
                resolution=RESOLUTION,
                expected_plan_id=plan.plan_id,
                operator_subject_id="operator",
                operator_credential_id="credential",
            )

    persisted = backend.open_run_store().get_run_internal(run_id)
    successor = backend.open_run_store().get_run_internal(successor_id)
    assert persisted is not None and persisted.status == RunStatus.RUNNING
    assert persisted.error_code == THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
    assert successor is not None and successor.status == RunStatus.QUEUED
    assert run_store.list_events(run_id) == before_events
    assert (
        run_store.load_thread_state_snapshot(
            thread_id,
            tenant_id=TENANT_ID,
            domain_id="travel",
            schema_version="1",
        )
        == before_checkpoint
    )
    assert workflow_store.read_run_snapshot(run_id) == before_workflow


def test_post_commit_checkpoint_change_returns_evidence_incomplete(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, thread_id, _successor_id, run_store, _workflow_store = create_quarantine(
        backend,
        suffix="post-commit-verification",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    commit = run_store.apply_quarantine_resolution(
        run_id,
        tenant_id=TENANT_ID,
        target=target,
        resolution=RESOLUTION,
        expected_plan_id=plan.plan_id,
        operator_subject_id="operator",
        operator_credential_id="credential",
    )
    with closing(sqlite3.connect(backend.database_path, timeout=30)) as connection, connection:
        connection.execute(
            """
            UPDATE thread_states SET state_json = ?
            WHERE tenant_id = ? AND thread_id = ?
            """,
            (
                AgentState(
                    thread_id=thread_id,
                    destination="Tampered",
                    budget=1,
                ).model_dump_json(),
                TENANT_ID,
                thread_id,
            ),
        )

    with pytest.raises(QuarantineResolutionEvidenceIncompleteError):
        run_store.verify_quarantine_resolution(commit)


def test_concurrent_exact_apply_commits_once_and_reuses_once(
    backend: SQLiteConformanceBackend,
) -> None:
    run_id, _thread_id, _successor_id, run_store, _workflow_store = create_quarantine(
        backend,
        suffix="concurrent-exact-apply",
    )
    target, plan = plan_for(run_store, run_id)
    assert plan.plan_id is not None
    barrier = threading.Barrier(3)

    def apply(store, credential_id: str):
        barrier.wait(timeout=5)
        return store.apply_quarantine_resolution(
            run_id,
            tenant_id=TENANT_ID,
            target=target,
            resolution=RESOLUTION,
            expected_plan_id=plan.plan_id,
            operator_subject_id="operator",
            operator_credential_id=credential_id,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            apply,
            backend.open_run_store(),
            "credential-a",
        )
        second = executor.submit(
            apply,
            backend.open_run_store(),
            "credential-b",
        )
        barrier.wait(timeout=5)
        commits = [first.result(timeout=5), second.result(timeout=5)]

    assert sorted(commit.reused for commit in commits) == [False, True]
    events = backend.open_run_store().list_events(run_id)
    assert sum(
        event.event_type == "quarantine.resolution_applied" for event in events
    ) == 1
    assert sum(event.event_type == "run.failed" for event in events) == 1
