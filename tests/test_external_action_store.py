from __future__ import annotations

from copy import deepcopy
import sqlite3

import pytest

from agent.contracts import RuntimeExecutionAuthority
from runtime_service.models import RunRecord, RunStatus
from runtime_service.store import SQLiteRunStore
from runtime_service.workflow_store import (
    ExternalActionDispatchOutcome,
    ExternalActionPrepareOutcome,
    ExternalActionRetryMode,
    ExternalActionStatus,
    SQLiteWorkflowStore,
    StaleAttemptError,
    StaleDispatchError,
    StepReuseOutcome,
    ToolCallStatus,
)


WORKFLOW_TYPE = "external-action-test:1.0.0"
TOOL_NAME = "create_record"
PROVIDER_NAME = "test-provider"
PROVIDER_IDENTITY = "provider-account-test-v1"
INPUT_HASH = "a" * 64
ARGUMENTS_JSON = '{"value": 1, "label": "test"}'


def _create_run(run_store: SQLiteRunStore, run_id: str) -> None:
    run_store.create_run(
        RunRecord(
            run_id=run_id,
            tenant_id="tenant-a",
            thread_id=f"thread-{run_id}",
            agent_id="generic-agent",
            agent_version="1.0.0",
            domain_id="generic",
            schema_version="1",
            status=RunStatus.QUEUED,
            input={"request": "create a record"},
            execution_authority=RuntimeExecutionAuthority(
                tenant_id="tenant-a",
                subject_id="subject-a",
                permissions=("external-actions:execute", "tools:execute"),
            ),
        )
    )
    claim = run_store.claim_next_run(
        owner_id=f"test-owner-{run_id}",
        lease_duration_seconds=300,
    )
    assert claim is not None
    assert claim.run.run_id == run_id


def _current_lease_token(
    workflow_store: SQLiteWorkflowStore,
    run_id: str,
) -> str:
    with sqlite3.connect(workflow_store.database_path) as connection:
        row = connection.execute(
            "SELECT lease_token FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    assert row is not None
    assert row[0] is not None
    return str(row[0])


def _claim_step(
    workflow_store: SQLiteWorkflowStore,
    run_id: str,
    *,
    tool_name: str = TOOL_NAME,
    input_hash: str = INPUT_HASH,
):
    lease_token = _current_lease_token(workflow_store, run_id)
    workflow_store.create_or_get_execution(
        run_id,
        WORKFLOW_TYPE,
        input_hash,
        lease_token=lease_token,
    )
    workflow_store.mark_running(run_id, lease_token=lease_token)
    claim = workflow_store.claim_step(
        run_id,
        "call-0001",
        tool_name,
        input_hash,
        max_attempts=3,
        lease_token=lease_token,
    )
    assert claim.attempt_token is not None
    return claim


def _prepare_values(
    workflow_store: SQLiteWorkflowStore,
    run_id: str,
    tool_attempt_token: str,
) -> dict:
    return {
        "run_id": run_id,
        "step_id": "call-0001",
        "tool_attempt_token": tool_attempt_token,
        "tenant_id": "tenant-a",
        "subject_id": "subject-a",
        "workflow_type": WORKFLOW_TYPE,
        "tool_name": TOOL_NAME,
        "provider_name": PROVIDER_NAME,
        "provider_identity": PROVIDER_IDENTITY,
        "input_hash": INPUT_HASH,
        "arguments_json": ARGUMENTS_JSON,
        "retry_mode": ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
        "idempotency_key": f"idem-{run_id}",
        "lease_token": _current_lease_token(workflow_store, run_id),
    }


def _seed_prepared_action(
    tmp_path,
    run_id: str = "run-action",
    *,
    retry_mode: ExternalActionRetryMode = ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
):
    database_path = tmp_path / f"{run_id}.db"
    run_store = SQLiteRunStore(database_path)
    workflow_store = SQLiteWorkflowStore(database_path)
    _create_run(run_store, run_id)
    claim = _claim_step(workflow_store, run_id)
    values = _prepare_values(workflow_store, run_id, claim.attempt_token)
    values["retry_mode"] = retry_mode
    prepared = workflow_store.prepare_external_action(**values)
    assert prepared.action is not None
    return run_store, workflow_store, claim, prepared.action


def test_external_action_schema_has_durable_identity_and_parent_link(tmp_path):
    database_path = tmp_path / "schema.db"
    SQLiteWorkflowStore(database_path)

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        columns = [
            (row["name"], row["type"], row["notnull"], row["pk"])
            for row in connection.execute("PRAGMA table_info(external_actions)").fetchall()
        ]
        foreign_keys = connection.execute("PRAGMA foreign_key_list(external_actions)").fetchall()
        unique_indexes = {
            tuple(
                row["name"]
                for row in connection.execute(f'PRAGMA index_info("{index["name"]}")').fetchall()
            )
            for index in connection.execute("PRAGMA index_list(external_actions)").fetchall()
            if index["unique"]
        }
    finally:
        connection.close()

    assert columns == [
        ("action_id", "TEXT", 0, 1),
        ("run_id", "TEXT", 1, 0),
        ("step_id", "TEXT", 1, 0),
        ("tenant_id", "TEXT", 1, 0),
        ("subject_id", "TEXT", 1, 0),
        ("workflow_type", "TEXT", 1, 0),
        ("tool_name", "TEXT", 1, 0),
        ("provider_name", "TEXT", 1, 0),
        ("provider_identity", "TEXT", 1, 0),
        ("input_hash", "TEXT", 1, 0),
        ("arguments_json", "TEXT", 1, 0),
        ("retry_mode", "TEXT", 1, 0),
        ("idempotency_key", "TEXT", 1, 0),
        ("status", "TEXT", 1, 0),
        ("dispatch_count", "INTEGER", 1, 0),
        ("dispatch_token", "TEXT", 0, 0),
        ("provider_reference", "TEXT", 0, 0),
        ("result_json", "TEXT", 0, 0),
        ("error_code", "TEXT", 0, 0),
        ("created_at", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
        ("dispatched_at", "TEXT", 0, 0),
        ("finalized_at", "TEXT", 0, 0),
    ]
    assert {(row["table"], row["from"], row["to"]) for row in foreign_keys} == {
        ("tool_calls", "run_id", "run_id"),
        ("tool_calls", "step_id", "step_id"),
    }
    assert {("run_id", "step_id"), ("idempotency_key",)} <= unique_indexes


def test_prepare_requires_current_claim_and_is_idempotent(tmp_path):
    database_path = tmp_path / "prepare.db"
    run_store = SQLiteRunStore(database_path)
    store = SQLiteWorkflowStore(database_path)
    run_id = "run-prepare"
    _create_run(run_store, run_id)
    lease_token = _current_lease_token(store, run_id)
    store.create_or_get_execution(
        run_id,
        WORKFLOW_TYPE,
        INPUT_HASH,
        lease_token=lease_token,
    )
    store.mark_running(run_id, lease_token=lease_token)

    with pytest.raises(KeyError, match="Step not found"):
        store.prepare_external_action(
            **_prepare_values(store, run_id, "attempt-missing")
        )

    claim = store.claim_step(
        run_id,
        "call-0001",
        TOOL_NAME,
        INPUT_HASH,
        max_attempts=3,
        lease_token=lease_token,
    )
    stale = store.prepare_external_action(
        **_prepare_values(store, run_id, "attempt-stale")
    )
    assert stale.outcome == ExternalActionPrepareOutcome.TOOL_ATTEMPT_MISMATCH
    assert stale.action is None

    first = store.prepare_external_action(
        **_prepare_values(store, run_id, claim.attempt_token)
    )
    second = store.prepare_external_action(
        **_prepare_values(store, run_id, claim.attempt_token)
    )

    assert first.outcome == ExternalActionPrepareOutcome.CREATED
    assert second.outcome == ExternalActionPrepareOutcome.EXISTING
    assert first.action == second.action
    assert first.action is not None
    assert first.action.status == ExternalActionStatus.PREPARED
    assert first.action.dispatch_count == 0
    assert first.action.arguments_json == '{"label":"test","value":1}'
    assert [event.event_type for event in store.list_events(run_id)] == [
        "workflow.started",
        "step.claimed",
        "external_action.prepared",
    ]


def test_prepare_binds_tenant_and_subject_to_persisted_run_authority(tmp_path):
    database_path = tmp_path / "authority-binding.db"
    run_store = SQLiteRunStore(database_path)
    store = SQLiteWorkflowStore(database_path)
    run_id = "run-authority-binding"
    _create_run(run_store, run_id)
    claim = _claim_step(store, run_id)
    values = _prepare_values(store, run_id, claim.attempt_token)
    values["subject_id"] = "subject-attacker"

    result = store.prepare_external_action(**values)

    assert result.outcome == ExternalActionPrepareOutcome.IDENTITY_MISMATCH
    assert result.action is None
    assert store.list_external_actions(run_id) == []


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("tenant_id", "tenant-b"),
        ("subject_id", "subject-b"),
        ("workflow_type", "external-action-test:2.0.0"),
        ("tool_name", "different_tool"),
        ("provider_name", "different-provider"),
        ("provider_identity", "provider-account-test-v2"),
        ("input_hash", "b" * 64),
        ("arguments_json", '{"value":2,"label":"test"}'),
        ("retry_mode", ExternalActionRetryMode.UNSAFE),
        ("idempotency_key", "different-idempotency-key"),
    ],
)
def test_prepare_identity_mismatch_never_mutates_existing_action(
    tmp_path,
    field,
    replacement,
):
    _, store, claim, original = _seed_prepared_action(
        tmp_path,
        run_id=f"run-mismatch-{field}",
    )
    values = deepcopy(
        _prepare_values(store, original.run_id, claim.attempt_token)
    )
    values[field] = replacement
    before_events = store.list_events(original.run_id)

    mismatch = store.prepare_external_action(**values)

    assert mismatch.outcome == ExternalActionPrepareOutcome.IDENTITY_MISMATCH
    assert store.get_external_action(original.run_id, original.step_id) == original
    assert store.list_events(original.run_id) == before_events


def test_idempotency_key_cannot_be_reused_by_another_action(tmp_path):
    database_path = tmp_path / "idempotency.db"
    run_store = SQLiteRunStore(database_path)
    store = SQLiteWorkflowStore(database_path)
    for run_id in ("run-key-one", "run-key-two"):
        _create_run(run_store, run_id)
        _claim_step(store, run_id)

    first_claim = store.get_step("run-key-one", "call-0001")
    second_claim = store.get_step("run-key-two", "call-0001")
    assert first_claim is not None and first_claim.attempt_token is not None
    assert second_claim is not None and second_claim.attempt_token is not None
    first_values = _prepare_values(
        store,
        "run-key-one",
        first_claim.attempt_token,
    )
    first_values["idempotency_key"] = "shared-key"
    second_values = _prepare_values(
        store,
        "run-key-two",
        second_claim.attempt_token,
    )
    second_values["idempotency_key"] = "shared-key"

    assert (
        store.prepare_external_action(**first_values).outcome
        == ExternalActionPrepareOutcome.CREATED
    )
    collision = store.prepare_external_action(**second_values)

    assert collision.outcome == ExternalActionPrepareOutcome.IDENTITY_MISMATCH
    assert collision.action is None
    assert store.get_external_action("run-key-two", "call-0001") is None


def test_cancel_that_commits_before_dispatch_keeps_action_prepared(tmp_path):
    run_store, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id="run-cancel-before-dispatch",
    )
    run_store.request_cancel_atomically(action.run_id, tenant_id="tenant-a")

    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )

    assert dispatch.outcome == ExternalActionDispatchOutcome.RUN_CANCELLED
    persisted = store.get_external_action(action.run_id, action.step_id)
    assert persisted is not None
    assert persisted.status == ExternalActionStatus.PREPARED
    assert persisted.dispatch_count == 0
    assert not store.has_external_action_requiring_reconciliation(action.run_id)
    assert not any(
        event.event_type == "external_action.dispatch_started"
        for event in store.list_events(action.run_id)
    )


@pytest.mark.parametrize(
    "retry_mode",
    [
        ExternalActionRetryMode.SAFE,
        ExternalActionRetryMode.PROVIDER_IDEMPOTENT,
    ],
)
def test_retry_rotates_dispatch_token_and_fences_stale_results(tmp_path, retry_mode):
    _, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id=f"run-retry-{retry_mode.value}",
        retry_mode=retry_mode,
    )

    first = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert first.outcome == ExternalActionDispatchOutcome.CLAIMED
    assert first.dispatch_token is not None

    with pytest.raises(StaleDispatchError):
        store.retry_external_action_dispatch(
            action.run_id,
            action.step_id,
            previous_dispatch_token="dispatch-stale",
            tool_attempt_token=claim.attempt_token,
            lease_token=_current_lease_token(store, action.run_id),
        )

    second = store.retry_external_action_dispatch(
        action.run_id,
        action.step_id,
        previous_dispatch_token=first.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert second.outcome == ExternalActionDispatchOutcome.RETRY_CLAIMED
    assert second.dispatch_token is not None
    assert second.dispatch_token != first.dispatch_token
    assert second.action.dispatch_count == 2

    with pytest.raises(StaleDispatchError):
        store.finalize_external_action_succeeded(
            action.run_id,
            action.step_id,
            dispatch_token=first.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            result_json='{"created":true}',
            provider_reference="provider-ref-stale",
        )
    persisted = store.get_external_action(action.run_id, action.step_id)
    step = store.get_step(action.run_id, action.step_id)
    assert persisted is not None and persisted.status == ExternalActionStatus.DISPATCHING
    assert step is not None and step.status == ToolCallStatus.RUNNING


def test_stale_tool_attempt_rolls_back_action_terminal_transition(tmp_path):
    _, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id="run-stale-tool-token",
    )
    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert dispatch.dispatch_token is not None
    lease_token = _current_lease_token(store, action.run_id)
    store.recover_interrupted_step(
        action.run_id,
        action.step_id,
        lease_token=lease_token,
    )
    newer = store.claim_step(
        action.run_id,
        action.step_id,
        TOOL_NAME,
        INPUT_HASH,
        max_attempts=3,
        lease_token=lease_token,
    )
    assert newer.attempt_token is not None

    with pytest.raises(StaleAttemptError):
        store.finalize_external_action_succeeded(
            action.run_id,
            action.step_id,
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            result_json='{"created":true}',
            provider_reference="provider-ref",
        )

    persisted = store.get_external_action(action.run_id, action.step_id)
    step = store.get_step(action.run_id, action.step_id)
    assert persisted is not None and persisted.status == ExternalActionStatus.DISPATCHING
    assert step is not None and step.status == ToolCallStatus.RUNNING
    assert step.attempt_token == newer.attempt_token


def test_success_atomically_finalizes_action_and_tool_and_is_idempotent(tmp_path):
    _, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id="run-success",
    )
    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert dispatch.dispatch_token is not None

    succeeded = store.finalize_external_action_succeeded(
        action.run_id,
        action.step_id,
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        result_json='{"record_id":"record-1","created":true}',
        provider_reference="record-1",
    )
    events_after_first = store.list_events(action.run_id)
    duplicate = store.finalize_external_action_succeeded(
        action.run_id,
        action.step_id,
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        result_json='{"created":true,"record_id":"record-1"}',
        provider_reference="record-1",
    )

    step = store.get_step(action.run_id, action.step_id)
    assert succeeded.status == ExternalActionStatus.SUCCEEDED
    assert duplicate == succeeded
    assert succeeded.result_json == '{"created":true,"record_id":"record-1"}'
    assert step is not None and step.status == ToolCallStatus.COMPLETED
    assert step.result_json == succeeded.result_json
    assert store.has_external_action_requiring_reconciliation(action.run_id)
    assert store.list_events(action.run_id) == events_after_first
    assert [event.event_type for event in events_after_first][-2:] == [
        "external_action.succeeded",
        "step.completed",
    ]
    assert all(
        event.payload.get("evidence_id")
        for event in events_after_first
        if event.event_type.startswith("external_action.")
    )


@pytest.mark.parametrize(
    ("terminal", "expected_status", "event_type", "error_code"),
    [
        ("failed", ExternalActionStatus.FAILED, "external_action.failed", "provider_rejected"),
        (
            "unknown",
            ExternalActionStatus.OUTCOME_UNKNOWN,
            "external_action.outcome_unknown",
            "external_action_outcome_unknown",
        ),
    ],
)
def test_failure_and_unknown_atomically_finalize_action_and_tool(
    tmp_path,
    terminal,
    expected_status,
    event_type,
    error_code,
):
    _, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id=f"run-{terminal}",
    )
    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert dispatch.dispatch_token is not None

    if terminal == "failed":
        finalized = store.finalize_external_action_failed(
            action.run_id,
            action.step_id,
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            error_code=error_code,
        )
    else:
        finalized = store.finalize_external_action_outcome_unknown(
            action.run_id,
            action.step_id,
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            error_code=error_code,
        )

    step = store.get_step(action.run_id, action.step_id)
    assert finalized.status == expected_status
    assert finalized.error_code == error_code
    assert step is not None and step.status == ToolCallStatus.FAILED
    assert step.error_code == error_code
    assert [event.event_type for event in store.list_events(action.run_id)][-2:] == [
        event_type,
        "step.failed",
    ]


def test_raw_provider_exception_is_rejected_without_persistence(tmp_path):
    _, store, claim, action = _seed_prepared_action(
        tmp_path,
        run_id="run-raw-provider-error",
    )
    dispatch = store.begin_external_action_dispatch(
        action.run_id,
        action.step_id,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, action.run_id),
    )
    assert dispatch.dispatch_token is not None

    with pytest.raises(ValueError, match="stable machine code"):
        store.finalize_external_action_failed(
            action.run_id,
            action.step_id,
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            error_code="TimeoutError: provider secret leaked",
        )

    persisted = store.get_external_action(action.run_id, action.step_id)
    step = store.get_step(action.run_id, action.step_id)
    assert persisted is not None and persisted.status == ExternalActionStatus.DISPATCHING
    assert persisted.error_code is None
    assert step is not None and step.status == ToolCallStatus.RUNNING


def test_unsafe_dispatch_cannot_retry_and_restart_finalizes_unknown(tmp_path):
    database_path = tmp_path / "unsafe.db"
    run_store = SQLiteRunStore(database_path)
    store = SQLiteWorkflowStore(database_path)
    run_id = "run-unsafe"
    _create_run(run_store, run_id)
    claim = _claim_step(store, run_id)
    values = _prepare_values(store, run_id, claim.attempt_token)
    values["retry_mode"] = ExternalActionRetryMode.UNSAFE
    prepared = store.prepare_external_action(**values)
    assert prepared.action is not None
    dispatch = store.begin_external_action_dispatch(
        run_id,
        "call-0001",
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, run_id),
    )
    assert dispatch.dispatch_token is not None

    retry = store.retry_external_action_dispatch(
        run_id,
        "call-0001",
        previous_dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, run_id),
    )
    assert retry.outcome == ExternalActionDispatchOutcome.RETRY_UNSAFE
    assert retry.action.dispatch_count == 1

    unknown = store.finalize_unsafe_interrupted_action(
        run_id,
        "call-0001",
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(store, run_id),
    )
    assert unknown.status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert unknown.error_code == "external_action_outcome_unknown"
    assert store.get_step(run_id, "call-0001").status == ToolCallStatus.FAILED


def test_external_action_survives_store_reopen(tmp_path):
    database_path = tmp_path / "reopen.db"
    run_store = SQLiteRunStore(database_path)
    first_store = SQLiteWorkflowStore(database_path)
    run_id = "run-reopen"
    _create_run(run_store, run_id)
    claim = _claim_step(first_store, run_id)
    prepared = first_store.prepare_external_action(
        **_prepare_values(first_store, run_id, claim.attempt_token)
    )
    assert prepared.action is not None
    dispatch = first_store.begin_external_action_dispatch(
        run_id,
        "call-0001",
        tool_attempt_token=claim.attempt_token,
        lease_token=_current_lease_token(first_store, run_id),
    )
    assert dispatch.dispatch_token is not None

    reopened = SQLiteWorkflowStore(database_path)
    restored = reopened.get_external_action(run_id, "call-0001")

    assert restored is not None
    assert restored.action_id == prepared.action.action_id
    assert restored.status == ExternalActionStatus.DISPATCHING
    assert restored.dispatch_token == dispatch.dispatch_token
    assert restored.dispatch_count == 1


def test_selective_replay_rejects_external_action_evidence(tmp_path):
    database_path = tmp_path / "replay.db"
    run_store = SQLiteRunStore(database_path)
    store = SQLiteWorkflowStore(database_path)
    _create_run(run_store, "source")
    source_claim = _claim_step(store, "source")
    prepared = store.prepare_external_action(
        **_prepare_values(store, "source", source_claim.attempt_token)
    )
    assert prepared.action is not None
    dispatch = store.begin_external_action_dispatch(
        "source",
        "call-0001",
        tool_attempt_token=source_claim.attempt_token,
        lease_token=_current_lease_token(store, "source"),
    )
    assert dispatch.dispatch_token is not None
    store.finalize_external_action_succeeded(
        "source",
        "call-0001",
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=source_claim.attempt_token,
        result_json='{"record_id":"record-source"}',
        provider_reference="record-source",
    )
    store.finalize_ready(
        "source",
        result_json='{"status":"ready"}',
        lease_token=_current_lease_token(store, "source"),
    )

    store.create_or_get_execution("target", WORKFLOW_TYPE, INPUT_HASH)
    store.mark_running("target")
    replay = store.reuse_completed_step(
        "source",
        "target",
        "call-0001",
        TOOL_NAME,
        INPUT_HASH,
    )

    assert replay.outcome == StepReuseOutcome.NOT_REUSABLE
    assert replay.step is not None and replay.step.status == ToolCallStatus.COMPLETED
    assert store.get_step("target", "call-0001") is None
    assert store.list_external_actions("target") == []
