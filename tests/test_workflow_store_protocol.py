from __future__ import annotations

# Mypy checks structural assignability; these tests additionally pin the reflected method
# contract: declared method names plus SQLite parameter names, kinds, defaults, and annotations.

import sqlite3
from inspect import Parameter, signature
from typing import get_type_hints

import pytest

from domains.release_validation.runtime import ReleaseValidationWorkflow
from runtime_service.dynamic_loop import DynamicToolLoop
from runtime_service.evidence import EvidenceProjector
from runtime_service.external_action_coordinator import ExternalActionCoordinator
from runtime_service.store import RunLeaseLostError
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStore


WORKFLOW_STORE_METHODS = {
    "append_event",
    "begin_external_action_dispatch",
    "claim_step",
    "complete_step",
    "create_or_get_execution",
    "fail_step",
    "finalize_blocked",
    "finalize_external_action_failed",
    "finalize_external_action_outcome_unknown",
    "finalize_external_action_reconciliation_unknown",
    "finalize_external_action_succeeded",
    "finalize_failed",
    "finalize_ready",
    "finalize_unsafe_interrupted_action",
    "get_execution",
    "get_external_action",
    "get_step",
    "has_external_action_requiring_reconciliation",
    "list_events",
    "list_external_actions",
    "list_steps",
    "mark_running",
    "ping",
    "prepare_external_action",
    "read_run_snapshot",
    "recover_interrupted_step",
    "retry_external_action_dispatch",
    "reuse_completed_step",
}

ATTEMPT_OWNED_MUTATORS = {
    "append_event",
    "begin_external_action_dispatch",
    "claim_step",
    "complete_step",
    "create_or_get_execution",
    "fail_step",
    "finalize_blocked",
    "finalize_failed",
    "finalize_ready",
    "finalize_external_action_reconciliation_unknown",
    "finalize_unsafe_interrupted_action",
    "mark_running",
    "prepare_external_action",
    "recover_interrupted_step",
    "retry_external_action_dispatch",
    "reuse_completed_step",
}

PROVIDER_OUTCOME_FINALIZERS = {
    "finalize_external_action_failed",
    "finalize_external_action_outcome_unknown",
    "finalize_external_action_succeeded",
}

RUN_ID = "managed-run"
LEASE_TOKEN = "lease-current"


def _public_methods(owner: type[object]) -> set[str]:
    return {
        name
        for name, value in vars(owner).items()
        if not name.startswith("_") and callable(value)
    }


def test_workflow_store_protocol_has_an_explicit_backend_neutral_surface():
    assert _public_methods(WorkflowStore) == WORKFLOW_STORE_METHODS
    assert "initialize" not in WORKFLOW_STORE_METHODS
    assert "database_path" not in WORKFLOW_STORE_METHODS


def test_sqlite_workflow_store_matches_every_protocol_signature():
    for method_name in sorted(WORKFLOW_STORE_METHODS):
        protocol_method = getattr(WorkflowStore, method_name)
        sqlite_method = getattr(SQLiteWorkflowStore, method_name)
        assert signature(sqlite_method) == signature(protocol_method), method_name


def test_attempt_owned_mutators_expose_one_keyword_only_run_lease_token():
    assert ATTEMPT_OWNED_MUTATORS | PROVIDER_OUTCOME_FINALIZERS <= WORKFLOW_STORE_METHODS
    for method_name in sorted(ATTEMPT_OWNED_MUTATORS):
        parameter = signature(getattr(WorkflowStore, method_name)).parameters["lease_token"]
        assert parameter.kind is Parameter.KEYWORD_ONLY, method_name
        assert parameter.default is None, method_name


def test_provider_outcome_finalizers_remain_dispatch_and_tool_token_fenced():
    for method_name in sorted(PROVIDER_OUTCOME_FINALIZERS):
        parameters = signature(getattr(WorkflowStore, method_name)).parameters
        assert "lease_token" not in parameters, method_name
        assert parameters["dispatch_token"].kind is Parameter.KEYWORD_ONLY, method_name
        assert parameters["tool_attempt_token"].kind is Parameter.KEYWORD_ONLY, method_name


def test_runtime_consumers_depend_on_the_workflow_store_protocol():
    constructors = (
        DynamicToolLoop.__init__,
        EvidenceProjector.__init__,
        ExternalActionCoordinator.__init__,
    )
    for constructor in constructors:
        assert get_type_hints(constructor)["workflow_store"] is WorkflowStore
    assert get_type_hints(ReleaseValidationWorkflow.__init__)["store"] is WorkflowStore

    workflow_store_property = DynamicToolLoop.workflow_store
    assert workflow_store_property.fget is not None
    assert workflow_store_property.fset is not None
    assert get_type_hints(workflow_store_property.fget)["return"] is WorkflowStore
    assert get_type_hints(workflow_store_property.fset)["store"] is WorkflowStore


def _create_managed_run(
    database_path,
    *,
    run_id: str = RUN_ID,
    lease_token: str = LEASE_TOKEN,
) -> None:
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                execution_authority_json TEXT,
                status TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT,
                lease_expires_at INTEGER
            );
            """
        )
        now_ms = int(
            connection.execute(
                "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO runs (
                run_id, tenant_id, execution_authority_json, status,
                cancel_requested, lease_token, lease_expires_at
            ) VALUES (?, ?, ?, 'running', 0, ?, ?)
            """,
            (
                run_id,
                "tenant-a",
                '{"tenant_id":"tenant-a","subject_id":"subject-a"}',
                lease_token,
                now_ms + 60_000,
            ),
        )


def _invoke_attempt_owned_mutator(
    store: SQLiteWorkflowStore,
    method_name: str,
) -> None:
    if method_name == "create_or_get_execution":
        store.create_or_get_execution(RUN_ID, "workflow:test", "input-hash")
    elif method_name == "mark_running":
        store.mark_running(RUN_ID)
    elif method_name == "finalize_ready":
        store.finalize_ready(RUN_ID, "{}")
    elif method_name == "finalize_blocked":
        store.finalize_blocked(RUN_ID, "{}", "blocked")
    elif method_name == "finalize_failed":
        store.finalize_failed(RUN_ID, "failed")
    elif method_name == "reuse_completed_step":
        store.reuse_completed_step(
            "source-run",
            RUN_ID,
            "step-1",
            "tool",
            "input-hash",
        )
    elif method_name == "claim_step":
        store.claim_step(
            RUN_ID,
            "step-1",
            "tool",
            "input-hash",
            max_attempts=1,
        )
    elif method_name == "complete_step":
        store.complete_step(RUN_ID, "step-1", "attempt", "{}")
    elif method_name == "fail_step":
        store.fail_step(RUN_ID, "step-1", "attempt", "failed")
    elif method_name == "recover_interrupted_step":
        store.recover_interrupted_step(RUN_ID, "step-1")
    elif method_name == "finalize_external_action_reconciliation_unknown":
        store.finalize_external_action_reconciliation_unknown(
            RUN_ID,
            "step-1",
            dispatch_token="dispatch",
            tool_attempt_token="attempt",
            error_code="external_action_outcome_unknown",
        )
    elif method_name == "finalize_unsafe_interrupted_action":
        store.finalize_unsafe_interrupted_action(
            RUN_ID,
            "step-1",
            dispatch_token="dispatch",
            tool_attempt_token="attempt",
        )
    elif method_name == "append_event":
        store.append_event(RUN_ID, "test.event", {})
    elif method_name == "prepare_external_action":
        store.prepare_external_action(
            run_id=RUN_ID,
            step_id="step-1",
            tool_attempt_token="attempt",
            tenant_id="tenant-a",
            subject_id="subject-a",
            workflow_type="workflow:test",
            tool_name="tool",
            provider_name="provider",
            provider_identity="provider:test",
            input_hash="input-hash",
            arguments_json="{}",
            retry_mode="provider_idempotent",
            idempotency_key="idempotency-key",
        )
    elif method_name == "begin_external_action_dispatch":
        store.begin_external_action_dispatch(
            RUN_ID,
            "step-1",
            tool_attempt_token="attempt",
        )
    elif method_name == "retry_external_action_dispatch":
        store.retry_external_action_dispatch(
            RUN_ID,
            "step-1",
            previous_dispatch_token="dispatch",
            tool_attempt_token="attempt",
        )
    else:  # pragma: no cover - the parametrization and branch table must stay aligned
        raise AssertionError(f"Unmapped mutator: {method_name}")


@pytest.mark.parametrize("method_name", sorted(ATTEMPT_OWNED_MUTATORS))
def test_managed_attempt_owned_mutators_fail_closed_without_token(tmp_path, method_name):
    database_path = tmp_path / f"{method_name}.db"
    store = SQLiteWorkflowStore(database_path)
    _create_managed_run(database_path)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        _invoke_attempt_owned_mutator(store, method_name)


def test_standalone_workflow_without_matching_run_remains_supported(tmp_path):
    no_runs_table = SQLiteWorkflowStore(tmp_path / "standalone.db")
    no_runs_table.create_or_get_execution(RUN_ID, "workflow:test", "input-hash")
    no_runs_table.mark_running(RUN_ID)
    no_runs_table.append_event(RUN_ID, "test.event", {})

    unrelated_run_database = tmp_path / "unrelated-run.db"
    unrelated_run = SQLiteWorkflowStore(unrelated_run_database)
    _create_managed_run(unrelated_run_database, run_id="another-run")
    unrelated_run.create_or_get_execution(RUN_ID, "workflow:test", "input-hash")


@pytest.mark.parametrize("with_runs_table", [False, True], ids=["no-runs-table", "no-run-row"])
def test_presented_run_token_fails_closed_when_lease_cannot_be_enforced(
    tmp_path,
    with_runs_table,
):
    database_path = tmp_path / f"unenforceable-{with_runs_table}.db"
    store = SQLiteWorkflowStore(database_path)
    if with_runs_table:
        _create_managed_run(database_path, run_id="another-run")

    with pytest.raises(RunLeaseLostError, match="not enforceable"):
        store.create_or_get_execution(
            RUN_ID,
            "workflow:test",
            "input-hash",
            lease_token=LEASE_TOKEN,
        )


def test_managed_workflow_rejects_wrong_and_expired_tokens(tmp_path):
    database_path = tmp_path / "lease-validation.db"
    store = SQLiteWorkflowStore(database_path)
    _create_managed_run(database_path)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        store.create_or_get_execution(
            RUN_ID,
            "workflow:test",
            "input-hash",
            lease_token="lease-wrong",
        )

    store.create_or_get_execution(
        RUN_ID,
        "workflow:test",
        "input-hash",
        lease_token=LEASE_TOKEN,
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE runs SET lease_expires_at = 0 WHERE run_id = ?",
            (RUN_ID,),
        )

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        store.mark_running(RUN_ID, lease_token=LEASE_TOKEN)


def test_takeover_fences_local_reconciliation_but_accepts_late_provider_success(
    tmp_path,
):
    database_path = tmp_path / "provider-outcome.db"
    store = SQLiteWorkflowStore(database_path)
    _create_managed_run(database_path)
    store.create_or_get_execution(
        RUN_ID,
        "workflow:test",
        "input-hash",
        lease_token=LEASE_TOKEN,
    )
    store.mark_running(RUN_ID, lease_token=LEASE_TOKEN)
    claim = store.claim_step(
        RUN_ID,
        "step-1",
        "tool",
        "input-hash",
        max_attempts=1,
        lease_token=LEASE_TOKEN,
    )
    assert claim.attempt_token is not None
    prepared = store.prepare_external_action(
        run_id=RUN_ID,
        step_id="step-1",
        tool_attempt_token=claim.attempt_token,
        tenant_id="tenant-a",
        subject_id="subject-a",
        workflow_type="workflow:test",
        tool_name="tool",
        provider_name="provider",
        provider_identity="provider:test",
        input_hash="input-hash",
        arguments_json="{}",
        retry_mode="unsafe",
        idempotency_key="idempotency-key",
        lease_token=LEASE_TOKEN,
    )
    assert prepared.action is not None
    dispatch = store.begin_external_action_dispatch(
        RUN_ID,
        "step-1",
        tool_attempt_token=claim.attempt_token,
        lease_token=LEASE_TOKEN,
    )
    assert dispatch.dispatch_token is not None

    replacement_lease_token = "lease-replacement"
    with sqlite3.connect(database_path) as connection:
        now_ms = int(
            connection.execute(
                "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
            ).fetchone()[0]
        )
        connection.execute(
            """
            UPDATE runs SET lease_token = ?, lease_expires_at = ?
            WHERE run_id = ?
            """,
            (replacement_lease_token, now_ms + 60_000, RUN_ID),
        )

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        store.finalize_external_action_reconciliation_unknown(
            RUN_ID,
            "step-1",
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            error_code="external_action_outcome_unknown",
            lease_token=LEASE_TOKEN,
        )
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        store.finalize_unsafe_interrupted_action(
            RUN_ID,
            "step-1",
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=claim.attempt_token,
            lease_token=LEASE_TOKEN,
        )

    unchanged_action = store.get_external_action(RUN_ID, "step-1")
    unchanged_step = store.get_step(RUN_ID, "step-1")
    assert unchanged_action is not None
    assert unchanged_action.status.value == "dispatching"
    assert unchanged_step is not None
    assert unchanged_step.status.value == "running"

    # A response to the already-authorized provider call remains bound to the
    # dispatch and tool-attempt tokens, not to whichever manager now owns Run.
    finalized = store.finalize_external_action_succeeded(
        RUN_ID,
        "step-1",
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=claim.attempt_token,
        result_json='{"ok":true}',
        provider_reference="provider-reference",
    )
    assert finalized.status.value == "succeeded"
    step = store.get_step(RUN_ID, "step-1")
    assert step is not None and step.status.value == "completed"
