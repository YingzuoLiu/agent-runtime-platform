from __future__ import annotations

# Mypy checks structural assignability; these tests additionally pin the reflected method
# contract: declared method names plus SQLite parameter names, kinds, defaults, and annotations.

from inspect import signature
from typing import get_type_hints

from domains.release_validation.runtime import ReleaseValidationWorkflow
from runtime_service.dynamic_loop import DynamicToolLoop
from runtime_service.evidence import EvidenceProjector
from runtime_service.external_action_coordinator import ExternalActionCoordinator
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
