import inspect
import json
import sqlite3
from typing import Any, Dict, List, Optional

import pytest

from domains.release_validation.models import (
    BuildArtifact,
    ReleaseManifest,
    SelectiveReplayRequest,
)
from domains.release_validation import runtime as release_validation_runtime
from domains.release_validation.runtime import (
    MAX_ATTEMPTS,
    RELEASE_VALIDATION_DAG,
    STEP_SEQUENCE,
    ExecutionInputMismatchError,
    ReleaseValidationWorkflow,
    SelectiveReplayError,
    StepAlreadyRunningError,
    StepAttemptsExhaustedError,
    StepDefinitionMismatchError,
    StepPermanentFailureError,
    WorkflowExecutionFailedError,
    _canonicalize_manifest,
    _stable_hash,
)
from domains.release_validation.models import ReleaseValidationStatus
from domains.release_validation.tools import build_release_validation_tool_registry
from runtime_service.sandbox import ToolExecutionResult, ToolExecutionStatus, ToolSandbox
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStatus


def _manifest_hash_for_test(manifest: ReleaseManifest) -> str:
    """Mirror `ReleaseValidationWorkflow.run`'s own canonicalization so
    tests that manually seed the store agree with the workflow on what
    hash a given manifest produces."""
    return _stable_hash(_canonicalize_manifest(manifest).model_dump(mode="json"))


def _replay_hash_for_test(
    manifest: ReleaseManifest,
    replay: SelectiveReplayRequest,
) -> str:
    return _stable_hash(
        {
            "manifest": _canonicalize_manifest(manifest).model_dump(mode="json"),
            "replay": replay.model_dump(mode="json"),
        }
    )


def make_valid_manifest(**overrides: Any) -> ReleaseManifest:
    defaults: Dict[str, Any] = dict(
        release_id="rel-001",
        application_name="aurora-notes",
        release_version="2.4.0",
        required_artifacts=["aurora-notes-server", "aurora-notes-cli"],
        available_artifacts=[
            BuildArtifact(name="aurora-notes-server", checksum="a" * 64),
            BuildArtifact(name="aurora-notes-cli", checksum="b" * 64),
        ],
        required_test_suite="aurora-notes-full-suite",
        executed_test_suite="aurora-notes-full-suite",
        tests_passed=True,
        required_python_versions=["3.11", "3.12"],
        tested_python_versions=["3.11", "3.12"],
        deployment_environment="staging",
        configuration_requirements=["DATABASE_URL", "FEATURE_FLAGS_ENDPOINT"],
        actual_configuration_keys=["DATABASE_URL", "FEATURE_FLAGS_ENDPOINT", "LOG_LEVEL"],
    )
    defaults.update(overrides)
    return ReleaseManifest(**defaults)


class ScriptedFaultSandbox:
    """Test-only wrapper around a real `ToolSandbox`.

    For a given `tool_name`, replays a scripted queue of canned
    `ToolExecutionResult`s before falling back to the real sandbox. This
    is how transient/permanent tool failures are simulated -- production
    code (`ReleaseValidationWorkflow`, the tools, `sandbox_worker.py`)
    never carries any "fail on first attempt" logic.
    """

    def __init__(self, real_sandbox: ToolSandbox, script: Optional[Dict[str, List[ToolExecutionResult]]] = None):
        self._real = real_sandbox
        self._script = {name: list(results) for name, results in (script or {}).items()}
        self.call_log: List[str] = []

    def execute(self, tool_name: str, arguments: Dict[str, Any]) -> ToolExecutionResult:
        self.call_log.append(tool_name)
        queue = self._script.get(tool_name)
        if queue:
            return queue.pop(0)
        return self._real.execute(tool_name, arguments)


def fabricated_result(tool_name: str, status: ToolExecutionStatus, error: str | None = None) -> ToolExecutionResult:
    return ToolExecutionResult(
        execution_id=f"exec_fabricated_{tool_name}",
        tool_name=tool_name,
        status=status,
        result=None,
        error=error,
        duration_ms=1,
        exit_code=None if status != ToolExecutionStatus.FAILED else 4,
    )


@pytest.fixture
def workflow(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    sandbox = ToolSandbox(build_release_validation_tool_registry())
    return ReleaseValidationWorkflow(store, sandbox)


def test_happy_path_reaches_ready_through_all_six_tools(workflow):
    manifest = make_valid_manifest()
    result = workflow.run("run-1", manifest)

    assert result.status == ReleaseValidationStatus.READY
    assert result.findings == []

    steps = workflow.store.list_steps("run-1")
    assert len(steps) == len(STEP_SEQUENCE) == 6
    assert all(step.status.value == "completed" for step in steps)
    assert all(step.attempt_count == 1 for step in steps)

    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.READY


def test_release_validation_graph_has_five_roots_and_one_fan_in_node():
    assert RELEASE_VALIDATION_DAG.topological_order == tuple(
        step.step_id for step in STEP_SEQUENCE
    )
    assert RELEASE_VALIDATION_DAG.dependencies_for("generate_evidence") == frozenset(
        {
            "load_manifest",
            "inspect_artifacts",
            "run_unit_tests",
            "run_compatibility",
            "inspect_deployment",
        }
    )


def test_selective_replay_reruns_requested_node_and_descendants_only(workflow):
    manifest = make_valid_manifest()
    workflow.run("source", manifest)
    source_steps_before = {
        step.step_id: step.model_dump() for step in workflow.store.list_steps("source")
    }
    spy_sandbox = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy_sandbox

    result = workflow.run(
        "target",
        manifest,
        replay=SelectiveReplayRequest(
            source_run_id="source",
            step_ids=["run_unit_tests"],
        ),
    )

    assert result.status == ReleaseValidationStatus.READY
    assert spy_sandbox.call_log == ["run_unit_test_check", "generate_release_evidence"]
    assert result.replay is not None
    assert result.replay.requested_step_ids == ["run_unit_tests"]
    assert result.replay.replayed_step_ids == ["run_unit_tests", "generate_evidence"]
    assert result.replay.reused_step_ids == [
        "load_manifest",
        "inspect_artifacts",
        "run_compatibility",
        "inspect_deployment",
    ]
    assert result.replay.automatically_invalidated_step_ids == []
    target_steps = {step.step_id: step for step in workflow.store.list_steps("target")}
    assert target_steps["run_unit_tests"].attempt_count == 1
    assert target_steps["generate_evidence"].attempt_count == 1
    assert all(
        target_steps[step_id].attempt_count == 0
        for step_id in result.replay.reused_step_ids
    )
    assert {
        step.step_id: step.model_dump() for step in workflow.store.list_steps("source")
    } == source_steps_before


def test_selective_replay_accepts_pre3b_source_result_without_replay_field(workflow):
    manifest = make_valid_manifest()
    workflow.run("source", manifest)
    with sqlite3.connect(workflow.store.database_path) as connection:
        connection.execute(
            "UPDATE workflow_executions SET result_json = ? WHERE run_id = ?",
            (
                json.dumps({"run_id": "source", "status": "ready", "findings": []}),
                "source",
            ),
        )

    result = workflow.run(
        "target",
        manifest,
        replay=SelectiveReplayRequest(
            source_run_id="source",
            step_ids=["generate_evidence"],
        ),
    )

    assert result.status == ReleaseValidationStatus.READY
    assert result.replay is not None
    assert result.replay.replayed_step_ids == ["generate_evidence"]
    assert len(result.replay.reused_step_ids) == 5


def test_selective_replay_expands_when_unselected_step_input_changed(workflow):
    source_manifest = make_valid_manifest()
    workflow.run("source", source_manifest)
    spy_sandbox = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy_sandbox
    changed_manifest = make_valid_manifest(tested_python_versions=["3.11"])

    result = workflow.run(
        "target",
        changed_manifest,
        replay=SelectiveReplayRequest(
            source_run_id="source",
            step_ids=["inspect_artifacts"],
        ),
    )

    assert result.status == ReleaseValidationStatus.BLOCKED
    assert spy_sandbox.call_log == [
        "inspect_build_artifacts",
        "run_compatibility_check",
        "generate_release_evidence",
    ]
    assert result.replay is not None
    assert result.replay.automatically_invalidated_step_ids == ["run_compatibility"]
    assert result.replay.replayed_step_ids == [
        "inspect_artifacts",
        "run_compatibility",
        "generate_evidence",
    ]
    assert result.replay.reused_step_ids == [
        "load_manifest",
        "run_unit_tests",
        "inspect_deployment",
    ]
    assert [finding.rule_id for finding in result.findings] == ["python_versions_covered"]


def test_selective_replay_can_remediate_blocked_source_without_mutating_it(workflow):
    blocked = workflow.run("source", make_valid_manifest(tests_passed=False))
    assert blocked.status == ReleaseValidationStatus.BLOCKED

    replayed = workflow.run(
        "target",
        make_valid_manifest(tests_passed=True),
        replay=SelectiveReplayRequest(
            source_run_id="source",
            step_ids=["run_unit_tests"],
        ),
    )

    assert replayed.status == ReleaseValidationStatus.READY
    assert replayed.replay is not None
    assert replayed.replay.replayed_step_ids == ["run_unit_tests", "generate_evidence"]
    assert workflow.store.get_execution("source").status == WorkflowStatus.BLOCKED
    persisted_source = workflow.store.get_execution("source")
    assert persisted_source is not None and persisted_source.result_json is not None
    assert ReleaseValidationStatus.BLOCKED.value in persisted_source.result_json


def test_selective_replay_resumes_copied_and_running_target_steps_after_restart(workflow):
    manifest = _canonicalize_manifest(make_valid_manifest())
    workflow.run("source", manifest)
    replay = SelectiveReplayRequest(
        source_run_id="source",
        step_ids=["run_unit_tests"],
    )
    workflow.store.create_or_get_execution(
        "target",
        "release_validation",
        _replay_hash_for_test(manifest, replay),
    )
    workflow.store.mark_running("target")
    by_id = {step.step_id: step for step in STEP_SEQUENCE}
    for step_id in ("load_manifest", "inspect_artifacts"):
        step = by_id[step_id]
        arguments = step.build_arguments(manifest, {})
        workflow.store.reuse_completed_step(
            "source",
            "target",
            step_id,
            step.tool_name,
            _stable_hash(arguments),
        )
    interrupted = by_id["run_unit_tests"]
    interrupted_arguments = interrupted.build_arguments(manifest, {})
    workflow.store.claim_step(
        "target",
        interrupted.step_id,
        interrupted.tool_name,
        _stable_hash(interrupted_arguments),
        max_attempts=MAX_ATTEMPTS,
    )

    result = workflow.run(
        "target",
        manifest,
        replay=replay,
        resume_interrupted=True,
    )

    assert result.status == ReleaseValidationStatus.READY
    assert result.replay is not None
    assert result.replay.replayed_step_ids == ["run_unit_tests", "generate_evidence"]
    assert result.replay.reused_step_ids == [
        "load_manifest",
        "inspect_artifacts",
        "run_compatibility",
        "inspect_deployment",
    ]
    target_steps = {step.step_id: step for step in workflow.store.list_steps("target")}
    assert target_steps["run_unit_tests"].attempt_count == 2
    assert [event.event_type for event in workflow.store.list_events("target")].count(
        "step.replay_reused"
    ) == 4


def test_selective_replay_rejects_unknown_step_and_finalizes_target_failed(workflow):
    manifest = make_valid_manifest()
    workflow.run("source", manifest)

    with pytest.raises(SelectiveReplayError, match="unknown workflow node ids"):
        workflow.run(
            "target",
            manifest,
            replay=SelectiveReplayRequest(
                source_run_id="source",
                step_ids=["does_not_exist"],
            ),
        )

    assert workflow.store.get_execution("target").status == WorkflowStatus.FAILED


def test_selective_replay_requires_terminal_non_failed_source(workflow):
    manifest = make_valid_manifest()
    workflow.store.create_or_get_execution(
        "source", "release_validation", _manifest_hash_for_test(manifest)
    )
    workflow.store.mark_running("source")

    with pytest.raises(SelectiveReplayError, match="must be READY or BLOCKED"):
        workflow.run(
            "target",
            manifest,
            replay=SelectiveReplayRequest(
                source_run_id="source",
                step_ids=["run_unit_tests"],
            ),
        )


def test_second_run_with_same_manifest_hits_cache_and_skips_sandbox(workflow):
    manifest = make_valid_manifest()
    workflow.run("run-1", manifest)

    spy_sandbox = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy_sandbox
    result = workflow.run("run-1", manifest)

    assert result.status == ReleaseValidationStatus.READY
    assert spy_sandbox.call_log == []


def test_execution_input_mismatch_on_different_manifest_same_run_id(workflow):
    workflow.run("run-1", make_valid_manifest())

    with pytest.raises(ExecutionInputMismatchError):
        workflow.run("run-1", make_valid_manifest(release_version="9.9.9"))


def test_workflow_rejects_cached_row_from_different_registered_tool(workflow):
    manifest = _canonicalize_manifest(make_valid_manifest())
    workflow.store.create_or_get_execution(
        "run-1",
        "release_validation",
        _manifest_hash_for_test(manifest),
    )
    workflow.store.mark_running("run-1")
    first_step = STEP_SEQUENCE[0]
    arguments = first_step.build_arguments(manifest, {})
    wrong_tool_claim = workflow.store.claim_step(
        "run-1",
        first_step.step_id,
        "wrong_tool_same_arguments",
        _stable_hash(arguments),
        max_attempts=MAX_ATTEMPTS,
    )
    workflow.store.complete_step(
        "run-1",
        first_step.step_id,
        wrong_tool_claim.attempt_token,
        result_json="{}",
    )

    with pytest.raises(StepDefinitionMismatchError, match="wrong_tool_same_arguments"):
        workflow.run("run-1", manifest)

    assert workflow.store.get_execution("run-1").status == WorkflowStatus.FAILED


def test_timed_out_is_transient_and_retries_within_one_run_call(workflow):
    real_sandbox = workflow.sandbox
    scripted = ScriptedFaultSandbox(
        real_sandbox,
        {"run_unit_test_check": [fabricated_result("run_unit_test_check", ToolExecutionStatus.TIMED_OUT)]},
    )
    workflow.sandbox = scripted

    result = workflow.run("run-1", make_valid_manifest())

    assert result.status == ReleaseValidationStatus.READY
    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_unit_tests"].attempt_count == 2
    assert all(
        step.attempt_count == 1 for step_id, step in steps.items() if step_id != "run_unit_tests"
    )
    assert scripted.call_log.count("run_unit_test_check") == 2
    assert scripted.call_log.count("load_release_manifest") == 1


def test_production_runtime_module_carries_no_test_fault_vocabulary():
    """Guards against the exact class of bug the prior review flagged:
    a production default retry policy that recognizes test-only marker
    strings. No such vocabulary should exist anywhere in the module.
    """
    source = inspect.getsource(release_validation_runtime)
    for banned in ("transient_test_failure", "transient_worker_failure", "RETRYABLE_FAILURE_MARKERS"):
        assert banned not in source


def test_default_policy_only_retries_timed_out_not_failed_with_marker_text(workflow):
    """A `FAILED` result whose error text happens to contain marker-shaped
    vocabulary is still permanent under the *default* classifier -- only
    `TIMED_OUT` is retried by default. Recognizing that text is something
    only an explicitly injected classifier may do (see the next test).
    """
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "run_compatibility_check": [
                fabricated_result(
                    "run_compatibility_check",
                    ToolExecutionStatus.FAILED,
                    error="RuntimeError: transient_worker_failure",
                )
            ]
        },
    )
    workflow.sandbox = scripted

    with pytest.raises(StepPermanentFailureError):
        workflow.run("run-1", make_valid_manifest())

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_compatibility"].attempt_count == 1


def test_capability_unsupported_is_a_permanent_failure(workflow):
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "run_compatibility_check": [
                fabricated_result(
                    "run_compatibility_check",
                    ToolExecutionStatus.CAPABILITY_UNSUPPORTED,
                    error="filesystem=readonly cannot be enforced",
                )
            ]
        },
    )
    workflow.sandbox = scripted

    with pytest.raises(StepPermanentFailureError):
        workflow.run("run-1", make_valid_manifest())

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_compatibility"].attempt_count == 1


def test_injected_classifier_can_treat_a_specific_failed_result_as_transient(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    sandbox = ToolSandbox(build_release_validation_tool_registry())

    def classify_worker_failures_as_transient(execution: ToolExecutionResult) -> bool:
        return execution.status == ToolExecutionStatus.TIMED_OUT or (
            execution.status == ToolExecutionStatus.FAILED
            and execution.error is not None
            and "flaky_dependency_simulation" in execution.error
        )

    workflow = ReleaseValidationWorkflow(
        store, sandbox, is_transient_failure=classify_worker_failures_as_transient
    )
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "run_compatibility_check": [
                fabricated_result(
                    "run_compatibility_check",
                    ToolExecutionStatus.FAILED,
                    error="RuntimeError: flaky_dependency_simulation",
                )
            ]
        },
    )
    workflow.sandbox = scripted

    result = workflow.run("run-1", make_valid_manifest())

    assert result.status == ReleaseValidationStatus.READY
    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_compatibility"].attempt_count == 2


def test_failed_without_retryable_marker_is_permanent(workflow):
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "inspect_build_artifacts": [
                fabricated_result(
                    "inspect_build_artifacts",
                    ToolExecutionStatus.FAILED,
                    error="ValueError: unexpected schema drift in artifact registry",
                )
            ]
        },
    )
    workflow.sandbox = scripted

    with pytest.raises(StepPermanentFailureError):
        workflow.run("run-1", make_valid_manifest())

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["inspect_artifacts"].attempt_count == 1
    assert steps["inspect_artifacts"].status.value == "failed"
    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.FAILED


def test_denied_tool_is_permanent_not_retried(workflow):
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "inspect_deployment_configuration": [
                fabricated_result(
                    "inspect_deployment_configuration",
                    ToolExecutionStatus.DENIED,
                    error="Tool is not registered in the runtime allowlist.",
                )
            ]
        },
    )
    workflow.sandbox = scripted

    with pytest.raises(StepPermanentFailureError):
        workflow.run("run-1", make_valid_manifest())

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["inspect_deployment"].attempt_count == 1


def test_transient_failures_exhausting_max_attempts_fail_workflow_not_blocked(workflow):
    assert MAX_ATTEMPTS == 2
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "run_unit_test_check": [
                fabricated_result("run_unit_test_check", ToolExecutionStatus.TIMED_OUT),
                fabricated_result("run_unit_test_check", ToolExecutionStatus.TIMED_OUT),
            ]
        },
    )
    workflow.sandbox = scripted

    with pytest.raises(StepAttemptsExhaustedError):
        workflow.run("run-1", make_valid_manifest())

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_unit_tests"].attempt_count == 2
    assert steps["run_unit_tests"].status.value == "failed"
    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.FAILED


def test_already_running_step_stops_without_failing_the_workflow(workflow):
    manifest = make_valid_manifest()
    canonical_manifest = _canonicalize_manifest(manifest)
    workflow.store.create_or_get_execution(
        "run-1", "release_validation", _manifest_hash_for_test(manifest)
    )
    workflow.store.mark_running("run-1")
    first_step = STEP_SEQUENCE[0]
    step_hash = _stable_hash(first_step.build_arguments(canonical_manifest, {}))
    workflow.store.claim_step("run-1", first_step.step_id, first_step.tool_name, step_hash, max_attempts=2)

    with pytest.raises(StepAlreadyRunningError):
        workflow.run("run-1", manifest)

    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.RUNNING


def test_all_tools_succeed_but_python_compatibility_unmet_is_blocked(workflow):
    manifest = make_valid_manifest(
        required_python_versions=["3.11", "3.12"],
        tested_python_versions=["3.11"],
    )
    result = workflow.run("run-1", manifest)

    assert result.status == ReleaseValidationStatus.BLOCKED
    rule_ids = {finding.rule_id for finding in result.findings}
    assert "python_versions_covered" in rule_ids

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["run_compatibility"].status.value == "completed"
    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.BLOCKED


def test_completed_tool_result_missing_required_evidence_is_blocked(workflow):
    manifest = make_valid_manifest(
        evidence_checks_included=["artifacts", "tests", "deployment"],
    )
    result = workflow.run("run-1", manifest)

    assert result.status == ReleaseValidationStatus.BLOCKED
    rule_ids = {finding.rule_id for finding in result.findings}
    assert "evidence_complete" in rule_ids

    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps["generate_evidence"].status.value == "completed"


def test_invalid_deployment_configuration_is_blocked(workflow):
    manifest = make_valid_manifest(
        configuration_requirements=["DATABASE_URL", "FEATURE_FLAGS_ENDPOINT"],
        actual_configuration_keys=["DATABASE_URL"],
    )
    result = workflow.run("run-1", manifest)

    assert result.status == ReleaseValidationStatus.BLOCKED
    rule_ids = {finding.rule_id for finding in result.findings}
    assert "deployment_configuration_present" in rule_ids


def test_unexpected_validator_exception_fails_workflow_not_blocked(workflow):
    """A malformed tool result crashing the deterministic validator itself
    is a system FAILED, not a business BLOCKED -- the two must not be
    conflated even when the crash happens right after every tool call
    reported success.
    """
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {
            "generate_release_evidence": [
                fabricated_result(
                    "generate_release_evidence",
                    ToolExecutionStatus.COMPLETED,
                    error=None,
                )
            ]
        },
    )
    # `fabricated_result` defaults `result=None`, simulating a malformed
    # COMPLETED result a real bug could produce. `_run_step`'s own
    # invariant check catches this before it ever reaches the validator --
    # still an unexpected exception the workflow did not anticipate, so it
    # must still land on FAILED, not BLOCKED.
    workflow.sandbox = scripted

    with pytest.raises(AssertionError):
        workflow.run("run-1", make_valid_manifest())

    execution = workflow.store.get_execution("run-1")
    assert execution.status == WorkflowStatus.FAILED


def test_event_trace_explains_a_retry_in_order(workflow):
    scripted = ScriptedFaultSandbox(
        workflow.sandbox,
        {"run_unit_test_check": [fabricated_result("run_unit_test_check", ToolExecutionStatus.TIMED_OUT)]},
    )
    workflow.sandbox = scripted
    workflow.run("run-1", make_valid_manifest())

    events = workflow.store.list_events("run-1")
    unit_test_events = [event for event in events if event.payload.get("step_id") == "run_unit_tests"]
    event_types = [event.event_type for event in unit_test_events]
    assert event_types == ["step.claimed", "step.failed", "step.claimed", "step.completed"]
    assert unit_test_events[1].payload["attempt_count"] == 1
    assert unit_test_events[3].payload["attempt_count"] == 2


def test_reopened_store_can_resume_an_interrupted_step(tmp_path):
    database_path = tmp_path / "workflow.db"
    manifest = make_valid_manifest()

    canonical_manifest = _canonicalize_manifest(manifest)
    store = SQLiteWorkflowStore(database_path)
    store.create_or_get_execution(
        "run-1", "release_validation", _manifest_hash_for_test(manifest)
    )
    store.mark_running("run-1")
    first_step = STEP_SEQUENCE[0]
    step_hash = _stable_hash(first_step.build_arguments(canonical_manifest, {}))
    store.claim_step("run-1", first_step.step_id, first_step.tool_name, step_hash, max_attempts=2)

    # A brand-new store + workflow instance against the same file stands in
    # for a new process picking up after a restart.
    reopened_store = SQLiteWorkflowStore(database_path)
    reopened_sandbox = ToolSandbox(build_release_validation_tool_registry())
    reopened_workflow = ReleaseValidationWorkflow(reopened_store, reopened_sandbox)

    result = reopened_workflow.run("run-1", manifest, resume_interrupted=True)

    assert result.status == ReleaseValidationStatus.READY
    steps = {step.step_id: step for step in reopened_store.list_steps("run-1")}
    assert steps[first_step.step_id].attempt_count == 2


def test_second_call_after_permanent_failure_does_not_reexecute_and_stays_failed(tmp_path):
    """This is the exact false-success path the prior review caught: a
    permanently-failed execution must never come back as READY, and the
    second call must not touch the sandbox at all.
    """
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    real_sandbox = ToolSandbox(build_release_validation_tool_registry())
    scripted = ScriptedFaultSandbox(
        real_sandbox,
        {
            "run_unit_test_check": [
                fabricated_result("run_unit_test_check", ToolExecutionStatus.FAILED, error="ValueError: real_bug")
            ]
        },
    )
    workflow = ReleaseValidationWorkflow(store, scripted)
    manifest = make_valid_manifest()

    with pytest.raises(StepPermanentFailureError):
        workflow.run("run-1", manifest)

    execution = store.get_execution("run-1")
    assert execution.status == WorkflowStatus.FAILED

    scripted.call_log.clear()
    with pytest.raises(WorkflowExecutionFailedError) as exc_info:
        workflow.run("run-1", manifest)

    assert scripted.call_log == []
    assert exc_info.value.run_id == "run-1"
    assert exc_info.value.error_code is not None

    execution_after = store.get_execution("run-1")
    assert execution_after.status == WorkflowStatus.FAILED


def test_second_call_after_ready_reuses_persisted_result_without_sandbox_calls(workflow):
    manifest = make_valid_manifest()
    first = workflow.run("run-1", manifest)
    assert first.status == ReleaseValidationStatus.READY

    spy = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy
    events_before = len(workflow.store.list_events("run-1"))

    second = workflow.run("run-1", manifest)

    assert second.status == ReleaseValidationStatus.READY
    assert second.findings == []
    assert spy.call_log == []

    events = workflow.store.list_events("run-1")
    assert len(events) == events_before + 1
    assert events[-1].event_type == "workflow.result_reused"
    assert events[-1].payload["persisted_status"] == "ready"
    assert events[-1].payload["outcome"] == "reused"


def test_second_call_after_blocked_reuses_persisted_findings_without_sandbox_calls(workflow):
    manifest = make_valid_manifest(
        required_python_versions=["3.11", "3.12"],
        tested_python_versions=["3.11"],
    )
    first = workflow.run("run-1", manifest)
    assert first.status == ReleaseValidationStatus.BLOCKED

    spy = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy

    second = workflow.run("run-1", manifest)

    assert second.status == ReleaseValidationStatus.BLOCKED
    assert {finding.rule_id for finding in second.findings} == {
        finding.rule_id for finding in first.findings
    }
    assert spy.call_log == []
    events = workflow.store.list_events("run-1")
    assert events[-1].event_type == "workflow.result_reused"


def test_finalize_cas_miss_does_not_return_a_stale_ready_result(tmp_path):
    """Simulates another caller finalizing this execution as FAILED for an
    unrelated reason in the narrow window between this call computing
    READY and persisting it. `finalize_ready`'s own CAS will not apply --
    `run()` must surface the real persisted FAILED state, never the READY
    value it happened to compute in memory.
    """
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    sandbox = ToolSandbox(build_release_validation_tool_registry())
    workflow = ReleaseValidationWorkflow(store, sandbox)

    original_finalize_ready = store.finalize_ready

    def racing_finalize_ready(run_id: str, result_json: str):
        store.finalize_failed(run_id, error_code="raced_by_another_caller")
        return original_finalize_ready(run_id, result_json)

    store.finalize_ready = racing_finalize_ready  # type: ignore[method-assign]

    with pytest.raises(WorkflowExecutionFailedError) as exc_info:
        workflow.run("run-1", make_valid_manifest())

    assert exc_info.value.error_code == "raced_by_another_caller"
    execution = store.get_execution("run-1")
    assert execution.status == WorkflowStatus.FAILED


def test_cache_reused_event_emitted_for_completed_steps_during_interrupted_continuation(tmp_path):
    store = SQLiteWorkflowStore(tmp_path / "workflow.db")
    sandbox = ToolSandbox(build_release_validation_tool_registry())
    workflow = ReleaseValidationWorkflow(store, sandbox)
    manifest = make_valid_manifest()
    canonical_manifest = _canonicalize_manifest(manifest)

    store.create_or_get_execution("run-1", "release_validation", _manifest_hash_for_test(manifest))
    store.mark_running("run-1")

    # Manually complete the first three steps for real, exactly like
    # `_run_step` would, so they are genuinely CACHED-eligible afterwards.
    step_results: Dict[str, Dict[str, Any]] = {}
    for step in STEP_SEQUENCE[:3]:
        arguments = step.build_arguments(canonical_manifest, step_results)
        step_hash = _stable_hash(arguments)
        claim = store.claim_step("run-1", step.step_id, step.tool_name, step_hash, max_attempts=2)
        execution = sandbox.execute(step.tool_name, arguments)
        store.complete_step("run-1", step.step_id, claim.attempt_token, json.dumps(execution.result))
        step_results[step.step_id] = execution.result

    # Leave the fourth step claimed but never finished: simulates a crash mid-step.
    fourth_step = STEP_SEQUENCE[3]
    fourth_arguments = fourth_step.build_arguments(canonical_manifest, step_results)
    store.claim_step(
        "run-1", fourth_step.step_id, fourth_step.tool_name, _stable_hash(fourth_arguments), max_attempts=2
    )

    result = workflow.run("run-1", manifest, resume_interrupted=True)

    assert result.status == ReleaseValidationStatus.READY
    events = store.list_events("run-1")
    cache_events = [event for event in events if event.event_type == "step.cache_reused"]
    cached_step_ids = {event.payload["step_id"] for event in cache_events}
    assert cached_step_ids == {step.step_id for step in STEP_SEQUENCE[:3]}
    for event in cache_events:
        assert set(event.payload.keys()) == {"step_id", "tool_name", "attempt_count", "outcome"}


def test_reordered_order_insensitive_lists_produce_the_same_hash_and_no_reexecution(workflow):
    manifest_a = make_valid_manifest(
        required_python_versions=["3.11", "3.12"],
        tested_python_versions=["3.11", "3.12"],
    )
    manifest_b = make_valid_manifest(
        required_python_versions=["3.12", "3.11"],
        tested_python_versions=["3.12", "3.11"],
    )
    assert _manifest_hash_for_test(manifest_a) == _manifest_hash_for_test(manifest_b)

    result_a = workflow.run("run-1", manifest_a)
    assert result_a.status == ReleaseValidationStatus.READY

    spy = ScriptedFaultSandbox(workflow.sandbox)
    workflow.sandbox = spy
    result_b = workflow.run("run-1", manifest_b)

    assert result_b.status == ReleaseValidationStatus.READY
    assert spy.call_log == []


def test_genuinely_different_list_content_still_triggers_input_mismatch(workflow):
    manifest_a = make_valid_manifest(required_python_versions=["3.11", "3.12"])
    manifest_b = make_valid_manifest(required_python_versions=["3.11", "3.13"])

    workflow.run("run-1", manifest_a)
    with pytest.raises(ExecutionInputMismatchError):
        workflow.run("run-1", manifest_b)


def test_resume_interrupted_recovers_and_completes_the_workflow(workflow):
    manifest = make_valid_manifest()
    canonical_manifest = _canonicalize_manifest(manifest)
    workflow.store.create_or_get_execution(
        "run-1", "release_validation", _manifest_hash_for_test(manifest)
    )
    workflow.store.mark_running("run-1")
    first_step = STEP_SEQUENCE[0]
    step_hash = _stable_hash(first_step.build_arguments(canonical_manifest, {}))
    workflow.store.claim_step("run-1", first_step.step_id, first_step.tool_name, step_hash, max_attempts=2)

    result = workflow.run("run-1", manifest, resume_interrupted=True)

    assert result.status == ReleaseValidationStatus.READY
    steps = {step.step_id: step for step in workflow.store.list_steps("run-1")}
    assert steps[first_step.step_id].attempt_count == 2
    assert all(
        step.attempt_count == 1 for step_id, step in steps.items() if step_id != first_step.step_id
    )
