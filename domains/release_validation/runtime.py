from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from agent.contracts import RuntimeExecutionContext, RuntimeResponse, TraceEvent
from runtime_service.dag import WorkflowDag, WorkflowGraphError, WorkflowNode
from runtime_service.sandbox import ToolExecutionResult, ToolExecutionStatus, ToolSandbox
from runtime_service.workflow_store import (
    ClaimOutcome,
    ExecutionOutcome,
    StepReuseOutcome,
    WorkflowStore,
    WorkflowExecutionRecord,
    WorkflowStatus,
)

from .models import (
    ReleaseManifest,
    ReleaseValidationInput,
    ReleaseValidationResult,
    ReleaseValidationState,
    ReleaseValidationStatus,
    ReleaseValidationInputV1,
    SelectiveReplayRequest,
    SelectiveReplaySummary,
    ValidationFinding,
)

WORKFLOW_TYPE = "release_validation"
MAX_ATTEMPTS = 2

# Fields whose *order* carries no meaning in this domain -- each is used
# downstream only via membership/equality checks, never by position.
# Canonicalizing them before hashing (and before building tool arguments)
# means two manifests that list the same requirements in a different
# order are the same input, not a mismatch. Duplicates are preserved:
# `sorted()` does not deduplicate.
_ORDER_INSENSITIVE_STRING_LIST_FIELDS = (
    "required_artifacts",
    "required_python_versions",
    "tested_python_versions",
    "configuration_requirements",
    "actual_configuration_keys",
    "evidence_checks_included",
)

# Bounds the retry/recovery loop below independently of `MAX_ATTEMPTS`, as
# a defense-in-depth guard against a future logic error turning this into
# an infinite loop -- not expected to ever be hit in practice, since
# `claim_step`'s own `max_attempts` gate already bounds real retries.
_STEP_LOOP_GUARD = MAX_ATTEMPTS + 3

# A classifier decides, from the sandbox's own result, whether a failed
# tool call is worth one more attempt. The default only ever retries
# `TIMED_OUT`: a `FAILED` result could be a real bug, and guessing which
# ones are "probably transient" from free-text `error` content belongs to
# whoever is simulating a flaky dependency in a test, not to the
# production default. Tests that need a retryable `FAILED` inject their
# own classifier instead of the workflow recognizing test-only vocabulary.
TransientFailureClassifier = Callable[[ToolExecutionResult], bool]


def _default_is_transient_failure(execution: ToolExecutionResult) -> bool:
    return execution.status == ToolExecutionStatus.TIMED_OUT


class ExecutionInputMismatchError(Exception):
    """`run_id` was already used with a manifest that hashes differently.

    Not implicitly resolved in place: terminal and in-progress executions
    are immutable with respect to their own input. Selective replay creates
    a different target `run_id` and references the old run as its source.
    """


class WorkflowTypeMismatchError(Exception):
    """`run_id` was already used for a different `workflow_type`."""


class WorkflowExecutionFailedError(Exception):
    """`run_id` already finalized as FAILED in a previous call.

    A FAILED execution is terminal: this call does not re-enter the step
    loop, does not re-claim any step, and does not attempt to move the
    execution back to RUNNING. The original exception that caused the
    failure is not reconstructed (its type was never persisted) -- this
    always carries the persisted `error_code` describing what failed.
    """

    def __init__(self, run_id: str, error_code: Optional[str]):
        self.run_id = run_id
        self.error_code = error_code
        super().__init__(
            f"execution {run_id!r} already finalized as FAILED (error_code={error_code!r})"
        )


class SelectiveReplayError(Exception):
    """A selective replay request has invalid source or graph semantics."""


class StepAlreadyRunningError(Exception):
    """A step is already `running` and `resume_interrupted` was not set.

    This is the normal/expected outcome for a genuinely still-in-progress
    step -- raised instead of silently reclaiming it. `run()` does not
    finalize the execution as failed for this: the workflow really is
    still in progress, not broken, and a later call (with or without
    `resume_interrupted=True`) can pick it back up.
    """


class StepInputMismatchError(Exception):
    """A step's argument signature no longer matches its persisted claim.

    Every current step's arguments are a pure function of the manifest
    alone, and the manifest hash is already checked once at the execution
    level -- so this should not be reachable in normal operation. If it
    fires, it means a step's arguments changed without the execution-level
    hash catching it, which is treated as an unexpected internal
    inconsistency (workflow FAILED), not a normal blocked/in-progress case.
    """


class StepDefinitionMismatchError(Exception):
    """A persisted step id belongs to a different registered tool."""


class StepAttemptsExhaustedError(Exception):
    """A step ran out of retry attempts. The workflow is FAILED, not BLOCKED:
    this means the tool itself could not produce a result, not that the
    tool produced a result the validator rejected.
    """


class StepPermanentFailureError(Exception):
    """A step failed with an error the classifier did not treat as transient."""


def _stable_hash(payload: Dict[str, Any]) -> str:
    """Canonical, deterministic signature for a JSON-shaped dict.

    Only ever fed data that already excludes timestamps, random ids, and
    attempt tokens (manifests and tool arguments built from them) -- there
    is nothing here to filter, because nothing non-deterministic is ever
    passed in.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonicalize_manifest(manifest: ReleaseManifest) -> ReleaseManifest:
    data = manifest.model_dump(mode="json")
    for field in _ORDER_INSENSITIVE_STRING_LIST_FIELDS:
        data[field] = sorted(data[field])
    data["available_artifacts"] = sorted(
        data["available_artifacts"], key=lambda item: (item["name"], item["checksum"])
    )
    return ReleaseManifest.model_validate(data)


@dataclass(frozen=True)
class StepDefinition:
    step_id: str
    tool_name: str
    build_arguments: Callable[[ReleaseManifest, Dict[str, Dict[str, Any]]], Dict[str, Any]]
    dependencies: tuple[str, ...] = ()


def _manifest_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "release_id": manifest.release_id,
        "application_name": manifest.application_name,
        "release_version": manifest.release_version,
        "required_artifacts": manifest.required_artifacts,
        "required_test_suite": manifest.required_test_suite,
        "required_python_versions": manifest.required_python_versions,
        "deployment_environment": manifest.deployment_environment,
        "configuration_requirements": manifest.configuration_requirements,
    }


def _artifacts_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "required_artifacts": manifest.required_artifacts,
        "available_artifacts": [artifact.model_dump(mode="json") for artifact in manifest.available_artifacts],
    }


def _unit_test_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "required_test_suite": manifest.required_test_suite,
        "executed_suite": manifest.executed_test_suite,
        "tests_passed": manifest.tests_passed,
    }


def _compatibility_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "required_python_versions": manifest.required_python_versions,
        "tested_python_versions": manifest.tested_python_versions,
    }


def _deployment_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "configuration_requirements": manifest.configuration_requirements,
        "actual_configuration_keys": manifest.actual_configuration_keys,
    }


def _evidence_arguments(manifest: ReleaseManifest, _results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "release_id": manifest.release_id,
        "include_checks": manifest.evidence_checks_included,
    }


# The five evidence-producing checks are independent roots. The final
# evidence node is a fan-in over every root. Execution is intentionally
# serial for this phase, but readiness is decided from this explicit graph
# rather than from adjacency in a hardcoded list.
STEP_DEFINITIONS: List[StepDefinition] = [
    StepDefinition("load_manifest", "load_release_manifest", _manifest_arguments),
    StepDefinition("inspect_artifacts", "inspect_build_artifacts", _artifacts_arguments),
    StepDefinition("run_unit_tests", "run_unit_test_check", _unit_test_arguments),
    StepDefinition("run_compatibility", "run_compatibility_check", _compatibility_arguments),
    StepDefinition("inspect_deployment", "inspect_deployment_configuration", _deployment_arguments),
    StepDefinition(
        "generate_evidence",
        "generate_release_evidence",
        _evidence_arguments,
        dependencies=(
            "load_manifest",
            "inspect_artifacts",
            "run_unit_tests",
            "run_compatibility",
            "inspect_deployment",
        ),
    ),
]
RELEASE_VALIDATION_DAG = WorkflowDag(
    WorkflowNode(step.step_id, step.dependencies) for step in STEP_DEFINITIONS
)
_STEP_BY_ID = {step.step_id: step for step in STEP_DEFINITIONS}
# Backward-compatible public view used by existing callers and tests. It is
# now derived from the validated DAG's deterministic topological order.
STEP_SEQUENCE: List[StepDefinition] = [
    _STEP_BY_ID[step_id] for step_id in RELEASE_VALIDATION_DAG.topological_order
]


def validate_release_readiness(step_results: Dict[str, Dict[str, Any]]) -> List[ValidationFinding]:
    """Deterministic readiness checklist -- five independent rules.

    Each rule inspects the *content* of a completed tool result, not just
    whether the tool call succeeded: a step can be `completed` and still
    fail its rule here (e.g. `run_compatibility_check` completing while
    reporting a missing Python version). This is what lets the workflow
    distinguish "every tool call succeeded" from "the release is actually
    ready" -- a tool-call success is not a readiness verdict.

    Deliberately a flat list of independent checks, not a rule engine:
    no priority, no composition, no configuration. Extending this to a
    generic policy language is explicitly out of scope.
    """
    findings: List[ValidationFinding] = []

    artifacts = step_results["inspect_artifacts"]
    if artifacts["missing_artifacts"] or artifacts["invalid_checksums"]:
        findings.append(
            ValidationFinding(
                rule_id="artifacts_present_and_valid",
                message=(
                    f"missing={artifacts['missing_artifacts']} "
                    f"invalid_checksums={artifacts['invalid_checksums']}"
                ),
            )
        )

    tests = step_results["run_unit_tests"]
    if not tests["matches_required_suite"] or not tests["passed"]:
        findings.append(
            ValidationFinding(
                rule_id="unit_test_suite_passed",
                message=(
                    f"suite={tests['suite_name']} "
                    f"matches_required={tests['matches_required_suite']} passed={tests['passed']}"
                ),
            )
        )

    compatibility = step_results["run_compatibility"]
    if compatibility["missing_versions"]:
        findings.append(
            ValidationFinding(
                rule_id="python_versions_covered",
                message=f"missing_versions={compatibility['missing_versions']}",
            )
        )

    deployment = step_results["inspect_deployment"]
    if deployment["missing_keys"]:
        findings.append(
            ValidationFinding(
                rule_id="deployment_configuration_present",
                message=f"missing_keys={deployment['missing_keys']}",
            )
        )

    evidence = step_results["generate_evidence"]
    if not evidence["evidence_complete"]:
        findings.append(
            ValidationFinding(
                rule_id="evidence_complete",
                message=f"referenced_checks={evidence['referenced_checks']}",
            )
        )

    return findings


class ReleaseValidationWorkflow:
    """Dependency-aware, serial registered-tool workflow.

    Uses the structural `WorkflowStore` contract and `ToolSandbox` directly.
    The service composition root supplies the SQLite implementation. The
    workflow itself has no manager or HTTP concerns;
    `ManagedReleaseValidationRuntime` below is the explicit integration adapter.
    """

    def __init__(
        self,
        store: WorkflowStore,
        sandbox: ToolSandbox,
        *,
        is_transient_failure: Optional[TransientFailureClassifier] = None,
    ):
        self.store = store
        self.sandbox = sandbox
        self._is_transient_failure = is_transient_failure or _default_is_transient_failure

    def run(
        self,
        run_id: str,
        manifest: ReleaseManifest,
        *,
        resume_interrupted: bool = False,
        replay: SelectiveReplayRequest | None = None,
        legacy_fixed_order: bool = False,
        lease_token: str | None = None,
    ) -> ReleaseValidationResult:
        manifest = _canonicalize_manifest(manifest)
        identity_payload = manifest.model_dump(mode="json")
        if replay is not None:
            identity_payload = {
                "manifest": identity_payload,
                "replay": replay.model_dump(mode="json"),
            }
        execution_hash = _stable_hash(identity_payload)
        claim = self.store.create_or_get_execution(
            run_id,
            WORKFLOW_TYPE,
            execution_hash,
            lease_token=lease_token,
        )
        if claim.outcome == ExecutionOutcome.WORKFLOW_TYPE_MISMATCH:
            raise WorkflowTypeMismatchError(
                f"run_id {run_id!r} was previously used for workflow_type "
                f"{claim.execution.workflow_type!r}, not {WORKFLOW_TYPE!r}"
            )
        if claim.outcome == ExecutionOutcome.INPUT_MISMATCH:
            raise ExecutionInputMismatchError(
                f"run_id {run_id!r} was already started with a different manifest"
            )

        # The execution may already be terminal from a previous call. Each
        # terminal status has exactly one legal response, decided from the
        # persisted record alone -- never from re-running anything.
        execution_record = claim.execution
        if execution_record.status in (WorkflowStatus.READY, WorkflowStatus.BLOCKED):
            return self._reuse_terminal_result(
                run_id,
                execution_record,
                lease_token=lease_token,
            )
        if execution_record.status == WorkflowStatus.FAILED:
            raise WorkflowExecutionFailedError(run_id, execution_record.error_code)
        if execution_record.status == WorkflowStatus.PENDING:
            self.store.mark_running(run_id, lease_token=lease_token)
        # WorkflowStatus.RUNNING: fall through into the step loop, whose
        # own ALREADY_RUNNING/interrupted-recovery handling covers it.

        try:
            if legacy_fixed_order and replay is not None:
                raise SelectiveReplayError(
                    "release-validation:1.0.0 does not support selective replay"
                )
            step_results: Dict[str, Dict[str, Any]] = {}
            invalidated_step_ids: set[str] = set()
            replayed_step_ids: set[str] = set()
            reused_step_ids: set[str] = set()
            automatically_invalidated_step_ids: set[str] = set()
            if replay is not None:
                self._validate_replay_request(run_id, replay)
                try:
                    invalidated_step_ids.update(
                        RELEASE_VALIDATION_DAG.descendants(replay.step_ids)
                    )
                except WorkflowGraphError as exc:
                    raise SelectiveReplayError(str(exc)) from exc

            scheduled_steps = STEP_DEFINITIONS if legacy_fixed_order else STEP_SEQUENCE
            for step in scheduled_steps:
                if not legacy_fixed_order:
                    missing_dependencies = (
                        RELEASE_VALIDATION_DAG.dependencies_for(step.step_id)
                        - step_results.keys()
                    )
                    if missing_dependencies:
                        raise RuntimeError(
                            f"scheduler selected {step.step_id!r} before dependencies "
                            f"{sorted(missing_dependencies)!r} completed"
                        )
                arguments = step.build_arguments(manifest, step_results)

                if replay is not None and step.step_id not in invalidated_step_ids:
                    reuse = self.store.reuse_completed_step(
                        replay.source_run_id,
                        run_id,
                        step.step_id,
                        step.tool_name,
                        _stable_hash(arguments),
                        lease_token=lease_token,
                    )
                    if reuse.outcome in {StepReuseOutcome.COPIED, StepReuseOutcome.EXISTING}:
                        assert reuse.step is not None and reuse.step.result_json is not None
                        step_results[step.step_id] = json.loads(reuse.step.result_json)
                        reused_step_ids.add(step.step_id)
                        continue

                    automatically_invalidated_step_ids.add(step.step_id)
                    invalidated_step_ids.update(
                        RELEASE_VALIDATION_DAG.descendants([step.step_id])
                    )

                step_results[step.step_id] = self._run_step(
                    run_id,
                    step,
                    arguments,
                    resume_interrupted=resume_interrupted,
                    lease_token=lease_token,
                )
                if replay is not None:
                    replayed_step_ids.add(step.step_id)

            replay_summary = None
            if replay is not None:
                ordered_ids = RELEASE_VALIDATION_DAG.topological_order
                replay_summary = SelectiveReplaySummary(
                    source_run_id=replay.source_run_id,
                    requested_step_ids=list(replay.step_ids),
                    replayed_step_ids=[
                        step_id for step_id in ordered_ids if step_id in replayed_step_ids
                    ],
                    reused_step_ids=[
                        step_id for step_id in ordered_ids if step_id in reused_step_ids
                    ],
                    automatically_invalidated_step_ids=[
                        step_id
                        for step_id in ordered_ids
                        if step_id in automatically_invalidated_step_ids
                    ],
                )

            findings = validate_release_readiness(step_results)
            if findings:
                candidate = ReleaseValidationResult(
                    run_id=run_id,
                    status=ReleaseValidationStatus.BLOCKED,
                    findings=findings,
                    replay=replay_summary,
                )
                record = self.store.finalize_blocked(
                    run_id,
                    candidate.model_dump_json(),
                    error_code="readiness_requirements_unmet",
                    lease_token=lease_token,
                )
            else:
                candidate = ReleaseValidationResult(
                    run_id=run_id,
                    status=ReleaseValidationStatus.READY,
                    findings=[],
                    replay=replay_summary,
                )
                record = self.store.finalize_ready(
                    run_id,
                    candidate.model_dump_json(),
                    lease_token=lease_token,
                )

            return self._settle_finalize(
                run_id,
                candidate,
                record,
                lease_token=lease_token,
            )
        except (
            ExecutionInputMismatchError,
            WorkflowTypeMismatchError,
            WorkflowExecutionFailedError,
            StepAlreadyRunningError,
        ):
            # Pre-execution identity conflicts, an already-FAILED terminal
            # execution, and a genuinely still-in-progress step are not
            # workflow failures raised *by this call*: nothing is
            # (re-)finalized here.
            raise
        except Exception as exc:
            record = self.store.finalize_failed(
                run_id,
                error_code=f"{type(exc).__name__}: {exc}",
                lease_token=lease_token,
            )
            if record.status != WorkflowStatus.FAILED:
                # The CAS did not apply -- do not let a late failure handler
                # override or misreport an execution that some other call
                # already finalized. Surface the real persisted state
                # instead of silently swallowing the discrepancy.
                raise RuntimeError(
                    f"finalize_failed did not apply for {run_id!r}; persisted status is "
                    f"{record.status.value!r}, not FAILED as expected"
                ) from exc
            raise

    def _validate_replay_request(
        self,
        target_run_id: str,
        replay: SelectiveReplayRequest,
    ) -> None:
        if replay.source_run_id == target_run_id:
            raise SelectiveReplayError("selective replay must create a new target run_id")
        source = self.store.get_execution(replay.source_run_id)
        if source is None:
            raise SelectiveReplayError(
                f"selective replay source not found: {replay.source_run_id!r}"
            )
        if source.workflow_type != WORKFLOW_TYPE:
            raise SelectiveReplayError(
                f"selective replay source {replay.source_run_id!r} has workflow_type "
                f"{source.workflow_type!r}, not {WORKFLOW_TYPE!r}"
            )
        if source.status not in {WorkflowStatus.READY, WorkflowStatus.BLOCKED}:
            raise SelectiveReplayError(
                f"selective replay source {replay.source_run_id!r} must be READY or BLOCKED, "
                f"not {source.status.value!r}"
            )

    def _settle_finalize(
        self,
        run_id: str,
        candidate: ReleaseValidationResult,
        record: WorkflowExecutionRecord,
        *,
        lease_token: str | None,
    ) -> ReleaseValidationResult:
        """Reconcile what `run()` computed with what actually got persisted.

        `finalize_ready`/`finalize_blocked` are compare-and-set: the
        UPDATE only applies if the execution was still RUNNING. If it
        didn't apply, `record` already carries the *true* persisted state,
        and the in-memory `candidate` this call computed must never be
        returned as if it had. This is the check that closes the false
        "READY returned, FAILED persisted" gap.
        """
        if record.status.value == candidate.status.value:
            return candidate
        if record.status in (WorkflowStatus.READY, WorkflowStatus.BLOCKED):
            return self._reuse_terminal_result(
                run_id,
                record,
                lease_token=lease_token,
            )
        if record.status == WorkflowStatus.FAILED:
            raise WorkflowExecutionFailedError(run_id, record.error_code)
        raise RuntimeError(
            f"finalize for {run_id!r} did not reach a terminal status as expected "
            f"(persisted status={record.status.value!r}); internal consistency error"
        )

    def _reuse_terminal_result(
        self,
        run_id: str,
        record: WorkflowExecutionRecord,
        *,
        lease_token: str | None,
    ) -> ReleaseValidationResult:
        if record.result_json is None:
            raise RuntimeError(
                f"terminal execution {run_id!r} (status={record.status.value}) has no "
                "persisted result_json to reuse"
            )
        result = ReleaseValidationResult.model_validate_json(record.result_json)
        if result.status.value != record.status.value:
            raise RuntimeError(
                f"persisted result status {result.status.value!r} does not match execution "
                f"status {record.status.value!r} for {run_id!r}"
            )
        self.store.append_event(
            run_id,
            "workflow.result_reused",
            {"run_id": run_id, "persisted_status": record.status.value, "outcome": "reused"},
            lease_token=lease_token,
        )
        return result

    def _run_step(
        self,
        run_id: str,
        step: StepDefinition,
        arguments: Dict[str, Any],
        *,
        resume_interrupted: bool,
        lease_token: str | None,
    ) -> Dict[str, Any]:
        step_hash = _stable_hash(arguments)

        for _ in range(_STEP_LOOP_GUARD):
            claim = self.store.claim_step(
                run_id,
                step.step_id,
                step.tool_name,
                step_hash,
                max_attempts=MAX_ATTEMPTS,
                lease_token=lease_token,
            )

            if claim.outcome == ClaimOutcome.CACHED:
                assert claim.step is not None and claim.step.result_json is not None
                self.store.append_event(
                    run_id,
                    "step.cache_reused",
                    {
                        "step_id": step.step_id,
                        "tool_name": step.tool_name,
                        "attempt_count": claim.step.attempt_count,
                        "outcome": "cached",
                    },
                    lease_token=lease_token,
                )
                return json.loads(claim.step.result_json)

            if claim.outcome == ClaimOutcome.INPUT_MISMATCH:
                raise StepInputMismatchError(
                    f"step {run_id}/{step.step_id} was previously claimed with "
                    "different arguments"
                )

            if claim.outcome == ClaimOutcome.DEFINITION_MISMATCH:
                assert claim.step is not None
                raise StepDefinitionMismatchError(
                    f"step {run_id}/{step.step_id} was previously claimed by tool "
                    f"{claim.step.tool_name!r}, not {step.tool_name!r}"
                )

            if claim.outcome == ClaimOutcome.ATTEMPTS_EXHAUSTED:
                raise StepAttemptsExhaustedError(
                    f"step {run_id}/{step.step_id} exhausted {MAX_ATTEMPTS} attempts"
                )

            if claim.outcome == ClaimOutcome.ALREADY_RUNNING:
                if not resume_interrupted:
                    raise StepAlreadyRunningError(
                        f"step {run_id}/{step.step_id} is already running; pass "
                        "resume_interrupted=True to explicitly recover it"
                    )
                self.store.recover_interrupted_step(
                    run_id,
                    step.step_id,
                    lease_token=lease_token,
                )
                continue  # re-claim: the row is now FAILED(interrupted), eligible for a fresh attempt

            assert claim.outcome == ClaimOutcome.CLAIMED
            assert claim.attempt_token is not None
            execution = self.sandbox.execute(step.tool_name, arguments)

            if execution.status == ToolExecutionStatus.COMPLETED:
                result_json = json.dumps(execution.result)
                self.store.complete_step(
                    run_id,
                    step.step_id,
                    claim.attempt_token,
                    result_json,
                    lease_token=lease_token,
                )
                assert execution.result is not None
                return execution.result

            error_code = _step_error_code(execution.status, execution.error)
            self.store.fail_step(
                run_id,
                step.step_id,
                claim.attempt_token,
                error_code=error_code,
                lease_token=lease_token,
            )

            if self._is_transient_failure(execution):
                continue  # re-claim: claim_step decides retry vs ATTEMPTS_EXHAUSTED

            raise StepPermanentFailureError(
                f"tool {step.tool_name} failed permanently for {run_id}/{step.step_id}: "
                f"{execution.status.value}: {execution.error}"
            )

        raise RuntimeError(
            f"step {run_id}/{step.step_id} did not resolve within {_STEP_LOOP_GUARD} "
            "claim attempts; this indicates an internal logic error, not a normal retry"
        )


def _step_error_code(status: ToolExecutionStatus, error: str | None) -> str:
    if status == ToolExecutionStatus.FAILED and error:
        return error.strip()[:200]
    return status.value


class ManagedReleaseValidationRuntime:
    """Adapter that exposes the DAG workflow through ``RuntimeManager``.

    The workflow remains statically defined and serial. This adapter adds
    lifecycle integration, typed input/state validation and a domain checkpoint.
    The DAG and replay targets are deterministic inputs, never planner-selected.
    """

    def __init__(
        self,
        workflow: ReleaseValidationWorkflow,
        *,
        legacy_fixed_order: bool = False,
    ):
        self.workflow = workflow
        self.legacy_fixed_order = legacy_fixed_order

    def initial_state(self, thread_id: str) -> ReleaseValidationState:
        return ReleaseValidationState(thread_id=thread_id)

    def execute(
        self,
        state: ReleaseValidationState,
        runtime_input: ReleaseValidationInputV1 | ReleaseValidationInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[ReleaseValidationState]:
        replay = (
            runtime_input.replay
            if isinstance(runtime_input, ReleaseValidationInput)
            else None
        )
        result = self.workflow.run(
            context.run_id,
            runtime_input.manifest,
            resume_interrupted=(
                runtime_input.resume_interrupted or context.recovered_after_restart
            ),
            replay=replay,
            legacy_fixed_order=self.legacy_fixed_order,
            lease_token=context.lease_token,
        )
        updated_state = state.model_copy(
            update={
                "manifest": runtime_input.manifest,
                "result": result,
                "current_stage": result.status.value,
                "execution_trace": [
                    *state.execution_trace,
                    TraceEvent(
                        event="release_validation_finished",
                        reason=result.status.value,
                        payload={
                            "run_id": context.run_id,
                            "finding_count": len(result.findings),
                            "replay_source_run_id": (
                                result.replay.source_run_id if result.replay is not None else None
                            ),
                            "replayed_step_count": (
                                len(result.replay.replayed_step_ids)
                                if result.replay is not None
                                else 0
                            ),
                        },
                    ),
                ],
            }
        )
        validation_errors = [
            f"{finding.rule_id}: {finding.message}" for finding in result.findings
        ]
        return RuntimeResponse[ReleaseValidationState](
            message=(
                "Release validation passed."
                if result.status == ReleaseValidationStatus.READY
                else f"Release validation blocked by {len(result.findings)} finding(s)."
            ),
            state=updated_state,
            validation_errors=validation_errors,
        )
