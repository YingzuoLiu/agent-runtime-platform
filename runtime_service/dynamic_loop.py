from __future__ import annotations

import json
from enum import Enum
from typing import Any, NoReturn, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from agent.contracts import RuntimeExecutionContext, RuntimeExecutionError

from .canonical import decode_tool_result, stable_hash
from .external_action_coordinator import ExternalActionCoordinator
from .external_actions import (
    ExternalActionDispatcher,
    ExternalActionReconciliationPendingError,
)
from .evidence import EvidenceProjector, RunEventSink
from .planner import (
    PLANNER_DECISION_ADAPTER,
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    Planner,
    PlannerContext,
    PlannerProviderError,
    RequestClarificationDecision,
    ToolObservation,
)
from .sandbox import (
    ToolEffect,
    ToolExecutionStatus,
    ToolRegistry,
    ToolRetryMode,
    ToolSandbox,
)
from .workflow_store import (
    ClaimOutcome,
    ExecutionOutcome,
    ExternalActionRecord,
    ToolCallRecord,
    ToolCallStatus,
    WorkflowStore,
    WorkflowStatus,
)


class DynamicLoopOutcome(str, Enum):
    FINISHED = "finished"
    CLARIFICATION = "clarification"
    BLOCKED = "blocked"


class DynamicLoopResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: DynamicLoopOutcome
    message: str
    output: dict[str, Any] = Field(default_factory=dict)
    validation_errors: list[str] = Field(default_factory=list)
    observations: list[ToolObservation] = Field(default_factory=list)


class FinishEvaluation(BaseModel):
    """Domain-owned validation result for a structurally valid FINISH decision."""

    model_config = ConfigDict(extra="forbid")

    outcome: DynamicLoopOutcome
    message: str
    output: dict[str, Any] = Field(default_factory=dict)
    validation_errors: list[str] = Field(default_factory=list)


class FinishEvaluator(Protocol):
    def __call__(
        self,
        decision: FinishDecision,
        observations: list[ToolObservation],
    ) -> FinishEvaluation:
        ...


class DynamicToolLoop:
    """Durable, policy-governed Planner -> Tool -> Observation loop.

    The loop owns no domain rules. A concrete runtime supplies the planner,
    state snapshot and FINISH evaluator; this class owns decision validation,
    execution policy, durable indexed tool calls and stable failure semantics.
    """

    TOOL_EXECUTE_PERMISSION = "tools:execute"
    EXTERNAL_ACTION_EXECUTE_PERMISSION = "external-actions:execute"
    MAX_EXTERNAL_ACTION_DISPATCHES = 2
    _FAILURE_MESSAGES = {
        "unknown_tool": "Planner selected a tool outside the runtime allowlist.",
        "invalid_tool_arguments": (
            "Planner supplied arguments that do not match the tool schema."
        ),
        "tool_permission_denied": (
            "Execution authority does not allow tool execution."
        ),
        "tool_timed_out": "Tool execution exceeded its configured deadline.",
        "tool_execution_failed": "Tool execution failed.",
        "step_limit_exceeded": "The dynamic tool-call limit was exceeded.",
        "invalid_planner_decision": "Planner decision validation failed.",
        "planner_provider_failed": "Planner provider failed.",
        "external_action_permission_denied": (
            "Execution authority does not allow external actions."
        ),
        "external_action_not_configured": (
            "External action execution is not configured for this tool."
        ),
        "external_action_idempotency_unsupported": (
            "The configured provider does not support required idempotency."
        ),
        "external_action_not_requested": (
            "The external action was not explicitly requested by this run."
        ),
        "external_action_failed": "External action failed definitively.",
        "external_action_outcome_unknown": (
            "External action outcome is unknown and was not retried again."
        ),
        "external_action_evidence_incomplete": (
            "External action completed, but durable run evidence is incomplete."
        ),
        "run_cancel_requested": (
            "Run cancellation was requested before external action dispatch."
        ),
    }

    def __init__(
        self,
        *,
        planner: Planner,
        tool_registry: ToolRegistry,
        tool_sandbox: ToolSandbox,
        workflow_store: WorkflowStore,
        run_event_sink: RunEventSink,
        workflow_type: str,
        max_tool_calls: int = 4,
        external_action_dispatcher: ExternalActionDispatcher | None = None,
    ) -> None:
        if max_tool_calls < 1:
            raise ValueError("max_tool_calls must be at least 1")
        self.planner = planner
        self.tool_registry = tool_registry
        self.tool_sandbox = tool_sandbox
        self.workflow_store = workflow_store
        self.run_event_sink = run_event_sink
        self.evidence_projector = EvidenceProjector(
            workflow_store=workflow_store,
            run_event_sink=run_event_sink,
        )
        self.workflow_type = workflow_type
        self.max_tool_calls = max_tool_calls
        self.external_action_dispatcher = external_action_dispatcher
        self.external_action_coordinator = ExternalActionCoordinator(
            workflow_store=workflow_store,
            dispatcher=external_action_dispatcher,
            workflow_type=workflow_type,
            evidence_projector=self.evidence_projector,
            fail=lambda run_id, code, detail: self._fail(run_id, code, detail),
            failure_messages=lambda: self._FAILURE_MESSAGES,
            max_dispatches=lambda: self.MAX_EXTERNAL_ACTION_DISPATCHES,
        )

    @property
    def workflow_store(self) -> WorkflowStore:
        return self._workflow_store

    @workflow_store.setter
    def workflow_store(self, store: WorkflowStore) -> None:
        self._workflow_store = store
        evidence_projector = getattr(self, "evidence_projector", None)
        if evidence_projector is not None:
            evidence_projector.workflow_store = store
        coordinator = getattr(self, "external_action_coordinator", None)
        if coordinator is not None:
            coordinator.workflow_store = store

    @property
    def run_event_sink(self) -> RunEventSink:
        return self._run_event_sink

    @run_event_sink.setter
    def run_event_sink(self, sink: RunEventSink) -> None:
        self._run_event_sink = sink
        evidence_projector = getattr(self, "evidence_projector", None)
        if evidence_projector is not None:
            evidence_projector.run_event_sink = sink

    @property
    def workflow_type(self) -> str:
        return self._workflow_type

    @workflow_type.setter
    def workflow_type(self, workflow_type: str) -> None:
        self._workflow_type = workflow_type
        coordinator = getattr(self, "external_action_coordinator", None)
        if coordinator is not None:
            coordinator.workflow_type = workflow_type

    @property
    def external_action_dispatcher(self) -> ExternalActionDispatcher | None:
        coordinator = getattr(self, "external_action_coordinator", None)
        if coordinator is not None:
            return coordinator.dispatcher
        return self._external_action_dispatcher

    @external_action_dispatcher.setter
    def external_action_dispatcher(
        self,
        dispatcher: ExternalActionDispatcher | None,
    ) -> None:
        self._external_action_dispatcher = dispatcher
        coordinator = getattr(self, "external_action_coordinator", None)
        if coordinator is not None:
            coordinator.dispatcher = dispatcher

    def execute(
        self,
        *,
        runtime_input: dict[str, Any],
        state: dict[str, Any],
        context: RuntimeExecutionContext,
        finish_evaluator: FinishEvaluator,
    ) -> DynamicLoopResult:
        try:
            return self._execute_impl(
                runtime_input=runtime_input,
                state=state,
                context=context,
                finish_evaluator=finish_evaluator,
            )
        except Exception as exc:
            if isinstance(exc, ExternalActionReconciliationPendingError):
                raise
            if (
                isinstance(exc, RuntimeExecutionError)
                and exc.code
                in {
                    "external_action_outcome_unknown",
                    "external_action_evidence_incomplete",
                }
            ):
                raise
            # This is the final safety net around every local persistence,
            # mirror, Planner, and recovery boundary. Once any provider
            # dispatch was durably claimed, an unrelated exception must not
            # escape as generic FAILED and make the run unrecoverable while
            # leaving a possibly-applied action in DISPATCHING.
            self.external_action_coordinator.reconcile_dispatched_action(
                context=context,
                include_terminal=False,
            )
            raise

    def _execute_impl(
        self,
        *,
        runtime_input: dict[str, Any],
        state: dict[str, Any],
        context: RuntimeExecutionContext,
        finish_evaluator: FinishEvaluator,
    ) -> DynamicLoopResult:
        input_hash = self._stable_hash(
            {
                "runtime_input": runtime_input,
                "state": {
                    key: value
                    for key, value in state.items()
                    if key != "execution_trace"
                },
                "workflow_type": self.workflow_type,
                "max_tool_calls": self.max_tool_calls,
            }
        )
        claim = self.workflow_store.create_or_get_execution(
            context.run_id,
            self.workflow_type,
            input_hash,
        )
        if claim.outcome in {
            ExecutionOutcome.INPUT_MISMATCH,
            ExecutionOutcome.WORKFLOW_TYPE_MISMATCH,
        }:
            self.external_action_coordinator.reconcile_dispatched_action(
                context=context,
            )
            raise RuntimeExecutionError(
                "invalid_planner_decision",
                "Persisted dynamic workflow identity does not match this execution.",
            )

        execution = claim.execution
        try:
            self._mirror_evidence(context.run_id)
        except Exception:
            # At the entry recovery boundary, a terminal action may only need
            # its public evidence repaired. Preserve its stronger durable
            # outcome instead of letting a mirror outage become generic FAILED.
            self.external_action_coordinator.reconcile_dispatched_action(
                context=context
            )
            raise
        if execution.status in {WorkflowStatus.READY, WorkflowStatus.BLOCKED}:
            if execution.result_json is None:
                raise RuntimeExecutionError(
                    "invalid_planner_decision",
                    "Terminal dynamic workflow is missing its result.",
                )
            try:
                return DynamicLoopResult.model_validate_json(execution.result_json)
            except ValidationError as exc:
                raise RuntimeExecutionError(
                    "invalid_planner_decision",
                    "Terminal dynamic workflow has an invalid result.",
                ) from exc
        if execution.status == WorkflowStatus.FAILED:
            code = execution.error_code or "tool_execution_failed"
            raise RuntimeExecutionError(code, "Dynamic workflow previously failed.")
        if execution.status == WorkflowStatus.PENDING:
            self.workflow_store.mark_running(context.run_id)

        observations = self._restore_observations(context)

        while True:
            decision_index = len(observations) + 1
            decision = self._decision_for_index(
                context=context,
                runtime_input=runtime_input,
                state=state,
                observations=observations,
                decision_index=decision_index,
            )

            if isinstance(decision, RequestClarificationDecision):
                result = DynamicLoopResult(
                    outcome=DynamicLoopOutcome.CLARIFICATION,
                    message=decision.question,
                    output={"question": decision.question},
                    observations=observations,
                )
                self._complete_loop(
                    context.run_id,
                    result,
                    evidence_id="loop:outcome",
                    blocked_code="clarification_required",
                )
                return result

            if isinstance(decision, FinishDecision):
                try:
                    evaluation = finish_evaluator(decision, observations)
                except (InvalidPlannerDecisionError, ValidationError, ValueError) as exc:
                    self._fail(
                        context.run_id,
                        "invalid_planner_decision",
                        f"FINISH payload was rejected: {type(exc).__name__}",
                    )
                if evaluation.outcome == DynamicLoopOutcome.CLARIFICATION:
                    self._fail(
                        context.run_id,
                        "invalid_planner_decision",
                        "A FINISH evaluator cannot return clarification.",
                    )
                result = DynamicLoopResult(
                    outcome=evaluation.outcome,
                    message=evaluation.message,
                    output=evaluation.output,
                    validation_errors=evaluation.validation_errors,
                    observations=observations,
                )
                self._complete_loop(
                    context.run_id,
                    result,
                    evidence_id="loop:outcome",
                    blocked_code=(
                        "domain_validation_failed"
                        if result.outcome == DynamicLoopOutcome.BLOCKED
                        else None
                    ),
                )
                return result

            assert isinstance(decision, CallToolDecision)
            step_id = f"call-{decision_index:04d}"
            normalized_arguments = self._authorize_call(
                context=context,
                runtime_input=runtime_input,
                decision=decision,
                step_id=step_id,
                tool_call_count=len(observations),
            )
            observation = self._execute_call(
                context=context,
                decision=decision,
                step_id=step_id,
                normalized_arguments=normalized_arguments,
            )
            observations.append(observation)

    def _decision_for_index(
        self,
        *,
        context: RuntimeExecutionContext,
        runtime_input: dict[str, Any],
        state: dict[str, Any],
        observations: list[ToolObservation],
        decision_index: int,
    ) -> Any:
        evidence_id = f"planner:{decision_index}"
        persisted = self._workflow_evidence(context.run_id, "planner.decision", evidence_id)
        if persisted is not None:
            if persisted.get("outcome") == "rejected":
                self._ensure_run_evidence(context.run_id, "planner.decision", persisted)
                self._fail(
                    context.run_id,
                    str(persisted.get("error_code") or "invalid_planner_decision"),
                    "Persisted planner attempt was rejected.",
                )
            try:
                decision = PLANNER_DECISION_ADAPTER.validate_python(
                    persisted["decision"]
                )
            except (KeyError, ValidationError) as exc:
                self._fail(
                    context.run_id,
                    "invalid_planner_decision",
                    f"Persisted planner decision is invalid: {type(exc).__name__}",
                )
            self._ensure_run_evidence(context.run_id, "planner.decision", persisted)
            return decision

        planner_context = PlannerContext(
            run_id=context.run_id,
            thread_id=context.thread_id,
            runtime_input=runtime_input,
            state=state,
            tools=self.tool_registry.list_tools(),
            observations=observations,
            tool_call_count=len(observations),
            max_tool_calls=self.max_tool_calls,
        )
        try:
            raw_decision = self.planner.decide(planner_context)
        except InvalidPlannerDecisionError as exc:
            self._record_planner_failure(
                context.run_id,
                evidence_id,
                decision_index,
                "invalid_planner_decision",
            )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                f"Planner returned an invalid decision: {exc}",
            )
        except PlannerProviderError as exc:
            self._record_planner_failure(
                context.run_id,
                evidence_id,
                decision_index,
                "planner_provider_failed",
            )
            self._fail(
                context.run_id,
                "planner_provider_failed",
                f"Planner provider failed: {exc}",
            )
        except Exception as exc:
            self._record_planner_failure(
                context.run_id,
                evidence_id,
                decision_index,
                "planner_provider_failed",
            )
            self._fail(
                context.run_id,
                "planner_provider_failed",
                f"Planner provider failed: {type(exc).__name__}",
            )

        try:
            decision = PLANNER_DECISION_ADAPTER.validate_python(raw_decision)
        except ValidationError as exc:
            self._record_planner_failure(
                context.run_id,
                evidence_id,
                decision_index,
                "invalid_planner_decision",
            )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                f"Planner decision failed schema validation: {exc.title}",
            )

        payload: dict[str, Any] = {
            "evidence_id": evidence_id,
            "decision_index": decision_index,
            "outcome": "accepted",
            "decision": decision.model_dump(mode="json"),
        }
        if isinstance(decision, CallToolDecision):
            payload["step_id"] = f"call-{decision_index:04d}"
        self._record_evidence(context.run_id, "planner.decision", payload)
        return decision

    def _authorize_call(
        self,
        *,
        context: RuntimeExecutionContext,
        runtime_input: dict[str, Any],
        decision: CallToolDecision,
        step_id: str,
        tool_call_count: int,
    ) -> dict[str, Any]:
        evidence_id = f"policy:{step_id}"
        dispatched_action = self.external_action_coordinator.dispatched_action(
            context.run_id,
            step_id,
        )
        if tool_call_count >= self.max_tool_calls:
            self._record_policy_denial(
                context.run_id,
                evidence_id,
                step_id,
                decision.tool_name,
                "step_limit_exceeded",
            )
            self._fail(
                context.run_id,
                "step_limit_exceeded",
                f"Tool-call limit {self.max_tool_calls} was exceeded.",
            )

        spec = self.tool_registry.resolve(decision.tool_name)
        if spec is None:
            if dispatched_action is not None:
                self.external_action_coordinator.reconcile_dispatched_action(
                    context=context,
                    action=dispatched_action,
                )
            self._record_policy_denial(
                context.run_id,
                evidence_id,
                step_id,
                decision.tool_name,
                "unknown_tool",
            )
            self._fail(
                context.run_id,
                "unknown_tool",
                "Planner selected a tool outside the runtime allowlist.",
            )

        if not context.authority.allows(self.TOOL_EXECUTE_PERMISSION):
            if dispatched_action is not None:
                self.external_action_coordinator.reconcile_dispatched_action(
                    context=context,
                    action=dispatched_action,
                )
            self._record_policy_denial(
                context.run_id,
                evidence_id,
                step_id,
                decision.tool_name,
                "tool_permission_denied",
            )
            self._fail(
                context.run_id,
                "tool_permission_denied",
                "Execution authority does not allow tool execution.",
            )

        if spec.effect != ToolEffect.EXTERNAL_WRITE and dispatched_action is not None:
            self.external_action_coordinator.reconcile_dispatched_action(
                context=context,
                action=dispatched_action,
            )

        if spec.effect == ToolEffect.EXTERNAL_WRITE:
            if not context.authority.allows(
                self.EXTERNAL_ACTION_EXECUTE_PERMISSION
            ):
                if dispatched_action is not None:
                    self.external_action_coordinator.reconcile_dispatched_action(
                        context=context,
                        action=dispatched_action,
                    )
                self._record_policy_denial(
                    context.run_id,
                    evidence_id,
                    step_id,
                    decision.tool_name,
                    "external_action_permission_denied",
                )
                self._fail(
                    context.run_id,
                    "external_action_permission_denied",
                    "Execution authority does not allow external actions.",
                )

            try:
                action_was_requested = (
                    spec.runtime_input_gate is None
                    or spec.runtime_input_gate(runtime_input)
                )
            except Exception:
                action_was_requested = False
            if not action_was_requested:
                if dispatched_action is not None:
                    self.external_action_coordinator.reconcile_dispatched_action(
                        context=context,
                        action=dispatched_action,
                    )
                self._record_policy_denial(
                    context.run_id,
                    evidence_id,
                    step_id,
                    decision.tool_name,
                    "external_action_not_requested",
                )
                self._fail(
                    context.run_id,
                    "external_action_not_requested",
                    "External action requires an explicit runtime-input request.",
                )

            provider = self.external_action_coordinator.provider_for(spec)
            if provider is None:
                if dispatched_action is not None:
                    self.external_action_coordinator.reconcile_dispatched_action(
                        context=context,
                        action=dispatched_action,
                    )
                self._record_policy_denial(
                    context.run_id,
                    evidence_id,
                    step_id,
                    decision.tool_name,
                    "external_action_not_configured",
                )
                self._fail(
                    context.run_id,
                    "external_action_not_configured",
                    "External action provider is not configured.",
                )
            if (
                spec.retry_mode == ToolRetryMode.PROVIDER_IDEMPOTENT
                and not provider.supports_idempotency
            ):
                if dispatched_action is not None:
                    self.external_action_coordinator.reconcile_dispatched_action(
                        context=context,
                        action=dispatched_action,
                    )
                self._record_policy_denial(
                    context.run_id,
                    evidence_id,
                    step_id,
                    decision.tool_name,
                    "external_action_idempotency_unsupported",
                )
                self._fail(
                    context.run_id,
                    "external_action_idempotency_unsupported",
                    "External action provider does not support idempotency.",
                )

        try:
            validated = spec.input_model.model_validate(decision.arguments)
        except ValidationError:
            if dispatched_action is not None:
                self.external_action_coordinator.reconcile_dispatched_action(
                    context=context,
                    action=dispatched_action,
                )
            self._record_policy_denial(
                context.run_id,
                evidence_id,
                step_id,
                decision.tool_name,
                "invalid_tool_arguments",
            )
            self._fail(
                context.run_id,
                "invalid_tool_arguments",
                "Planner supplied arguments that do not match the tool schema.",
            )

        normalized = validated.model_dump(mode="json")
        allowed_payload: dict[str, Any] = {
            "evidence_id": evidence_id,
            "step_id": step_id,
            "tool_name": decision.tool_name,
            "outcome": "allowed",
            "error_code": None,
        }
        if spec.effect == ToolEffect.EXTERNAL_WRITE:
            allowed_payload.update(
                {
                    "effect": spec.effect.value,
                    "retry_mode": spec.retry_mode.value,
                    "provider_name": spec.provider_name,
                }
            )
        self._record_evidence(
            context.run_id,
            "policy.decision",
            allowed_payload,
        )
        return normalized

    def _execute_call(
        self,
        *,
        context: RuntimeExecutionContext,
        decision: CallToolDecision,
        step_id: str,
        normalized_arguments: dict[str, Any],
    ) -> ToolObservation:
        spec = self.tool_registry.resolve(decision.tool_name)
        if spec is None:
            self._fail(
                context.run_id,
                "unknown_tool",
                "Authorized tool disappeared from the runtime allowlist.",
            )
        if spec.effect == ToolEffect.EXTERNAL_WRITE:
            return self.external_action_coordinator.execute(
                context=context,
                tool_name=decision.tool_name,
                spec=spec,
                step_id=step_id,
                normalized_arguments=normalized_arguments,
            )
        return self._execute_read_only_call(
            context=context,
            decision=decision,
            step_id=step_id,
            normalized_arguments=normalized_arguments,
        )

    def _execute_read_only_call(
        self,
        *,
        context: RuntimeExecutionContext,
        decision: CallToolDecision,
        step_id: str,
        normalized_arguments: dict[str, Any],
    ) -> ToolObservation:
        input_hash = self._stable_hash(normalized_arguments)
        existing = self.workflow_store.get_step(context.run_id, step_id)
        if existing is not None and existing.status == ToolCallStatus.FAILED:
            if existing.error_code != "interrupted":
                code = existing.error_code or "tool_execution_failed"
                self._record_tool_failure(
                    context.run_id,
                    step_id,
                    decision.tool_name,
                    code,
                )
                self._fail(context.run_id, code, "Tool call previously failed.")
        if existing is not None and existing.status == ToolCallStatus.RUNNING:
            if not context.recovered_after_restart:
                self._fail(
                    context.run_id,
                    "tool_execution_failed",
                    "Tool call is already running without a restart recovery boundary.",
                )
            self.workflow_store.recover_interrupted_step(context.run_id, step_id)

        claim = self.workflow_store.claim_step(
            context.run_id,
            step_id,
            decision.tool_name,
            input_hash,
            max_attempts=2,
        )
        if claim.outcome == ClaimOutcome.CACHED:
            assert claim.step is not None and claim.step.result_json is not None
            result = self._decode_tool_result_or_fail(context.run_id, claim.step)
            self._record_tool_success(
                context.run_id,
                step_id,
                decision.tool_name,
                result,
            )
            return ToolObservation(
                step_id=step_id,
                tool_name=decision.tool_name,
                arguments=normalized_arguments,
                result=result,
                cached=True,
            )
        if claim.outcome in {ClaimOutcome.INPUT_MISMATCH, ClaimOutcome.DEFINITION_MISMATCH}:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Persisted tool-call identity does not match the planner decision.",
            )
        if claim.outcome in {
            ClaimOutcome.ALREADY_RUNNING,
            ClaimOutcome.ATTEMPTS_EXHAUSTED,
        }:
            self._fail(
                context.run_id,
                "tool_execution_failed",
                f"Tool call could not be claimed: {claim.outcome.value}.",
            )
        assert claim.outcome == ClaimOutcome.CLAIMED
        assert claim.attempt_token is not None

        try:
            execution = self.tool_sandbox.execute(
                decision.tool_name,
                normalized_arguments,
            )
        except Exception as exc:
            code = "tool_execution_failed"
            self.workflow_store.fail_step(
                context.run_id,
                step_id,
                claim.attempt_token,
                code,
            )
            self._record_tool_failure(
                context.run_id,
                step_id,
                decision.tool_name,
                code,
            )
            self._fail(
                context.run_id,
                code,
                f"Tool executor raised {type(exc).__name__}.",
            )

        if execution.status != ToolExecutionStatus.COMPLETED or execution.result is None:
            code = self._tool_error_code(execution.status)
            self.workflow_store.fail_step(
                context.run_id,
                step_id,
                claim.attempt_token,
                code,
            )
            self._record_tool_failure(
                context.run_id,
                step_id,
                decision.tool_name,
                code,
            )
            self._fail(context.run_id, code, "Tool execution did not complete successfully.")

        result = execution.result
        self.workflow_store.complete_step(
            context.run_id,
            step_id,
            claim.attempt_token,
            json.dumps(result, sort_keys=True, separators=(",", ":")),
        )
        self._record_tool_success(
            context.run_id,
            step_id,
            decision.tool_name,
            result,
        )
        return ToolObservation(
            step_id=step_id,
            tool_name=decision.tool_name,
            arguments=normalized_arguments,
            result=result,
        )

    def _restore_observations(
        self,
        context: RuntimeExecutionContext,
    ) -> list[ToolObservation]:
        observations: list[ToolObservation] = []
        for step in sorted(self.workflow_store.list_steps(context.run_id), key=lambda item: item.step_id):
            action = self.workflow_store.get_external_action(
                context.run_id,
                step.step_id,
            )
            if step.status == ToolCallStatus.COMPLETED:
                decision_payload = self._decision_payload_for_step(
                    context,
                    step.step_id,
                    action=action,
                )
                try:
                    decision = PLANNER_DECISION_ADAPTER.validate_python(
                        decision_payload["decision"]
                    )
                except (KeyError, ValidationError) as exc:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail=f"Persisted planner decision is invalid: {type(exc).__name__}",
                    )
                if not isinstance(decision, CallToolDecision):
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Completed tool step is not backed by a CALL_TOOL decision.",
                    )
                spec = self.tool_registry.resolve(decision.tool_name)
                if spec is None:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="unknown_tool",
                        detail="A completed tool is no longer registered.",
                    )
                try:
                    normalized = spec.input_model.model_validate(
                        decision.arguments
                    ).model_dump(mode="json")
                except ValidationError:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_tool_arguments",
                        detail="Persisted tool arguments no longer pass schema validation.",
                    )
                if step.tool_name != decision.tool_name or step.input_hash != self._stable_hash(normalized):
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Completed tool evidence does not match its planner decision.",
                    )
                if spec.effect == ToolEffect.EXTERNAL_WRITE:
                    observations.append(
                        self.external_action_coordinator.restore_success(
                            context=context,
                            step=step,
                            spec=spec,
                            normalized_arguments=normalized,
                            input_hash=step.input_hash,
                            idempotency_key=self.external_action_coordinator.idempotency_key(
                                context=context,
                                step_id=step.step_id,
                                tool_name=step.tool_name,
                                input_hash=step.input_hash,
                            ),
                        )
                    )
                    continue
                if action is not None:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Read-only completed step unexpectedly has an external action.",
                    )
                result = self._decode_tool_result_or_fail(context.run_id, step)
                self._record_tool_success(
                    context.run_id,
                    step.step_id,
                    step.tool_name,
                    result,
                )
                observations.append(
                    ToolObservation(
                        step_id=step.step_id,
                        tool_name=step.tool_name,
                        arguments=normalized,
                        result=result,
                        cached=True,
                    )
                )
                continue
            if step.status == ToolCallStatus.FAILED and step.error_code != "interrupted":
                decision_payload = self._decision_payload_for_step(
                    context,
                    step.step_id,
                    action=action,
                )
                try:
                    decision = PLANNER_DECISION_ADAPTER.validate_python(
                        decision_payload["decision"]
                    )
                except (KeyError, ValidationError) as exc:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail=f"Persisted planner decision is invalid: {type(exc).__name__}",
                    )
                if not isinstance(decision, CallToolDecision):
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Failed tool step is not backed by a CALL_TOOL decision.",
                    )
                spec = self.tool_registry.resolve(decision.tool_name)
                if spec is None:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="unknown_tool",
                        detail="A failed tool is no longer registered.",
                    )
                try:
                    normalized = spec.input_model.model_validate(
                        decision.arguments
                    ).model_dump(mode="json")
                except ValidationError:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_tool_arguments",
                        detail="Persisted tool arguments no longer pass schema validation.",
                    )
                input_hash = self._stable_hash(normalized)
                if step.tool_name != decision.tool_name or step.input_hash != input_hash:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Failed tool evidence does not match its planner decision.",
                    )
                if spec.effect == ToolEffect.EXTERNAL_WRITE:
                    self.external_action_coordinator.restore_failure(
                        context=context,
                        step=step,
                        spec=spec,
                        normalized_arguments=normalized,
                        input_hash=input_hash,
                        idempotency_key=self.external_action_coordinator.idempotency_key(
                            context=context,
                            step_id=step.step_id,
                            tool_name=step.tool_name,
                            input_hash=input_hash,
                        ),
                    )
                if action is not None:
                    self._fail_restored_step(
                        context=context,
                        action=action,
                        code="invalid_planner_decision",
                        detail="Read-only failed step unexpectedly has an external action.",
                    )
                code = step.error_code or "tool_execution_failed"
                self._record_tool_failure(
                    context.run_id,
                    step.step_id,
                    step.tool_name,
                    code,
                )
                self._fail(context.run_id, code, "Persisted tool call failed.")
        return observations

    def _complete_loop(
        self,
        run_id: str,
        result: DynamicLoopResult,
        *,
        evidence_id: str,
        blocked_code: str | None,
    ) -> None:
        self._record_evidence(
            run_id,
            "loop.outcome",
            {
                "evidence_id": evidence_id,
                "outcome": result.outcome.value,
                "message": result.message,
                "validation_errors": result.validation_errors,
            },
        )
        serialized = result.model_dump_json()
        if blocked_code is None:
            self.workflow_store.finalize_ready(run_id, serialized)
        else:
            self.workflow_store.finalize_blocked(run_id, serialized, blocked_code)

    def _fail(self, run_id: str, code: str, _unsafe_detail: str) -> NoReturn:
        # Failure evidence is user-visible and must be identical when a crash
        # occurs after appending the outcome but before finalizing the workflow.
        # Keep provider/model details out of the ledger and treat an existing
        # failed outcome as the authority during recovery.
        message = self._FAILURE_MESSAGES.get(code, "Dynamic tool loop failed.")
        existing = self._workflow_evidence(run_id, "loop.outcome", "loop:outcome")
        if existing is None:
            self._record_evidence(
                run_id,
                "loop.outcome",
                {
                    "evidence_id": "loop:outcome",
                    "outcome": "failed",
                    "error_code": code,
                    "message": message,
                },
            )
        else:
            if existing.get("outcome") != "failed" or existing.get("error_code") != code:
                raise RuntimeExecutionError(
                    "invalid_planner_decision",
                    "Durable loop failure evidence does not match recovery state.",
                )
            persisted_message = existing.get("message")
            if not isinstance(persisted_message, str) or not persisted_message:
                raise RuntimeExecutionError(
                    "invalid_planner_decision",
                    "Durable loop failure evidence has an invalid message.",
                )
            message = persisted_message
            self._ensure_run_evidence(run_id, "loop.outcome", existing)
        execution = self.workflow_store.get_execution(run_id)
        if execution is not None and execution.status == WorkflowStatus.RUNNING:
            self.workflow_store.finalize_failed(run_id, code)
        raise RuntimeExecutionError(code, message)

    def _record_policy_denial(
        self,
        run_id: str,
        evidence_id: str,
        step_id: str,
        tool_name: str,
        error_code: str,
    ) -> None:
        self._record_evidence(
            run_id,
            "policy.decision",
            {
                "evidence_id": evidence_id,
                "step_id": step_id,
                "tool_name": tool_name,
                "outcome": "denied",
                "error_code": error_code,
            },
        )

    def _record_planner_failure(
        self,
        run_id: str,
        evidence_id: str,
        decision_index: int,
        error_code: str,
    ) -> None:
        self._record_evidence(
            run_id,
            "planner.decision",
            {
                "evidence_id": evidence_id,
                "decision_index": decision_index,
                "outcome": "rejected",
                "error_code": error_code,
            },
        )

    def _record_tool_success(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        result: dict[str, Any],
    ) -> None:
        self.evidence_projector.record_tool_success(run_id, step_id, tool_name, result)

    def _record_tool_failure(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        error_code: str,
    ) -> None:
        self.evidence_projector.record_tool_failure(run_id, step_id, tool_name, error_code)

    def _record_evidence(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        self.evidence_projector.record(run_id, event_type, payload)

    def _mirror_evidence(self, run_id: str) -> None:
        self.evidence_projector.mirror(run_id)

    def _ensure_run_evidence(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        self.evidence_projector.ensure_run_evidence(run_id, event_type, payload)

    def _workflow_evidence(
        self,
        run_id: str,
        event_type: str,
        evidence_id: str,
    ) -> dict[str, Any] | None:
        return self.evidence_projector.workflow_evidence(
            run_id,
            event_type,
            evidence_id,
        )

    def _fail_restored_step(
        self,
        *,
        context: RuntimeExecutionContext,
        action: ExternalActionRecord | None,
        code: str,
        detail: str,
    ) -> NoReturn:
        self.external_action_coordinator.fail_restored_step(
            context=context,
            action=action,
            code=code,
            detail=detail,
        )

    def _decision_payload_for_step(
        self,
        context: RuntimeExecutionContext,
        step_id: str,
        *,
        action: ExternalActionRecord | None,
    ) -> dict[str, Any]:
        for event in self.workflow_store.list_events(context.run_id):
            if event.event_type == "planner.decision" and event.payload.get("step_id") == step_id:
                return event.payload
        self._fail_restored_step(
            context=context,
            action=action,
            code="invalid_planner_decision",
            detail=f"Tool step {step_id} has no durable planner decision.",
        )

    _decode_tool_result = staticmethod(decode_tool_result)

    def _decode_tool_result_or_fail(
        self,
        run_id: str,
        step: ToolCallRecord,
    ) -> dict[str, Any]:
        try:
            return self._decode_tool_result(step)
        except RuntimeExecutionError as exc:
            self._fail(run_id, exc.code, str(exc))

    _stable_hash = staticmethod(stable_hash)

    @staticmethod
    def _canonical_json(payload: dict[str, Any]) -> str:
        """Backward-compatible access to external-action canonical JSON."""

        return ExternalActionCoordinator.canonical_json(payload)

    @staticmethod
    def _tool_error_code(status: ToolExecutionStatus) -> str:
        return {
            ToolExecutionStatus.DENIED: "unknown_tool",
            ToolExecutionStatus.INVALID_INPUT: "invalid_tool_arguments",
            ToolExecutionStatus.TIMED_OUT: "tool_timed_out",
            ToolExecutionStatus.FAILED: "tool_execution_failed",
        }.get(status, "tool_execution_failed")
