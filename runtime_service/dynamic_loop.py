from __future__ import annotations

import json
from collections.abc import Callable
from enum import Enum
from hashlib import sha256
from typing import Any, NoReturn, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from agent.contracts import RuntimeExecutionContext, RuntimeExecutionError

from .external_actions import (
    DefinitiveExternalActionError,
    ExternalActionDispatcher,
    ExternalActionProviderResult,
    ExternalActionRequest,
)
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
    ToolSpec,
)
from .workflow_store import (
    ClaimOutcome,
    ExecutionOutcome,
    ExternalActionDispatchOutcome,
    ExternalActionPrepareOutcome,
    ExternalActionRecord,
    ExternalActionStatus,
    SQLiteWorkflowStore,
    ToolCallRecord,
    ToolCallStatus,
    WorkflowStatus,
)


class RunEventSink(Protocol):
    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        ...

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[Any]:
        ...


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
        workflow_store: SQLiteWorkflowStore,
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
        self.workflow_type = workflow_type
        self.max_tool_calls = max_tool_calls
        self.external_action_dispatcher = external_action_dispatcher

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
            self._fail_dispatched_action_reconciliation(
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
            self._fail_dispatched_action_reconciliation(
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
            self._fail_dispatched_action_reconciliation(context=context)
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
        dispatched_action = self._dispatched_action(context.run_id, step_id)
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
                self._fail_dispatched_action_reconciliation(
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
                self._fail_dispatched_action_reconciliation(
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
            self._fail_dispatched_action_reconciliation(
                context=context,
                action=dispatched_action,
            )

        if spec.effect == ToolEffect.EXTERNAL_WRITE:
            if not context.authority.allows(
                self.EXTERNAL_ACTION_EXECUTE_PERMISSION
            ):
                if dispatched_action is not None:
                    self._fail_dispatched_action_reconciliation(
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
                    self._fail_dispatched_action_reconciliation(
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

            provider = None
            if (
                self.external_action_dispatcher is not None
                and spec.provider_name is not None
            ):
                provider = self.external_action_dispatcher.registry.resolve(
                    spec.provider_name
                )
            if provider is None:
                if dispatched_action is not None:
                    self._fail_dispatched_action_reconciliation(
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
                    self._fail_dispatched_action_reconciliation(
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
                self._fail_dispatched_action_reconciliation(
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

    def _dispatched_action(
        self,
        run_id: str,
        step_id: str,
    ) -> ExternalActionRecord | None:
        action = self.workflow_store.get_external_action(run_id, step_id)
        if action is None or action.dispatch_count < 1:
            return None
        return action

    def _fail_dispatched_action_reconciliation(
        self,
        *,
        context: RuntimeExecutionContext,
        action: ExternalActionRecord | None = None,
        include_terminal: bool = True,
    ) -> None:
        """Fail safely when preflight/identity drift blocks ledger recovery.

        Once dispatch_count is non-zero, current registry, permission, provider,
        input-schema, or thread-state drift must never downgrade the run to an
        ordinary validation/configuration failure. The provider may already
        have applied the write.
        """

        if action is None:
            actions = [
                candidate
                for candidate in self.workflow_store.list_external_actions(
                    context.run_id
                )
                if candidate.dispatch_count > 0
                and (include_terminal or not candidate.status.is_terminal)
            ]
            if not actions:
                return
            priority = {
                ExternalActionStatus.DISPATCHING: 0,
                ExternalActionStatus.OUTCOME_UNKNOWN: 1,
                ExternalActionStatus.SUCCEEDED: 2,
                ExternalActionStatus.FAILED: 3,
                ExternalActionStatus.PREPARED: 4,
            }
            action = sorted(
                actions,
                key=lambda candidate: priority[candidate.status],
            )[0]
        elif action.status.is_terminal and not include_terminal:
            return

        try:
            self._mirror_evidence(context.run_id)
        except Exception:
            pass
        if action.status == ExternalActionStatus.DISPATCHING:
            step = self.workflow_store.get_step(context.run_id, action.step_id)
            if step is not None:
                self._fail_external_dispatch_binding(
                    context=context,
                    step=step,
                    action=action,
                )
            code = "external_action_outcome_unknown"
        elif action.status == ExternalActionStatus.OUTCOME_UNKNOWN:
            code = "external_action_outcome_unknown"
        elif action.status == ExternalActionStatus.SUCCEEDED:
            code = "external_action_evidence_incomplete"
        elif action.status == ExternalActionStatus.FAILED:
            code = "external_action_failed"
        else:
            # PREPARED with dispatch_count > 0 is corrupt and cannot prove that
            # a provider call did not happen.
            code = "external_action_outcome_unknown"

        try:
            self._fail(
                context.run_id,
                code,
                "Dispatched external action could not be reconciled.",
            )
        except RuntimeExecutionError as exc:
            if exc.code == code:
                raise
        except Exception:
            pass
        raise RuntimeExecutionError(code, self._FAILURE_MESSAGES[code])

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
            return self._execute_external_action(
                context=context,
                decision=decision,
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

    def _execute_external_action(
        self,
        *,
        context: RuntimeExecutionContext,
        decision: CallToolDecision,
        spec: ToolSpec,
        step_id: str,
        normalized_arguments: dict[str, Any],
    ) -> ToolObservation:
        provider_name = spec.provider_name
        if provider_name is None or self.external_action_dispatcher is None:
            # Authorization performs this check before a step is claimed. Keep
            # the execution boundary fail-closed if a caller invokes it
            # independently or mutates server configuration between phases.
            self._fail(
                context.run_id,
                "external_action_not_configured",
                "External action provider is not configured.",
            )
        provider = self.external_action_dispatcher.registry.resolve(provider_name)
        if provider is None:
            self._fail(
                context.run_id,
                "external_action_not_configured",
                "External action provider is not configured.",
            )
        provider_identity = provider.provider_identity

        input_hash = self._stable_hash(normalized_arguments)
        arguments_json = self._canonical_json(normalized_arguments)
        idempotency_key = self._external_action_idempotency_key(
            context=context,
            step_id=step_id,
            tool_name=decision.tool_name,
            input_hash=input_hash,
        )
        existing = self.workflow_store.get_step(context.run_id, step_id)
        if existing is not None and existing.status == ToolCallStatus.COMPLETED:
            return self._restore_external_success(
                context=context,
                step=existing,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        if existing is not None and existing.status == ToolCallStatus.FAILED:
            self._restore_external_failure(
                context=context,
                step=existing,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        if existing is not None and existing.status == ToolCallStatus.RUNNING:
            if not context.recovered_after_restart:
                self._fail(
                    context.run_id,
                    "tool_execution_failed",
                    "External action is already running without a recovery boundary.",
                )
            if existing.attempt_token is None:
                self._fail(
                    context.run_id,
                    "invalid_planner_decision",
                    "Running external action step has no attempt token.",
                )
            step = existing
            attempt_token = existing.attempt_token
            recovered_dispatch = True
        else:
            claim = self.workflow_store.claim_step(
                context.run_id,
                step_id,
                decision.tool_name,
                input_hash,
                max_attempts=1,
            )
            if claim.outcome == ClaimOutcome.CACHED:
                assert claim.step is not None
                return self._restore_external_success(
                    context=context,
                    step=claim.step,
                    spec=spec,
                    normalized_arguments=normalized_arguments,
                    input_hash=input_hash,
                    idempotency_key=idempotency_key,
                )
            if claim.outcome in {
                ClaimOutcome.INPUT_MISMATCH,
                ClaimOutcome.DEFINITION_MISMATCH,
            }:
                self._fail(
                    context.run_id,
                    "invalid_planner_decision",
                    "Persisted external action identity does not match the decision.",
                )
            if claim.outcome in {
                ClaimOutcome.ALREADY_RUNNING,
                ClaimOutcome.ATTEMPTS_EXHAUSTED,
            }:
                self._fail(
                    context.run_id,
                    "tool_execution_failed",
                    f"External action step could not be claimed: {claim.outcome.value}.",
                )
            assert claim.outcome == ClaimOutcome.CLAIMED
            assert claim.step is not None and claim.attempt_token is not None
            step = claim.step
            attempt_token = claim.attempt_token
            recovered_dispatch = False

        try:
            prepared = self.workflow_store.prepare_external_action(
                run_id=context.run_id,
                step_id=step_id,
                tool_attempt_token=attempt_token,
                tenant_id=context.authority.tenant_id,
                subject_id=context.authority.subject_id,
                workflow_type=self.workflow_type,
                tool_name=decision.tool_name,
                provider_name=provider_name,
                provider_identity=provider_identity,
                input_hash=input_hash,
                arguments_json=arguments_json,
                retry_mode=spec.retry_mode.value,
                idempotency_key=idempotency_key,
            )
        except Exception:
            if recovered_dispatch:
                self._fail(
                    context.run_id,
                    "external_action_outcome_unknown",
                    "Recovered external action state could not be validated.",
                )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "External action preparation failed before dispatch.",
            )
        self._mirror_evidence(context.run_id)
        if prepared.outcome in {
            ExternalActionPrepareOutcome.IDENTITY_MISMATCH,
            ExternalActionPrepareOutcome.TOOL_ATTEMPT_MISMATCH,
        }:
            mismatched_action = prepared.action
            if mismatched_action is None:
                try:
                    mismatched_action = self.workflow_store.get_external_action(
                        context.run_id,
                        step_id,
                    )
                except Exception:
                    mismatched_action = None
            if (
                mismatched_action is not None
                and mismatched_action.status == ExternalActionStatus.DISPATCHING
            ):
                self._fail_external_dispatch_binding(
                    context=context,
                    step=step,
                    action=mismatched_action,
                )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Persisted external action binding does not match this execution.",
            )
        assert prepared.action is not None
        action = prepared.action
        self._validate_external_action_binding(
            context=context,
            step=step,
            action=action,
            spec=spec,
            provider_identity=provider_identity,
            normalized_arguments=normalized_arguments,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
        )
        try:
            self._external_action_request(action)
        except (json.JSONDecodeError, RuntimeExecutionError, ValidationError):
            if action.status == ExternalActionStatus.DISPATCHING:
                self._fail_external_dispatch_binding(
                    context=context,
                    step=step,
                    action=action,
                )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Prepared external action cannot form a provider request.",
            )

        if action.status == ExternalActionStatus.PREPARED:
            dispatch = self.workflow_store.begin_external_action_dispatch(
                context.run_id,
                step_id,
                tool_attempt_token=attempt_token,
            )
            self._mirror_evidence(context.run_id)
            if dispatch.outcome == ExternalActionDispatchOutcome.RUN_CANCELLED:
                raise RuntimeExecutionError(
                    "run_cancel_requested",
                    self._FAILURE_MESSAGES["run_cancel_requested"],
                )
            if dispatch.outcome == ExternalActionDispatchOutcome.TERMINAL:
                return self._handle_external_terminal(
                    context=context,
                    step=step,
                    action=dispatch.action,
                    spec=spec,
                    normalized_arguments=normalized_arguments,
                    input_hash=input_hash,
                    idempotency_key=idempotency_key,
                )
            if dispatch.outcome != ExternalActionDispatchOutcome.CLAIMED:
                self._fail(
                    context.run_id,
                    "tool_execution_failed",
                    f"External action dispatch could not be claimed: "
                    f"{dispatch.outcome.value}.",
                )
            action = dispatch.action
            dispatch_token = dispatch.dispatch_token
        elif action.status == ExternalActionStatus.DISPATCHING:
            if not recovered_dispatch:
                self._fail(
                    context.run_id,
                    "tool_execution_failed",
                    "External action dispatch is already in progress.",
                )
            return self._recover_external_dispatch(
                context=context,
                step=step,
                action=action,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
                attempt_token=attempt_token,
            )
        else:
            return self._handle_external_terminal(
                context=context,
                step=step,
                action=action,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )

        if dispatch_token is None:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Claimed external action dispatch has no token.",
            )
        return self._dispatch_external_action(
            context=context,
            step=step,
            action=action,
            spec=spec,
            normalized_arguments=normalized_arguments,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            attempt_token=attempt_token,
            dispatch_token=dispatch_token,
        )

    def _recover_external_dispatch(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
        spec: ToolSpec,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
        attempt_token: str,
    ) -> ToolObservation:
        dispatch_token = action.dispatch_token
        if dispatch_token is None:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Dispatching external action has no dispatch token.",
            )
        if spec.retry_mode == ToolRetryMode.UNSAFE:
            self._raise_external_outcome_unknown(
                context=context,
                step=step,
                finalizer=lambda: self.workflow_store.finalize_unsafe_interrupted_action(
                    context.run_id,
                    step.step_id,
                    dispatch_token=dispatch_token,
                    tool_attempt_token=attempt_token,
                ),
            )
        if action.dispatch_count >= self.MAX_EXTERNAL_ACTION_DISPATCHES:
            self._finalize_external_unknown(
                context=context,
                step=step,
                dispatch_token=dispatch_token,
                attempt_token=attempt_token,
            )

        retry = self.workflow_store.retry_external_action_dispatch(
            context.run_id,
            step.step_id,
            previous_dispatch_token=dispatch_token,
            tool_attempt_token=attempt_token,
        )
        self._mirror_evidence(context.run_id)
        if retry.outcome == ExternalActionDispatchOutcome.TERMINAL:
            return self._handle_external_terminal(
                context=context,
                step=step,
                action=retry.action,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        if (
            retry.outcome != ExternalActionDispatchOutcome.RETRY_CLAIMED
            or retry.dispatch_token is None
        ):
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                f"Persisted external action could not be safely recovered: "
                f"{retry.outcome.value}.",
            )
        return self._dispatch_external_action(
            context=context,
            step=step,
            action=retry.action,
            spec=spec,
            normalized_arguments=normalized_arguments,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            attempt_token=attempt_token,
            dispatch_token=retry.dispatch_token,
        )

    def _dispatch_external_action(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
        spec: ToolSpec,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
        attempt_token: str,
        dispatch_token: str,
    ) -> ToolObservation:
        assert spec.provider_name is not None
        assert self.external_action_dispatcher is not None

        while True:
            provider = self.external_action_dispatcher.registry.resolve(
                spec.provider_name
            )
            if (
                provider is None
                or provider.provider_identity != action.provider_identity
            ):
                # The dispatch claim already won its durable race. If routing
                # changes after preparation, do not send the key to a different
                # provider/account ledger; conservatively close as unknown.
                self._finalize_external_unknown(
                    context=context,
                    step=step,
                    dispatch_token=dispatch_token,
                    attempt_token=attempt_token,
                )
            request = self._external_action_request(action)
            try:
                provider_result: ExternalActionProviderResult = (
                    self.external_action_dispatcher.dispatch(
                        provider_name=spec.provider_name,
                        retry_mode=spec.retry_mode,
                        request=request,
                    )
                )
            except DefinitiveExternalActionError:
                try:
                    self.workflow_store.finalize_external_action_failed(
                        context.run_id,
                        step.step_id,
                        dispatch_token=dispatch_token,
                        tool_attempt_token=attempt_token,
                        error_code="external_action_failed",
                    )
                except Exception:
                    if not self._external_failure_was_committed(
                        context=context,
                        step=step,
                        expected_action=action,
                        dispatch_token=dispatch_token,
                    ):
                        self._raise_external_outcome_unknown(
                            context=context,
                            step=step,
                            finalizer=lambda: (
                                self.workflow_store.finalize_external_action_outcome_unknown(
                                    context.run_id,
                                    step.step_id,
                                    dispatch_token=dispatch_token,
                                    tool_attempt_token=attempt_token,
                                    error_code="external_action_outcome_unknown",
                                )
                            ),
                        )
                self._raise_external_terminal_failure(
                    context=context,
                    step=step,
                    error_code="external_action_failed",
                )
            except Exception:
                retry = self._retry_after_ambiguous_result(
                    context=context,
                    step=step,
                    action=action,
                    spec=spec,
                    dispatch_token=dispatch_token,
                    attempt_token=attempt_token,
                )
                if retry is None:
                    self._finalize_external_unknown(
                        context=context,
                        step=step,
                        dispatch_token=dispatch_token,
                        attempt_token=attempt_token,
                    )
                action, dispatch_token = retry
                continue

            trusted_result = {
                **provider_result.result,
                "provider_reference": provider_result.provider_reference,
            }
            assert spec.output_model is not None  # ToolRegistry invariant
            try:
                if self._contains_sensitive_text(
                    trusted_result,
                    idempotency_key,
                ):
                    raise ValueError(
                        "Provider output contains a runtime idempotency key"
                    )
                trusted_result = spec.output_model.model_validate(
                    trusted_result,
                    context={"arguments": normalized_arguments},
                ).model_dump(mode="json")
                if self._contains_sensitive_text(
                    trusted_result,
                    idempotency_key,
                ):
                    raise ValueError(
                        "Normalized provider output contains a runtime idempotency key"
                    )
                result_json = self._canonical_json(trusted_result)
            except (TypeError, ValueError, ValidationError):
                retry = self._retry_after_ambiguous_result(
                    context=context,
                    step=step,
                    action=action,
                    spec=spec,
                    dispatch_token=dispatch_token,
                    attempt_token=attempt_token,
                )
                if retry is None:
                    self._finalize_external_unknown(
                        context=context,
                        step=step,
                        dispatch_token=dispatch_token,
                        attempt_token=attempt_token,
                    )
                action, dispatch_token = retry
                continue

            try:
                self.workflow_store.finalize_external_action_succeeded(
                    context.run_id,
                    step.step_id,
                    dispatch_token=dispatch_token,
                    tool_attempt_token=attempt_token,
                    result_json=result_json,
                    provider_reference=provider_result.provider_reference,
                )
            except Exception:
                if not self._external_success_was_committed(
                    context=context,
                    step=step,
                    expected_action=action,
                    dispatch_token=dispatch_token,
                    result_json=result_json,
                    provider_reference=provider_result.provider_reference,
                ):
                    self._raise_external_outcome_unknown(
                        context=context,
                        step=step,
                        finalizer=lambda: (
                            self.workflow_store.finalize_external_action_outcome_unknown(
                                context.run_id,
                                step.step_id,
                                dispatch_token=dispatch_token,
                                tool_attempt_token=attempt_token,
                                error_code="external_action_outcome_unknown",
                            )
                        ),
                    )
            self._record_external_success_evidence(
                context=context,
                step=step,
                result=trusted_result,
            )
            return ToolObservation(
                step_id=step.step_id,
                tool_name=step.tool_name,
                arguments=normalized_arguments,
                result=trusted_result,
            )

    def _retry_after_ambiguous_result(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
        spec: ToolSpec,
        dispatch_token: str,
        attempt_token: str,
    ) -> tuple[ExternalActionRecord, str] | None:
        if (
            spec.retry_mode == ToolRetryMode.UNSAFE
            or action.dispatch_count >= self.MAX_EXTERNAL_ACTION_DISPATCHES
        ):
            return None
        retry = self.workflow_store.retry_external_action_dispatch(
            context.run_id,
            step.step_id,
            previous_dispatch_token=dispatch_token,
            tool_attempt_token=attempt_token,
        )
        self._mirror_evidence(context.run_id)
        if (
            retry.outcome == ExternalActionDispatchOutcome.RETRY_CLAIMED
            and retry.dispatch_token is not None
        ):
            return retry.action, retry.dispatch_token
        if retry.outcome == ExternalActionDispatchOutcome.TERMINAL:
            # A concurrent terminal write cannot be returned from this helper
            # without revalidating its tool/result binding. Treat it as a
            # durable integrity failure rather than dispatching again.
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "External action became terminal during an ambiguous retry.",
            )
        if retry.outcome == ExternalActionDispatchOutcome.RETRY_UNSAFE:
            return None
        self._fail(
            context.run_id,
            "invalid_planner_decision",
            f"External action retry state is invalid: {retry.outcome.value}.",
        )

    def _finalize_external_unknown(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        dispatch_token: str,
        attempt_token: str,
    ) -> NoReturn:
        self._raise_external_outcome_unknown(
            context=context,
            step=step,
            finalizer=lambda: self.workflow_store.finalize_external_action_outcome_unknown(
                context.run_id,
                step.step_id,
                dispatch_token=dispatch_token,
                tool_attempt_token=attempt_token,
                error_code="external_action_outcome_unknown",
            ),
        )

    def _external_success_was_committed(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        expected_action: ExternalActionRecord,
        dispatch_token: str,
        result_json: str,
        provider_reference: str,
    ) -> bool:
        """Resolve an exception thrown after the terminal transaction boundary.

        A wrapper, connection cleanup, or injected crash can raise after SQLite
        committed. Re-reading exact action/tool identity prevents downgrading a
        durable success to unknown while still refusing to trust a different
        terminal record.
        """

        try:
            action = self.workflow_store.get_external_action(
                context.run_id,
                step.step_id,
            )
            current_step = self.workflow_store.get_step(
                context.run_id,
                step.step_id,
            )
        except Exception:
            return False
        if action is None or current_step is None:
            return False
        identity = (
            action.action_id,
            action.run_id,
            action.step_id,
            action.tenant_id,
            action.subject_id,
            action.workflow_type,
            action.tool_name,
            action.provider_name,
            action.provider_identity,
            action.input_hash,
            action.arguments_json,
            action.retry_mode,
            action.idempotency_key,
        )
        expected_identity = (
            expected_action.action_id,
            expected_action.run_id,
            expected_action.step_id,
            expected_action.tenant_id,
            expected_action.subject_id,
            expected_action.workflow_type,
            expected_action.tool_name,
            expected_action.provider_name,
            expected_action.provider_identity,
            expected_action.input_hash,
            expected_action.arguments_json,
            expected_action.retry_mode,
            expected_action.idempotency_key,
        )
        return bool(
            identity == expected_identity
            and action.status == ExternalActionStatus.SUCCEEDED
            and action.dispatch_token == dispatch_token
            and action.provider_reference == provider_reference
            and action.result_json == result_json
            and action.error_code is None
            and current_step.status == ToolCallStatus.COMPLETED
            and current_step.attempt_token == step.attempt_token
            and current_step.result_json == result_json
            and current_step.error_code is None
        )

    def _external_failure_was_committed(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        expected_action: ExternalActionRecord,
        dispatch_token: str,
    ) -> bool:
        """Resolve an exception raised after a definitive terminal commit."""

        try:
            action = self.workflow_store.get_external_action(
                context.run_id,
                step.step_id,
            )
            current_step = self.workflow_store.get_step(
                context.run_id,
                step.step_id,
            )
        except Exception:
            return False
        if action is None or current_step is None:
            return False
        identity = (
            action.action_id,
            action.run_id,
            action.step_id,
            action.tenant_id,
            action.subject_id,
            action.workflow_type,
            action.tool_name,
            action.provider_name,
            action.provider_identity,
            action.input_hash,
            action.arguments_json,
            action.retry_mode,
            action.idempotency_key,
        )
        expected_identity = (
            expected_action.action_id,
            expected_action.run_id,
            expected_action.step_id,
            expected_action.tenant_id,
            expected_action.subject_id,
            expected_action.workflow_type,
            expected_action.tool_name,
            expected_action.provider_name,
            expected_action.provider_identity,
            expected_action.input_hash,
            expected_action.arguments_json,
            expected_action.retry_mode,
            expected_action.idempotency_key,
        )
        return bool(
            identity == expected_identity
            and action.status == ExternalActionStatus.FAILED
            and action.dispatch_token == dispatch_token
            and action.provider_reference is None
            and action.result_json is None
            and action.error_code == "external_action_failed"
            and current_step.status == ToolCallStatus.FAILED
            and current_step.attempt_token == step.attempt_token
            and current_step.result_json is None
            and current_step.error_code == "external_action_failed"
        )

    def _record_external_success_evidence(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        result: dict[str, Any],
    ) -> None:
        # A mirror can fail after its workflow event committed. One complete
        # retry repairs either side of that gap without invoking the provider.
        for _ in range(2):
            try:
                self._mirror_evidence(context.run_id)
                self._record_tool_success(
                    context.run_id,
                    step.step_id,
                    step.tool_name,
                    result,
                )
                return
            except Exception:
                continue
        raise RuntimeExecutionError(
            "external_action_evidence_incomplete",
            self._FAILURE_MESSAGES["external_action_evidence_incomplete"],
        )

    def _raise_external_terminal_failure(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        error_code: str,
    ) -> NoReturn:
        """Preserve a proven terminal failure across run-evidence outages."""

        for _ in range(2):
            try:
                self._mirror_evidence(context.run_id)
                self._record_tool_failure(
                    context.run_id,
                    step.step_id,
                    step.tool_name,
                    error_code,
                )
                break
            except Exception:
                continue
        try:
            self._fail(
                context.run_id,
                error_code,
                "Persisted external action failed.",
            )
        except RuntimeExecutionError as exc:
            if exc.code == error_code:
                raise
        except Exception:
            pass
        raise RuntimeExecutionError(error_code, self._FAILURE_MESSAGES[error_code])

    def _raise_external_outcome_unknown(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        finalizer: Callable[[], Any],
    ) -> NoReturn:
        finalized = False
        try:
            finalizer()
            finalized = True
        except Exception:
            # The provider may have applied the action even when its terminal
            # ledger write fails. Never allow that local exception to become a
            # generic retryable runtime failure.
            finalized = False
        if finalized:
            try:
                self._mirror_evidence(context.run_id)
                self._record_tool_failure(
                    context.run_id,
                    step.step_id,
                    step.tool_name,
                    "external_action_outcome_unknown",
                )
            except Exception:
                pass
        try:
            self._fail(
                context.run_id,
                "external_action_outcome_unknown",
                "External action outcome could not be durably proven.",
            )
        except RuntimeExecutionError as exc:
            if exc.code == "external_action_outcome_unknown":
                raise
        except Exception:
            pass
        raise RuntimeExecutionError(
            "external_action_outcome_unknown",
            self._FAILURE_MESSAGES["external_action_outcome_unknown"],
        )

    def _handle_external_terminal(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
        spec: ToolSpec,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
    ) -> ToolObservation:
        current_step = self.workflow_store.get_step(context.run_id, step.step_id)
        if current_step is None:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Terminal external action lost its tool step.",
            )
        if action.status == ExternalActionStatus.SUCCEEDED:
            return self._restore_external_success(
                context=context,
                step=current_step,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        if action.status in {
            ExternalActionStatus.FAILED,
            ExternalActionStatus.OUTCOME_UNKNOWN,
        }:
            self._restore_external_failure(
                context=context,
                step=current_step,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        self._fail(
            context.run_id,
            "invalid_planner_decision",
            "External action was reported terminal with a non-terminal status.",
        )

    def _restore_external_success(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        spec: ToolSpec,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
    ) -> ToolObservation:
        action = self.workflow_store.get_external_action(
            context.run_id,
            step.step_id,
        )
        if action is None:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Completed external-write step has no durable action.",
            )
        self._validate_external_action_binding(
            context=context,
            step=step,
            action=action,
            spec=spec,
            normalized_arguments=normalized_arguments,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
        )
        if (
            step.status != ToolCallStatus.COMPLETED
            or action.status != ExternalActionStatus.SUCCEEDED
            or not action.provider_reference
            or action.error_code is not None
        ):
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Completed external-write step has an invalid action outcome.",
            )
        try:
            result = self._decode_tool_result(step)
        except RuntimeExecutionError as exc:
            self._fail_restored_step(
                context=context,
                action=action,
                code=exc.code,
                detail=str(exc),
            )
        if spec.output_model is None:  # pragma: no cover - registry invariant
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="External-write tool has no output schema.",
            )
        try:
            normalized_result = spec.output_model.model_validate(
                result,
                context={"arguments": normalized_arguments},
            ).model_dump(mode="json")
        except ValidationError:
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail=(
                    "Persisted external action result no longer passes output validation."
                ),
            )
        if normalized_result != result:
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Persisted external action result is not canonical.",
            )
        if self._contains_sensitive_text(result, idempotency_key):
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Persisted external action result contains its idempotency key.",
            )
        if action.result_json != self._canonical_json(result):
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="External action result does not match its completed tool step.",
            )
        if result.get("provider_reference") != action.provider_reference:
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="External action result has an untrusted provider reference.",
            )
        self._record_external_success_evidence(
            context=context,
            step=step,
            result=result,
        )
        return ToolObservation(
            step_id=step.step_id,
            tool_name=step.tool_name,
            arguments=normalized_arguments,
            result=result,
            cached=True,
        )

    def _restore_external_failure(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        spec: ToolSpec,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
    ) -> NoReturn:
        action = self.workflow_store.get_external_action(
            context.run_id,
            step.step_id,
        )
        if action is None:
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Failed external-write step has no durable action.",
            )
        self._validate_external_action_binding(
            context=context,
            step=step,
            action=action,
            spec=spec,
            normalized_arguments=normalized_arguments,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
        )
        expected_code = {
            ExternalActionStatus.FAILED: "external_action_failed",
            ExternalActionStatus.OUTCOME_UNKNOWN: "external_action_outcome_unknown",
        }.get(action.status)
        if (
            step.status != ToolCallStatus.FAILED
            or expected_code is None
            or step.error_code != expected_code
            or action.error_code != expected_code
            or action.result_json is not None
            or step.result_json is not None
        ):
            self._fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Failed external-write step has an invalid action outcome.",
            )
        self._raise_external_terminal_failure(
            context=context,
            step=step,
            error_code=expected_code,
        )

    def _validate_external_action_binding(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
        spec: ToolSpec,
        provider_identity: str | None = None,
        normalized_arguments: dict[str, Any],
        input_hash: str,
        idempotency_key: str,
    ) -> None:
        expected = (
            context.run_id,
            step.step_id,
            context.authority.tenant_id,
            context.authority.subject_id,
            self.workflow_type,
            step.tool_name,
            spec.provider_name,
            provider_identity or action.provider_identity,
            input_hash,
            self._canonical_json(normalized_arguments),
            spec.retry_mode.value,
            idempotency_key,
        )
        actual = (
            action.run_id,
            action.step_id,
            action.tenant_id,
            action.subject_id,
            action.workflow_type,
            action.tool_name,
            action.provider_name,
            action.provider_identity,
            action.input_hash,
            action.arguments_json,
            action.retry_mode.value,
            action.idempotency_key,
        )
        if (
            actual != expected
            or step.run_id != context.run_id
            or step.tool_name != action.tool_name
            or step.input_hash != input_hash
            or not action.action_id
            or len(action.action_id) > 200
        ):
            if action.status == ExternalActionStatus.DISPATCHING:
                self._fail_external_dispatch_binding(
                    context=context,
                    step=step,
                    action=action,
                )
            if action.dispatch_count > 0:
                self._fail_dispatched_action_reconciliation(
                    context=context,
                    action=action,
                )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Durable external action binding does not match the execution context.",
            )
        if action.status == ExternalActionStatus.PREPARED:
            valid_state = action.dispatch_count == 0 and action.dispatch_token is None
        else:
            valid_state = action.dispatch_count >= 1 and action.dispatch_token is not None
        if not valid_state:
            if action.status == ExternalActionStatus.DISPATCHING:
                self._fail_external_dispatch_binding(
                    context=context,
                    step=step,
                    action=action,
                )
            self._fail(
                context.run_id,
                "invalid_planner_decision",
                "Durable external action dispatch state is invalid.",
            )

    def _fail_external_dispatch_binding(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        action: ExternalActionRecord,
    ) -> NoReturn:
        # A DISPATCHING record means the provider may already have committed.
        # Even corrupt identity metadata must therefore fail as unknown, never
        # as an ordinary validation error that cancellation could mask.
        if action.dispatch_token is None or step.attempt_token is None:
            raise RuntimeExecutionError(
                "external_action_outcome_unknown",
                self._FAILURE_MESSAGES["external_action_outcome_unknown"],
            )
        dispatch_token = action.dispatch_token
        attempt_token = step.attempt_token
        self._raise_external_outcome_unknown(
            context=context,
            step=step,
            finalizer=lambda: self.workflow_store.finalize_external_action_outcome_unknown(
                context.run_id,
                step.step_id,
                dispatch_token=dispatch_token,
                tool_attempt_token=attempt_token,
                error_code="external_action_outcome_unknown",
            ),
        )

    @staticmethod
    def _external_action_request(action: ExternalActionRecord) -> ExternalActionRequest:
        arguments = json.loads(action.arguments_json)
        if not isinstance(arguments, dict):  # pragma: no cover - store canonicalizes
            raise RuntimeExecutionError(
                "invalid_planner_decision",
                "External action arguments are not an object.",
            )
        return ExternalActionRequest(
            action_id=action.action_id,
            run_id=action.run_id,
            step_id=action.step_id,
            tenant_id=action.tenant_id,
            subject_id=action.subject_id,
            workflow_type=action.workflow_type,
            tool_name=action.tool_name,
            arguments=arguments,
            idempotency_key=action.idempotency_key,
        )

    def _external_action_idempotency_key(
        self,
        *,
        context: RuntimeExecutionContext,
        step_id: str,
        tool_name: str,
        input_hash: str,
    ) -> str:
        digest = self._stable_hash(
            {
                "tenant_id": context.authority.tenant_id,
                "run_id": context.run_id,
                "workflow_type": self.workflow_type,
                "step_id": step_id,
                "tool_name": tool_name,
                "input_hash": input_hash,
            }
        )
        return f"external_action_{digest}"

    @staticmethod
    def _canonical_json(payload: dict[str, Any]) -> str:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @staticmethod
    def _contains_sensitive_text(value: Any, sensitive_text: str) -> bool:
        """Detect exact server secrets/keys embedded anywhere in provider output."""

        seen: set[int] = set()

        def contains(candidate: Any) -> bool:
            if isinstance(candidate, str):
                return sensitive_text in candidate
            if isinstance(candidate, (bytes, bytearray)):
                return sensitive_text.encode("utf-8") in candidate
            if isinstance(candidate, dict):
                identity = id(candidate)
                if identity in seen:
                    return False
                seen.add(identity)
                return any(
                    contains(key) or contains(item)
                    for key, item in candidate.items()
                )
            if isinstance(candidate, (list, tuple, set, frozenset)):
                identity = id(candidate)
                if identity in seen:
                    return False
                seen.add(identity)
                return any(contains(item) for item in candidate)
            return False

        return contains(value)

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
                        self._restore_external_success(
                            context=context,
                            step=step,
                            spec=spec,
                            normalized_arguments=normalized,
                            input_hash=step.input_hash,
                            idempotency_key=self._external_action_idempotency_key(
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
                    self._restore_external_failure(
                        context=context,
                        step=step,
                        spec=spec,
                        normalized_arguments=normalized,
                        input_hash=input_hash,
                        idempotency_key=self._external_action_idempotency_key(
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
        self._record_evidence(
            run_id,
            "tool.result",
            {
                "evidence_id": f"tool-result:{step_id}",
                "step_id": step_id,
                "tool_name": tool_name,
                "status": "completed",
                "result": result,
                "error_code": None,
            },
        )

    def _record_tool_failure(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        error_code: str,
    ) -> None:
        self._record_evidence(
            run_id,
            "tool.result",
            {
                "evidence_id": f"tool-result:{step_id}",
                "step_id": step_id,
                "tool_name": tool_name,
                "status": "failed",
                "result": None,
                "error_code": error_code,
            },
        )

    def _record_evidence(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        evidence_id = str(payload["evidence_id"])
        existing = self._workflow_evidence(run_id, event_type, evidence_id)
        if existing is None:
            self.workflow_store.append_event(run_id, event_type, payload)
            existing = payload
        elif existing != payload:
            raise RuntimeExecutionError(
                "invalid_planner_decision",
                f"Durable evidence mismatch for {evidence_id}.",
            )
        self._ensure_run_evidence(run_id, event_type, existing)

    def _mirror_evidence(self, run_id: str) -> None:
        for event in self.workflow_store.list_events(run_id):
            if not (
                event.event_type.startswith("external_action.")
                or event.event_type
                in {
                    "planner.decision",
                    "policy.decision",
                    "tool.result",
                    "loop.outcome",
                }
            ):
                continue
            if "evidence_id" not in event.payload:
                continue
            self._ensure_run_evidence(run_id, event.event_type, event.payload)

    def _ensure_run_evidence(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        evidence_id = payload.get("evidence_id")
        for event in self.run_event_sink.list_events(run_id):
            if event.event_type == event_type and event.payload.get("evidence_id") == evidence_id:
                return
        self.run_event_sink.append_event(run_id, event_type, payload)

    def _workflow_evidence(
        self,
        run_id: str,
        event_type: str,
        evidence_id: str,
    ) -> dict[str, Any] | None:
        for event in self.workflow_store.list_events(run_id):
            if event.event_type == event_type and event.payload.get("evidence_id") == evidence_id:
                return event.payload
        return None

    def _fail_restored_step(
        self,
        *,
        context: RuntimeExecutionContext,
        action: ExternalActionRecord | None,
        code: str,
        detail: str,
    ) -> NoReturn:
        if action is not None and action.dispatch_count > 0:
            self._fail_dispatched_action_reconciliation(
                context=context,
                action=action,
            )
        self._fail(context.run_id, code, detail)

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

    @staticmethod
    def _decode_tool_result(step: ToolCallRecord) -> dict[str, Any]:
        if step.result_json is None:
            raise RuntimeExecutionError(
                "tool_execution_failed",
                f"Completed tool step {step.step_id} is missing its result.",
            )
        try:
            value = json.loads(step.result_json)
        except json.JSONDecodeError as exc:
            raise RuntimeExecutionError(
                "tool_execution_failed",
                f"Tool step {step.step_id} persisted invalid JSON.",
            ) from exc
        if not isinstance(value, dict):
            raise RuntimeExecutionError(
                "tool_execution_failed",
                f"Tool step {step.step_id} did not persist an object result.",
            )
        return value

    def _decode_tool_result_or_fail(
        self,
        run_id: str,
        step: ToolCallRecord,
    ) -> dict[str, Any]:
        try:
            return self._decode_tool_result(step)
        except RuntimeExecutionError as exc:
            self._fail(run_id, exc.code, str(exc))

    @staticmethod
    def _stable_hash(payload: dict[str, Any]) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return sha256(encoded).hexdigest()

    @staticmethod
    def _tool_error_code(status: ToolExecutionStatus) -> str:
        return {
            ToolExecutionStatus.DENIED: "unknown_tool",
            ToolExecutionStatus.INVALID_INPUT: "invalid_tool_arguments",
            ToolExecutionStatus.TIMED_OUT: "tool_timed_out",
            ToolExecutionStatus.FAILED: "tool_execution_failed",
        }.get(status, "tool_execution_failed")
