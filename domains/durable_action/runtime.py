from __future__ import annotations

import json
from typing import NoReturn

from pydantic import ValidationError

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionContext,
    RuntimeExecutionError,
    RuntimeResponse,
)
from runtime_service.action_gateway import (
    ACTION_DOMAIN_ID,
    ACTION_SCHEMA_VERSION,
    ACTION_STEP_ID,
    ACTION_WORKFLOW_TYPE,
    DurableActionInput,
    WebhookSendInput,
    WebhookSendOutput,
    action_fingerprint,
    canonical_action_json,
)
from runtime_service.evidence import EvidenceProjector, RunEventSink
from runtime_service.external_action_coordinator import ExternalActionCoordinator
from runtime_service.external_actions import (
    ExternalActionDispatcher,
    ExternalActionReconciliationPendingError,
)
from runtime_service.sandbox import ToolEffect, ToolPolicy, ToolRetryMode, ToolSpec
from runtime_service.workflow_store import ExecutionOutcome, WorkflowStatus, WorkflowStore


class DurableActionState(BaseRuntimeState):
    domain_id = ACTION_DOMAIN_ID
    schema_version = ACTION_SCHEMA_VERSION

    result: WebhookSendOutput | None = None


class DurableActionGatewayRuntime:
    """Private one-step domain that executes one registered webhook side effect."""

    _FAILURE_MESSAGES = {
        "external_action_not_configured": "The Action destination is not configured.",
        "external_action_permission_denied": (
            "Execution authority does not allow external actions."
        ),
        "tool_permission_denied": "Execution authority does not allow tool execution.",
        "external_action_failed": "The external action failed definitively.",
        "external_action_outcome_unknown": (
            "The external action outcome is unknown and was not retried again."
        ),
        "external_action_evidence_incomplete": (
            "Durable external-action evidence is incomplete."
        ),
        "run_cancel_requested": "The Action was cancelled before dispatch.",
    }

    def __init__(
        self,
        *,
        workflow_store: WorkflowStore,
        run_event_sink: RunEventSink,
        dispatcher: ExternalActionDispatcher,
    ) -> None:
        self.workflow_store = workflow_store
        self.evidence_projector = EvidenceProjector(
            workflow_store=workflow_store,
            run_event_sink=run_event_sink,
        )
        self.provider_registry = dispatcher.registry
        self.coordinator = ExternalActionCoordinator(
            workflow_store=workflow_store,
            dispatcher=dispatcher,
            workflow_type=ACTION_WORKFLOW_TYPE,
            evidence_projector=self.evidence_projector,
            fail=self._fail,
            failure_messages=self._FAILURE_MESSAGES,
            max_dispatches=2,
        )

    def initial_state(self, thread_id: str) -> DurableActionState:
        return DurableActionState(thread_id=thread_id)

    def execute(
        self,
        state: DurableActionState,
        runtime_input: DurableActionInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[DurableActionState]:
        try:
            return self._execute_impl(state, runtime_input, context)
        except ExternalActionReconciliationPendingError:
            raise
        except RuntimeExecutionError:
            self.coordinator.reconcile_dispatched_action(
                context=context,
                include_terminal=False,
            )
            raise
        except Exception:
            self.coordinator.reconcile_dispatched_action(
                context=context,
                include_terminal=False,
            )
            self._fail(
                context.run_id,
                "external_action_evidence_incomplete",
                "The private Action workflow failed outside a proven provider outcome.",
            )

    def _execute_impl(
        self,
        state: DurableActionState,
        runtime_input: DurableActionInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[DurableActionState]:
        execution_hash = action_fingerprint(runtime_input)
        claim = self.workflow_store.create_or_get_execution(
            context.run_id,
            ACTION_WORKFLOW_TYPE,
            execution_hash,
        )
        if claim.outcome in {
            ExecutionOutcome.INPUT_MISMATCH,
            ExecutionOutcome.WORKFLOW_TYPE_MISMATCH,
        }:
            self.coordinator.reconcile_dispatched_action(context=context)
            self._fail(
                context.run_id,
                "external_action_evidence_incomplete",
                "Persisted Action workflow identity does not match its Run input.",
            )

        execution = claim.execution
        if execution.status == WorkflowStatus.READY:
            return self._restore_ready(state, execution.result_json)
        if execution.status in {WorkflowStatus.BLOCKED, WorkflowStatus.FAILED}:
            self.coordinator.reconcile_dispatched_action(context=context)
            code = execution.error_code or "external_action_evidence_incomplete"
            raise RuntimeExecutionError(code, self._safe_message(code))
        if execution.status == WorkflowStatus.PENDING:
            self.workflow_store.mark_running(context.run_id)

        if not context.authority.allows("tools:execute"):
            self._fail(
                context.run_id,
                "tool_permission_denied",
                "The persisted authority does not include tools:execute.",
            )
        if not context.authority.allows("external-actions:execute"):
            self._fail(
                context.run_id,
                "external_action_permission_denied",
                "The persisted authority does not include external-actions:execute.",
            )

        provider = self.provider_registry.resolve(runtime_input.destination)
        if provider is None:
            self.coordinator.reconcile_dispatched_action(context=context)
            self._fail(
                context.run_id,
                "external_action_not_configured",
                "The destination alias has no registered provider.",
            )
        retry_mode = (
            ToolRetryMode.PROVIDER_IDEMPOTENT
            if provider.supports_idempotency
            else ToolRetryMode.UNSAFE
        )
        spec = ToolSpec(
            name="webhook.send",
            description="Send one server-routed webhook envelope.",
            input_model=WebhookSendInput,
            output_model=WebhookSendOutput,
            policy=ToolPolicy(timeout_seconds=5.0),
            effect=ToolEffect.EXTERNAL_WRITE,
            retry_mode=retry_mode,
            provider_name=runtime_input.destination,
        )
        arguments = runtime_input.input.model_dump(mode="json")
        observation = self.coordinator.execute(
            context=context,
            tool_name="webhook.send",
            spec=spec,
            step_id=ACTION_STEP_ID,
            normalized_arguments=arguments,
        )
        try:
            result = WebhookSendOutput.model_validate(observation.result)
        except ValidationError:
            self._fail(
                context.run_id,
                "external_action_evidence_incomplete",
                "Completed provider evidence failed the Action output contract.",
            )
        record = self.workflow_store.finalize_ready(
            context.run_id,
            canonical_action_json(result.model_dump(mode="json")),
        )
        if record.status != WorkflowStatus.READY:
            self.coordinator.reconcile_dispatched_action(context=context)
            self._fail(
                context.run_id,
                "external_action_evidence_incomplete",
                "Action workflow finalization did not preserve the provider outcome.",
            )
        return RuntimeResponse(
            message="Action completed.",
            state=state.model_copy(update={"result": result}),
            validation_errors=[],
        )

    def _restore_ready(
        self,
        state: DurableActionState,
        result_json: str | None,
    ) -> RuntimeResponse[DurableActionState]:
        try:
            decoded = json.loads(result_json or "")
            result = WebhookSendOutput.model_validate(decoded)
        except (json.JSONDecodeError, ValidationError):
            raise RuntimeExecutionError(
                "external_action_evidence_incomplete",
                self._safe_message("external_action_evidence_incomplete"),
            ) from None
        return RuntimeResponse(
            message="Action completed.",
            state=state.model_copy(update={"result": result}),
            validation_errors=[],
        )

    def _fail(self, run_id: str, code: str, _unsafe_detail: str) -> NoReturn:
        execution = self.workflow_store.get_execution(run_id)
        if execution is not None and execution.status == WorkflowStatus.RUNNING:
            self.workflow_store.finalize_failed(run_id, code)
        raise RuntimeExecutionError(code, self._safe_message(code))

    def _safe_message(self, code: str) -> str:
        return self._FAILURE_MESSAGES.get(code, "The Action could not be completed.")
