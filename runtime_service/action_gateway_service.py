from __future__ import annotations

import json
from dataclasses import dataclass

from pydantic import ValidationError

from .action_gateway import (
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    ACTION_STEP_ID,
    ACTION_WORKFLOW_TYPE,
    ActionCreateRequest,
    ActionEvent,
    ActionEvidenceStatus,
    ActionResource,
    ActionStatus,
    DurableActionInput,
    WebhookSendOutput,
    action_client_request_id,
    action_fingerprint,
    action_thread_id,
    is_action_run,
    persisted_action_input,
    to_durable_action_input,
)
from .auth import TenantContext
from .canonical import stable_hash
from .external_actions import ExternalActionProviderRegistry
from .manager import RuntimeManager
from .models import RunCreateRequest, RunRecord, RunStatus
from .run_store import RunStore
from .sandbox import ToolRetryMode
from .store import THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
from .workflow_store import (
    ExternalActionRecord,
    ExternalActionStatus,
    ToolCallStatus,
    WorkflowExecutionRecord,
    WorkflowStore,
)


class ActionGatewayError(Exception):
    pass


class ActionTypeNotRegisteredError(ActionGatewayError):
    pass


class DestinationNotRegisteredError(ActionGatewayError):
    pass


class IdempotencyKeyReusedError(ActionGatewayError):
    pass


class ActionEvidenceIncompleteError(ActionGatewayError):
    pass


@dataclass(frozen=True)
class SubmittedAction:
    resource: ActionResource
    existed: bool


class ActionProjector:
    _UNKNOWN_CODE = "external_action_outcome_unknown"
    _FAILURE_MESSAGES = {
        "external_action_failed": "The external action failed definitively.",
        "external_action_outcome_unknown": (
            "The external action outcome is unknown; do not retry with a new key."
        ),
        "action_cancelled": "The Action was cancelled before dispatch.",
        "action_execution_failed": "The Action failed before a provider outcome existed.",
    }
    _EVENT_STATUSES = {
        "external_action.prepared": ActionEvidenceStatus.PREPARED,
        "external_action.dispatch_started": ActionEvidenceStatus.DISPATCHING,
        "external_action.succeeded": ActionEvidenceStatus.SUCCEEDED,
        "external_action.failed": ActionEvidenceStatus.FAILED,
        "external_action.outcome_unknown": ActionEvidenceStatus.OUTCOME_UNKNOWN,
    }

    def __init__(self, workflow_store: WorkflowStore) -> None:
        self.workflow_store = workflow_store

    def project(self, run: RunRecord) -> ActionResource:
        runtime_input = self._validated_identity(run)
        snapshot = self.workflow_store.read_run_snapshot(run.run_id)
        execution = snapshot.execution
        steps = snapshot.steps
        actions = snapshot.external_actions

        if len(actions) > 1:
            if any(action.dispatch_count > 0 for action in actions):
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    error_code=self._UNKNOWN_CODE,
                )
            raise ActionEvidenceIncompleteError("Action has multiple pre-dispatch ledger rows")

        action = actions[0] if actions else None
        if (
            (execution is None and action is not None)
            or (
                execution is not None
                and not self._execution_binding_matches(
                    run,
                    runtime_input,
                    execution,
                )
            )
        ):
            if action is not None and action.dispatch_count > 0:
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    action=action,
                    error_code=self._UNKNOWN_CODE,
                )
            raise ActionEvidenceIncompleteError("Action workflow identity is inconsistent")
        if action is not None and not self._action_binding_matches(
            run,
            runtime_input,
            action,
        ):
            if action.dispatch_count > 0:
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    action=action,
                    error_code=self._UNKNOWN_CODE,
                )
            raise ActionEvidenceIncompleteError("Prepared Action binding is inconsistent")

        if (
            (action is not None and action.status == ExternalActionStatus.OUTCOME_UNKNOWN)
            or run.error_code == self._UNKNOWN_CODE
        ):
            return self._resource(
                run,
                runtime_input,
                ActionStatus.OUTCOME_UNKNOWN,
                action=action,
                error_code=self._UNKNOWN_CODE,
            )

        if action is None:
            if run.error_code in {
                "external_action_reconciliation_pending",
                THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
            }:
                raise ActionEvidenceIncompleteError(
                    "Reconciliation marker has no external-action ledger row"
                )
            return self._project_without_action(run, runtime_input, execution)

        step = next(
            (candidate for candidate in steps if candidate.step_id == action.step_id),
            None,
        )
        if action.status == ExternalActionStatus.SUCCEEDED:
            result = self._validated_success(action, step)
            return self._resource(
                run,
                runtime_input,
                ActionStatus.SUCCEEDED,
                action=action,
                result=result,
            )
        if action.status == ExternalActionStatus.FAILED:
            if not self._valid_failed_action(action, step):
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    action=action,
                    error_code=self._UNKNOWN_CODE,
                )
            return self._resource(
                run,
                runtime_input,
                ActionStatus.FAILED,
                action=action,
                error_code="external_action_failed",
            )
        if action.status == ExternalActionStatus.DISPATCHING:
            if (
                action.dispatch_count < 1
                or action.dispatch_token is None
                or step is None
                or step.status != ToolCallStatus.RUNNING
                or run.status.is_terminal
                or (execution is not None and execution.status.is_terminal)
            ):
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    action=action,
                    error_code=self._UNKNOWN_CODE,
                )
            status = (
                ActionStatus.RECONCILING
                if run.error_code
                in {
                    "external_action_reconciliation_pending",
                    THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
                }
                else ActionStatus.RUNNING
            )
            return self._resource(run, runtime_input, status, action=action)

        if action.status != ExternalActionStatus.PREPARED:
            if action.dispatch_count > 0:
                return self._resource(
                    run,
                    runtime_input,
                    ActionStatus.OUTCOME_UNKNOWN,
                    action=action,
                    error_code=self._UNKNOWN_CODE,
                )
            raise ActionEvidenceIncompleteError("Action ledger has an unknown status")
        if action.dispatch_count != 0 or action.dispatch_token is not None:
            return self._resource(
                run,
                runtime_input,
                ActionStatus.OUTCOME_UNKNOWN,
                action=action,
                error_code=self._UNKNOWN_CODE,
            )
        if (
            step is None
            or not self._step_binding_matches(action, step)
            or step.status != ToolCallStatus.RUNNING
        ):
            raise ActionEvidenceIncompleteError(
                "Prepared Action tool evidence is inconsistent"
            )
        if run.status == RunStatus.CANCELLED:
            return self._resource(
                run,
                runtime_input,
                ActionStatus.CANCELLED,
                action=action,
                error_code="action_cancelled",
            )
        if run.status == RunStatus.FAILED:
            return self._resource(
                run,
                runtime_input,
                ActionStatus.FAILED,
                action=action,
                error_code="action_execution_failed",
            )
        if run.status == RunStatus.COMPLETED:
            raise ActionEvidenceIncompleteError(
                "Completed Run has only a prepared external action"
            )
        return self._resource(run, runtime_input, ActionStatus.RUNNING, action=action)

    def list_events(
        self,
        run: RunRecord,
        *,
        after_sequence: int = 0,
    ) -> list[ActionEvent]:
        runtime_input = self._validated_identity(run)
        snapshot = self.workflow_store.read_run_snapshot(
            run.run_id,
            after_sequence=after_sequence,
        )
        actions = snapshot.external_actions
        if not actions:
            return []
        if len(actions) != 1:
            raise ActionEvidenceIncompleteError("Action event ledger is not single-step")
        action = actions[0]
        execution = snapshot.execution
        if execution is None or not self._execution_binding_matches(
            run,
            runtime_input,
            execution,
        ):
            raise ActionEvidenceIncompleteError("Action event workflow is inconsistent")
        if not self._action_binding_matches(run, runtime_input, action):
            raise ActionEvidenceIncompleteError("Action event binding is inconsistent")
        step = next(
            (candidate for candidate in snapshot.steps if candidate.step_id == action.step_id),
            None,
        )

        projected: list[ActionEvent] = []
        for event in snapshot.events:
            status = self._EVENT_STATUSES.get(event.event_type)
            if status is None:
                continue
            if (
                event.payload.get("action_id") != action.action_id
                or event.payload.get("step_id") != action.step_id
                or event.payload.get("tool_name") != action.tool_name
                or event.payload.get("provider_name") != action.provider_name
                or event.payload.get("status") != status.value
            ):
                raise ActionEvidenceIncompleteError("Action event identity is inconsistent")
            if event.event_type == "external_action.prepared":
                if event.payload.get("retry_mode") != action.retry_mode.value:
                    raise ActionEvidenceIncompleteError(
                        "Action prepare event retry mode is inconsistent"
                    )
                dispatch_count = 0
            elif event.event_type == "external_action.dispatch_started":
                candidate = event.payload.get("dispatch_count")
                if (
                    isinstance(candidate, bool)
                    or not isinstance(candidate, int)
                    or not 1 <= candidate <= action.dispatch_count
                ):
                    raise ActionEvidenceIncompleteError(
                        "Action dispatch event count is inconsistent"
                    )
                dispatch_count = candidate
            else:
                candidate = event.payload.get("dispatch_count")
                if (
                    isinstance(candidate, bool)
                    or not isinstance(candidate, int)
                    or candidate != action.dispatch_count
                    or action.status.value != status.value
                ):
                    raise ActionEvidenceIncompleteError(
                        "Action outcome event is inconsistent"
                    )
                dispatch_count = action.dispatch_count

            provider_reference = None
            error_code = None
            if status == ActionEvidenceStatus.SUCCEEDED:
                provider_reference = self._validated_success(action, step).provider_reference
            elif status == ActionEvidenceStatus.FAILED:
                if not self._valid_failed_action(action, step):
                    raise ActionEvidenceIncompleteError(
                        "Failed Action event has inconsistent ledger evidence"
                    )
                error_code = "external_action_failed"
            elif status == ActionEvidenceStatus.OUTCOME_UNKNOWN:
                error_code = self._UNKNOWN_CODE
            if (
                event.event_type.startswith("external_action.")
                and event.event_type
                not in {"external_action.prepared", "external_action.dispatch_started"}
                and (
                    event.payload.get("provider_reference") != provider_reference
                    or event.payload.get("error_code") != error_code
                )
            ):
                raise ActionEvidenceIncompleteError(
                    "Action outcome event payload is inconsistent"
                )
            projected.append(
                ActionEvent(
                    sequence=event.sequence,
                    event_type=event.event_type,
                    status=status,
                    destination=runtime_input.destination,
                    dispatch_count=dispatch_count,
                    retry_mode=action.retry_mode.value,
                    provider_reference=provider_reference,
                    error_code=error_code,
                    created_at=event.created_at,
                )
            )
        return projected

    def _project_without_action(
        self,
        run: RunRecord,
        runtime_input: DurableActionInput,
        execution,
    ) -> ActionResource:
        if run.status == RunStatus.QUEUED:
            status = ActionStatus.QUEUED
            error_code = None
        elif run.status == RunStatus.RUNNING:
            if execution is not None and execution.status.is_terminal:
                raise ActionEvidenceIncompleteError(
                    "Terminal Action workflow has no external-action ledger row"
                )
            status = ActionStatus.RUNNING
            error_code = None
        elif run.status == RunStatus.CANCELLED:
            status = ActionStatus.CANCELLED
            error_code = "action_cancelled"
        elif run.status == RunStatus.FAILED:
            status = ActionStatus.FAILED
            error_code = "action_execution_failed"
        else:
            raise ActionEvidenceIncompleteError(
                "Completed Run has no external-action ledger row"
            )
        return self._resource(
            run,
            runtime_input,
            status,
            error_code=error_code,
        )

    def _validated_identity(self, run: RunRecord) -> DurableActionInput:
        if not is_action_run(run):
            raise ActionEvidenceIncompleteError("Run is not an Action resource")
        try:
            runtime_input = persisted_action_input(run)
        except (TypeError, ValueError, ValidationError):
            raise ActionEvidenceIncompleteError("Persisted Action input is invalid") from None
        expected_client_id = action_client_request_id(runtime_input.idempotency_key)
        expected_thread_id = action_thread_id(
            run.tenant_id,
            runtime_input.action_type,
            runtime_input.idempotency_key,
        )
        if (
            run.client_request_id != expected_client_id
            or run.thread_id != expected_thread_id
        ):
            raise ActionEvidenceIncompleteError("Persisted Action identity is invalid")
        return runtime_input

    @staticmethod
    def _execution_binding_matches(
        run: RunRecord,
        runtime_input: DurableActionInput,
        execution: WorkflowExecutionRecord,
    ) -> bool:
        return bool(
            execution.run_id == run.run_id
            and execution.workflow_type == ACTION_WORKFLOW_TYPE
            and execution.input_hash == action_fingerprint(runtime_input)
        )

    @staticmethod
    def _action_binding_matches(
        run: RunRecord,
        runtime_input: DurableActionInput,
        action: ExternalActionRecord,
    ) -> bool:
        arguments = runtime_input.input.model_dump(mode="json")
        input_hash = stable_hash(arguments)
        provider_key = "external_action_" + stable_hash(
            {
                "tenant_id": run.tenant_id,
                "run_id": run.run_id,
                "workflow_type": ACTION_WORKFLOW_TYPE,
                "step_id": ACTION_STEP_ID,
                "tool_name": "webhook.send",
                "input_hash": input_hash,
            }
        )
        authority = run.execution_authority
        return bool(
            action.run_id == run.run_id
            and action.step_id == ACTION_STEP_ID
            and action.tenant_id == run.tenant_id
            and authority is not None
            and authority.tenant_id == run.tenant_id
            and {"tools:execute", "external-actions:execute"}.issubset(
                authority.permissions
            )
            and action.subject_id == authority.subject_id
            and action.workflow_type == ACTION_WORKFLOW_TYPE
            and action.tool_name == "webhook.send"
            and action.provider_name == runtime_input.destination
            and 0 < len(action.provider_identity) <= 200
            and action.provider_identity == action.provider_identity.strip()
            and action.input_hash == input_hash
            and action.idempotency_key == provider_key
            and action.retry_mode
            in {ToolRetryMode.PROVIDER_IDEMPOTENT, ToolRetryMode.UNSAFE}
            and action.arguments_json
            == json.dumps(
                arguments,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )

    @staticmethod
    def _validated_success(action: ExternalActionRecord, step) -> WebhookSendOutput:
        if (
            action.dispatch_count < 1
            or action.dispatch_token is None
            or not action.provider_reference
            or action.result_json is None
            or action.error_code is not None
        ):
            raise ActionEvidenceIncompleteError("Succeeded Action evidence is incomplete")
        try:
            decoded = json.loads(action.result_json)
            result = WebhookSendOutput.model_validate(decoded)
        except (json.JSONDecodeError, ValidationError):
            raise ActionEvidenceIncompleteError("Succeeded Action result is invalid") from None
        if result.provider_reference != action.provider_reference:
            raise ActionEvidenceIncompleteError(
                "Succeeded Action provider reference is inconsistent"
            )
        if step is not None and (
            not ActionProjector._step_binding_matches(action, step)
            or step.status != ToolCallStatus.COMPLETED
            or step.result_json != action.result_json
            or step.error_code is not None
        ):
            raise ActionEvidenceIncompleteError("Succeeded Action tool evidence is inconsistent")
        return result

    @staticmethod
    def _valid_failed_action(action: ExternalActionRecord, step) -> bool:
        return bool(
            action.dispatch_count >= 1
            and action.dispatch_token is not None
            and action.result_json is None
            and action.error_code == "external_action_failed"
            and step is not None
            and ActionProjector._step_binding_matches(action, step)
            and step.status == ToolCallStatus.FAILED
            and step.result_json is None
            and step.error_code == "external_action_failed"
        )

    @staticmethod
    def _step_binding_matches(action: ExternalActionRecord, step) -> bool:
        return bool(
            step.run_id == action.run_id
            and step.step_id == action.step_id
            and step.tool_name == action.tool_name
            and step.input_hash == action.input_hash
        )

    def _resource(
        self,
        run: RunRecord,
        runtime_input: DurableActionInput,
        status: ActionStatus,
        *,
        action: ExternalActionRecord | None = None,
        result: WebhookSendOutput | None = None,
        error_code: str | None = None,
    ) -> ActionResource:
        updated_candidates = [run.updated_at]
        if action is not None:
            updated_candidates.append(action.updated_at)
        completed_at = None
        if status.is_terminal:
            completed_at = (
                action.finalized_at
                if action is not None and action.finalized_at is not None
                else run.completed_at
            )
        return ActionResource(
            action_id=run.run_id,
            action_type=runtime_input.action_type,
            destination=runtime_input.destination,
            idempotency_key=runtime_input.idempotency_key,
            status=status,
            result=result if status == ActionStatus.SUCCEEDED else None,
            error_code=error_code,
            error_message=(
                self._FAILURE_MESSAGES.get(error_code) if error_code is not None else None
            ),
            created_at=run.created_at,
            updated_at=max(updated_candidates),
            completed_at=completed_at,
        )


class DurableActionGateway:
    def __init__(
        self,
        *,
        manager: RuntimeManager,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        provider_registry: ExternalActionProviderRegistry,
    ) -> None:
        self.manager = manager
        self.run_store = run_store
        self.provider_registry = provider_registry
        self.projector = ActionProjector(workflow_store)

    def submit(
        self,
        request: ActionCreateRequest,
        *,
        tenant_context: TenantContext,
    ) -> SubmittedAction:
        internal_client_id = action_client_request_id(request.idempotency_key)
        existing = self.run_store.get_run_by_client_request_id(
            tenant_context.tenant_id,
            internal_client_id,
        )
        if existing is not None:
            if request.action_type != "webhook.send":
                raise IdempotencyKeyReusedError()
            runtime_input = to_durable_action_input(request)
            self._assert_same_request(existing, runtime_input)
            return SubmittedAction(self.projector.project(existing), existed=True)

        if request.action_type != "webhook.send":
            raise ActionTypeNotRegisteredError()
        runtime_input = to_durable_action_input(request)
        if self.provider_registry.resolve(request.destination) is None:
            raise DestinationNotRegisteredError()
        run_request = RunCreateRequest(
            thread_id=action_thread_id(
                tenant_context.tenant_id,
                request.action_type,
                request.idempotency_key,
            ),
            agent_id=ACTION_AGENT_ID,
            agent_version=ACTION_AGENT_VERSION,
            input=runtime_input.model_dump(mode="json"),
            client_request_id=internal_client_id,
        )
        run = self.manager.submit(run_request, tenant_context=tenant_context)
        self._assert_same_request(run, runtime_input)
        return SubmittedAction(self.projector.project(run), existed=False)

    def exists(
        self,
        action_id: str,
        *,
        tenant_context: TenantContext,
    ) -> bool:
        run = self.manager.get_run(action_id, tenant_context=tenant_context)
        return run is not None and is_action_run(run)

    def get(
        self,
        action_id: str,
        *,
        tenant_context: TenantContext,
    ) -> ActionResource | None:
        run = self.manager.get_run(action_id, tenant_context=tenant_context)
        if run is None or not is_action_run(run):
            return None
        return self.projector.project(run)

    def list_events(
        self,
        action_id: str,
        *,
        tenant_context: TenantContext,
        after_sequence: int = 0,
    ) -> list[ActionEvent] | None:
        run = self.manager.get_run(action_id, tenant_context=tenant_context)
        if run is None or not is_action_run(run):
            return None
        return self.projector.list_events(run, after_sequence=after_sequence)

    @staticmethod
    def _assert_same_request(
        run: RunRecord,
        expected: DurableActionInput,
    ) -> None:
        if not is_action_run(run):
            raise ActionEvidenceIncompleteError(
                "Reserved Action namespace is owned by a non-Action Run"
            )
        try:
            persisted = persisted_action_input(run)
        except (TypeError, ValueError, ValidationError):
            raise ActionEvidenceIncompleteError("Persisted Action input is invalid") from None
        if (
            persisted.idempotency_key != expected.idempotency_key
            or action_fingerprint(persisted) != action_fingerprint(expected)
        ):
            raise IdempotencyKeyReusedError()
