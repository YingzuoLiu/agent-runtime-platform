from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, NoReturn

from pydantic import ValidationError

from agent.contracts import RuntimeExecutionContext, RuntimeExecutionError

from .canonical import canonical_json, decode_tool_result, stable_hash
from .evidence import EvidenceProjector
from .external_actions import (
    DefinitiveExternalActionError,
    ExternalActionDispatcher,
    ExternalActionProvider,
    ExternalActionProviderResult,
    ExternalActionReconciliationPendingError,
    ExternalActionRequest,
)
from .planner import ToolObservation
from .sandbox import ToolRetryMode, ToolSpec
from .workflow_store import (
    ClaimOutcome,
    ExternalActionDispatchOutcome,
    ExternalActionPrepareOutcome,
    ExternalActionRecord,
    ExternalActionStatus,
    SQLiteWorkflowStore,
    ToolCallRecord,
    ToolCallStatus,
)


class ExternalActionCoordinator:
    """Coordinates the durable external-action state machine.

    The coordinator owns no per-call state. Every transition is fenced by the
    durable Workflow Store so retries and restart recovery preserve the same
    action identity and provider-dispatch semantics.
    """

    # Every message the coordinator can surface, so a host that supplies a
    # narrower table cannot turn a post-dispatch failure path into a KeyError.
    DEFAULT_FAILURE_MESSAGES: ClassVar[Mapping[str, str]] = {
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

    # Reconciliation prefers the least-proven known outcome, because a record
    # that cannot prove a provider result must not be masked by a terminal
    # sibling. Any unknown status, or a corrupt PREPARED row that claims a
    # dispatch already occurred, blocks selection entirely and keeps the Run
    # reconciliation-pending.
    _RECONCILE_PRIORITY: ClassVar[Mapping[ExternalActionStatus, int]] = {
        ExternalActionStatus.DISPATCHING: 0,
        ExternalActionStatus.OUTCOME_UNKNOWN: 1,
        ExternalActionStatus.SUCCEEDED: 2,
        ExternalActionStatus.FAILED: 3,
        ExternalActionStatus.PREPARED: 4,
    }

    def __init__(
        self,
        *,
        workflow_store: SQLiteWorkflowStore,
        dispatcher: ExternalActionDispatcher | None,
        workflow_type: str,
        evidence_projector: EvidenceProjector,
        fail: Callable[[str, str, str], NoReturn],
        failure_messages: Mapping[str, str] | Callable[[], Mapping[str, str]],
        max_dispatches: int | Callable[[], int] = 2,
    ) -> None:
        self.workflow_store = workflow_store
        self.dispatcher = dispatcher
        self.workflow_type = workflow_type
        self.evidence_projector = evidence_projector
        self.fail = fail
        self._failure_messages = failure_messages
        self._max_dispatches = max_dispatches

    @property
    def failure_messages(self) -> Mapping[str, str]:
        if callable(self._failure_messages):
            return self._failure_messages()
        return self._failure_messages

    def failure_message(self, code: str) -> str:
        """Resolve a safe message, falling back before a failure path can raise.

        These lookups happen only while terminalizing a run whose provider call
        may already have been applied, so a host table missing a code must not
        replace the outcome with a KeyError.
        """

        message = self.failure_messages.get(code)
        if message is None:
            return self.DEFAULT_FAILURE_MESSAGES.get(code, "External action failed.")
        return message

    @property
    def max_dispatches(self) -> int:
        if callable(self._max_dispatches):
            return self._max_dispatches()
        return self._max_dispatches

    def provider_for(self, spec: ToolSpec) -> ExternalActionProvider | None:
        """Resolve the server-owned provider used by policy preflight."""

        if self.dispatcher is None or spec.provider_name is None:
            return None
        return self.dispatcher.registry.resolve(spec.provider_name)

    def dispatched_action(
        self,
        run_id: str,
        step_id: str,
    ) -> ExternalActionRecord | None:
        action = self.workflow_store.get_external_action(run_id, step_id)
        if action is None or action.dispatch_count < 1:
            return None
        return action

    def reconcile_dispatched_action(
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
            dispatched_actions = [
                candidate
                for candidate in self.workflow_store.list_external_actions(context.run_id)
                if candidate.dispatch_count > 0
            ]
            if not dispatched_actions:
                return
            reconciliation_pending = any(
                candidate.status not in self._RECONCILE_PRIORITY
                or candidate.status == ExternalActionStatus.PREPARED
                for candidate in dispatched_actions
            )
            if not reconciliation_pending:
                actions = [
                    candidate
                    for candidate in dispatched_actions
                    if include_terminal or not candidate.status.is_terminal
                ]
                if not actions:
                    return
                action = min(
                    actions,
                    key=lambda candidate: self._RECONCILE_PRIORITY[candidate.status],
                )
        else:
            reconciliation_pending = (
                action.status not in self._RECONCILE_PRIORITY
                or action.status == ExternalActionStatus.PREPARED
            )
            if (
                not reconciliation_pending
                and action.status.is_terminal
                and not include_terminal
            ):
                return

        try:
            self._mirror_evidence(context.run_id)
        except Exception:
            pass
        if reconciliation_pending or action is None:
            raise ExternalActionReconciliationPendingError()
        if action.status == ExternalActionStatus.DISPATCHING:
            step = self.workflow_store.get_step(context.run_id, action.step_id)
            if step is None:
                raise ExternalActionReconciliationPendingError()
            self._fail_external_dispatch_binding(
                context=context,
                step=step,
                action=action,
            )
        elif action.status == ExternalActionStatus.OUTCOME_UNKNOWN:
            code = "external_action_outcome_unknown"
        elif action.status == ExternalActionStatus.SUCCEEDED:
            code = "external_action_evidence_incomplete"
        elif action.status == ExternalActionStatus.FAILED:
            code = "external_action_failed"
        else:
            # PREPARED with dispatch_count > 0 is corrupt and cannot prove that
            # a provider call did not happen.  It also cannot be fenced into a
            # terminal outcome, so keep the Run recoverable for reconciliation.
            raise ExternalActionReconciliationPendingError()

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
        raise RuntimeExecutionError(code, self.failure_message(code))

    def execute(
        self,
        *,
        context: RuntimeExecutionContext,
        tool_name: str,
        spec: ToolSpec,
        step_id: str,
        normalized_arguments: dict[str, Any],
    ) -> ToolObservation:
        provider_name = spec.provider_name
        if provider_name is None or self.dispatcher is None:
            # Authorization performs this check before a step is claimed. Keep
            # the execution boundary fail-closed if a caller invokes it
            # independently or mutates server configuration between phases.
            self._fail(
                context.run_id,
                "external_action_not_configured",
                "External action provider is not configured.",
            )
        provider = self.dispatcher.registry.resolve(provider_name)
        if provider is None:
            self._fail(
                context.run_id,
                "external_action_not_configured",
                "External action provider is not configured.",
            )
        provider_identity = provider.provider_identity

        input_hash = self._stable_hash(normalized_arguments)
        arguments_json = self.canonical_json(normalized_arguments)
        idempotency_key = self.idempotency_key(
            context=context,
            step_id=step_id,
            tool_name=tool_name,
            input_hash=input_hash,
        )
        existing = self.workflow_store.get_step(context.run_id, step_id)
        if existing is not None and existing.status == ToolCallStatus.COMPLETED:
            return self.restore_success(
                context=context,
                step=existing,
                spec=spec,
                normalized_arguments=normalized_arguments,
                input_hash=input_hash,
                idempotency_key=idempotency_key,
            )
        if existing is not None and existing.status == ToolCallStatus.FAILED:
            self.restore_failure(
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
                raise ExternalActionReconciliationPendingError()
            step = existing
            attempt_token = existing.attempt_token
            recovered_dispatch = True
        else:
            claim = self.workflow_store.claim_step(
                context.run_id,
                step_id,
                tool_name,
                input_hash,
                max_attempts=1,
            )
            if claim.outcome == ClaimOutcome.CACHED:
                assert claim.step is not None
                return self.restore_success(
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
                tool_name=tool_name,
                provider_name=provider_name,
                provider_identity=provider_identity,
                input_hash=input_hash,
                arguments_json=arguments_json,
                retry_mode=spec.retry_mode.value,
                idempotency_key=idempotency_key,
            )
        except Exception:
            if recovered_dispatch:
                # The existing RUNNING step may already own a DISPATCHING
                # action.  If its ledger row cannot be read or validated, keep
                # both Workflow and Run non-terminal for startup recovery.
                raise ExternalActionReconciliationPendingError() from None
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
                    self.failure_message("run_cancel_requested"),
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
                    f"External action dispatch could not be claimed: {dispatch.outcome.value}.",
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
            raise ExternalActionReconciliationPendingError()
        if spec.retry_mode == ToolRetryMode.UNSAFE:
            self._raise_external_outcome_unknown(
                context=context,
                step=step,
                dispatch_token=dispatch_token,
                attempt_token=attempt_token,
                finalizer=lambda: self.workflow_store.finalize_unsafe_interrupted_action(
                    context.run_id,
                    step.step_id,
                    dispatch_token=dispatch_token,
                    tool_attempt_token=attempt_token,
                ),
            )
        if action.dispatch_count >= self.max_dispatches:
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
                f"Persisted external action could not be safely recovered: {retry.outcome.value}.",
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
        assert self.dispatcher is not None

        while True:
            provider = self.dispatcher.registry.resolve(spec.provider_name)
            if provider is None or provider.provider_identity != action.provider_identity:
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
                provider_result: ExternalActionProviderResult = self.dispatcher.dispatch(
                    provider_name=spec.provider_name,
                    retry_mode=spec.retry_mode,
                    request=request,
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
                            dispatch_token=dispatch_token,
                            attempt_token=attempt_token,
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
                    raise ValueError("Provider output contains a runtime idempotency key")
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
                result_json = self.canonical_json(trusted_result)
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
                        dispatch_token=dispatch_token,
                        attempt_token=attempt_token,
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
        if spec.retry_mode == ToolRetryMode.UNSAFE or action.dispatch_count >= self.max_dispatches:
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
            dispatch_token=dispatch_token,
            attempt_token=attempt_token,
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
            self.failure_message("external_action_evidence_incomplete"),
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
        raise RuntimeExecutionError(error_code, self.failure_message(error_code))

    def _raise_external_outcome_unknown(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        dispatch_token: str,
        attempt_token: str,
        finalizer: Callable[[], Any],
    ) -> NoReturn:
        try:
            finalizer()
        except Exception:
            # A wrapper or connection cleanup can raise after SQLite committed.
            # Trust only an exact terminal read-back.  If the action is still
            # DISPATCHING, do not terminalize the Workflow: doing so would make
            # the in-flight action unreachable by startup recovery.
            if not self._external_outcome_unknown_was_committed(
                context=context,
                step=step,
                dispatch_token=dispatch_token,
                attempt_token=attempt_token,
            ):
                raise ExternalActionReconciliationPendingError() from None
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
            self.failure_message("external_action_outcome_unknown"),
        )

    def _external_outcome_unknown_was_committed(
        self,
        *,
        context: RuntimeExecutionContext,
        step: ToolCallRecord,
        dispatch_token: str,
        attempt_token: str,
    ) -> bool:
        """Recognize an exception raised after the unknown terminal commit."""

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
        return bool(
            action is not None
            and current_step is not None
            and action.run_id == context.run_id
            and action.step_id == step.step_id
            and action.tool_name == step.tool_name
            and action.input_hash == step.input_hash
            and action.status == ExternalActionStatus.OUTCOME_UNKNOWN
            and action.dispatch_token == dispatch_token
            and action.provider_reference is None
            and action.result_json is None
            and action.error_code == "external_action_outcome_unknown"
            and current_step.run_id == context.run_id
            and current_step.step_id == step.step_id
            and current_step.tool_name == step.tool_name
            and current_step.input_hash == step.input_hash
            and current_step.status == ToolCallStatus.FAILED
            and current_step.attempt_token == attempt_token
            and current_step.result_json is None
            and current_step.error_code == "external_action_outcome_unknown"
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
            return self.restore_success(
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
            self.restore_failure(
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

    def restore_success(
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
            self.fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Completed external-write step has an invalid action outcome.",
            )
        try:
            result = self._decode_tool_result(step)
        except RuntimeExecutionError as exc:
            self.fail_restored_step(
                context=context,
                action=action,
                code=exc.code,
                detail=str(exc),
            )
        if spec.output_model is None:  # pragma: no cover - registry invariant
            self.fail_restored_step(
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
            self.fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail=("Persisted external action result no longer passes output validation."),
            )
        if normalized_result != result:
            self.fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Persisted external action result is not canonical.",
            )
        if self._contains_sensitive_text(result, idempotency_key):
            self.fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="Persisted external action result contains its idempotency key.",
            )
        if action.result_json != self.canonical_json(result):
            self.fail_restored_step(
                context=context,
                action=action,
                code="invalid_planner_decision",
                detail="External action result does not match its completed tool step.",
            )
        if result.get("provider_reference") != action.provider_reference:
            self.fail_restored_step(
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

    def restore_failure(
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
            self.fail_restored_step(
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
            self.canonical_json(normalized_arguments),
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
                self.reconcile_dispatched_action(
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
            raise ExternalActionReconciliationPendingError()
        dispatch_token = action.dispatch_token
        attempt_token = step.attempt_token
        self._raise_external_outcome_unknown(
            context=context,
            step=step,
            dispatch_token=dispatch_token,
            attempt_token=attempt_token,
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

    def idempotency_key(
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

    canonical_json = staticmethod(canonical_json)

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
                return any(contains(key) or contains(item) for key, item in candidate.items())
            if isinstance(candidate, (list, tuple, set, frozenset)):
                identity = id(candidate)
                if identity in seen:
                    return False
                seen.add(identity)
                return any(contains(item) for item in candidate)
            return False

        return contains(value)

    def _fail(
        self,
        run_id: str,
        code: str,
        unsafe_detail: str,
    ) -> NoReturn:
        self.fail(run_id, code, unsafe_detail)

    def _mirror_evidence(self, run_id: str) -> None:
        self.evidence_projector.mirror(run_id)

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

    def _fail_restored_step(
        self,
        *,
        context: RuntimeExecutionContext,
        action: ExternalActionRecord | None,
        code: str,
        detail: str,
    ) -> NoReturn:
        if action is not None and action.dispatch_count > 0:
            self.reconcile_dispatched_action(
                context=context,
                action=action,
            )
        self._fail(context.run_id, code, detail)

    def fail_restored_step(
        self,
        *,
        context: RuntimeExecutionContext,
        action: ExternalActionRecord | None,
        code: str,
        detail: str,
    ) -> NoReturn:
        """Delegate through the pre-extraction hook for compatibility."""

        self._fail_restored_step(
            context=context,
            action=action,
            code=code,
            detail=detail,
        )

    _decode_tool_result = staticmethod(decode_tool_result)

    _stable_hash = staticmethod(stable_hash)
