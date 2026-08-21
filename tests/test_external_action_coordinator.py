from __future__ import annotations

from typing import Any, NoReturn

import pytest

from agent.contracts import (
    RuntimeExecutionAuthority,
    RuntimeExecutionContext,
    RuntimeExecutionError,
)
from runtime_service.evidence import EvidenceProjector
from runtime_service.external_action_coordinator import ExternalActionCoordinator
from runtime_service.external_actions import ExternalActionReconciliationPendingError
from runtime_service.sandbox import ToolRetryMode
from runtime_service.workflow_store import ExternalActionRecord, ExternalActionStatus


RUN_ID = "run-external-action-coordinator"


class StubWorkflowStore:
    """Only the surface `reconcile_dispatched_action` touches."""

    def __init__(self, actions: list[ExternalActionRecord]) -> None:
        self.actions = actions

    def list_external_actions(self, _run_id: str) -> list[ExternalActionRecord]:
        return list(self.actions)

    def list_events(self, _run_id: str, *, after_sequence: int = 0) -> list[Any]:
        del after_sequence
        return []


class StubRunEventSink:
    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        del run_id, event_type, payload

    def list_events(self, _run_id: str, *, after_sequence: int = 0) -> list[Any]:
        del after_sequence
        return []


def execution_context() -> RuntimeExecutionContext:
    return RuntimeExecutionContext(
        run_id=RUN_ID,
        thread_id=f"thread-{RUN_ID}",
        recovered_after_restart=False,
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-generic",
            subject_id="subject-generic",
            permissions=("external-actions:execute", "tools:execute"),
        ),
    )


def build_coordinator(
    *,
    actions: list[ExternalActionRecord] | None = None,
    failure_messages: dict[str, str] | None = None,
    host_fail_code: str | None = None,
) -> ExternalActionCoordinator:
    store = StubWorkflowStore(actions or [])

    def fail(
        context: RuntimeExecutionContext,
        code: str,
        detail: str,
    ) -> NoReturn:
        del context, detail
        # host_fail_code models a host whose own terminalization diverges, which
        # is what makes the coordinator fall through to its own message.
        raise RuntimeExecutionError(host_fail_code or code, "stub failure")

    return ExternalActionCoordinator(
        workflow_store=store,  # type: ignore[arg-type]
        dispatcher=None,
        workflow_type="generic-external-action:1.0.0",
        evidence_projector=EvidenceProjector(
            workflow_store=store,  # type: ignore[arg-type]
            run_event_sink=StubRunEventSink(),
        ),
        fail=fail,
        failure_messages=failure_messages if failure_messages is not None else {},
    )


def dispatched_action(status: ExternalActionStatus | str) -> ExternalActionRecord:
    # model_construct bypasses validation so an unranked status can be simulated
    # without first adding one to the enum.
    return ExternalActionRecord.model_construct(
        action_id="action_generic",
        run_id=RUN_ID,
        step_id="call-0001",
        tenant_id="tenant-generic",
        subject_id="subject-generic",
        workflow_type="generic-external-action:1.0.0",
        tool_name="create_hold",
        provider_name="trip-hold",
        provider_identity="trip-hold:test",
        input_hash="input-hash",
        arguments_json="{}",
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        idempotency_key="external_action_generic",
        status=status,
        dispatch_count=1,
        dispatch_token="dispatch-token",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )


def test_every_action_status_is_explicitly_ranked_for_reconciliation():
    assert set(ExternalActionCoordinator._RECONCILE_PRIORITY) == set(ExternalActionStatus)


@pytest.mark.parametrize("explicit_action", [False, True])
@pytest.mark.parametrize("include_terminal", [False, True])
def test_unranked_action_status_mirrors_before_staying_reconciliation_pending(
    explicit_action: bool,
    include_terminal: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    action = dispatched_action("quarantined")
    coordinator = build_coordinator(actions=[] if explicit_action else [action])
    mirror_calls: list[str] = []
    monkeypatch.setattr(
        coordinator,
        "_mirror_evidence",
        lambda context: mirror_calls.append(context.run_id),
    )

    # An unranked status cannot prove a safe terminal outcome, whether it is
    # discovered from the run ledger or supplied by an existing recovery path.
    with pytest.raises(ExternalActionReconciliationPendingError):
        coordinator.reconcile_dispatched_action(
            context=execution_context(),
            action=action if explicit_action else None,
            include_terminal=include_terminal,
        )
    assert mirror_calls == [RUN_ID]


@pytest.mark.parametrize("include_terminal", [False, True])
@pytest.mark.parametrize("blocking_first", [False, True])
@pytest.mark.parametrize(
    "blocking_status",
    ["quarantined", ExternalActionStatus.PREPARED],
)
def test_unreconcilable_status_blocks_a_terminal_sibling(
    include_terminal: bool,
    blocking_first: bool,
    blocking_status: ExternalActionStatus | str,
    monkeypatch: pytest.MonkeyPatch,
):
    blocking = dispatched_action(blocking_status)
    terminal = dispatched_action(ExternalActionStatus.SUCCEEDED)
    actions = [blocking, terminal] if blocking_first else [terminal, blocking]
    coordinator = build_coordinator(
        actions=actions,
    )
    mirror_calls: list[str] = []
    monkeypatch.setattr(
        coordinator,
        "_mirror_evidence",
        lambda context: mirror_calls.append(context.run_id),
    )

    # A known terminal sibling must not hide either a status the runtime cannot
    # understand or a PREPARED row that corruptly claims a dispatch occurred.
    with pytest.raises(ExternalActionReconciliationPendingError):
        coordinator.reconcile_dispatched_action(
            context=execution_context(),
            include_terminal=include_terminal,
        )
    assert mirror_calls == [RUN_ID]


@pytest.mark.parametrize("explicit_action", [False, True])
def test_mirror_failure_cannot_replace_reconciliation_pending(
    explicit_action: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    action = dispatched_action("quarantined")
    coordinator = build_coordinator(actions=[] if explicit_action else [action])

    def fail_mirror(_context: RuntimeExecutionContext) -> None:
        raise OSError("public evidence unavailable")

    monkeypatch.setattr(coordinator, "_mirror_evidence", fail_mirror)

    with pytest.raises(ExternalActionReconciliationPendingError):
        coordinator.reconcile_dispatched_action(
            context=execution_context(),
            action=action if explicit_action else None,
        )


@pytest.mark.parametrize(
    ("actions", "action", "include_terminal"),
    [
        (
            [
                dispatched_action(ExternalActionStatus.PREPARED).model_copy(
                    update={"dispatch_count": 0, "dispatch_token": None}
                )
            ],
            None,
            True,
        ),
        ([dispatched_action(ExternalActionStatus.SUCCEEDED)], None, False),
        ([], dispatched_action(ExternalActionStatus.SUCCEEDED), False),
    ],
)
def test_reconciliation_early_returns_do_not_mirror(
    actions: list[ExternalActionRecord],
    action: ExternalActionRecord | None,
    include_terminal: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    coordinator = build_coordinator(actions=actions)

    def unexpected_mirror(_context: RuntimeExecutionContext) -> None:
        raise AssertionError("early-return reconciliation must not mirror")

    monkeypatch.setattr(coordinator, "_mirror_evidence", unexpected_mirror)

    coordinator.reconcile_dispatched_action(
        context=execution_context(),
        action=action,
        include_terminal=include_terminal,
    )


def test_fail_restored_step_delegates_through_the_private_compatibility_hook():
    coordinator = build_coordinator()

    def overridden_hook(**_kwargs: Any) -> NoReturn:
        raise RuntimeExecutionError("private_hook_called", "override reached")

    coordinator._fail_restored_step = overridden_hook  # type: ignore[method-assign]

    with pytest.raises(RuntimeExecutionError) as raised:
        coordinator.fail_restored_step(
            context=execution_context(),
            action=None,
            code="tool_execution_failed",
            detail="test compatibility hook",
        )

    assert raised.value.code == "private_hook_called"


@pytest.mark.parametrize(
    "code",
    sorted(ExternalActionCoordinator.DEFAULT_FAILURE_MESSAGES),
)
def test_missing_host_failure_message_falls_back_instead_of_raising(code: str):
    coordinator = build_coordinator(failure_messages={})

    message = coordinator.failure_message(code)

    assert message == ExternalActionCoordinator.DEFAULT_FAILURE_MESSAGES[code]


def test_host_failure_messages_still_take_precedence():
    coordinator = build_coordinator(
        failure_messages={"external_action_failed": "host-owned message"},
    )

    assert coordinator.failure_message("external_action_failed") == "host-owned message"
    assert coordinator.failure_message("totally_unknown_code") == "External action failed."


def test_terminal_reconciliation_uses_a_message_the_host_did_not_supply():
    coordinator = build_coordinator(
        actions=[dispatched_action(ExternalActionStatus.OUTCOME_UNKNOWN)],
        failure_messages={},
        host_fail_code="tool_execution_failed",
    )

    with pytest.raises(RuntimeExecutionError) as raised:
        coordinator.reconcile_dispatched_action(context=execution_context())

    assert raised.value.code == "external_action_outcome_unknown"
    assert str(raised.value) == (
        ExternalActionCoordinator.DEFAULT_FAILURE_MESSAGES["external_action_outcome_unknown"]
    )
