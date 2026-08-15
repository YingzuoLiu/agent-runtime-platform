from __future__ import annotations

import hashlib
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
    TenantContext,
)
from runtime_service.action_gateway import action_thread_id
from runtime_service.external_actions import (
    AmbiguousExternalActionError,
    DefinitiveExternalActionError,
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.models import RunStatus
from runtime_service.workflow_store import ExternalActionStatus, SQLiteWorkflowStore


API_KEY = "action-recovery-key"
TENANT_ID = "action-recovery-tenant"
SUBJECT_ID = "action-recovery-subject"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="action-recovery-credential",
            api_key=API_KEY,
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            role=RuntimeRole.OPERATOR,
        )
    ]
)


def headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {API_KEY}"}


def payload(key: str = "recovery-key", *, text: str = "hello") -> dict:
    return {
        "action_type": "webhook.send",
        "destination": "demo",
        "idempotency_key": key,
        "input": {"payload": {"text": text}},
    }


def wait_for_action(
    client: TestClient,
    action_id: str,
    expected: set[str],
    *,
    timeout: float = 8.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/actions/{action_id}")
        assert response.status_code == 200, response.text
        action = response.json()
        if action["status"] in expected:
            return action
        time.sleep(0.02)
    raise AssertionError(f"Action did not reach {expected}: {action_id}")


def wait_for_terminal_run(run_store, action_id: str, *, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = run_store.get_run_internal(action_id)
        if run is not None and run.status.is_terminal:
            return run
        time.sleep(0.02)
    raise AssertionError(f"Run did not reach a terminal status: {action_id}")


class BaseProvider:
    provider_identity = "action-recovery-provider-v1"

    def __init__(self, *, supports_idempotency: bool) -> None:
        self.supports_idempotency = supports_idempotency
        self.requests: list[ExternalActionRequest] = []
        self._lock = threading.Lock()

    def record(self, request: ExternalActionRequest) -> None:
        with self._lock:
            self.requests.append(request)

    @staticmethod
    def result(request: ExternalActionRequest) -> ExternalActionProviderResult:
        reference = "delivery_" + hashlib.sha256(
            request.idempotency_key.encode("utf-8")
        ).hexdigest()[:16]
        return ExternalActionProviderResult(
            provider_reference=reference,
            result={},
        )


class SuccessProvider(BaseProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        return self.result(request)


class CommitThenAmbiguousProvider(BaseProvider):
    def __init__(self) -> None:
        super().__init__(supports_idempotency=True)
        self.effects: dict[str, ExternalActionProviderResult] = {}

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        existing = self.effects.get(request.idempotency_key)
        if existing is not None:
            return existing
        self.effects[request.idempotency_key] = self.result(request)
        raise AmbiguousExternalActionError()


class AmbiguousProvider(BaseProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        raise AmbiguousExternalActionError()


class DefinitiveFailureProvider(BaseProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        raise DefinitiveExternalActionError()


class LeakyProvider(BaseProvider):
    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        return ExternalActionProviderResult(
            provider_reference="delivery_leaky",
            result={
                "token": "provider-secret-token",
                "url": "https://private-provider.example/actions",
            },
        )


class BlockingProvider(BaseProvider):
    def __init__(self, *, outcome: str) -> None:
        super().__init__(supports_idempotency=False)
        self.outcome = outcome
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.record(request)
        self.started.set()
        if not self.release.wait(timeout=8):
            raise AssertionError("test did not release the provider")
        if self.outcome == "success":
            return self.result(request)
        if self.outcome == "failed":
            raise DefinitiveExternalActionError()
        raise AmbiguousExternalActionError()


def test_provider_capability_selects_retry_and_public_terminal_status(tmp_path):
    idempotent = CommitThenAmbiguousProvider()
    unsafe = AmbiguousProvider(supports_idempotency=False)
    definitive = DefinitiveFailureProvider(supports_idempotency=False)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={
            "demo": idempotent,
            "unsafe": unsafe,
            "definitive": definitive,
        },
    )

    with TestClient(app, headers=headers()) as client:
        succeeded = client.post("/actions?wait=5", json=payload("idempotent"))
        unknown_payload = payload("unsafe")
        unknown_payload["destination"] = "unsafe"
        unknown = client.post("/actions?wait=5", json=unknown_payload)
        failed_payload = payload("definitive")
        failed_payload["destination"] = "definitive"
        failed = client.post("/actions?wait=5", json=failed_payload)

    assert succeeded.status_code == 200
    assert succeeded.json()["status"] == "succeeded"
    assert len(idempotent.requests) == 2
    assert idempotent.requests[0].idempotency_key == idempotent.requests[1].idempotency_key
    assert len(idempotent.effects) == 1

    assert unknown.status_code == 200
    assert unknown.json()["status"] == "outcome_unknown"
    assert unknown.json()["error_code"] == "external_action_outcome_unknown"
    assert len(unsafe.requests) == 1

    assert failed.status_code == 200
    assert failed.json()["status"] == "failed"
    assert failed.json()["error_code"] == "external_action_failed"
    assert len(definitive.requests) == 1


def test_terminal_unknown_duplicate_post_never_dispatches_again(tmp_path):
    provider = AmbiguousProvider(supports_idempotency=False)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        first = client.post("/actions?wait=5", json=payload())
        repeated = client.post("/actions?wait=5", json=payload())
        ledger = client.app.state.workflow_store.list_external_actions(
            first.json()["action_id"]
        )

    assert first.json()["status"] == "outcome_unknown"
    assert repeated.json()["action_id"] == first.json()["action_id"]
    assert repeated.json()["status"] == "outcome_unknown"
    assert len(provider.requests) == 1
    assert ledger[0].dispatch_count == 1


@pytest.mark.parametrize(
    ("supports_idempotency", "expected_status", "expected_calls"),
    [(True, "succeeded", 2), (False, "outcome_unknown", 1)],
)
def test_pending_dispatch_recovers_safely_after_restart(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    supports_idempotency: bool,
    expected_status: str,
    expected_calls: int,
):
    database_path = tmp_path / "runtime.db"
    provider = SuccessProvider(supports_idempotency=supports_idempotency)
    first_app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(first_app, headers=headers()) as first_client:
        workflow_store = first_client.app.state.workflow_store

        def fail_terminal_write(*_args, **_kwargs):
            raise sqlite3.OperationalError("injected terminal write failure")

        monkeypatch.setattr(
            workflow_store,
            "finalize_external_action_succeeded",
            fail_terminal_write,
        )
        monkeypatch.setattr(
            workflow_store,
            "finalize_external_action_outcome_unknown",
            fail_terminal_write,
        )
        submitted = first_client.post("/actions", json=payload())
        action_id = submitted.json()["action_id"]
        pending = wait_for_action(first_client, action_id, {"reconciling"})
        first_ledger = workflow_store.list_external_actions(action_id)

    assert pending["status"] == "reconciling"
    assert len(provider.requests) == 1
    assert first_ledger[0].status == ExternalActionStatus.DISPATCHING
    assert first_ledger[0].dispatch_count == 1

    recovered_app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )
    with TestClient(recovered_app, headers=headers()) as recovered_client:
        recovered = wait_for_action(
            recovered_client,
            action_id,
            {"succeeded", "outcome_unknown"},
        )
        repeated = recovered_client.post("/actions?wait=5", json=payload())
        final_ledger = recovered_client.app.state.workflow_store.list_external_actions(
            action_id
        )

    assert recovered["status"] == expected_status
    assert repeated.json()["action_id"] == action_id
    assert repeated.json()["status"] == expected_status
    assert len(provider.requests) == expected_calls
    assert all(
        request.idempotency_key == provider.requests[0].idempotency_key
        for request in provider.requests
    )
    assert final_ledger[0].dispatch_count == expected_calls


def test_cancellation_before_dispatch_is_cancelled_with_zero_provider_calls(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    provider = SuccessProvider(supports_idempotency=True)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        workflow_store = client.app.state.workflow_store
        original_begin = workflow_store.begin_external_action_dispatch

        def cancel_then_begin(run_id: str, step_id: str, *, tool_attempt_token: str):
            client.app.state.runtime_manager.request_cancel(
                run_id,
                tenant_context=TenantContext(
                    tenant_id=TENANT_ID,
                    subject_id=SUBJECT_ID,
                ),
            )
            return original_begin(
                run_id,
                step_id,
                tool_attempt_token=tool_attempt_token,
            )

        monkeypatch.setattr(
            workflow_store,
            "begin_external_action_dispatch",
            cancel_then_begin,
        )
        response = client.post("/actions?wait=5", json=payload())
        ledger = workflow_store.list_external_actions(response.json()["action_id"])

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert provider.requests == []
    assert ledger[0].status == ExternalActionStatus.PREPARED
    assert ledger[0].dispatch_count == 0


@pytest.mark.parametrize(
    ("provider_outcome", "expected_action_status", "expected_run_status"),
    [
        ("success", "succeeded", RunStatus.CANCELLED),
        ("failed", "failed", RunStatus.FAILED),
        ("unknown", "outcome_unknown", RunStatus.FAILED),
    ],
)
def test_cancellation_after_dispatch_never_masks_provider_outcome(
    tmp_path,
    provider_outcome: str,
    expected_action_status: str,
    expected_run_status: RunStatus,
):
    provider = BlockingProvider(outcome=provider_outcome)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        submitted = client.post("/actions", json=payload())
        action_id = submitted.json()["action_id"]
        assert provider.started.wait(timeout=5)
        client.app.state.runtime_manager.request_cancel(
            action_id,
            tenant_context=TenantContext(
                tenant_id=TENANT_ID,
                subject_id=SUBJECT_ID,
            ),
        )
        during_dispatch = client.get(f"/actions/{action_id}").json()
        provider.release.set()
        action = wait_for_action(
            client,
            action_id,
            {"succeeded", "failed", "outcome_unknown"},
        )
        run = wait_for_terminal_run(client.app.state.run_store, action_id)

    assert during_dispatch["status"] == "running"
    assert action["status"] == expected_action_status
    assert run.status == expected_run_status
    assert len(provider.requests) == 1


def test_succeeded_ledger_stays_authoritative_after_later_run_failure(tmp_path):
    provider = SuccessProvider(supports_idempotency=True)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]
        run = client.app.state.run_store.get_run_internal(action_id)
        assert run is not None
        client.app.state.run_store.update_run(
            run.model_copy(
                update={
                    "status": RunStatus.FAILED,
                    "error_code": "runtime_execution_failed",
                    "error": "secret traceback from a later mirror failure",
                }
            )
        )
        projected = client.get(f"/actions/{action_id}")

    assert projected.status_code == 200
    assert projected.json()["status"] == "succeeded"
    assert projected.json()["error_code"] is None
    assert "secret traceback" not in projected.text


def test_dispatched_action_with_conflicting_workflow_identity_fails_safe(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = SuccessProvider(supports_idempotency=True)
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "UPDATE workflow_executions SET input_hash = ? WHERE run_id = ?",
                ("tampered-workflow-input", action_id),
            )
        projected = client.get(f"/actions/{action_id}")
        events = client.get(f"/actions/{action_id}/events")

    assert projected.status_code == 200
    assert projected.json()["status"] == "outcome_unknown"
    assert projected.json()["result"] is None
    assert events.status_code == 500
    assert events.json()["error"]["code"] == "action_evidence_incomplete"
    assert len(provider.requests) == 1


def test_completed_run_without_action_evidence_returns_stable_500(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": SuccessProvider(supports_idempotency=True)},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]
        with sqlite3.connect(database_path) as connection:
            connection.execute("DELETE FROM external_actions WHERE run_id = ?", (action_id,))
        projected = client.get(f"/actions/{action_id}")

    assert projected.status_code == 500
    assert projected.json() == {
        "error": {
            "code": "action_evidence_incomplete",
            "message": "The Action's durable evidence is incomplete.",
        }
    }


def test_provider_extras_are_never_persisted_or_exposed(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = LeakyProvider(supports_idempotency=True)
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        response = client.post("/actions?wait=5", json=payload())
        action_id = response.json()["action_id"]
        events = client.get(f"/actions/{action_id}/events")
        ledger = SQLiteWorkflowStore(database_path).list_external_actions(action_id)

    assert response.json()["status"] == "outcome_unknown"
    assert response.json()["result"] is None
    assert len(provider.requests) == 2
    assert ledger[0].result_json is None
    encoded = response.text + events.text
    assert "provider-secret-token" not in encoded
    assert "private-provider.example" not in encoded
    assert provider.requests[0].idempotency_key not in encoded


def test_action_events_use_authoritative_sequences_and_after_cursor(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": SuccessProvider(supports_idempotency=True)},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]
        public_events = client.get(f"/actions/{action_id}/events").json()
        cursor = public_events[0]["sequence"]
        after = client.get(
            f"/actions/{action_id}/events",
            params={"after_sequence": cursor},
        ).json()

    authoritative = [
        event
        for event in SQLiteWorkflowStore(database_path).list_events(action_id)
        if event.event_type.startswith("external_action.")
    ]
    assert [event["sequence"] for event in public_events] == [
        event.sequence for event in authoritative
    ]
    assert [event["event_type"] for event in public_events] == [
        event.event_type for event in authoritative
    ]
    assert after == public_events[1:]


def test_action_events_fail_closed_when_terminal_evidence_conflicts(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": SuccessProvider(supports_idempotency=True)},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]
        workflow_store = SQLiteWorkflowStore(database_path)
        action = workflow_store.list_external_actions(action_id)[0]
        workflow_store.append_event(
            action_id,
            "external_action.failed",
            {
                "evidence_id": f"action:{action.action_id}:tampered-outcome",
                "action_id": action.action_id,
                "step_id": action.step_id,
                "tool_name": action.tool_name,
                "provider_name": action.provider_name,
                "status": "failed",
                "dispatch_count": action.dispatch_count,
                "provider_reference": None,
                "error_code": "external_action_failed",
            },
        )
        events = client.get(f"/actions/{action_id}/events")

    assert events.status_code == 500
    assert events.json() == {
        "error": {
            "code": "action_evidence_incomplete",
            "message": "The Action's durable evidence is incomplete.",
        }
    }


def test_action_thread_is_stable_private_and_key_scoped_across_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = SuccessProvider(supports_idempotency=True)
    first_app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(first_app, headers=headers()) as first_client:
        first = first_client.post("/actions?wait=5", json=payload("stable-thread"))
        first_id = first.json()["action_id"]
        first_run = first_client.app.state.run_store.get_run_internal(first_id)

    second_app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )
    with TestClient(second_app, headers=headers()) as second_client:
        repeated = second_client.post(
            "/actions?wait=5",
            json=payload("stable-thread"),
        )
        other = second_client.post("/actions?wait=5", json=payload("other-thread"))
        repeated_run = second_client.app.state.run_store.get_run_internal(
            repeated.json()["action_id"]
        )
        other_run = second_client.app.state.run_store.get_run_internal(
            other.json()["action_id"]
        )

    assert first_run is not None and repeated_run is not None and other_run is not None
    assert repeated.json()["action_id"] == first_id
    assert first_run.thread_id == repeated_run.thread_id
    assert first_run.thread_id == action_thread_id(
        TENANT_ID,
        "webhook.send",
        "stable-thread",
    )
    assert other_run.thread_id != first_run.thread_id
    assert "thread_id" not in repeated.json()
