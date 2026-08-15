from __future__ import annotations

import hashlib
import threading

from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    AuthorizationError,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.action_gateway import (
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    action_client_request_id,
)
from runtime_service.external_actions import (
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.workflow_store import SQLiteWorkflowStore


OPERATOR_A_KEY = "action-operator-a"
VIEWER_A_KEY = "action-viewer-a"
OPERATOR_B_KEY = "action-operator-b"
TENANT_A = "action-tenant-a"
TENANT_B = "action-tenant-b"


AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="action-operator-a",
            api_key=OPERATOR_A_KEY,
            tenant_id=TENANT_A,
            subject_id="action-subject-a",
            role=RuntimeRole.OPERATOR,
        ),
        ApiKeyCredential(
            credential_id="action-viewer-a",
            api_key=VIEWER_A_KEY,
            tenant_id=TENANT_A,
            subject_id="action-viewer-subject-a",
            role=RuntimeRole.VIEWER,
        ),
        ApiKeyCredential(
            credential_id="action-operator-b",
            api_key=OPERATOR_B_KEY,
            tenant_id=TENANT_B,
            subject_id="action-subject-b",
            role=RuntimeRole.OPERATOR,
        ),
    ]
)


class RecordingWebhookProvider:
    provider_identity = "webhook-provider-test-v1"

    def __init__(self, *, supports_idempotency: bool = True) -> None:
        self.supports_idempotency = supports_idempotency
        self.requests: list[ExternalActionRequest] = []
        self._lock = threading.Lock()

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        with self._lock:
            self.requests.append(request)
        reference = "delivery_" + hashlib.sha256(
            request.idempotency_key.encode("utf-8")
        ).hexdigest()[:16]
        return ExternalActionProviderResult(
            provider_reference=reference,
            result={},
        )


def headers(key: str = OPERATOR_A_KEY) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def payload(
    *,
    key: str = "agent-job-42",
    destination: str = "demo",
    text: str = "hello",
    action_type: str = "webhook.send",
) -> dict:
    return {
        "action_type": action_type,
        "destination": destination,
        "idempotency_key": key,
        "input": {"payload": {"text": text}},
    }


def test_webhook_action_uses_private_run_lifecycle_and_safe_events(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = RecordingWebhookProvider()
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        response = client.post("/actions?wait=5", json=payload())
        assert response.status_code == 200, response.text
        action = response.json()
        fetched = client.get(f"/actions/{action['action_id']}")
        events = client.get(f"/actions/{action['action_id']}/events")
        agents = client.get("/agents")
        hidden_run = client.get(f"/runs/{action['action_id']}")
        hidden_events = client.get(f"/runs/{action['action_id']}/events")
        hidden_cancel = client.post(f"/runs/{action['action_id']}/cancel")
        internal_run = client.app.state.run_store.get_run_internal(action["action_id"])
        assert internal_run is not None
        hidden_thread = client.get(
            f"/threads/{internal_run.thread_id}/state",
            params={"domain_id": "durable-action", "schema_version": "1"},
        )

    assert action == fetched.json()
    assert action["status"] == "succeeded"
    assert action["result"]["provider_reference"].startswith("delivery_")
    assert "run_id" not in action
    assert "thread_id" not in action
    assert "client_request_id" not in action
    assert len(provider.requests) == 1
    assert provider.requests[0].tool_name == "webhook.send"
    assert provider.requests[0].arguments == {"payload": {"text": "hello"}}
    assert provider.requests[0].idempotency_key != "agent-job-42"

    assert [event["event_type"] for event in events.json()] == [
        "external_action.prepared",
        "external_action.dispatch_started",
        "external_action.succeeded",
    ]
    assert all(event["destination"] == "demo" for event in events.json())
    assert "action_id" not in events.json()[0]
    assert "step_id" not in events.json()[0]
    assert "provider_identity" not in events.text
    assert provider.requests[0].idempotency_key not in events.text

    assert (ACTION_AGENT_ID, ACTION_AGENT_VERSION) not in {
        (item["agent_id"], item["version"]) for item in agents.json()
    }
    assert hidden_run.status_code == 404
    assert hidden_events.status_code == 404
    assert hidden_cancel.status_code == 404
    assert hidden_thread.status_code == 404

    ledger = SQLiteWorkflowStore(database_path).list_external_actions(
        action["action_id"]
    )
    assert len(ledger) == 1
    assert ledger[0].retry_mode.value == "provider_idempotent"
    assert ledger[0].dispatch_count == 1


def test_exact_duplicate_returns_one_action_and_one_dispatch(tmp_path):
    provider = RecordingWebhookProvider()
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        first = client.post("/actions?wait=5", json=payload())
        second = client.post(
            "/actions?wait=5",
            json={
                "destination": "demo",
                "action_type": "webhook.send",
                "input": {"payload": {"text": "hello"}},
                "idempotency_key": "agent-job-42",
            },
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["action_id"] == second.json()["action_id"]
    assert len(provider.requests) == 1


def test_same_key_with_different_request_returns_conflict(tmp_path):
    provider = RecordingWebhookProvider()
    other = RecordingWebhookProvider()
    other.provider_identity = "webhook-provider-other-v1"
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider, "other": other},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        changed_input = client.post("/actions", json=payload(text="different"))
        changed_destination = client.post(
            "/actions",
            json=payload(destination="other"),
        )
        changed_type = client.post(
            "/actions",
            json=payload(action_type="email.send"),
        )

    assert created.status_code == 200
    for response in (changed_input, changed_destination, changed_type):
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "idempotency_key_reused"
    assert len(provider.requests) == 1
    assert other.requests == []


def test_unknown_type_and_destination_fail_before_run_creation(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": RecordingWebhookProvider()},
    )

    with TestClient(app, headers=headers()) as client:
        unknown_type = client.post(
            "/actions",
            json=payload(key="unknown-type", action_type="email.send"),
        )
        unknown_destination = client.post(
            "/actions",
            json=payload(key="unknown-destination", destination="missing"),
        )
        travel_only_destination = client.post(
            "/actions",
            json=payload(
                key="travel-only-destination",
                destination="travel-trip-hold",
            ),
        )
        store = client.app.state.run_store
        assert store.get_run_by_client_request_id(
            TENANT_A,
            action_client_request_id("unknown-type"),
        ) is None
        assert store.get_run_by_client_request_id(
            TENANT_A,
            action_client_request_id("unknown-destination"),
        ) is None
        assert store.get_run_by_client_request_id(
            TENANT_A,
            action_client_request_id("travel-only-destination"),
        ) is None

    assert unknown_type.status_code == 422
    assert unknown_type.json()["error"]["code"] == "action_type_not_registered"
    assert unknown_destination.status_code == 422
    assert unknown_destination.json()["error"]["code"] == "destination_not_registered"
    assert travel_only_destination.status_code == 422
    assert travel_only_destination.json()["error"]["code"] == "destination_not_registered"


def test_action_input_forbids_authority_routing_and_transport_fields(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": RecordingWebhookProvider()},
    )

    invalid_payloads = []
    for field, value in {
        "tenant": "attacker",
        "tenant_id": "attacker",
        "subject": "attacker",
        "subject_id": "attacker",
        "role": "operator",
        "thread_id": "caller-thread",
        "url": "https://attacker.example",
        "method": "DELETE",
        "headers": {"Authorization": "secret"},
        "token": "secret",
        "bearer_token": "secret",
        "timeout": 999,
        "provider_identity": "attacker-provider",
        "supports_idempotency": True,
        "definitive_status_codes": [409],
    }.items():
        candidate = payload(key=f"forbidden-{field}")
        candidate[field] = value
        invalid_payloads.append(candidate)
    nested = payload(key="forbidden-nested")
    nested["input"]["headers"] = {"Authorization": "secret"}
    invalid_payloads.append(nested)

    with TestClient(app, headers=headers()) as client:
        responses = [client.post("/actions", json=item) for item in invalid_payloads]

    assert all(response.status_code == 422 for response in responses)
    assert all(
        response.json()["error"]["code"] == "invalid_action_input"
        for response in responses
    )


def test_action_auth_permissions_and_tenant_isolation_use_stable_errors(tmp_path):
    provider = RecordingWebhookProvider()
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app) as client:
        unauthenticated = client.post("/actions", json=payload())
        viewer_denied = client.post(
            "/actions",
            headers=headers(VIEWER_A_KEY),
            json=payload(),
        )
        created = client.post(
            "/actions?wait=5",
            headers=headers(OPERATOR_A_KEY),
            json=payload(),
        )
        action_id = created.json()["action_id"]
        viewer_read = client.get(
            f"/actions/{action_id}",
            headers=headers(VIEWER_A_KEY),
        )
        viewer_events = client.get(
            f"/actions/{action_id}/events",
            headers=headers(VIEWER_A_KEY),
        )
        other_tenant = client.get(
            f"/actions/{action_id}",
            headers=headers(OPERATOR_B_KEY),
        )
        other_events = client.get(
            f"/actions/{action_id}/events",
            headers=headers(OPERATOR_B_KEY),
        )

    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["error"]["code"] == "invalid_api_key"
    assert viewer_denied.status_code == 403
    assert viewer_denied.json()["error"]["code"] == "operation_not_permitted"
    assert viewer_read.status_code == 200
    assert viewer_events.status_code == 200
    assert other_tenant.status_code == 404
    assert other_tenant.json()["error"]["code"] == "action_not_found"
    assert other_events.status_code == 404


def test_action_lookup_precedes_read_permission_but_projection_follows_it(tmp_path):
    class NoReadAuthorizer:
        def authorize(self, principal, permission):
            del principal
            if permission in {
                RuntimePermission.RUNS_READ,
                RuntimePermission.RUN_EVENTS_READ,
            }:
                raise AuthorizationError("denied")

    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        authorizer=NoReadAuthorizer(),
        action_providers={"demo": RecordingWebhookProvider()},
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions?wait=5", json=payload())
        action_id = created.json()["action_id"]

        def projection_must_not_run(_run):
            raise AssertionError("projection ran before authorization")

        client.app.state.action_gateway.projector.project = projection_must_not_run
        denied_get = client.get(f"/actions/{action_id}")
        denied_events = client.get(f"/actions/{action_id}/events")
        missing = client.get("/actions/missing")

    assert denied_get.status_code == 403
    assert denied_get.json()["error"]["code"] == "operation_not_permitted"
    assert denied_events.status_code == 403
    assert denied_events.json()["error"]["code"] == "operation_not_permitted"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "action_not_found"


def test_action_routes_render_stable_validation_errors(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": RecordingWebhookProvider()},
    )

    with TestClient(app, headers=headers()) as client:
        malformed = client.post(
            "/actions",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        invalid_wait = client.post("/actions?wait=6", json=payload(key="bad-wait"))
        created = client.post("/actions?wait=5", json=payload(key="cursor"))
        invalid_cursor = client.get(
            f"/actions/{created.json()['action_id']}/events?after_sequence=bad"
        )

    assert malformed.status_code == 422
    assert malformed.json()["error"] == {
        "code": "invalid_action_input",
        "message": "The Action request body is invalid.",
    }
    assert invalid_wait.status_code == 422
    assert invalid_wait.json()["error"] == {
        "code": "invalid_action_input",
        "message": "wait must be a number between 0 and 5 seconds.",
    }
    assert invalid_cursor.status_code == 422
    assert invalid_cursor.json()["error"] == {
        "code": "invalid_action_input",
        "message": "after_sequence must be a non-negative integer.",
    }


def test_private_action_domain_cannot_be_submitted_through_runs(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": RecordingWebhookProvider()},
    )

    with TestClient(app, headers=headers()) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "caller-controlled",
                "agent_id": ACTION_AGENT_ID,
                "agent_version": ACTION_AGENT_VERSION,
                "input": {
                    "contract": "durable-action:1",
                    "action_type": "webhook.send",
                    "destination": "demo",
                    "idempotency_key": "bypass",
                    "input": {"payload": {}},
                },
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "Agent version is not available through /runs"
