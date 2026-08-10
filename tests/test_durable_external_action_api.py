from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    AuthorizationError,
    HttpExternalActionProvider,
    RoleAuthorizer,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.external_actions import (
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.workflow_store import (
    ExternalActionStatus,
    SQLiteWorkflowStore,
)


API_KEY = "phase7a-operator-key"
TENANT_ID = "phase7a-tenant"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="phase7a-credential",
            api_key=API_KEY,
            tenant_id=TENANT_ID,
            subject_id="phase7a-subject",
            role=RuntimeRole.OPERATOR,
        )
    ]
)


class DenyExternalActionAuthorizer:
    def authorize(self, principal, permission: RuntimePermission) -> None:
        if permission == RuntimePermission.EXTERNAL_ACTIONS_EXECUTE:
            raise AuthorizationError("Operation not permitted")
        RoleAuthorizer().authorize(principal, permission)


class MismatchedTravelActionProvider:
    supports_idempotency = True
    provider_identity = "travel-provider-mismatch-test-v1"

    def __init__(self) -> None:
        self.requests: list[ExternalActionRequest] = []

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.requests.append(request)
        return ExternalActionProviderResult(
            provider_reference="hold_mismatched_reference",
            result={
                "status": "held",
                "destination": "Paris",
                "selected_option_name": request.arguments["selected_option_name"],
                "quoted_total": request.arguments["quoted_total"],
                "hold_minutes": request.arguments["hold_minutes"],
            },
        )


def client_for(app) -> TestClient:
    return TestClient(app, headers={"Authorization": f"Bearer {API_KEY}"})


def wait_for_terminal(client: TestClient, run_id: str, timeout: float = 8.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/runs/{run_id}").json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"run did not finish: {run_id}")


def action_payload(
    thread_id: str,
    *,
    requested_action: str = "create_hold",
    client_request_id: str | None = None,
) -> dict:
    payload = {
        "thread_id": thread_id,
        "agent_id": "travel-agent",
        "agent_version": "1.2.0",
        "input": {
            "user_message": (
                "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
            ),
            "requested_action": requested_action,
        },
    }
    if client_request_id is not None:
        payload["client_request_id"] = client_request_id
    return payload


def test_explicit_hold_runs_through_durable_action_lifecycle_and_sse(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path, authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        agents = {
            (item["agent_id"], item["version"]): item
            for item in client.get("/agents").json()
        }
        assert agents[("travel-agent", "1.2.0")]["domain_id"] == "travel"
        assert "requested_action" not in agents[("travel-agent", "1.0.0")][
            "input_schema"
        ]["properties"]
        assert "requested_action" not in agents[("travel-agent", "1.1.0")][
            "input_schema"
        ]["properties"]
        assert "requested_action" in agents[("travel-agent", "1.2.0")][
            "input_schema"
        ]["properties"]
        # The shared sandbox catalog stays backward compatible; the write
        # tool exists only in Travel 1.2's private durable registry.
        assert {tool["name"] for tool in client.get("/tools").json()} == {
            "search_trip_options",
            "rank_trip_options",
            "route_cost_summary",
        }

        submitted = client.post(
            "/runs",
            json=action_payload(
                "phase7a-hold",
                client_request_id="phase7a-idempotent-submit",
            ),
        )
        assert submitted.status_code == 202, submitted.text
        run_id = submitted.json()["run_id"]
        result = wait_for_terminal(client, run_id)

        repeated = client.post(
            "/runs",
            json=action_payload(
                "phase7a-hold",
                client_request_id="phase7a-idempotent-submit",
            ),
        )
        assert repeated.status_code == 202
        assert repeated.json()["run_id"] == run_id

        events = client.get(f"/runs/{run_id}/events").json()
        streamed = client.get(f"/runs/{run_id}/events/stream")

        assert result["status"] == "completed"
        assert result["error_code"] is None
        assert result["state"]["current_stage"] == "planned"
        action_output = result["state"]["tool_outputs"]["call-0004"]
        assert action_output["tool_name"] == "create_trip_hold"
        assert action_output["result"]["status"] == "held"
        assert action_output["result"]["provider_reference"].startswith("hold_")

        actions = SQLiteWorkflowStore(database_path).list_external_actions(run_id)
        assert len(actions) == 1
        assert actions[0].status == ExternalActionStatus.SUCCEEDED
        assert actions[0].dispatch_count == 1
        assert actions[0].provider_reference == action_output["result"][
            "provider_reference"
        ]
        assert client.app.state.travel_action_provider.count_holds() == 1
        assert [
            (memory["key"], memory["value"])
            for memory in client.get("/memories").json()
        ] == [("flight.avoid_red_eye", True)]

        action_event_types = [
            event["event_type"]
            for event in events
            if event["event_type"].startswith("external_action.")
        ]
        assert action_event_types == [
            "external_action.prepared",
            "external_action.dispatch_started",
            "external_action.succeeded",
        ]
        public_evidence = json.dumps(events)
        assert "idempotency_key" not in public_evidence
        assert "phase7a-operator-key" not in public_evidence
        assert "permissions" not in public_evidence

        streamed_events = [
            json.loads(line.removeprefix("data: "))
            for line in streamed.text.splitlines()
            if line.startswith("data: ")
        ]
        assert streamed_events == events


def test_plan_only_1_2_run_never_prepares_an_external_action(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path, authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        submitted = client.post(
            "/runs",
            json=action_payload("phase7a-plan-only", requested_action="plan_only"),
        )
        result = wait_for_terminal(client, submitted.json()["run_id"])

        assert result["status"] == "completed"
        assert set(result["state"]["tool_outputs"]) == {
            "call-0001",
            "call-0002",
            "call-0003",
        }
        assert SQLiteWorkflowStore(database_path).list_external_actions(
            result["run_id"]
        ) == []
        assert client.app.state.travel_action_provider.count_holds() == 0


def test_mismatched_provider_result_is_not_committed_or_exposed(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = MismatchedTravelActionProvider()
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        travel_action_provider=provider,
    )
    with client_for(app) as client:
        submitted = client.post(
            "/runs",
            json=action_payload("phase7a-mismatched-provider-result"),
        )
        result = wait_for_terminal(client, submitted.json()["run_id"])
        events = client.get(f"/runs/{result['run_id']}/events").json()

    assert result["status"] == "failed"
    assert result["error_code"] == "external_action_outcome_unknown"
    assert len(provider.requests) == 2
    actions = SQLiteWorkflowStore(database_path).list_external_actions(
        result["run_id"]
    )
    assert len(actions) == 1
    assert actions[0].status == ExternalActionStatus.OUTCOME_UNKNOWN
    assert actions[0].result_json is None
    assert "Paris" not in json.dumps({"result": result, "events": events})


def test_direct_tool_endpoint_cannot_bypass_external_action_ledger(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path, authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        response = client.post(
            "/tools/create_trip_hold/execute",
            json={
                "arguments": {
                    "destination": "Tokyo",
                    "selected_option_name": "Tokyo value itinerary",
                    "quoted_total": 7800,
                    "hold_minutes": 15,
                }
            },
        )

        assert response.status_code == 409
        assert response.json()["detail"] == (
            "External-write tools require the durable run lifecycle"
        )
        assert client.app.state.travel_action_provider.count_holds() == 0


def test_http_action_provider_can_be_selected_from_server_environment(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_URL",
        "https://provider.example.test/v1/trip-holds",
    )
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN",
        "server-only-test-token",
    )
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY",
        "travel-provider-account-test-v1",
    )
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY",
        "true",
    )
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
    )

    with client_for(app) as client:
        assert client.get("/ready").status_code == 200
        assert isinstance(
            client.app.state.travel_action_provider,
            HttpExternalActionProvider,
        )
        assert (
            client.app.state.travel_action_provider.provider_identity
            == "travel-provider-account-test-v1"
        )
        assert client.app.state.travel_action_provider.supports_idempotency is True


def test_http_action_provider_requires_explicit_stable_identity(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_URL",
        "https://provider.example.test/v1/trip-holds",
    )
    monkeypatch.delenv("RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY", raising=False)
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
    )

    with pytest.raises(ValueError, match="PROVIDER_IDENTITY is required"):
        with client_for(app):
            pass


def test_direct_external_action_route_requires_external_action_permission(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        authorizer=DenyExternalActionAuthorizer(),
    )

    with client_for(app) as client:
        response = client.post(
            "/tools/create_trip_hold/execute",
            json={
                "arguments": {
                    "destination": "Tokyo",
                    "selected_option_name": "Tokyo value itinerary",
                    "quoted_total": 7800,
                }
            },
        )

        assert response.status_code == 403
        assert response.json() == {"detail": "Operation not permitted"}
        assert client.app.state.travel_action_provider.count_holds() == 0
