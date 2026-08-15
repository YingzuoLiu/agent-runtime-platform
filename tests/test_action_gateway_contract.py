from __future__ import annotations

import ast
import json
import math
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent.contracts import BaseRuntimeState, RuntimeResponse
from api.main import create_app
from runtime_service import (
    AgentRegistry,
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.action_gateway import (
    ACTION_REQUEST_NAMESPACE_PREFIX,
    ActionCreateRequest,
    WebhookSendInput,
    action_client_request_id,
    action_fingerprint,
    action_thread_id,
    load_action_providers_from_environment,
    to_durable_action_input,
)
from tests.test_api import TestClient


class PrivateState(BaseRuntimeState):
    domain_id = "private-test"
    schema_version = "1"


class PrivateInput(WebhookSendInput):
    pass


class PrivateRuntime:
    def initial_state(self, thread_id: str) -> PrivateState:
        return PrivateState(thread_id=thread_id)

    def execute(self, state, runtime_input, context) -> RuntimeResponse[PrivateState]:
        del runtime_input, context
        return RuntimeResponse(message="ok", state=state, validation_errors=[])


def action_request(**overrides) -> ActionCreateRequest:
    payload = {
        "action_type": "webhook.send",
        "destination": "demo",
        "idempotency_key": "agent-job-42",
        "input": {"payload": {"text": "hello"}},
    }
    payload.update(overrides)
    return ActionCreateRequest.model_validate(payload)


def test_action_identity_uses_exact_client_key_and_versioned_canonical_material():
    request = action_request()
    runtime_input = to_durable_action_input(request)

    assert action_client_request_id(request.idempotency_key).startswith(
        "action-request:v1:"
    )
    assert request.idempotency_key not in action_client_request_id(
        request.idempotency_key
    )
    assert action_fingerprint(runtime_input) == action_fingerprint(
        to_durable_action_input(
            action_request(input={"payload": {"text": "hello"}})
        )
    )
    assert action_thread_id("tenant-a", "webhook.send", "key") == action_thread_id(
        "tenant-a", "webhook.send", "key"
    )
    assert action_thread_id("tenant-a", "webhook.send", "key") != action_thread_id(
        "tenant-a", "webhook.send", "other-key"
    )
    assert action_thread_id("tenant-a", "webhook.send", "key") != action_thread_id(
        "tenant-b", "webhook.send", "key"
    )


def test_typed_defaults_and_json_key_order_have_the_same_fingerprint():
    omitted_default = action_request(input={})
    explicit_default = action_request(input={"payload": {}})
    reordered = action_request(
        input={"payload": {"second": 2, "first": {"b": 2, "a": 1}}}
    )
    ordered = action_request(
        input={"payload": {"first": {"a": 1, "b": 2}, "second": 2}}
    )

    assert action_fingerprint(to_durable_action_input(omitted_default)) == action_fingerprint(
        to_durable_action_input(explicit_default)
    )
    assert action_fingerprint(to_durable_action_input(reordered)) == action_fingerprint(
        to_durable_action_input(ordered)
    )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_webhook_payload_rejects_non_finite_numbers(value: float):
    with pytest.raises(ValidationError, match="NaN or Infinity"):
        WebhookSendInput(payload={"value": value})


def test_webhook_payload_rejects_excessive_depth_and_bytes():
    too_deep: dict[str, object] = {}
    cursor = too_deep
    for _ in range(17):
        nested: dict[str, object] = {}
        cursor["nested"] = nested
        cursor = nested

    with pytest.raises(ValidationError, match="levels of nesting"):
        WebhookSendInput(payload=too_deep)
    with pytest.raises(ValidationError, match="UTF-8 bytes"):
        WebhookSendInput(payload={"text": "x" * 40_000})
    with pytest.raises(ValidationError, match="valid Unicode"):
        WebhookSendInput(payload={"text": "\ud800"})


def test_action_idempotency_key_requires_valid_unicode():
    with pytest.raises(ValidationError, match="unicode"):
        action_request(idempotency_key="\ud800")


def test_registry_hides_private_registration_but_can_resolve_it():
    registry = AgentRegistry()
    registry.register(
        "private-agent",
        "1.0.0",
        PrivateRuntime,
        description="private",
        input_model=PrivateInput,
        state_model=PrivateState,
        public_runs_api=False,
    )

    assert registry.list_agents() == []
    assert registry.registration("private-agent", "1.0.0").public_runs_api is False
    assert isinstance(registry.resolve("private-agent", "1.0.0"), PrivateRuntime)


def test_runs_rejects_the_reserved_action_namespace(tmp_path):
    authenticator = StaticApiKeyAuthenticator(
        [
            ApiKeyCredential(
                credential_id="action-contract-operator",
                api_key="action-contract-key",
                tenant_id="action-contract-tenant",
                subject_id="action-contract-subject",
                role=RuntimeRole.OPERATOR,
            )
        ]
    )
    app = create_app(database_path=tmp_path / "runtime.db", authenticator=authenticator)

    with TestClient(app, api_key="action-contract-key") as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "reserved-namespace",
                "input": {"user_message": "Plan Tokyo"},
                "client_request_id": f"{ACTION_REQUEST_NAMESPACE_PREFIX}caller-value",
            },
        )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "reserved_client_request_namespace",
            "message": "client_request_id uses a namespace reserved for the Action API.",
        }
    }


def test_action_provider_environment_builds_server_owned_destination(monkeypatch):
    monkeypatch.setenv(
        "RUNTIME_ACTION_PROVIDERS_JSON",
        json.dumps(
            {
                "demo": {
                    "endpoint": "https://provider.example/actions",
                    "provider_identity": "demo-provider-v1",
                    "bearer_token": "server-secret",
                    "supports_idempotency": True,
                    "definitive_status_codes": [400, 401, 403, 404],
                    "timeout_seconds": 4,
                    "max_response_bytes": 4096,
                }
            }
        ),
    )

    providers = load_action_providers_from_environment()

    assert set(providers) == {"demo"}
    assert providers["demo"].provider_identity == "demo-provider-v1"
    assert providers["demo"].supports_idempotency is True

    monkeypatch.setenv(
        "RUNTIME_ACTION_PROVIDERS_JSON",
        json.dumps(
            {
                "invalid alias": {
                    "endpoint": "https://provider.example/actions",
                    "provider_identity": "demo-provider-v1",
                }
            }
        ),
    )
    with pytest.raises(ValueError, match="invalid destination alias"):
        load_action_providers_from_environment()


def test_action_waiter_limit_does_not_treat_zero_as_an_omitted_value(tmp_path):
    with pytest.raises(ValueError, match="action waiter limit"):
        create_app(database_path=tmp_path / "runtime.db", action_waiter_limit=0)


def test_action_gateway_release_bumps_the_api_version(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")

    assert app.version == "1.3.0"


def test_external_agent_example_meets_the_physical_line_budget():
    example_path = Path(__file__).parents[1] / "examples" / "external_agent.py"
    source = example_path.read_text(encoding="utf-8")
    executable_physical_lines = [
        line
        for line in source.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    tree = ast.parse(source)
    statement_lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.stmt)
    ]

    assert len(executable_physical_lines) <= 10
    assert len(statement_lines) == len(set(statement_lines))
    assert "runtime_service" not in source
    assert "RUNTIME_API_KEY" in source
    assert "requests.post" in source
