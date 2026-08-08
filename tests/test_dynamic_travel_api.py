from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from domains.travel.planner import (
    ScriptedTravelPlanner,
    build_travel_planner_from_environment,
)
from runtime_service import (
    ApiKeyCredential,
    AuthorizationError,
    FinishDecision,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStatus


API_KEY = "phase5a-operator-key"
TENANT_ID = "phase5a-tenant"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="phase5a-credential",
            api_key=API_KEY,
            tenant_id=TENANT_ID,
            subject_id="phase5a-subject",
            role=RuntimeRole.OPERATOR,
        )
    ]
)


def client_for(app) -> TestClient:
    return TestClient(app, headers={"Authorization": f"Bearer {API_KEY}"})


def wait_for_terminal(client: TestClient, run_id: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/runs/{run_id}").json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"run did not finish: {run_id}")


def submit_dynamic(client: TestClient, thread_id: str, message: str) -> tuple[str, dict]:
    response = client.post(
        "/runs",
        json={
            "thread_id": thread_id,
            "agent_id": "travel-agent",
            "agent_version": "1.0.0",
            "user_message": message,
        },
    )
    assert response.status_code == 202, response.text
    run_id = response.json()["run_id"]
    return run_id, wait_for_terminal(client, run_id)


def test_dynamic_travel_happy_path_is_visible_through_existing_api_and_sse(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db", authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        agents = client.get("/agents").json()
        assert ("travel-agent", "1.0.0") in {
            (agent["agent_id"], agent["version"]) for agent in agents
        }
        assert {tool["name"] for tool in client.get("/tools").json()} == {
            "search_trip_options",
            "rank_trip_options",
            "route_cost_summary",
        }

        run_id, result = submit_dynamic(
            client,
            "dynamic-happy",
            "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
        )
        assert result["status"] == "completed"
        assert result["error_code"] is None
        assert result["state"]["current_stage"] == "planned"
        assert result["state"]["itinerary"] == {
            "destination": "Tokyo",
            "days": 5,
            "flight_type": "daytime",
            "hotel_tier": "standard hotel",
            "poi_style": "balanced itinerary",
            "total_cost": 7800,
            "notes": [
                "Built from synthetic reference tool evidence.",
                "Selected option: Tokyo value itinerary.",
            ],
        }
        assert result["validation_errors"] == []

        events = client.get(f"/runs/{run_id}/events").json()
        evidence_types = [
            event["event_type"]
            for event in events
            if event["event_type"]
            in {"planner.decision", "policy.decision", "tool.result", "loop.outcome"}
        ]
        assert evidence_types == [
            "planner.decision",
            "policy.decision",
            "tool.result",
            "planner.decision",
            "policy.decision",
            "tool.result",
            "planner.decision",
            "policy.decision",
            "tool.result",
            "planner.decision",
            "loop.outcome",
        ]
        planner_calls = [
            event["payload"]["decision"]
            for event in events
            if event["event_type"] == "planner.decision"
            and event["payload"]["decision"]["decision_type"] == "CALL_TOOL"
        ]
        assert [decision["tool_name"] for decision in planner_calls] == [
            "search_trip_options",
            "rank_trip_options",
            "route_cost_summary",
        ]
        search_result = next(
            event["payload"]["result"]
            for event in events
            if event["event_type"] == "tool.result"
            and event["payload"]["tool_name"] == "search_trip_options"
        )
        assert planner_calls[1]["arguments"]["options"] == [
            {
                "name": option["name"],
                "cost": option["cost"],
                "duration_hours": option["duration_hours"],
            }
            for option in search_result["options"]
        ]

        streamed = client.get(f"/runs/{run_id}/events/stream")
        assert streamed.status_code == 200
        streamed_events = [
            json.loads(line.removeprefix("data: "))
            for line in streamed.text.splitlines()
            if line.startswith("data: ")
        ]
        assert [event["sequence"] for event in streamed_events] == [
            event["sequence"] for event in events
        ]
        assert streamed_events == events

        thread_state = client.get("/threads/dynamic-happy/state").json()
        assert thread_state == result["state"]
        internal = client.app.state.run_store.get_run_internal(run_id)
        assert internal is not None and internal.execution_authority is not None
        assert internal.execution_authority.subject_id == "phase5a-subject"
        assert "tools:execute" in internal.execution_authority.permissions
        assert "execution_authority" not in result
        assert "permissions" not in json.dumps(events)


def test_clarification_completes_and_same_thread_follow_up_continues(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db", authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        first_id, first = submit_dynamic(
            client,
            "dynamic-clarification",
            "Plan a trip to Tokyo.",
        )
        assert first["status"] == "completed"
        assert first["state"]["current_stage"] == "needs_clarification"
        assert "days, budget" in first["output_message"]
        assert SQLiteWorkflowStore(tmp_path / "runtime.db").list_steps(first_id) == []

        _, second = submit_dynamic(
            client,
            "dynamic-clarification",
            "Make it 5 days with a budget of 9000 SGD and avoid red-eye flights.",
        )
        assert second["status"] == "completed"
        assert second["state"]["current_stage"] == "planned"
        assert second["state"]["destination"] == "Tokyo"
        assert second["state"]["budget"] == 9000


def test_low_budget_branch_depends_on_route_cost_result_then_can_continue(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db", authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        run_id, first = submit_dynamic(
            client,
            "dynamic-low-budget",
            "I want a 5-day Tokyo trip under 1000 SGD.",
        )
        assert first["status"] == "completed"
        assert first["state"]["current_stage"] == "needs_clarification"
        assert "above the budget" in first["output_message"]
        steps = SQLiteWorkflowStore(tmp_path / "runtime.db").list_steps(run_id)
        assert [step.tool_name for step in steps] == [
            "search_trip_options",
            "rank_trip_options",
            "route_cost_summary",
        ]

        _, second = submit_dynamic(
            client,
            "dynamic-low-budget",
            "Raise the budget to 9000 SGD.",
        )
        assert second["state"]["current_stage"] == "planned"


class TamperedFinishPlanner(ScriptedTravelPlanner):
    def decide(self, context):
        decision = super().decide(context)
        if isinstance(decision, FinishDecision):
            return decision.model_copy(
                update={
                    "output": {
                        **decision.output,
                        "destination": "Paris",
                    }
                }
            )
        return decision


def test_structurally_valid_finish_cannot_bypass_travel_validator(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        travel_planner=TamperedFinishPlanner(),
    )
    with client_for(app) as client:
        run_id, result = submit_dynamic(
            client,
            "dynamic-blocked-finish",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )

    assert result["status"] == "completed"
    assert result["state"]["current_stage"] == "blocked"
    assert result["error_code"] is None
    assert result["validation_errors"] == [
        "Planner finish destination does not match requested destination"
    ]
    execution = SQLiteWorkflowStore(database_path).get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.BLOCKED
    assert execution.error_code == "domain_validation_failed"


class FabricatedSelectionPlanner(ScriptedTravelPlanner):
    def decide(self, context):
        decision = super().decide(context)
        if isinstance(decision, FinishDecision):
            return decision.model_copy(
                update={
                    "output": {
                        **decision.output,
                        "selected_option_name": "Fabricated option",
                    }
                }
            )
        return decision


def test_finish_selection_absent_from_search_evidence_completes_blocked(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        travel_planner=FabricatedSelectionPlanner(),
    )
    with client_for(app) as client:
        run_id, result = submit_dynamic(
            client,
            "dynamic-fabricated-selection",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )

    assert result["status"] == "completed"
    assert result["state"]["current_stage"] == "blocked"
    assert result["state"]["itinerary"] is None
    assert result["error_code"] is None
    assert result["validation_errors"] == [
        "Planner finish selection does not match ranking evidence",
        "Planner finish selection is absent from search evidence",
    ]
    execution = SQLiteWorkflowStore(database_path).get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.BLOCKED
    assert execution.error_code == "domain_validation_failed"


class DenyToolExecutionAuthorizer:
    def authorize(self, _principal, permission: RuntimePermission) -> None:
        if permission == RuntimePermission.TOOLS_EXECUTE:
            raise AuthorizationError("Operation not permitted")


def test_worker_uses_persisted_effective_authority_and_denies_before_claim(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        authorizer=DenyToolExecutionAuthorizer(),
    )
    with client_for(app) as client:
        run_id, result = submit_dynamic(
            client,
            "dynamic-tool-denied",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        events = client.get(f"/runs/{run_id}/events").json()

    assert result["status"] == "failed"
    assert result["error_code"] == "tool_permission_denied"
    assert SQLiteWorkflowStore(database_path).list_steps(run_id) == []
    assert next(
        event["payload"]
        for event in events
        if event["event_type"] == "policy.decision"
    )["error_code"] == "tool_permission_denied"
    assert next(
        event["payload"]
        for event in events
        if event["event_type"] == "run.failed"
    )["error_code"] == "tool_permission_denied"


def test_request_body_cannot_spoof_execution_authority(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db", authenticator=AUTHENTICATOR)
    with client_for(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "authority-spoof",
                "agent_version": "1.0.0",
                "input": {"user_message": "Plan Tokyo"},
                "execution_authority": {
                    "tenant_id": TENANT_ID,
                    "subject_id": "attacker",
                    "permissions": ["tools:execute"],
                },
            },
        )
    assert response.status_code == 422


def test_planner_provider_environment_is_explicit_and_fail_closed(monkeypatch):
    monkeypatch.delenv("RUNTIME_PLANNER_PROVIDER", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    assert isinstance(build_travel_planner_from_environment(), ScriptedTravelPlanner)

    monkeypatch.setenv("RUNTIME_PLANNER_PROVIDER", "openai")
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        build_travel_planner_from_environment()

    monkeypatch.setenv("RUNTIME_PLANNER_PROVIDER", "not-a-provider")
    with pytest.raises(ValueError, match="scripted.*openai"):
        build_travel_planner_from_environment()


def test_new_core_execution_modules_do_not_import_travel_or_release_domains():
    repository_root = Path(__file__).resolve().parents[1]
    for relative_path in (
        "runtime_service/planner.py",
        "runtime_service/dynamic_loop.py",
        "runtime_service/sandbox.py",
        "runtime_service/sandbox_worker.py",
        "runtime_service/openai_planner.py",
    ):
        source = (repository_root / relative_path).read_text(encoding="utf-8")
        assert "domains.travel" not in source
        assert "domains.release_validation" not in source
        assert "route_cost_summary" not in source
        assert "search_trip_options" not in source
