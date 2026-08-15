from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import SQLiteRunStore


DEMO_API_KEY = "phase7b-demo-test-key"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def authorization_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def wait_for_terminal(
    client: TestClient,
    run_id: str,
    *,
    api_key: str,
    timeout: float = 8.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(
            f"/runs/{run_id}",
            headers=authorization_headers(api_key),
        )
        assert response.status_code == 200, response.text
        body = response.json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"run did not finish: {run_id}")


def test_normal_runtime_does_not_expose_or_enable_demo_credentials(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("RUNTIME_DEMO_MODE", raising=False)
    monkeypatch.delenv("RUNTIME_API_KEYS_JSON", raising=False)
    app = create_app(database_path=tmp_path / "normal-runtime.db")

    with TestClient(app) as client:
        assert client.get("/").status_code == 404
        assert client.get("/demo").status_code == 404
        assert client.get("/demo/session").status_code == 404
        assert client.get("/demo-assets/app.js").status_code == 404
        protected = client.post(
            "/runs",
            json={
                "thread_id": "normal-runtime-remains-closed",
                "agent_id": "travel-agent",
                "agent_version": "1.2.0",
                "input": {
                    "user_message": "Plan a 5-day Tokyo trip under 9000 SGD.",
                    "requested_action": "plan_only",
                },
            },
        )

    assert protected.status_code == 401
    assert protected.json() == {"detail": "Invalid or missing API key"}


def test_demo_console_submits_through_runtime_and_reads_persisted_evidence(
    tmp_path,
    monkeypatch,
    caplog,
):
    database_path = tmp_path / "demo-runtime.db"
    monkeypatch.setenv("RUNTIME_DEMO_MODE", "true")
    monkeypatch.delenv("RUNTIME_API_KEYS_JSON", raising=False)
    with caplog.at_level(logging.WARNING, logger="api.main"):
        app = create_app(
            database_path=database_path,
            demo_api_key=DEMO_API_KEY,
        )

    assert "RUNTIME_DEMO_MODE is enabled" in caplog.text
    assert "/demo/session exposes an ephemeral Operator credential" in caplog.text
    assert DEMO_API_KEY not in caplog.text

    with TestClient(app) as client:
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 307
        assert root.headers["location"] == "/demo"

        console = client.get("/demo")
        assert console.status_code == 200
        assert console.headers["cache-control"] == "no-store"
        assert "Agent Runtime Platform" in console.text

        script = client.get("/demo-assets/app.js")
        stylesheet = client.get("/demo-assets/styles.css")
        assert script.status_code == 200
        assert stylesheet.status_code == 200
        assert script.headers["cache-control"] == "no-store"
        assert stylesheet.headers["cache-control"] == "no-store"

        session_response = client.get("/demo/session")
        assert session_response.status_code == 200
        assert session_response.headers["cache-control"] == "no-store"
        session = session_response.json()
        assert session == {
            "api_key": DEMO_API_KEY,
            "agent_id": "travel-agent",
            "agent_version": "1.2.0",
            "default_message": (
                "Plan a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
            ),
            "requested_action": "plan_only",
        }

        assert client.post("/runs", json={}).status_code == 401
        submitted = client.post(
            "/runs",
            headers=authorization_headers(session["api_key"]),
            json={
                "thread_id": "phase7b-runtime-console",
                "client_request_id": "phase7b-runtime-console-request",
                "agent_id": session["agent_id"],
                "agent_version": session["agent_version"],
                "input": {
                    "user_message": session["default_message"],
                    "requested_action": session["requested_action"],
                },
            },
        )
        assert submitted.status_code == 202, submitted.text
        run_id = submitted.json()["run_id"]
        result = wait_for_terminal(
            client,
            run_id,
            api_key=session["api_key"],
        )
        events_response = client.get(
            f"/runs/{run_id}/events",
            headers=authorization_headers(session["api_key"]),
        )
        assert events_response.status_code == 200
        events = events_response.json()
        cursor = events[len(events) // 2]["sequence"]
        incremental_response = client.get(
            f"/runs/{run_id}/events?after_sequence={cursor}",
            headers=authorization_headers(session["api_key"]),
        )
        assert incremental_response.status_code == 200
        incremental_events = incremental_response.json()

    assert result["status"] == "completed"
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
    evidence = [
        event
        for event in events
        if event["event_type"]
        in {"planner.decision", "policy.decision", "tool.result", "loop.outcome"}
    ]
    assert [event["event_type"] for event in evidence] == [
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
    assert [
        event["payload"]["tool_name"]
        for event in evidence
        if event["event_type"] == "tool.result"
    ] == [
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
    ]
    tool_results = {
        event["payload"]["tool_name"]: event["payload"]["result"]
        for event in evidence
        if event["event_type"] == "tool.result"
    }
    selected = tool_results["search_trip_options"]["options"][0]
    assert tool_results["search_trip_options"]["source"] == (
        "synthetic_reference_catalog"
    )
    assert selected == {
        "index": 0,
        "name": "Tokyo value itinerary",
        "cost": 7800.0,
        "duration_hours": 10.5,
        "transport_cost": 2300,
        "hotel_cost": 3250,
        "activity_cost": 2250,
        "flight_type": "daytime",
        "hotel_tier": "standard hotel",
        "poi_style": "balanced itinerary",
    }
    assert tool_results["rank_trip_options"]["ranking"][0] == {
        "index": 0,
        "name": "Tokyo value itinerary",
        "score": 0.106522,
    }
    assert tool_results["route_cost_summary"] == {
        "total_cost": 7800,
        "budget": 9000,
        "remaining_budget": 1200,
        "within_budget": True,
    }
    rank_call = next(
        event["payload"]["decision"]
        for event in evidence
        if event["event_type"] == "planner.decision"
        and event["payload"]["decision"].get("tool_name") == "rank_trip_options"
    )
    assert rank_call["arguments"]["cost_weight"] == 0.7
    assert rank_call["arguments"]["duration_weight"] == 0.3
    assert incremental_events == [
        event for event in events if event["sequence"] > cursor
    ]
    assert all(event["sequence"] > cursor for event in incremental_events)
    persisted = SQLiteRunStore(database_path).list_events(run_id)
    assert [event.model_dump(mode="json") for event in persisted] == events


def test_demo_clarification_is_a_completed_turn_without_an_itinerary(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("RUNTIME_DEMO_MODE", "true")
    monkeypatch.delenv("RUNTIME_API_KEYS_JSON", raising=False)
    app = create_app(
        database_path=tmp_path / "clarification-runtime.db",
        demo_api_key=DEMO_API_KEY,
    )

    with TestClient(app) as client:
        submitted = client.post(
            "/runs",
            headers=authorization_headers(DEMO_API_KEY),
            json={
                "thread_id": "phase7b-clarification-console",
                "client_request_id": "phase7b-clarification-request",
                "agent_id": "travel-agent",
                "agent_version": "1.2.0",
                "input": {
                    "user_message": (
                        "Plan a 5-day Tokyo trip under 7000 SGD and avoid red-eye flights."
                    ),
                    "requested_action": "plan_only",
                },
            },
        )
        assert submitted.status_code == 202, submitted.text
        run_id = submitted.json()["run_id"]
        result = wait_for_terminal(client, run_id, api_key=DEMO_API_KEY)
        events_response = client.get(
            f"/runs/{run_id}/events",
            headers=authorization_headers(DEMO_API_KEY),
        )
        assert events_response.status_code == 200
        events = events_response.json()

    assert result["status"] == "completed"
    assert result["state"]["itinerary"] is None
    assert "above the budget of 7000" in result["output_message"]
    planner_decisions = [
        event["payload"]["decision"]["decision_type"]
        for event in events
        if event["event_type"] == "planner.decision"
    ]
    assert planner_decisions[-1] == "REQUEST_CLARIFICATION"
    assert [
        event["payload"]["outcome"]
        for event in events
        if event["event_type"] == "loop.outcome"
    ] == ["clarification"]
    assert all(event["event_type"] != "run.failed" for event in events)


def test_demo_mode_is_explicit_and_cannot_mix_with_production_credentials(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "RUNTIME_API_KEYS_JSON",
        '[{"credential_id":"prod","api_key":"prod-key","tenant_id":"prod",'
        '"subject_id":"prod","role":"operator"}]',
    )
    with pytest.raises(
        ValueError,
        match="cannot be combined with RUNTIME_API_KEYS_JSON",
    ):
        create_app(database_path=tmp_path / "mixed.db", demo_mode=True)

    monkeypatch.delenv("RUNTIME_API_KEYS_JSON")
    with pytest.raises(ValueError, match="demo_api_key requires demo mode"):
        create_app(
            database_path=tmp_path / "implicit-demo-key.db",
            demo_api_key="must-not-enable-demo",
        )


def test_compose_enables_loopback_demo_with_independent_provider(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    compose_path = REPOSITORY_ROOT / "docker-compose.yml"
    compose = compose_path.read_text(encoding="utf-8")
    provider_service, runtime_and_volumes = compose.split("  runtime:\n", maxsplit=1)
    runtime_service, named_volumes = runtime_and_volumes.split("\nvolumes:\n", maxsplit=1)
    assert compose.startswith("# LOCAL DEMO ONLY")
    assert '"127.0.0.1:8000:8000"' in provider_service
    assert '"127.0.0.1:8100:8100"' in provider_service
    assert "demo_provider.app:app" in provider_service
    assert "--no-access-log" in provider_service
    assert "provider-data:/app/provider_data" in provider_service
    assert "http://127.0.0.1:8100/health" in provider_service
    assert 'restart: "no"' in provider_service
    assert "network_mode: service:demo-provider" in runtime_service
    assert "condition: service_healthy" in runtime_service
    assert "ports:" not in runtime_service
    assert "http://127.0.0.1:8000/health" in runtime_service
    assert "runtime-data:/app/runtime_data" in runtime_service
    assert 'RUNTIME_DEMO_MODE: "true"' in runtime_service
    assert 'restart: "no"' in runtime_service
    assert "provider-data:" in named_volumes
    assert "runtime-data:" in named_volumes
    assert "RUNTIME_API_KEYS_JSON" not in compose
