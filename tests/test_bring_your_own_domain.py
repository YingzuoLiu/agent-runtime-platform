from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from agent.contracts import RuntimeExecutionAuthority
from api.main import create_app
from domains.incident_triage import IncidentTriageExtension
from domains.incident_triage.planner import ScriptedIncidentTriagePlanner
from domains.incident_triage.policy import classify_risk
from runtime_service import (
    ApiKeyCredential,
    RunRecord,
    RunStatus,
    RuntimePermission,
    RuntimeRole,
    SQLiteRunStore,
    StaticApiKeyAuthenticator,
)
from runtime_service.planner import CallToolDecision, FinishDecision
from runtime_service.workflow_store import (
    SQLiteWorkflowStore,
    ToolCallStatus,
    WorkflowStatus,
)


API_KEY = "phase7c-operator-key"
TENANT_ID = "phase7c-tenant"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="phase7c-credential",
            api_key=API_KEY,
            tenant_id=TENANT_ID,
            subject_id="phase7c-subject",
            role=RuntimeRole.OPERATOR,
        )
    ]
)


def client_for(app) -> TestClient:
    return TestClient(app, headers={"Authorization": f"Bearer {API_KEY}"})


def wait_for_terminal(client: TestClient, run_id: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/runs/{run_id}")
        assert response.status_code == 200, response.text
        body = response.json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"Run did not finish: {run_id}")


def submit_incident(
    client: TestClient,
    *,
    thread_id: str,
    runtime_input: dict,
) -> tuple[str, dict]:
    response = client.post(
        "/runs",
        json={
            "thread_id": thread_id,
            "agent_id": "incident-triage",
            "agent_version": "1.0.0",
            "client_request_id": f"{thread_id}-request",
            "input": runtime_input,
        },
    )
    assert response.status_code == 202, response.text
    run_id = response.json()["run_id"]
    return run_id, wait_for_terminal(client, run_id)


def test_extension_is_opt_in_and_runs_through_the_existing_durable_api(tmp_path):
    default_app = create_app(
        database_path=tmp_path / "default.db",
        authenticator=AUTHENTICATOR,
    )
    with client_for(default_app) as client:
        default_agents = client.get("/agents").json()
        assert "incident-triage" not in {
            agent["agent_id"] for agent in default_agents
        }

    database_path = tmp_path / "extended.db"
    extended_app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(extended_app) as client:
        descriptor = next(
            agent
            for agent in client.get("/agents").json()
            if agent["agent_id"] == "incident-triage"
        )
        assert descriptor["version"] == "1.0.0"
        assert descriptor["domain_id"] == "incident-triage"
        assert descriptor["schema_version"] == "1"
        assert set(descriptor["input_schema"]["required"]) == {
            "alert_id",
            "severity",
            "error_rate_percent",
            "recent_deployment",
        }
        # Agent-private tools stay behind the version-pinned Planner loop.
        assert "inspect_incident_signal" not in {
            tool["name"] for tool in client.get("/tools").json()
        }

        run_id, result = submit_incident(
            client,
            thread_id="incident-happy",
            runtime_input={
                "alert_id": "alert-checkout-001",
                "service": "checkout-api",
                "severity": "critical",
                "error_rate_percent": 8.0,
                "recent_deployment": True,
            },
        )
        assert result["status"] == "completed"
        assert result["error_code"] is None
        assert result["validation_errors"] == []
        assert result["state"]["current_stage"] == "triaged"
        assert result["state"]["result"] == {
            "alert_id": "alert-checkout-001",
            "service": "checkout-api",
            "risk_level": "high",
            "recommended_action": "prepare_rollback_review",
            "evidence_source": "synthetic_incident_fixture",
            "action_executed": False,
        }
        assert "No external action was executed" in result["output_message"]

        events = client.get(f"/runs/{run_id}/events").json()
        persisted = [
            event.model_dump(mode="json")
            for event in client.app.state.run_store.list_events(run_id)
        ]
        assert persisted == events
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
            "loop.outcome",
        ]
        policy_event = next(
            event for event in events if event["event_type"] == "policy.decision"
        )
        assert policy_event["payload"]["outcome"] == "allowed"
        assert policy_event["payload"]["tool_name"] == "inspect_incident_signal"
        assert not any(
            event["event_type"].startswith("external_action.")
            for event in events
        )

        checkpoint = client.get(
            "/threads/incident-happy/state",
            params={"domain_id": "incident-triage", "schema_version": "1"},
        )
        assert checkpoint.status_code == 200
        checkpoint_state = checkpoint.json()
        assert result["state"]["execution_trace"]
        assert checkpoint_state["execution_trace"] == []
        assert {
            key: value
            for key, value in checkpoint_state.items()
            if key != "execution_trace"
        } == {
            key: value
            for key, value in result["state"].items()
            if key != "execution_trace"
        }

    workflow_store = SQLiteWorkflowStore(database_path)
    execution = workflow_store.get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.READY
    steps = workflow_store.list_steps(run_id)
    assert [(step.tool_name, step.status) for step in steps] == [
        ("inspect_incident_signal", ToolCallStatus.COMPLETED)
    ]
    assert workflow_store.list_external_actions(run_id) == []


def test_missing_service_completes_as_clarification_without_tool_execution(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app) as client:
        run_id, result = submit_incident(
            client,
            thread_id="incident-clarification",
            runtime_input={
                "alert_id": "alert-unknown-001",
                "severity": "warning",
                "error_rate_percent": 1.0,
                "recent_deployment": False,
            },
        )
        events = client.get(f"/runs/{run_id}/events").json()

    assert result["status"] == "completed"
    assert result["state"]["current_stage"] == "needs_clarification"
    assert result["state"]["result"] is None
    assert "Which service" in result["output_message"]
    assert not any(event["event_type"] == "tool.result" for event in events)
    workflow_store = SQLiteWorkflowStore(database_path)
    assert workflow_store.list_steps(run_id) == []
    assert workflow_store.list_external_actions(run_id) == []


def test_tool_argument_allowlist_denies_unsupported_service_before_handler(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app) as client:
        run_id, result = submit_incident(
            client,
            thread_id="incident-unsupported-service",
            runtime_input={
                "alert_id": "alert-database-001",
                "service": "database-prod",
                "severity": "critical",
                "error_rate_percent": 9.0,
                "recent_deployment": True,
            },
        )
        events = client.get(f"/runs/{run_id}/events").json()

    assert result["status"] == "failed"
    assert result["error_code"] == "invalid_tool_arguments"
    denial = next(
        event for event in events if event["event_type"] == "policy.decision"
    )
    assert denial["payload"]["outcome"] == "denied"
    assert denial["payload"]["error_code"] == "invalid_tool_arguments"
    assert not any(event["event_type"] == "tool.result" for event in events)
    workflow_store = SQLiteWorkflowStore(database_path)
    assert workflow_store.list_steps(run_id) == []
    assert workflow_store.list_external_actions(run_id) == []


def test_catalog_fixture_produces_a_low_risk_observe_recommendation(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app) as client:
        _, result = submit_incident(
            client,
            thread_id="incident-catalog-low-risk",
            runtime_input={
                "alert_id": "alert-catalog-001",
                "service": "catalog-api",
                "severity": "warning",
                "error_rate_percent": 0.8,
                "recent_deployment": False,
            },
        )

    assert result["status"] == "completed"
    assert result["validation_errors"] == []
    assert result["state"]["result"]["risk_level"] == "low"
    assert result["state"]["result"]["recommended_action"] == "observe"


@pytest.mark.parametrize(
    ("severity", "error_rate_percent", "expected"),
    [
        ("warning", 0.8, "low"),
        ("warning", 2.0, "elevated"),
        ("critical", 5.0, "high"),
    ],
)
def test_incident_risk_thresholds(severity, error_rate_percent, expected):
    assert classify_risk(
        severity=severity,
        error_rate_percent=error_rate_percent,
    ) == expected


class TamperedIncidentPlanner(ScriptedIncidentTriagePlanner):
    def decide(self, context):
        decision = super().decide(context)
        if isinstance(decision, FinishDecision):
            return decision.model_copy(
                update={
                    "output": {
                        **decision.output,
                        "recommended_action": "observe",
                    }
                }
            )
        return decision


def test_finish_recommendation_cannot_bypass_persisted_evidence_validation(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(
            IncidentTriageExtension(planner_factory=TamperedIncidentPlanner),
        ),
    )
    with client_for(app) as client:
        run_id, result = submit_incident(
            client,
            thread_id="incident-tampered-finish",
            runtime_input={
                "alert_id": "alert-checkout-002",
                "service": "checkout-api",
                "severity": "critical",
                "error_rate_percent": 8.0,
                "recent_deployment": True,
            },
        )

    assert result["status"] == "completed"
    assert result["state"]["current_stage"] == "blocked"
    assert result["state"]["result"] is None
    assert result["validation_errors"] == [
        "Planner recommendation does not match deterministic policy"
    ]
    execution = SQLiteWorkflowStore(database_path).get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.BLOCKED
    assert execution.error_code == "domain_validation_failed"


def test_submitted_signal_claims_cannot_override_server_owned_fixture(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app) as client:
        run_id, result = submit_incident(
            client,
            thread_id="incident-claim-mismatch",
            runtime_input={
                "alert_id": "alert-checkout-claim-mismatch",
                "service": "checkout-api",
                "severity": "warning",
                "error_rate_percent": 0.5,
                "recent_deployment": False,
            },
        )

    assert result["status"] == "completed"
    assert result["state"]["current_stage"] == "blocked"
    assert result["state"]["result"] is None
    assert set(result["validation_errors"]) == {
        "Incident evidence does not match the requested severity",
        "Incident evidence does not match the requested error rate",
        "Incident evidence does not match deployment recency",
    }
    execution = SQLiteWorkflowStore(database_path).get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.BLOCKED


class UnregisteredActionPlanner:
    def decide(self, context):
        del context
        return CallToolDecision(
            tool_name="execute_rollback",
            arguments={"service": "checkout-api"},
            reason="injected unregistered action attempt",
        )


def test_unregistered_action_is_denied_without_tool_execution(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(
            IncidentTriageExtension(planner_factory=UnregisteredActionPlanner),
        ),
    )
    with client_for(app) as client:
        run_id, result = submit_incident(
            client,
            thread_id="incident-unregistered-action",
            runtime_input={
                "alert_id": "alert-checkout-003",
                "service": "checkout-api",
                "severity": "critical",
                "error_rate_percent": 8.0,
                "recent_deployment": True,
            },
        )
        events = client.get(f"/runs/{run_id}/events").json()

    assert result["status"] == "failed"
    assert result["error_code"] == "unknown_tool"
    denial = next(
        event for event in events if event["event_type"] == "policy.decision"
    )
    assert denial["payload"]["tool_name"] == "execute_rollback"
    assert denial["payload"]["outcome"] == "denied"
    assert not any(event["event_type"] == "tool.result" for event in events)
    workflow_store = SQLiteWorkflowStore(database_path)
    assert workflow_store.list_steps(run_id) == []
    assert workflow_store.list_external_actions(run_id) == []


def test_custom_input_is_validated_before_a_run_is_queued(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "incident-invalid-input",
                "agent_id": "incident-triage",
                "agent_version": "1.0.0",
                "input": {
                    "alert_id": "alert-invalid-001",
                    "service": "checkout-api",
                    "error_rate_percent": 8.0,
                    "recent_deployment": True,
                    "unexpected": "rejected",
                },
            },
        )

    assert response.status_code == 422
    assert "severity" in response.json()["detail"]
    assert "unexpected" in response.json()["detail"]


def test_duplicate_extension_registration_fails_application_startup(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(), IncidentTriageExtension()),
    )
    with pytest.raises(
        ValueError,
        match="Agent already registered: incident-triage:1.0.0",
    ):
        with client_for(app):
            pass


def test_missing_extension_fails_startup_without_consuming_recoverable_work(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    run = RunRecord(
        run_id="run_phase7c_recovery",
        tenant_id=TENANT_ID,
        thread_id="incident-recovery",
        agent_id="incident-triage",
        agent_version="1.0.0",
        domain_id="incident-triage",
        schema_version="1",
        status=RunStatus.QUEUED,
        input={
            "alert_id": "alert-checkout-recovery",
            "service": "checkout-api",
            "severity": "critical",
            "error_rate_percent": 8.0,
            "recent_deployment": True,
        },
        execution_authority=RuntimeExecutionAuthority(
            tenant_id=TENANT_ID,
            subject_id="phase7c-subject",
            permissions=(RuntimePermission.TOOLS_EXECUTE.value,),
        ),
    )
    store.create_run_with_event(
        run,
        event_type="run.queued",
        payload={"recovery_fixture": True},
    )

    app_without_extension = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "recoverable run run_phase7c_recovery requires unregistered Agent "
            "version incident-triage:1.0.0"
        ),
    ):
        with client_for(app_without_extension):
            pass

    untouched = store.get_run_internal(run.run_id)
    assert untouched is not None
    assert untouched.status == RunStatus.QUEUED
    assert [event.event_type for event in store.list_events(run.run_id)] == [
        "run.queued"
    ]

    app_with_extension = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        runtime_extensions=(IncidentTriageExtension(),),
    )
    with client_for(app_with_extension) as client:
        recovered = wait_for_terminal(client, run.run_id)

    assert recovered["status"] == "completed"
    assert recovered["state"]["current_stage"] == "triaged"
