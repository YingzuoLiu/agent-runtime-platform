import time

from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import SQLiteRunStore
from runtime_service.workflow_store import SQLiteWorkflowStore


def valid_release_manifest() -> dict:
    return {
        "release_id": "rel-api-001",
        "application_name": "aurora-notes",
        "release_version": "2.4.0",
        "required_artifacts": ["aurora-notes-server", "aurora-notes-cli"],
        "available_artifacts": [
            {"name": "aurora-notes-server", "checksum": "a" * 64},
            {"name": "aurora-notes-cli", "checksum": "b" * 64},
        ],
        "required_test_suite": "aurora-notes-full-suite",
        "executed_test_suite": "aurora-notes-full-suite",
        "tests_passed": True,
        "required_python_versions": ["3.11", "3.12"],
        "tested_python_versions": ["3.11", "3.12"],
        "deployment_environment": "staging",
        "configuration_requirements": ["DATABASE_URL", "FEATURE_FLAGS_ENDPOINT"],
        "actual_configuration_keys": [
            "DATABASE_URL",
            "FEATURE_FLAGS_ENDPOINT",
            "LOG_LEVEL",
        ],
    }


def wait_for_run(client: TestClient, run_id: str, timeout: float = 3.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/runs/{run_id}").json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError("run did not finish")


def test_fastapi_agent_message_endpoint(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        response = client.post(
            "/agent/message",
            json={
                "thread_id": "api_test_thread",
                "user_message": "I want a 5-day Tokyo trip under 7000 SGD.",
            },
        )
    assert response.status_code == 200
    body = response.json()
    assert body["updated_state"]["destination"] == "Tokyo"
    assert body["updated_state"]["budget"] == 7000


def test_legacy_agent_message_rejects_mismatched_explicit_state_thread(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path)
    with TestClient(app) as client:
        response = client.post(
            "/agent/message",
            json={
                "thread_id": "request-thread",
                "user_message": "Plan a five-day trip to Tokyo.",
                "state": {"thread_id": "different-thread"},
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "state.thread_id must match request.thread_id"
    store = SQLiteRunStore(database_path)
    assert store.load_thread_state("request-thread") is None
    assert store.load_thread_state("different-thread") is None


def test_sync_and_async_endpoints_share_thread_state(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        first = client.post(
            "/agent/message",
            json={
                "thread_id": "shared-thread",
                "user_message": "I want a 5-day Tokyo trip under 7000 SGD.",
            },
        )
        assert first.status_code == 200
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "shared-thread",
                "user_message": "Change the budget to 9000 and avoid red-eye flights.",
            },
        )
        result = wait_for_run(client, submitted.json()["run_id"])
        assert result["state"]["destination"] == "Tokyo"
        assert result["state"]["budget"] == 9000
        assert result["state"]["preferences"]["avoid_red_eye"] is True


def test_async_run_api_and_event_history(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "api-run-thread",
                "user_message": "I want a 5-day Tokyo trip under 9000 SGD.",
            },
        )
        assert response.status_code == 202
        run_id = response.json()["run_id"]
        body = wait_for_run(client, run_id)
        # The moment an external reader observes COMPLETED, the checkpoint
        # and describing events must already be committed alongside it --
        # this is the atomicity guarantee `finalize_completed_run` provides.
        # No extra wait/sleep here: the assertions run immediately after the
        # first observation of the terminal status.
        assert body["status"] == "completed"
        assert body["agent_version"] == "0.3.0"
        events = client.get(f"/runs/{run_id}/events").json()
        event_types = [event["event_type"] for event in events]
        assert event_types[0] == "run.queued"
        assert "checkpoint.saved" in event_types
        assert event_types[-2] == "checkpoint.saved"
        assert event_types[-1] == "run.completed"
        state = client.get("/threads/api-run-thread/state")
        assert state.status_code == 200
        assert state.json()["destination"] == "Tokyo"
        assert state.json()["budget"] == body["state"]["budget"]


def test_async_run_can_opt_into_evidence_review_agent_version(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "review-api-thread",
                "user_message": "I want a 5-day Tokyo trip under 7000 SGD.",
                "agent_version": "0.5.0",
            },
        )
        result = wait_for_run(client, submitted.json()["run_id"])

    assert result["status"] == "completed"
    assert result["agent_version"] == "0.5.0"
    assert result["state"]["itinerary"]["total_cost"] == 5800
    assert result["validation_errors"] == []
    assert any(
        event["event"] == "review_workflow_finished"
        for event in result["state"]["execution_trace"]
    )


def test_tool_sandbox_api_and_run_event_linkage(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        tools = client.get("/tools")
        assert tools.status_code == 200
        assert {item["name"] for item in tools.json()} == {
            "rank_trip_options",
            "route_cost_summary",
        }

        submitted = client.post(
            "/runs",
            json={
                "thread_id": "sandbox-run-thread",
                "user_message": "I want a 5-day Tokyo trip under 9000 SGD.",
            },
        )
        run_id = submitted.json()["run_id"]
        wait_for_run(client, run_id)

        execution = client.post(
            "/tools/route_cost_summary/execute",
            json={
                "run_id": run_id,
                "arguments": {
                    "transport_cost": 2000,
                    "hotel_cost": 3000,
                    "activity_cost": 1000,
                    "budget": 7000,
                },
            },
        )
        assert execution.status_code == 200
        assert execution.json()["status"] == "completed"
        assert execution.json()["result"]["remaining_budget"] == 1000

        events = client.get(f"/runs/{run_id}/events").json()
        event_types = [event["event_type"] for event in events]
        assert "sandbox.execution_started" in event_types
        assert "sandbox.execution_finished" in event_types


def test_run_submission_is_idempotent(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    payload = {
        "thread_id": "idempotent-thread",
        "user_message": "I want a 5-day Tokyo trip under 9000 SGD.",
        "client_request_id": "client-request-001",
    }
    with TestClient(app) as client:
        first = client.post("/runs", json=payload)
        second = client.post("/runs", json=payload)
        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json()["run_id"] == second.json()["run_id"]


def test_unknown_agent_version_is_rejected(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "bad-version",
                "user_message": "Plan Tokyo",
                "agent_version": "99.0.0",
            },
        )
    assert response.status_code == 422


def test_agents_expose_typed_multi_domain_contracts(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        response = client.get("/agents")

    assert response.status_code == 200
    agents = {(item["agent_id"], item["version"]): item for item in response.json()}
    assert agents[("travel-agent", "0.3.0")]["domain_id"] == "travel"
    legacy_release = agents[("release-validation", "1.0.0")]
    assert legacy_release["domain_id"] == "release-validation"
    assert "replay" not in legacy_release["input_schema"]["properties"]
    release = agents[("release-validation", "1.1.0")]
    assert release["domain_id"] == "release-validation"
    assert release["schema_version"] == "1"
    assert "manifest" in release["input_schema"]["properties"]
    assert "replay" in release["input_schema"]["properties"]


def test_release_validation_runs_through_unified_lifecycle_api(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "release-api-thread",
                "agent_id": "release-validation",
                "agent_version": "1.1.0",
                "input": {"manifest": valid_release_manifest()},
            },
        )
        assert submitted.status_code == 202
        result = wait_for_run(client, submitted.json()["run_id"], timeout=10.0)
        checkpoint = client.get(
            "/threads/release-api-thread/state",
            params={"domain_id": "release-validation", "schema_version": "1"},
        )
        events = client.get(f"/runs/{result['run_id']}/events").json()

    assert result["status"] == "completed"
    assert result["domain_id"] == "release-validation"
    assert result["state"]["current_stage"] == "ready"
    assert result["state"]["result"]["status"] == "ready"
    assert result["validation_errors"] == []
    assert checkpoint.status_code == 200
    assert checkpoint.json()["result"]["run_id"] == result["run_id"]
    assert [event["event_type"] for event in events][-1] == "run.completed"


def test_legacy_agent_message_rejects_release_bound_thread_without_mutating_checkpoint(
    tmp_path,
):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "release-bound-thread",
                "agent_id": "release-validation",
                "agent_version": "1.1.0",
                "input": {"manifest": valid_release_manifest()},
            },
        )
        result = wait_for_run(client, submitted.json()["run_id"], timeout=10.0)
        checkpoint_before = client.get(
            "/threads/release-bound-thread/state",
            params={"domain_id": "release-validation", "schema_version": "1"},
        )

        conflict = client.post(
            "/agent/message",
            json={
                "thread_id": "release-bound-thread",
                "user_message": "Plan a five-day trip to Tokyo.",
            },
        )
        explicit_state_conflict = client.post(
            "/agent/message",
            json={
                "thread_id": "release-bound-thread",
                "user_message": "Plan a five-day trip to Tokyo.",
                "state": {"thread_id": "release-bound-thread"},
            },
        )
        checkpoint_after = client.get(
            "/threads/release-bound-thread/state",
            params={"domain_id": "release-validation", "schema_version": "1"},
        )

    assert result["status"] == "completed"
    assert checkpoint_before.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == (
        "Thread 'release-bound-thread' belongs to domain 'release-validation', not 'travel'"
    )
    assert explicit_state_conflict.status_code == 409
    assert explicit_state_conflict.json()["detail"] == conflict.json()["detail"]
    assert checkpoint_after.status_code == 200
    assert checkpoint_after.json() == checkpoint_before.json()


def test_release_validation_selective_replay_uses_unified_runs_api(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path)
    with TestClient(app) as client:
        source = client.post(
            "/runs",
            json={
                "thread_id": "release-replay-source",
                "agent_id": "release-validation",
                "agent_version": "1.1.0",
                "input": {"manifest": valid_release_manifest()},
            },
        )
        source_result = wait_for_run(client, source.json()["run_id"], timeout=10.0)
        replayed = client.post(
            "/runs",
            json={
                "thread_id": "release-replay-target",
                "agent_id": "release-validation",
                "agent_version": "1.1.0",
                "input": {
                    "manifest": valid_release_manifest(),
                    "replay": {
                        "source_run_id": source_result["run_id"],
                        "step_ids": ["run_unit_tests"],
                    },
                },
            },
        )
        replay_result = wait_for_run(client, replayed.json()["run_id"], timeout=10.0)

    assert replay_result["status"] == "completed"
    summary = replay_result["state"]["result"]["replay"]
    assert summary["source_run_id"] == source_result["run_id"]
    assert summary["replayed_step_ids"] == ["run_unit_tests", "generate_evidence"]
    assert len(summary["reused_step_ids"]) == 4
    target_steps = {
        step.step_id: step for step in SQLiteWorkflowStore(database_path).list_steps(
            replay_result["run_id"]
        )
    }
    assert target_steps["run_unit_tests"].attempt_count == 1
    assert sum(step.attempt_count == 0 for step in target_steps.values()) == 4


def test_release_validation_replay_contract_rejects_duplicate_steps_before_queueing(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path)
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "duplicate-replay-steps",
                "client_request_id": "duplicate-replay-steps-request",
                "agent_id": "release-validation",
                "agent_version": "1.1.0",
                "input": {
                    "manifest": valid_release_manifest(),
                    "replay": {
                        "source_run_id": "run_source",
                        "step_ids": ["run_unit_tests", "run_unit_tests"],
                    },
                },
            },
        )

    assert response.status_code == 422
    assert (
        SQLiteRunStore(database_path).get_run_by_client_request_id(
            "duplicate-replay-steps-request"
        )
        is None
    )


def test_release_validation_v1_contract_rejects_phase3b_replay_before_queueing(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path)
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "legacy-replay-contract",
                "client_request_id": "legacy-replay-contract-request",
                "agent_id": "release-validation",
                "agent_version": "1.0.0",
                "input": {
                    "manifest": valid_release_manifest(),
                    "replay": {
                        "source_run_id": "run_source",
                        "step_ids": ["run_unit_tests"],
                    },
                },
            },
        )

    assert response.status_code == 422
    assert "Extra inputs are not permitted" in response.json()["detail"]
    assert (
        SQLiteRunStore(database_path).get_run_by_client_request_id(
            "legacy-replay-contract-request"
        )
        is None
    )


def test_release_business_block_is_not_reported_as_runtime_failure(tmp_path):
    manifest = valid_release_manifest()
    manifest["tests_passed"] = False
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "blocked-release-thread",
                "agent_id": "release-validation",
                "agent_version": "1.0.0",
                "input": {"manifest": manifest},
            },
        )
        result = wait_for_run(client, submitted.json()["run_id"], timeout=10.0)

    assert result["status"] == "completed"
    assert result["state"]["result"]["status"] == "blocked"
    assert any("unit_test_suite_passed" in error for error in result["validation_errors"])


def test_unified_api_rejects_domain_invalid_input_before_queueing(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "invalid-release-input",
                "agent_id": "release-validation",
                "agent_version": "1.0.0",
                "input": {"manifest": {"release_id": "incomplete"}},
            },
        )

    assert response.status_code == 422


def test_unified_api_rejects_release_state_for_travel_agent(tmp_path):
    database_path = tmp_path / "runtime.db"
    app = create_app(database_path=database_path)
    with TestClient(app) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "wrong-domain-state",
                "client_request_id": "wrong-domain-state-request",
                "agent_id": "travel-agent",
                "agent_version": "0.3.0",
                "input": {"user_message": "Plan a five-day trip to Tokyo."},
                "state": {
                    "thread_id": "wrong-domain-state",
                    "execution_trace": [],
                    "manifest": valid_release_manifest(),
                    "result": None,
                    "current_stage": "initialized",
                },
            },
        )

    assert response.status_code == 422
    assert "Extra inputs are not permitted" in response.json()["detail"]
    assert (
        SQLiteRunStore(database_path).get_run_by_client_request_id(
            "wrong-domain-state-request"
        )
        is None
    )


def test_same_thread_id_cannot_silently_cross_domain_checkpoint_boundary(tmp_path):
    app = create_app(database_path=tmp_path / "runtime.db")
    with TestClient(app) as client:
        travel = client.post(
            "/runs",
            json={
                "thread_id": "globally-scoped-thread",
                "input": {"user_message": "I want a 5-day Tokyo trip under 9000 SGD."},
            },
        )
        travel_result = wait_for_run(client, travel.json()["run_id"])
        release = client.post(
            "/runs",
            json={
                "thread_id": "globally-scoped-thread",
                "agent_id": "release-validation",
                "agent_version": "1.0.0",
                "input": {"manifest": valid_release_manifest()},
            },
        )
        release_result = wait_for_run(client, release.json()["run_id"])
        travel_checkpoint = client.get("/threads/globally-scoped-thread/state")

    assert travel_result["status"] == "completed"
    assert release_result["status"] == "failed"
    assert "belongs to domain 'travel'" in release_result["error"]
    assert travel_checkpoint.json()["destination"] == "Tokyo"
