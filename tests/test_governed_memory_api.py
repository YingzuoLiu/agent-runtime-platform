from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    AuthorizationError,
    MemoryKind,
    MemoryWrite,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)


SUBJECT_A_KEY = "memory-subject-a-key"
SUBJECT_A_VIEWER_KEY = "memory-subject-a-viewer-key"
SUBJECT_B_KEY = "memory-subject-b-key"
TENANT_B_KEY = "memory-tenant-b-key"
TENANT_A = "memory-tenant-a"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="memory-subject-a",
            api_key=SUBJECT_A_KEY,
            tenant_id=TENANT_A,
            subject_id="subject-a",
            role=RuntimeRole.OPERATOR,
        ),
        ApiKeyCredential(
            credential_id="memory-subject-a-viewer",
            api_key=SUBJECT_A_VIEWER_KEY,
            tenant_id=TENANT_A,
            subject_id="subject-a",
            role=RuntimeRole.VIEWER,
        ),
        ApiKeyCredential(
            credential_id="memory-subject-b",
            api_key=SUBJECT_B_KEY,
            tenant_id=TENANT_A,
            subject_id="subject-b",
            role=RuntimeRole.OPERATOR,
        ),
        ApiKeyCredential(
            credential_id="memory-tenant-b",
            api_key=TENANT_B_KEY,
            tenant_id="memory-tenant-b",
            subject_id="subject-a",
            role=RuntimeRole.OPERATOR,
        ),
    ]
)


def headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def wait_for_terminal(
    client: TestClient,
    run_id: str,
    *,
    api_key: str = SUBJECT_A_KEY,
    timeout: float = 5.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/runs/{run_id}", headers=headers(api_key))
        assert response.status_code == 200, response.text
        body = response.json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"run did not finish: {run_id}")


def submit_memory_run(
    client: TestClient,
    thread_id: str,
    message: str,
    *,
    api_key: str = SUBJECT_A_KEY,
) -> tuple[str, dict]:
    response = client.post(
        "/runs",
        headers=headers(api_key),
        json={
            "thread_id": thread_id,
            "agent_id": "travel-agent",
            "agent_version": "1.1.0",
            "input": {"user_message": message},
        },
    )
    assert response.status_code == 202, response.text
    run_id = response.json()["run_id"]
    return run_id, wait_for_terminal(client, run_id, api_key=api_key)


def search_arguments(client: TestClient, run_id: str, *, api_key: str) -> dict:
    events = client.get(
        f"/runs/{run_id}/events",
        headers=headers(api_key),
    ).json()
    return next(
        event["payload"]["decision"]["arguments"]
        for event in events
        if event["event_type"] == "planner.decision"
        and event["payload"]["decision"]["decision_type"] == "CALL_TOOL"
        and event["payload"]["decision"]["tool_name"] == "search_trip_options"
    )


def test_cross_thread_memory_survives_restart_is_subject_scoped_and_can_be_forgotten(
    tmp_path,
):
    database_path = tmp_path / "runtime.db"
    first_app = create_app(database_path=database_path, authenticator=AUTHENTICATOR)
    with TestClient(first_app, headers=headers(SUBJECT_A_KEY)) as client:
        source_run_id, source = submit_memory_run(
            client,
            "memory-thread-a",
            "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
        )
        assert source["status"] == "completed"
        assert source["state"]["itinerary"]["flight_type"] == "daytime"
        assert "avoid_red_eye" not in source["state"]["preferences"]
        memories = client.get("/memories").json()
        assert [(memory["key"], memory["value"], memory["version"]) for memory in memories] == [
            ("flight.avoid_red_eye", True, 1)
        ]
        source_events = client.get(f"/runs/{source_run_id}/events").json()
        assert "memory.created" in {event["event_type"] for event in source_events}
        memory_id = memories[0]["memory_id"]

    restarted_app = create_app(database_path=database_path, authenticator=AUTHENTICATOR)
    with TestClient(restarted_app, headers=headers(SUBJECT_A_KEY)) as client:
        retrieved_run_id, retrieved = submit_memory_run(
            client,
            "memory-thread-b",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert retrieved["status"] == "completed"
        assert retrieved["state"]["itinerary"]["flight_type"] == "daytime"
        assert "avoid_red_eye" not in retrieved["state"]["preferences"]
        assert search_arguments(
            client,
            retrieved_run_id,
            api_key=SUBJECT_A_KEY,
        )["avoid_red_eye"] is True
        retrieved_events = client.get(f"/runs/{retrieved_run_id}/events").json()
        memory_event = next(
            event for event in retrieved_events if event["event_type"] == "memory.retrieved"
        )
        assert memory_event["payload"]["count"] == 1
        assert memory_event["payload"]["memories"] == [
            {
                "memory_id": memory_id,
                "kind": "preference",
                "key": "flight.avoid_red_eye",
                "version": 1,
            }
        ]

        viewer_read = client.get(
            "/memories",
            headers=headers(SUBJECT_A_VIEWER_KEY),
        )
        assert viewer_read.status_code == 200
        assert viewer_read.json()[0]["memory_id"] == memory_id
        assert client.delete(
            f"/memories/{memory_id}",
            headers=headers(SUBJECT_A_VIEWER_KEY),
        ).status_code == 403

        assert client.get(
            "/memories",
            headers=headers(SUBJECT_B_KEY),
        ).json() == []
        assert client.get(
            "/memories",
            headers=headers(TENANT_B_KEY),
        ).json() == []
        assert client.delete(
            f"/memories/{memory_id}",
            headers=headers(SUBJECT_B_KEY),
        ).status_code == 404

        other_run_id, other_subject = submit_memory_run(
            client,
            "memory-subject-b-thread",
            "I want a 5-day Tokyo trip under 9000 SGD.",
            api_key=SUBJECT_B_KEY,
        )
        assert other_subject["state"]["itinerary"]["flight_type"] == "red_eye"
        assert search_arguments(
            client,
            other_run_id,
            api_key=SUBJECT_B_KEY,
        )["avoid_red_eye"] is False

        deleted = client.delete(f"/memories/{memory_id}")
        assert deleted.status_code == 200
        assert deleted.json()["status"] == "deleted"
        assert deleted.json()["value"] is None
        assert client.get("/memories").json() == []
        inactive = client.get("/memories", params={"include_inactive": True}).json()
        assert inactive[0]["memory_id"] == memory_id
        assert inactive[0]["value"] is None

        after_delete_run_id, after_delete = submit_memory_run(
            client,
            "memory-thread-b",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert after_delete["state"]["itinerary"]["flight_type"] == "red_eye"
        assert search_arguments(
            client,
            after_delete_run_id,
            api_key=SUBJECT_A_KEY,
        )["avoid_red_eye"] is False
        assert next(
            event
            for event in client.get(f"/runs/{after_delete_run_id}/events").json()
            if event["event_type"] == "memory.retrieved"
        )["payload"]["count"] == 0

        audit_events = client.app.state.memory_store.list_events_for_subject(
            tenant_id=TENANT_A,
            subject_id="subject-a",
        )
        assert [event.event_type for event in audit_events] == [
            "memory.created",
            "memory.deleted",
        ]


class DenyMemoryPermissionAuthorizer:
    def __init__(self, denied: RuntimePermission) -> None:
        self.denied = denied

    def authorize(self, _principal, permission: RuntimePermission) -> None:
        if permission == self.denied:
            raise AuthorizationError("Operation not permitted")


@pytest.mark.parametrize(
    ("denied_permission", "expect_snapshot"),
    [
        (RuntimePermission.MEMORY_READ, False),
        (RuntimePermission.MEMORY_WRITE, True),
    ],
)
def test_worker_uses_persisted_memory_permissions_and_fails_closed(
    tmp_path,
    denied_permission,
    expect_snapshot,
):
    database_path = tmp_path / f"{denied_permission.value}.db"
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        authorizer=DenyMemoryPermissionAuthorizer(denied_permission),
    )
    with TestClient(app, headers=headers(SUBJECT_A_KEY)) as client:
        run_id, result = submit_memory_run(
            client,
            f"permission-{denied_permission.value}",
            "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
        )
        assert result["status"] == "failed"
        assert result["error_code"] == "memory_permission_denied"
        assert client.app.state.memory_store.list_memories(
            tenant_id=TENANT_A,
            subject_id="subject-a",
        ) == []
        snapshot = client.app.state.memory_store.get_run_snapshot(run_id)
        assert (snapshot is not None) is expect_snapshot
        failed = next(
            event
            for event in client.get(f"/runs/{run_id}/events").json()
            if event["event_type"] == "run.failed"
        )
        assert failed["payload"]["error_code"] == "memory_permission_denied"


def test_invalid_stored_memory_fails_before_planner_execution(tmp_path):
    app = create_app(
        database_path=tmp_path / "invalid-memory.db",
        authenticator=AUTHENTICATOR,
    )
    with TestClient(app, headers=headers(SUBJECT_A_KEY)) as client:
        client.app.state.memory_store.upsert(
            tenant_id=TENANT_A,
            subject_id="subject-a",
            domain_id="travel",
            write=MemoryWrite(
                kind=MemoryKind.PREFERENCE,
                key="flight.avoid_red_eye",
                value="not-a-boolean",
            ),
            source_run_id="internal-seed",
            source_thread_id="internal-seed",
            actor_subject_id="subject-a",
        )

        run_id, result = submit_memory_run(
            client,
            "invalid-memory-thread",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert result["status"] == "failed"
        assert result["error_code"] == "invalid_memory_record"
        events = client.get(f"/runs/{run_id}/events").json()
        assert "memory.retrieved" in {event["event_type"] for event in events}
        assert "planner.decision" not in {event["event_type"] for event in events}


def test_phase5a_version_remains_memory_free(tmp_path):
    app = create_app(
        database_path=tmp_path / "phase5a.db",
        authenticator=AUTHENTICATOR,
    )
    with TestClient(app, headers=headers(SUBJECT_A_KEY)) as client:
        response = client.post(
            "/runs",
            json={
                "thread_id": "phase5a-memory-boundary",
                "agent_id": "travel-agent",
                "agent_version": "1.0.0",
                "input": {
                    "user_message": (
                        "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
                    )
                },
            },
        )
        assert response.status_code == 202
        run_id = response.json()["run_id"]
        assert wait_for_terminal(client, run_id)["status"] == "completed"
        assert client.get("/memories").json() == []
        assert client.app.state.memory_store.get_run_snapshot(run_id) is None
        events = client.get(f"/runs/{run_id}/events").json()
        assert not any(event["event_type"].startswith("memory.") for event in events)


def test_current_message_overrides_snapshot_and_supersedes_active_memory(tmp_path):
    app = create_app(
        database_path=tmp_path / "memory-conflict.db",
        authenticator=AUTHENTICATOR,
    )
    with TestClient(app, headers=headers(SUBJECT_A_KEY)) as client:
        _, initial = submit_memory_run(
            client,
            "memory-conflict-a",
            "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
        )
        assert initial["state"]["itinerary"]["flight_type"] == "daytime"

        update_run_id, updated = submit_memory_run(
            client,
            "memory-conflict-b",
            "I want a 5-day Tokyo trip under 9000 SGD and allow red-eye flights.",
        )
        assert updated["state"]["itinerary"]["flight_type"] == "red_eye"
        assert search_arguments(
            client,
            update_run_id,
            api_key=SUBJECT_A_KEY,
        )["avoid_red_eye"] is False
        active = client.get("/memories").json()
        assert [(record["version"], record["value"]) for record in active] == [
            (2, False)
        ]
        update_events = client.get(f"/runs/{update_run_id}/events").json()
        assert {event["event_type"] for event in update_events} >= {
            "memory.retrieved",
            "memory.superseded",
            "memory.created",
        }

        latest_run_id, latest = submit_memory_run(
            client,
            "memory-conflict-c",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert latest["state"]["itinerary"]["flight_type"] == "red_eye"
        latest_memory_event = next(
            event
            for event in client.get(f"/runs/{latest_run_id}/events").json()
            if event["event_type"] == "memory.retrieved"
        )
        assert latest_memory_event["payload"]["memories"][0]["version"] == 2


def test_explicit_reverse_preferences_supersede_memory_without_negation_drift(tmp_path):
    app = create_app(
        database_path=tmp_path / "memory-negation.db",
        authenticator=AUTHENTICATOR,
    )
    with TestClient(app, headers=headers(SUBJECT_A_KEY)) as client:
        _, initial = submit_memory_run(
            client,
            "memory-negation-a",
            (
                "I want a 5-day Tokyo trip under 9000 SGD, avoid red-eye flights, "
                "prefer a hotel near subway, and like relaxed travel."
            ),
        )
        assert initial["state"]["itinerary"]["flight_type"] == "daytime"
        assert {
            record["key"]: (record["version"], record["value"])
            for record in client.get("/memories").json()
        } == {
            "flight.avoid_red_eye": (1, True),
            "hotel.near_subway": (1, True),
            "travel.style": (1, "relaxed"),
        }

        update_run_id, updated = submit_memory_run(
            client,
            "memory-negation-b",
            (
                "I want a 5-day Tokyo trip under 9000 SGD. "
                "I do not mind red-eye flights, I do not want a hotel near subway, "
                "and I prefer NOT a relaxed travel style."
            ),
        )
        assert updated["state"]["itinerary"]["flight_type"] == "red_eye"
        arguments = search_arguments(
            client,
            update_run_id,
            api_key=SUBJECT_A_KEY,
        )
        assert {
            key: arguments[key]
            for key in ("avoid_red_eye", "hotel_near_subway", "travel_style")
        } == {
            "avoid_red_eye": False,
            "hotel_near_subway": False,
            "travel_style": "balanced",
        }
        assert {
            record["key"]: (record["version"], record["value"])
            for record in client.get("/memories").json()
        } == {
            "flight.avoid_red_eye": (2, False),
            "hotel.near_subway": (2, False),
            "travel.style": (2, "balanced"),
        }
        assert {event["event_type"] for event in client.get(
            f"/runs/{update_run_id}/events"
        ).json()} >= {"memory.superseded", "memory.created"}

        latest_run_id, latest = submit_memory_run(
            client,
            "memory-negation-c",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert latest["state"]["itinerary"]["flight_type"] == "red_eye"
        assert search_arguments(
            client,
            latest_run_id,
            api_key=SUBJECT_A_KEY,
        )["travel_style"] == "balanced"
