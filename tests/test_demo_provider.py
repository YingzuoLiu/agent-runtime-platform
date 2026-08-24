from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from demo_provider.app import create_demo_provider_app
from runtime_service.external_actions import ExternalActionRequest


def provider_request(*, run_id: str, key: str) -> ExternalActionRequest:
    return ExternalActionRequest(
        action_id=f"provider-action-{run_id}",
        run_id=run_id,
        step_id="dispatch",
        tenant_id="demo-tenant",
        subject_id="demo-subject",
        workflow_type="durable-action:webhook.send:1",
        tool_name="webhook.send",
        arguments={"payload": {"message": "hello"}},
        idempotency_key=key,
    )


def wait_for_held_effect(
    client: TestClient,
    run_id: str,
    *,
    expected_attempts: int,
) -> dict:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/proof/actions/{run_id}")
        if response.status_code == 200:
            state = response.json()
            if (
                state["waiting_for_release"]
                and state["attempt_count"] == expected_attempts
            ):
                return state
        time.sleep(0.02)
    raise AssertionError(
        f"Provider did not hold attempt {expected_attempts} for {run_id}"
    )


def test_demo_provider_health_is_public_and_minimal(tmp_path):
    app = create_demo_provider_app(tmp_path / "provider.db")
    with TestClient(app) as client:
        response = client.get("/health")
        docs = client.get("/docs")
        schema = client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert docs.status_code == 404
    assert schema.status_code == 404


def test_idempotent_provider_persists_one_effect_and_replays_receipt_after_restart(
    tmp_path,
):
    database_path = tmp_path / "provider.db"
    run_id = "run-idempotent-proof"
    provider_key = "private-server-derived-key"
    body = provider_request(run_id=run_id, key=provider_key).model_dump(mode="json")
    headers = {"Idempotency-Key": provider_key}
    first_app = create_demo_provider_app(database_path)

    with TestClient(first_app) as first_client:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(
                first_client.post,
                "/actions/idempotent",
                json=body,
                headers=headers,
            )
            held = wait_for_held_effect(
                first_client,
                run_id,
                expected_attempts=1,
            )
            released = first_client.post(f"/proof/actions/{run_id}/release", json={})
            first_response = pending.result(timeout=5)

    assert held["attempt_count"] == 1
    assert held["effect_count"] == 1
    assert released.status_code == 200
    assert first_response.status_code == 503

    recovered_app = create_demo_provider_app(database_path)
    with TestClient(recovered_app) as recovered_client:
        receipt = recovered_client.post(
            "/actions/idempotent",
            json=body,
            headers=headers,
        )
        proof = recovered_client.get(f"/proof/actions/{run_id}")

    assert receipt.status_code == 200
    assert receipt.json() == {
        "provider_reference": proof.json()["provider_reference"],
        "result": {},
    }
    assert proof.status_code == 200
    assert proof.json()["attempt_count"] == 2
    assert proof.json()["effect_count"] == 1
    assert proof.json()["request_identity_count"] == 1
    assert proof.json()["idempotency_identity_count"] == 1
    assert proof.json()["waiting_for_release"] is False
    assert [event["event_type"] for event in proof.json()["events"]] == [
        "attempt.received",
        "effect.committed",
        "fault.release_requested",
        "response.ambiguous",
        "attempt.received",
        "receipt.replayed",
    ]
    rendered = proof.text + receipt.text
    assert provider_key not in rendered
    assert "tenant_id" not in rendered
    assert "subject_id" not in rendered


def test_unsafe_provider_does_not_deduplicate_a_repeated_transport_request(tmp_path):
    run_id = "run-unsafe-proof"
    provider_key = "unsafe-server-derived-key"
    body = provider_request(run_id=run_id, key=provider_key).model_dump(mode="json")
    app = create_demo_provider_app(tmp_path / "provider.db")

    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(
                client.post,
                "/actions/unsafe",
                json=body,
                headers={"Idempotency-Key": provider_key},
            )
            wait_for_held_effect(client, run_id, expected_attempts=1)
            first_released = client.post(
                f"/proof/actions/{run_id}/release",
                json={},
            )
            first_response = pending.result(timeout=5)
            repeated = executor.submit(
                client.post,
                "/actions/unsafe",
                json=body,
                headers={"Idempotency-Key": provider_key},
            )
            second_held = wait_for_held_effect(
                client,
                run_id,
                expected_attempts=2,
            )
            second_released = client.post(
                f"/proof/actions/{run_id}/release",
                json={},
            )
            second_response = repeated.result(timeout=5)
        proof = client.get(f"/proof/actions/{run_id}").json()

    assert first_response.status_code == 503
    assert second_response.status_code == 503
    assert first_released.status_code == 200
    assert second_released.status_code == 200
    assert second_held["attempt_count"] == 2
    assert second_held["effect_count"] == 2
    assert proof["scenario"] == "unsafe"
    assert proof["attempt_count"] == 2
    assert proof["effect_count"] == 2
    assert proof["request_identity_count"] == 1
    assert proof["idempotency_identity_count"] == 1
    assert "receipt.replayed" not in {event["event_type"] for event in proof["events"]}


def test_provider_rejects_header_mismatch_before_writing_ledger(tmp_path):
    run_id = "run-header-mismatch"
    body = provider_request(run_id=run_id, key="body-key").model_dump(mode="json")
    app = create_demo_provider_app(tmp_path / "provider.db")

    with TestClient(app) as client:
        response = client.post(
            "/actions/idempotent",
            json=body,
            headers={"Idempotency-Key": "other-key"},
        )
        proof = client.get(f"/proof/actions/{run_id}")

    assert response.status_code == 400
    assert proof.status_code == 404


def test_known_success_and_failure_are_provider_controls(tmp_path):
    app = create_demo_provider_app(tmp_path / "provider.db")
    success = provider_request(run_id="run-known-success", key="success-key")
    failure = provider_request(run_id="run-known-failure", key="failure-key")

    with TestClient(app) as client:
        success_response = client.post(
            "/actions/known-success",
            json=success.model_dump(mode="json"),
            headers={"Idempotency-Key": success.idempotency_key},
        )
        failure_response = client.post(
            "/actions/known-failure",
            json=failure.model_dump(mode="json"),
            headers={"Idempotency-Key": failure.idempotency_key},
        )
        success_proof = client.get("/proof/actions/run-known-success").json()
        failure_proof = client.get("/proof/actions/run-known-failure").json()

    assert success_response.status_code == 200
    assert success_proof["scenario"] == "known_success"
    assert success_proof["attempt_count"] == 1
    assert success_proof["effect_count"] == 1
    assert success_proof["request_identity_count"] == 1
    assert success_proof["idempotency_identity_count"] == 1
    assert success_proof["waiting_for_release"] is False
    assert [event["event_type"] for event in success_proof["events"]] == [
        "attempt.received",
        "effect.committed",
        "response.success",
    ]

    assert failure_response.status_code == 422
    assert failure_response.json() == {
        "error": {"code": "injected_definitive_failure"}
    }
    assert failure_proof["scenario"] == "known_failure"
    assert failure_proof["attempt_count"] == 1
    assert failure_proof["effect_count"] == 0
    assert failure_proof["request_identity_count"] == 1
    assert failure_proof["idempotency_identity_count"] == 1
    assert failure_proof["provider_reference"] is None
    assert failure_proof["waiting_for_release"] is False
    assert [event["event_type"] for event in failure_proof["events"]] == [
        "attempt.received",
        "failure.definitive",
    ]
