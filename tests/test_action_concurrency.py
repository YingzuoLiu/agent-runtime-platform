from __future__ import annotations

import hashlib
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.external_actions import (
    ExternalActionProviderResult,
    ExternalActionRequest,
)


TENANT_A_KEY = "action-concurrency-a"
TENANT_B_KEY = "action-concurrency-b"
TENANT_A = "action-concurrency-tenant-a"
TENANT_B = "action-concurrency-tenant-b"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="action-concurrency-a",
            api_key=TENANT_A_KEY,
            tenant_id=TENANT_A,
            subject_id="action-concurrency-subject-a",
            role=RuntimeRole.OPERATOR,
        ),
        ApiKeyCredential(
            credential_id="action-concurrency-b",
            api_key=TENANT_B_KEY,
            tenant_id=TENANT_B,
            subject_id="action-concurrency-subject-b",
            role=RuntimeRole.OPERATOR,
        ),
    ]
)


def headers(key: str = TENANT_A_KEY) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def payload(*, key: str = "concurrent-key", text: str = "hello") -> dict:
    return {
        "action_type": "webhook.send",
        "destination": "demo",
        "idempotency_key": key,
        "input": {"payload": {"text": text}},
    }


class SharedProvider:
    supports_idempotency = True
    provider_identity = "action-concurrency-provider-v1"

    def __init__(self) -> None:
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


class BlockingProvider(SharedProvider):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        with self._lock:
            self.requests.append(request)
        self.started.set()
        if not self.release.wait(timeout=10):
            raise AssertionError("test did not release provider")
        reference = "delivery_" + hashlib.sha256(
            request.idempotency_key.encode("utf-8")
        ).hexdigest()[:16]
        return ExternalActionProviderResult(
            provider_reference=reference,
            result={},
        )


def wait_for_status(client: TestClient, action_id: str, expected: str) -> dict:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        body = client.get(f"/actions/{action_id}").json()
        if body["status"] == expected:
            return body
        time.sleep(0.02)
    raise AssertionError(f"Action did not reach {expected}")


def test_two_runtime_managers_share_one_idempotent_action_and_dispatch(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = SharedProvider()
    app_a = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )
    app_b = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with (
        TestClient(app_a, headers=headers()) as client_a,
        TestClient(app_b, headers=headers()) as client_b,
    ):
        barrier = threading.Barrier(3)

        def submit(client: TestClient):
            barrier.wait()
            return client.post("/actions?wait=5", json=payload())

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(submit, client_a)
            second = executor.submit(submit, client_b)
            barrier.wait()
            responses = [first.result(timeout=8), second.result(timeout=8)]

    assert [response.status_code for response in responses] == [200, 200]
    assert len({response.json()["action_id"] for response in responses}) == 1
    assert len(provider.requests) == 1


def test_cross_manager_different_payload_race_executes_only_the_winner(tmp_path):
    database_path = tmp_path / "runtime.db"
    provider = SharedProvider()
    app_a = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )
    app_b = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with (
        TestClient(app_a, headers=headers()) as client_a,
        TestClient(app_b, headers=headers()) as client_b,
    ):
        barrier = threading.Barrier(3)

        def submit(client: TestClient, text: str):
            barrier.wait()
            return client.post("/actions?wait=5", json=payload(text=text))

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(submit, client_a, "first")
            second = executor.submit(submit, client_b, "second")
            barrier.wait()
            responses = [first.result(timeout=8), second.result(timeout=8)]

    assert sorted(response.status_code for response in responses) == [200, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["error"]["code"] == "idempotency_key_reused"
    assert len(provider.requests) == 1
    assert provider.requests[0].arguments["payload"]["text"] in {"first", "second"}


def test_same_key_is_isolated_by_authenticated_tenant(tmp_path):
    provider = SharedProvider()
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app) as client:
        tenant_a = client.post(
            "/actions?wait=5",
            headers=headers(TENANT_A_KEY),
            json=payload(),
        )
        tenant_b = client.post(
            "/actions?wait=5",
            headers=headers(TENANT_B_KEY),
            json=payload(),
        )

    assert tenant_a.status_code == 200
    assert tenant_b.status_code == 200
    assert tenant_a.json()["action_id"] != tenant_b.json()["action_id"]
    assert len(provider.requests) == 2
    assert provider.requests[0].tenant_id != provider.requests[1].tenant_id
    assert provider.requests[0].idempotency_key != provider.requests[1].idempotency_key


def test_bounded_wait_times_out_to_202_with_polling_headers(tmp_path):
    provider = BlockingProvider()
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app, headers=headers()) as client:
        started = time.monotonic()
        response = client.post("/actions?wait=0.05", json=payload())
        elapsed = time.monotonic() - started
        assert provider.started.wait(timeout=5)
        provider.release.set()
        completed = wait_for_status(client, response.json()["action_id"], "succeeded")

    assert response.status_code == 202
    assert response.headers["location"] == f"/actions/{response.json()['action_id']}"
    assert response.headers["retry-after"] == "1"
    assert elapsed < 1
    assert completed["status"] == "succeeded"


def test_waiter_limit_returns_202_immediately_instead_of_queueing(tmp_path):
    provider = BlockingProvider()
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
        action_waiter_limit=1,
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions", json=payload())
        assert provider.started.wait(timeout=5)
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(
                client.post,
                "/actions?wait=5",
                json=payload(),
            )
            deadline = time.monotonic() + 5
            while client.app.state.action_waiter_semaphore.value != 0:
                if time.monotonic() >= deadline:
                    raise AssertionError("first waiter did not acquire its slot")
                time.sleep(0.01)
            started = time.monotonic()
            overflow = client.post("/actions?wait=5", json=payload())
            elapsed = time.monotonic() - started
            provider.release.set()
            completed = waiting.result(timeout=8)

    assert created.status_code == 202
    assert overflow.status_code == 202
    assert overflow.json()["status"] in {"queued", "running"}
    assert elapsed < 0.5
    assert completed.status_code == 200
    assert completed.json()["status"] == "succeeded"


def test_many_waiters_do_not_starve_health_threadpool(tmp_path):
    provider = BlockingProvider()
    waiter_count = 48
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
        action_waiter_limit=waiter_count,
    )

    with TestClient(app, headers=headers()) as client:
        created = client.post("/actions", json=payload())
        assert created.status_code == 202
        assert provider.started.wait(timeout=5)
        barrier = threading.Barrier(waiter_count + 1)

        def wait_request():
            barrier.wait()
            return client.post("/actions?wait=5", json=payload())

        with ThreadPoolExecutor(max_workers=waiter_count) as executor:
            futures = [executor.submit(wait_request) for _ in range(waiter_count)]
            barrier.wait()
            deadline = time.monotonic() + 8
            while client.app.state.action_waiter_semaphore.value != 0:
                if time.monotonic() >= deadline:
                    raise AssertionError("not all bounded waiters entered async wait")
                time.sleep(0.01)

            with ThreadPoolExecutor(max_workers=1) as health_executor:
                started = time.monotonic()
                health = health_executor.submit(client.get, "/health").result(timeout=2)
                health_elapsed = time.monotonic() - started

            provider.release.set()
            responses = [future.result(timeout=8) for future in futures]

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert health_elapsed < 1
    assert all(response.status_code == 200 for response in responses), Counter(
        response.status_code for response in responses
    )
    assert all(response.json()["status"] == "succeeded" for response in responses)
