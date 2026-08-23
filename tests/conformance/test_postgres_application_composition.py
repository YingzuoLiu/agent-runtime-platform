from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

from agent.contracts import RuntimeExecutionAuthority
from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    RunRecord,
    RunStatus,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.external_actions import (
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.postgres_memory_store import PostgresMemoryStore
from runtime_service.postgres_schema import (
    POSTGRES_MEMORY_SCHEMA_VERSION,
    POSTGRES_SCHEMA_VERSION,
    PostgresSchemaError,
    bootstrap_postgres_application_schema,
    inspect_postgres_application_schema,
    open_postgres_connection,
    validate_postgres_application_schema,
)
from runtime_service.postgres_store import PostgresRunStore
from runtime_service.postgres_workflow_store import PostgresWorkflowStore
from runtime_service.storage import (
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)

from .backends import PostgresConformanceBackend, StoreConformanceBackend


ROOT = Path(__file__).resolve().parents[2]
API_KEY = "postgres-application-key"
TENANT_ID = "postgres-application-tenant"
SUBJECT_ID = "postgres-application-subject"
AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="postgres-application-credential",
            api_key=API_KEY,
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            role=RuntimeRole.OPERATOR,
        )
    ]
)


class NeverCalledTravelProvider:
    supports_idempotency = True
    provider_identity = "postgres-application-never-called-v1"

    def execute(self, _request: ExternalActionRequest) -> ExternalActionProviderResult:
        raise AssertionError("Memory composition test must not dispatch a travel action")


def _postgres_backend(
    store_backend: StoreConformanceBackend,
) -> PostgresConformanceBackend:
    if not isinstance(store_backend, PostgresConformanceBackend):
        pytest.skip("PostgreSQL application-composition proof")
    return store_backend


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {API_KEY}"}


def _wait_for_terminal(client: TestClient, run_id: str) -> dict:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        response = client.get(f"/runs/{run_id}")
        assert response.status_code == 200, response.text
        body = response.json()
        if body["status"] in {"completed", "failed", "cancelled"}:
            return body
        time.sleep(0.02)
    raise AssertionError(f"PostgreSQL-composed Run did not finish: {run_id}")


def _submit_memory_run(client: TestClient, thread_id: str, message: str) -> tuple[str, dict]:
    response = client.post(
        "/runs",
        json={
            "thread_id": thread_id,
            "agent_id": "travel-agent",
            "agent_version": "1.1.0",
            "input": {"user_message": message},
        },
    )
    assert response.status_code == 202, response.text
    run_id = response.json()["run_id"]
    return run_id, _wait_for_terminal(client, run_id)


def test_postgres_application_validator_is_read_only_and_bootstrap_rereads(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    before = inspect_postgres_application_schema(backend.dsn, schema=backend.schema)
    assert before.schema_exists is False
    assert before.components == {}

    with pytest.raises(PostgresSchemaError, match="not initialized"):
        validate_postgres_application_schema(backend.dsn, schema=backend.schema)
    after_validation = inspect_postgres_application_schema(
        backend.dsn,
        schema=backend.schema,
    )
    assert after_validation == before

    versions = bootstrap_postgres_application_schema(
        backend.dsn,
        schema=backend.schema,
    )
    assert versions == {
        "execution-plane": POSTGRES_SCHEMA_VERSION,
        "memory": POSTGRES_MEMORY_SCHEMA_VERSION,
    }
    assert validate_postgres_application_schema(
        backend.dsn,
        schema=backend.schema,
    ) == versions
    assert inspect_postgres_application_schema(
        backend.dsn,
        schema=backend.schema,
    ).compatible


def test_postgres_bootstrap_command_dry_run_apply_and_repeat(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    environment = os.environ.copy()
    environment.pop("RUNTIME_DB_PATH", None)
    environment["RUNTIME_STORE_BACKEND"] = "postgres"
    environment["RUNTIME_POSTGRES_DSN"] = backend.dsn
    environment["RUNTIME_POSTGRES_SCHEMA"] = backend.schema

    def command(action: str) -> dict:
        completed = subprocess.run(
            [sys.executable, "-m", "runtime_service.postgres_bootstrap", action],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert completed.returncode == 0, completed.stdout
        assert backend.dsn not in completed.stdout
        return json.loads(completed.stdout)

    planned = command("--dry-run")
    assert planned["action"] == "would_bootstrap_or_validate"
    assert planned["schema_exists"] is False

    applied = command("--apply")
    assert applied == {
        "action": "applied_and_validated",
        "components": {"execution-plane": 1, "memory": 1},
        "schema": backend.schema,
        "status": "ok",
    }
    assert command("--dry-run")["action"] == "no_change"


def test_postgres_store_bundle_uses_one_validated_authority(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    bootstrap_postgres_application_schema(backend.dsn, schema=backend.schema)
    config = resolve_runtime_storage_config(
        backend="postgres",
        postgres_dsn=backend.dsn,
        postgres_schema=backend.schema,
        environment={},
    )
    bundle = build_runtime_store_bundle(config)

    assert isinstance(bundle.run_store, PostgresRunStore)
    assert isinstance(bundle.memory_store, PostgresMemoryStore)
    assert isinstance(bundle.workflow_store, PostgresWorkflowStore)
    assert {
        bundle.run_store.schema,
        bundle.memory_store.schema,
        bundle.workflow_store.schema,
    } == {backend.schema}
    assert bundle.metadata.schema_versions == {
        "execution-plane": 1,
        "memory": 1,
    }
    assert backend.dsn not in json.dumps(bundle.metadata.public_dict())
    assert bundle.metadata.connection_policy[
        "idle_in_transaction_session_timeout_seconds"
    ] == 5.0
    connection = bundle.run_store._connect()
    try:
        timeout = connection.execute(
            """
            SELECT extract(
                epoch FROM current_setting('idle_in_transaction_session_timeout')::interval
            ) AS seconds
            """
        ).fetchone()
    finally:
        connection.close()
    assert timeout is not None and float(timeout["seconds"]) == 5.0
    bundle.run_store.ping()
    bundle.memory_store.ping()
    bundle.workflow_store.ping()


def test_postgres_stalled_transaction_holder_is_terminated(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    connection = open_postgres_connection(
        backend.dsn,
        schema=backend.schema,
        idle_in_transaction_session_timeout_seconds=0.25,
    )
    try:
        connection.execute("BEGIN")
        connection.execute("SELECT 1").fetchone()
        time.sleep(0.75)
        with pytest.raises(psycopg.errors.IdleInTransactionSessionTimeout):
            connection.execute("SELECT 1").fetchone()
    finally:
        connection.close()


def test_postgres_application_never_falls_back_to_sqlite_provider(
    store_backend: StoreConformanceBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _postgres_backend(store_backend)
    monkeypatch.delenv("RUNTIME_DB_PATH", raising=False)
    monkeypatch.delenv("RUNTIME_TRAVEL_ACTION_PROVIDER_URL", raising=False)
    bootstrap_postgres_application_schema(backend.dsn, schema=backend.schema)
    app = create_app(
        store_backend="postgres",
        postgres_dsn=backend.dsn,
        postgres_schema=backend.schema,
        authenticator=AUTHENTICATOR,
    )

    with pytest.raises(ValueError, match="SQLite provider cannot be selected implicitly"):
        with TestClient(app):
            pass


def test_postgres_application_processes_restarts_and_administers_memory(
    store_backend: StoreConformanceBackend,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backend = _postgres_backend(store_backend)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RUNTIME_DB_PATH", raising=False)
    bootstrap_postgres_application_schema(backend.dsn, schema=backend.schema)
    app_options = {
        "store_backend": "postgres",
        "postgres_dsn": backend.dsn,
        "postgres_schema": backend.schema,
        "authenticator": AUTHENTICATOR,
        "travel_action_provider": NeverCalledTravelProvider(),
        "worker_count": 1,
    }

    first_app = create_app(**app_options)
    with TestClient(first_app, headers=_headers()) as client:
        ready = client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["storage"] == {
            "backend": "postgres",
            "schema": backend.schema,
            "schema_versions": {"execution-plane": 1, "memory": 1},
            "connection_policy": {
                "mode": "short-lived-per-operation",
                "connect_timeout_seconds": 5.0,
                "statement_timeout_seconds": 30.0,
                "lock_timeout_seconds": 5.0,
                "idle_in_transaction_session_timeout_seconds": 5.0,
                "lease_operation_timeout_seconds": 1.0,
            },
        }
        _run_id, completed = _submit_memory_run(
            client,
            "postgres-memory-source",
            "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights.",
        )
        assert completed["status"] == "completed"
        memories = client.get("/memories").json()
        assert [(item["key"], item["value"]) for item in memories] == [
            ("flight.avoid_red_eye", True)
        ]
        memory_id = memories[0]["memory_id"]

    recovery_store = PostgresRunStore(
        backend.dsn,
        schema=backend.schema,
        initialize=False,
    )
    recoverable = RunRecord(
        run_id="postgres-application-recoverable",
        tenant_id=TENANT_ID,
        thread_id="postgres-application-recovery-thread",
        agent_id="travel-agent",
        agent_version="1.0.0",
        domain_id="travel",
        schema_version="1",
        status=RunStatus.QUEUED,
        input={"user_message": "Plan a 5-day Tokyo trip under 9000 SGD."},
        execution_authority=RuntimeExecutionAuthority(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            permissions=(RuntimePermission.TOOLS_EXECUTE.value,),
        ),
    )
    recovery_store.create_run_with_event(recoverable, event_type="run.queued")
    abandoned = recovery_store.claim_next_run(
        owner_id="terminated-worker",
        lease_duration_seconds=60,
    )
    assert abandoned is not None and abandoned.run.run_id == recoverable.run_id
    assert recovery_store.expire_run_lease(
        recoverable.run_id,
        lease_token=abandoned.lease_token,
    )

    restarted_app = create_app(**app_options)
    with TestClient(restarted_app, headers=_headers()) as client:
        recovered = _wait_for_terminal(client, recoverable.run_id)
        assert recovered["status"] == "completed"
        assert recovered["attempt"] == 2
        _run_id, completed = _submit_memory_run(
            client,
            "postgres-memory-restart",
            "I want a 5-day Tokyo trip under 9000 SGD.",
        )
        assert completed["status"] == "completed"
        assert completed["state"]["itinerary"]["flight_type"] == "daytime"
        deleted = client.delete(f"/memories/{memory_id}")
        assert deleted.status_code == 200
        assert deleted.json()["status"] == "deleted"
        assert client.get("/memories").json() == []

    assert list(tmp_path.rglob("*.db")) == []
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        idle = connection.execute(
            """
            SELECT count(*) AS count
            FROM pg_stat_activity
            WHERE datname = current_database()
                AND usename = current_user
                AND state = 'idle in transaction'
            """
        ).fetchone()
    finally:
        connection.close()
    assert idle is not None and int(idle["count"]) == 0
