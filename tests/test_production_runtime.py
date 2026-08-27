from __future__ import annotations

import json
import logging
import os
import shlex
import time
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.deployment import (
    RuntimeDeploymentConfigurationError,
    RuntimeReleaseIdentity,
    resolve_runtime_deployment_config,
)
from runtime_service.external_actions import (
    DefinitiveExternalActionError,
    DisabledExternalActionProvider,
    ExternalActionRequest,
)
from runtime_service.serve import main as serve
from runtime_service.storage import (
    RuntimeStorageConfig,
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)
from runtime_service.structured_logging import (
    JsonLogFormatter,
    runtime_secret_redactions,
)
from runtime_service.workflow_store import SQLiteWorkflowStore


ROOT = Path(__file__).resolve().parents[1]
SOURCE_REVISION = "a" * 40
IMAGE_DIGEST = f"sha256:{'b' * 64}"


def _dockerfile_environment_defaults() -> dict[str, str]:
    logical_lines = (ROOT / "Dockerfile").read_text(encoding="utf-8").replace(
        "\\\n", " "
    )
    defaults: dict[str, str] = {}
    for line in logical_lines.splitlines():
        stripped = line.strip()
        if not stripped.startswith("ENV "):
            continue
        for assignment in shlex.split(stripped.removeprefix("ENV ")):
            name, separator, value = assignment.partition("=")
            if not separator:
                raise AssertionError(f"unsupported Dockerfile ENV syntax: {line}")
            defaults[name] = value
    return defaults


def test_default_deployment_config_is_bounded_and_cloud_neutral() -> None:
    config = resolve_runtime_deployment_config({})

    assert config.worker_count == 1
    assert config.manager_shutdown_grace_seconds == 5
    assert config.http_concurrency_limit == 32
    assert config.server_graceful_shutdown_seconds == 15
    assert config.log_level == "info"
    assert config.external_action_mode == "enabled"
    assert config.release_identity is None


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"RUNTIME_WORKER_COUNT": "0"}, "RUNTIME_WORKER_COUNT"),
        ({"RUNTIME_WORKER_COUNT": "17"}, "RUNTIME_WORKER_COUNT"),
        ({"RUNTIME_WORKER_COUNT": "many"}, "RUNTIME_WORKER_COUNT"),
        ({"RUNTIME_HTTP_CONCURRENCY_LIMIT": "0"}, "RUNTIME_HTTP_CONCURRENCY_LIMIT"),
        ({"RUNTIME_HTTP_CONCURRENCY_LIMIT": "257"}, "RUNTIME_HTTP_CONCURRENCY_LIMIT"),
        (
            {"RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS": "-1"},
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS",
        ),
        (
            {"RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS": "121"},
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS",
        ),
        (
            {"RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS": "5"},
            "must be greater than RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS",
        ),
        ({"RUNTIME_LOG_LEVEL": "verbose"}, "RUNTIME_LOG_LEVEL"),
        ({"RUNTIME_EXTERNAL_ACTION_MODE": "auto"}, "RUNTIME_EXTERNAL_ACTION_MODE"),
        (
            {
                "RUNTIME_EXTERNAL_ACTION_MODE": "disabled",
                "RUNTIME_ACTION_PROVIDERS_JSON": "{}",
            },
            "cannot be combined with external Action provider configuration",
        ),
        (
            {"RUNTIME_RELEASE_IDENTITY_REQUIRED": "sometimes"},
            "RUNTIME_RELEASE_IDENTITY_REQUIRED",
        ),
    ],
)
def test_deployment_config_rejects_unbounded_or_ambiguous_inputs(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(RuntimeDeploymentConfigurationError, match=message):
        resolve_runtime_deployment_config(environment)


def test_release_identity_is_complete_validated_and_public() -> None:
    config = resolve_runtime_deployment_config(
        {
            "RUNTIME_RELEASE_IDENTITY_REQUIRED": "true",
            "RUNTIME_SOURCE_REVISION": SOURCE_REVISION,
            "RUNTIME_IMAGE_DIGEST": IMAGE_DIGEST,
        }
    )

    assert config.release_identity is not None
    assert config.release_identity.public_dict() == {
        "source_revision": SOURCE_REVISION,
        "image_digest": IMAGE_DIGEST,
    }


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        (
            {"RUNTIME_RELEASE_IDENTITY_REQUIRED": "true"},
            "release identity is required",
        ),
        (
            {"RUNTIME_SOURCE_REVISION": SOURCE_REVISION},
            "must be configured together",
        ),
        (
            {
                "RUNTIME_SOURCE_REVISION": "main",
                "RUNTIME_IMAGE_DIGEST": IMAGE_DIGEST,
            },
            "RUNTIME_SOURCE_REVISION",
        ),
        (
            {
                "RUNTIME_SOURCE_REVISION": SOURCE_REVISION,
                "RUNTIME_IMAGE_DIGEST": "latest",
            },
            "RUNTIME_IMAGE_DIGEST",
        ),
    ],
)
def test_release_identity_fails_closed_when_missing_or_unpinned(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(RuntimeDeploymentConfigurationError, match=message):
        resolve_runtime_deployment_config(environment)


def test_explicit_zero_worker_count_cannot_fall_back_to_environment_default(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeDeploymentConfigurationError, match="RUNTIME_WORKER_COUNT"):
        create_app(database_path=tmp_path / "runtime.db", worker_count=0)


def _external_action_request() -> ExternalActionRequest:
    return ExternalActionRequest(
        action_id="action-disabled",
        run_id="run-disabled",
        step_id="step-disabled",
        tenant_id="tenant-disabled",
        subject_id="subject-disabled",
        workflow_type="proof:1",
        tool_name="external.write",
        arguments={},
        idempotency_key="idempotency-disabled",
    )


def test_disabled_external_action_provider_is_explicit_and_definitive() -> None:
    provider = DisabledExternalActionProvider("proof-external-actions-disabled")

    assert provider.provider_identity == "proof-external-actions-disabled"
    assert provider.supports_idempotency is False
    with pytest.raises(DefinitiveExternalActionError):
        provider.execute(_external_action_request())


def test_disabled_external_action_mode_starts_without_a_provider(
    tmp_path: Path,
) -> None:
    config = replace(
        resolve_runtime_deployment_config({}),
        external_action_mode="disabled",
    )
    app = create_app(
        database_path=tmp_path / "runtime.db",
        deployment_config=config,
    )

    with TestClient(app) as client:
        assert client.get("/ready").status_code == 200
        assert isinstance(
            app.state.travel_action_provider,
            DisabledExternalActionProvider,
        )


def test_disabled_external_action_mode_fails_closed_through_the_run_api(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "runtime.db"
    api_key = "disabled-action-proof-key"
    authenticator = StaticApiKeyAuthenticator(
        [
            ApiKeyCredential(
                credential_id="disabled-action-proof",
                api_key=api_key,
                tenant_id="disabled-action-proof-tenant",
                subject_id="disabled-action-proof-subject",
                role=RuntimeRole.OPERATOR,
            )
        ]
    )
    config = replace(
        resolve_runtime_deployment_config({}),
        external_action_mode="disabled",
    )
    app = create_app(
        database_path=database_path,
        authenticator=authenticator,
        deployment_config=config,
    )

    with TestClient(
        app,
        headers={"Authorization": f"Bearer {api_key}"},
    ) as client:
        submitted = client.post(
            "/runs",
            json={
                "thread_id": "disabled-action-proof-thread",
                "agent_id": "travel-agent",
                "agent_version": "1.2.0",
                "input": {
                    "user_message": (
                        "I want a 5-day Tokyo trip under 9000 SGD and avoid "
                        "red-eye flights."
                    ),
                    "requested_action": "create_hold",
                },
            },
        )
        assert submitted.status_code == 202, submitted.text
        run_id = submitted.json()["run_id"]
        deadline = time.monotonic() + 8
        while True:
            result = client.get(f"/runs/{run_id}").json()
            if result["status"] in {"completed", "failed", "cancelled"}:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"run did not finish: {run_id}")
            time.sleep(0.02)
        events = client.get(f"/runs/{run_id}/events").json()

    assert result["status"] == "failed", {"result": result, "events": events}
    assert result["error_code"] == "external_action_idempotency_unsupported"
    assert SQLiteWorkflowStore(database_path).list_external_actions(run_id) == []
    event_types = {event["event_type"] for event in events}
    assert "policy.decision" in event_types
    assert not any(event_type.startswith("external_action.") for event_type in event_types)


def test_disabled_external_action_mode_rejects_http_provider_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "RUNTIME_TRAVEL_ACTION_PROVIDER_URL",
        "https://provider.example/actions",
    )
    config = replace(
        resolve_runtime_deployment_config({}),
        external_action_mode="disabled",
    )
    with pytest.raises(ValueError, match="disabled external actions"):
        create_app(
            database_path=tmp_path / "runtime.db",
            deployment_config=config,
        )


def test_disabled_external_action_mode_rejects_action_gateway_provider(
    tmp_path: Path,
) -> None:
    config = replace(
        resolve_runtime_deployment_config({}),
        external_action_mode="disabled",
    )

    with pytest.raises(ValueError, match="disabled external actions"):
        create_app(
            database_path=tmp_path / "runtime.db",
            deployment_config=config,
            action_providers={
                "configured": DisabledExternalActionProvider("configured-provider")
            },
        )


def test_readiness_reports_only_supplied_release_identity(tmp_path: Path) -> None:
    identity = RuntimeReleaseIdentity(
        source_revision=SOURCE_REVISION,
        image_digest=IMAGE_DIGEST,
    )
    config = replace(
        resolve_runtime_deployment_config({}),
        release_identity=identity,
    )
    app = create_app(
        database_path=tmp_path / "runtime.db",
        deployment_config=config,
    )

    with TestClient(app) as client:
        ready = client.get("/ready").json()

    assert ready["release"] == identity.public_dict()
    assert set(ready) == {"status", "storage", "release"}


def test_json_formatter_redacts_configured_secret_material() -> None:
    dsn_password = "db-password-canary"
    api_key = "api-key-canary"
    provider_token = "provider-token-canary"
    model_api_key = "model-api-key-canary"
    environment = {
        "OPENAI_API_KEY": model_api_key,
        "RUNTIME_POSTGRES_DSN": (
            f"postgresql://runtime:{dsn_password}@database.example/runtime"
        ),
        "RUNTIME_API_KEYS_JSON": json.dumps(
            [{"credential_id": "proof", "api_key": api_key}]
        ),
        "RUNTIME_ACTION_PROVIDERS_JSON": json.dumps(
            {"proof": {"bearer_token": provider_token}}
        ),
    }
    formatter = JsonLogFormatter(
        redacted_values=runtime_secret_redactions(environment)
    )
    record = logging.LogRecord(
        name="runtime.proof",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="dsn=%s api=%s provider=%s model=%s",
        args=(
            environment["RUNTIME_POSTGRES_DSN"],
            api_key,
            provider_token,
            model_api_key,
        ),
        exc_info=None,
    )

    encoded = formatter.format(record)
    payload = json.loads(encoded)

    assert payload["level"] == "error"
    assert payload["logger"] == "runtime.proof"
    assert payload["message"].count("[REDACTED]") == 4
    for secret in (dsn_password, api_key, provider_token, model_api_key):
        assert secret not in encoded


def test_portable_server_entrypoint_fixes_one_asgi_process_and_bounded_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(app: str, **kwargs: object) -> None:
        observed["app"] = app
        observed.update(kwargs)

    monkeypatch.setattr("runtime_service.serve.uvicorn.run", fake_run)
    result = serve(
        environment={
            "RUNTIME_HTTP_CONCURRENCY_LIMIT": "12",
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS": "4",
            "RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS": "18",
            "RUNTIME_LOG_LEVEL": "warning",
        }
    )

    assert result == 0
    assert observed["app"] == "api.main:app"
    assert observed["host"] == "0.0.0.0"
    assert observed["port"] == 8000
    assert observed["workers"] == 1
    assert observed["limit_concurrency"] == 12
    assert observed["timeout_graceful_shutdown"] == 18
    assert observed["log_level"] == "warning"
    assert observed["proxy_headers"] is False
    log_config = observed["log_config"]
    assert isinstance(log_config, dict)
    assert log_config["root"] == {"handlers": ["default"], "level": "WARNING"}
    assert set(log_config["loggers"]) == {"uvicorn", "uvicorn.error", "uvicorn.access"}


def test_server_check_is_non_mutating_json_and_credential_clean(
    capsys: pytest.CaptureFixture[str],
) -> None:
    postgres_dsn = "postgresql://runtime:check-secret@database.example/runtime"

    assert (
        serve(
            ["--check"],
            environment={
                "RUNTIME_STORE_BACKEND": "postgres",
                "RUNTIME_POSTGRES_DSN": postgres_dsn,
                "RUNTIME_EXTERNAL_ACTION_MODE": "disabled",
                "RUNTIME_SOURCE_REVISION": SOURCE_REVISION,
                "RUNTIME_IMAGE_DIGEST": IMAGE_DIGEST,
            },
        )
        == 0
    )

    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["status"] == "ok"
    assert result["deployment"]["external_action_mode"] == "disabled"
    assert result["deployment"]["release"] == {
        "source_revision": SOURCE_REVISION,
        "image_digest": IMAGE_DIGEST,
    }
    assert postgres_dsn not in output
    assert "check-secret" not in output


def test_server_check_rejects_postgres_without_dsn_without_connecting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        serve(
            ["--check"],
            environment={
                "RUNTIME_STORE_BACKEND": "postgres",
                "RUNTIME_EXTERNAL_ACTION_MODE": "disabled",
            },
        )
        == 2
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed",
        "error": "RUNTIME_POSTGRES_DSN is required when PostgreSQL storage is selected",
    }


def test_server_check_reports_value_free_json_on_invalid_configuration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    invalid_value = "do-not-render-this-value"

    assert (
        serve(
            ["--check"],
            environment={"RUNTIME_WORKER_COUNT": invalid_value},
        )
        == 2
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    result = json.loads(captured.err)
    assert result == {
        "status": "failed",
        "error": "RUNTIME_WORKER_COUNT must be an integer",
    }
    assert invalid_value not in captured.err


def test_production_image_has_postgres_and_no_image_level_sqlite_authority() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "requirements.txt requirements-postgres.txt" in dockerfile
    assert "-r requirements.txt -r requirements-postgres.txt" in dockerfile
    assert "RUNTIME_STORE_BACKEND=postgres" in dockerfile
    assert "RUNTIME_EXTERNAL_ACTION_MODE=disabled" in dockerfile
    assert "RUNTIME_DB_PATH=" not in dockerfile
    assert "STOPSIGNAL SIGTERM" in dockerfile
    assert "http://127.0.0.1:8000/health" in dockerfile
    assert 'CMD ["python", "-m", "runtime_service.serve"]' in dockerfile
    assert "RUNTIME_STORE_BACKEND: sqlite" in compose
    assert "RUNTIME_DB_PATH: /app/runtime_data/runtime.db" in compose
    assert "RUNTIME_EXTERNAL_ACTION_MODE: enabled" in compose


def test_production_image_default_environment_starts_postgres_app_without_provider(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    for name in tuple(os.environ):
        if name.startswith("RUNTIME_"):
            monkeypatch.delenv(name)
    image_environment = _dockerfile_environment_defaults()
    for name, value in image_environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv(
        "RUNTIME_POSTGRES_DSN",
        "postgresql://runtime:image-default-proof@database.example/runtime",
    )

    sqlite_bundle = build_runtime_store_bundle(
        resolve_runtime_storage_config(
            backend="sqlite",
            database_path=tmp_path / "runtime.db",
            environment={},
        )
    )
    observed_storage: list[RuntimeStorageConfig] = []

    def fake_store_bundle(config: RuntimeStorageConfig):
        observed_storage.append(config)
        return sqlite_bundle

    monkeypatch.setattr("api.main.build_runtime_store_bundle", fake_store_bundle)

    app = create_app()
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}

    assert [config.backend for config in observed_storage] == ["postgres"]
    assert isinstance(app.state.travel_action_provider, DisabledExternalActionProvider)
