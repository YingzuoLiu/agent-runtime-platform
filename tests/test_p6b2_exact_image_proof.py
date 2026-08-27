from __future__ import annotations

import json
from pathlib import Path

import pytest

from examples.p6b2_exact_image_proof import (
    API_KEY_CANARY,
    DB_PASSWORD_CANARY,
    ProofFailure,
    SHUTDOWN_MARKERS,
    _select_image,
    credential_clean_environment,
    image_manifest_digest,
    parse_json_log_lines,
    shutdown_evidence,
    validate_readiness,
)
from examples import p6b2_tls_negative_control as tls_negative_control
from examples.p6b2_tls_negative_control import classify_tls_failure


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy" / "portable" / "p6b2" / "compose.yml"
PROOF_SCRIPT = ROOT / "examples" / "p6b2_exact_image_proof.py"
SOURCE_REVISION = "a" * 40
IMAGE_DIGEST = f"sha256:{'b' * 64}"
EXPECTED_SHUTDOWN_MARKERS = (
    "Shutting down",
    "Waiting for application shutdown.",
    "Application shutdown complete.",
    "Finished server process",
)


def test_build_metadata_uses_manifest_digest_not_config_digest() -> None:
    metadata = {
        "containerimage.config.digest": f"sha256:{'c' * 64}",
        "containerimage.descriptor": {
            "digest": IMAGE_DIGEST,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
        },
    }

    assert image_manifest_digest(metadata) == IMAGE_DIGEST

    with pytest.raises(ProofFailure, match="manifest digest"):
        image_manifest_digest(
            {"containerimage.config.digest": f"sha256:{'c' * 64}"}
        )


def test_external_image_must_be_digest_pinned_before_any_pull(
    tmp_path: Path,
) -> None:
    with pytest.raises(ProofFailure, match="pinned"):
        _select_image(
            {
                "P6B2_IMAGE_REF": "registry.example/runtime:latest",
                "P6B2_IMAGE_DIGEST": IMAGE_DIGEST,
            },
            SOURCE_REVISION,
            tmp_path,
        )


def test_proof_environment_removes_ambient_aws_credentials() -> None:
    values = credential_clean_environment(
        {
            "PATH": "/proof/bin",
            "AWS_ACCESS_KEY_ID": "AKIAFAKEFAKEFAKEFAKE",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_SESSION_TOKEN": "token",
            "AWS_PROFILE": "default",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "/tmp/token",
            "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/fake",
        }
    )

    assert values == {
        "PATH": "/proof/bin",
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def test_tls_negative_control_reduces_only_hostname_mismatch_to_safe_evidence() -> None:
    message = (
        'connection failed: server certificate for "postgres-tls" '
        'does not match host name "postgres-wrong-host"'
    )

    assert classify_tls_failure(message) == "tls_hostname_rejected"
    assert (
        classify_tls_failure("connection refused before certificate negotiation")
        == "unexpected_connection_failure"
    )
    assert (
        classify_tls_failure("hostname mismatch while resolving a routing alias")
        == "unexpected_connection_failure"
    )


@pytest.mark.parametrize(
    "parameters",
    [
        {
            "host": "postgres-tls",
            "sslmode": "verify-full",
            "sslrootcert": "/run/secrets/postgres-ca.crt",
        },
        {
            "host": "postgres-wrong-host",
            "sslmode": "require",
            "sslrootcert": "/run/secrets/postgres-ca.crt",
        },
        {
            "host": "postgres-wrong-host",
            "sslmode": "verify-full",
        },
    ],
    ids=("expected-wrong-host", "verify-full", "root-certificate"),
)
def test_tls_negative_control_rejects_invalid_dsn_preconditions(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    parameters: dict[str, str],
) -> None:
    secret_dsn = "postgresql://runtime:dsn-canary@postgres-wrong-host/runtime"
    monkeypatch.setenv("RUNTIME_POSTGRES_DSN", secret_dsn)
    monkeypatch.setattr(
        tls_negative_control,
        "conninfo_to_dict",
        lambda _dsn: parameters,
    )

    def unexpected_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("invalid negative control attempted a connection")

    monkeypatch.setattr(tls_negative_control.psycopg, "connect", unexpected_connect)

    assert tls_negative_control.main() == 2
    output = capsys.readouterr().out
    assert secret_dsn not in output
    assert json.loads(output) == {
        "result": "invalid_negative_control",
        "sslmode": "verify-full",
        "status": "failed",
        "target": "postgres-wrong-host",
    }


def test_readiness_binds_postgres_schema_and_exact_release_identity() -> None:
    readiness = {
        "status": "ready",
        "storage": {
            "backend": "postgres",
            "schema": "agent_runtime",
            "schema_versions": {"execution-plane": 1, "memory": 1},
        },
        "release": {
            "source_revision": SOURCE_REVISION,
            "image_digest": IMAGE_DIGEST,
        },
    }

    validate_readiness(
        readiness,
        source_revision=SOURCE_REVISION,
        image_digest=IMAGE_DIGEST,
    )

    readiness["release"] = {
        "source_revision": SOURCE_REVISION,
        "image_digest": f"sha256:{'d' * 64}",
    }
    with pytest.raises(ProofFailure, match="release identity"):
        validate_readiness(
            readiness,
            source_revision=SOURCE_REVISION,
            image_digest=IMAGE_DIGEST,
        )


def _shutdown_log_without(missing_marker: str | None = None) -> str:
    lines = [
        {
            "timestamp": "2026-08-27T00:00:00.000Z",
            "level": "info",
            "logger": "uvicorn.error",
            "message": message,
        }
        for message in EXPECTED_SHUTDOWN_MARKERS
        if message != missing_marker
    ]
    return "\n".join(json.dumps(line) for line in lines)


def test_shutdown_evidence_requires_json_sequence_and_signal_exit() -> None:
    assert SHUTDOWN_MARKERS == EXPECTED_SHUTDOWN_MARKERS
    rendered = _shutdown_log_without()

    assert len(parse_json_log_lines(rendered)) == 4
    assert shutdown_evidence(rendered, 143)["exit_code"] == 143

    with pytest.raises(ProofFailure, match="exit code 143"):
        shutdown_evidence(rendered, 0)
    with pytest.raises(ProofFailure, match="non-JSON"):
        parse_json_log_lines(rendered + "\nplain text")


@pytest.mark.parametrize("missing_marker", EXPECTED_SHUTDOWN_MARKERS)
def test_shutdown_evidence_requires_every_marker(missing_marker: str) -> None:
    with pytest.raises(ProofFailure, match="shutdown log sequence omitted"):
        shutdown_evidence(_shutdown_log_without(missing_marker), 143)


def test_compose_proves_verify_full_bootstrap_and_two_independent_runtimes() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")

    assert compose.count("sslmode=verify-full") == 4
    assert "postgres-wrong-host" in compose
    assert compose.count("\n          - postgres-wrong-host\n") == 1
    assert "examples.p6b2_tls_negative_control" in compose
    assert "condition: service_healthy" in compose
    assert "condition: service_completed_successfully" in compose
    assert "runtime-a:" in compose
    assert "runtime-b:" in compose
    assert compose.count("pull_policy: never") == 4
    assert compose.count("RUNTIME_EXTERNAL_ACTION_MODE: disabled") == 4
    assert compose.count("RUNTIME_RELEASE_IDENTITY_REQUIRED: \"true\"") == 2
    assert "RUNTIME_DB_PATH" not in compose
    assert "aws" not in compose.lower()


def test_proof_harness_keeps_canaries_out_of_commands_and_cloud_boundaries() -> None:
    source = PROOF_SCRIPT.read_text(encoding="utf-8")

    assert DB_PASSWORD_CANARY not in COMPOSE.read_text(encoding="utf-8")
    assert API_KEY_CANARY not in COMPOSE.read_text(encoding="utf-8")
    assert "shell=True" not in source
    assert "boto3" not in source
    assert "aws " not in source.lower()
    assert '"secret_canary_occurrences": 0' in source
