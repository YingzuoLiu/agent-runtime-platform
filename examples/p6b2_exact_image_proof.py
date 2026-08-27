"""Credential-clean exact-image PostgreSQL TLS and lifecycle proof.

The proof is intentionally deterministic.  It builds one production image, runs two
independent Runtime containers from that image, and records only bounded evidence.
It never uses cloud credentials or a cloud API.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, cast


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "portable" / "p6b2" / "compose.yml"
ARTIFACT_PATH = ROOT / "artifacts" / "p6b2-exact-image-proof.json"
PROOF_ID = "p6b2-exact-image-lifecycle:1"
DB_PASSWORD_CANARY = "p6b2-db-password-canary"
API_KEY_CANARY = "p6b2-api-key-canary"
SOURCE_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
SHUTDOWN_MARKERS = (
    "Shutting down",
    "Waiting for application shutdown.",
    "Application shutdown complete.",
    "Finished server process",
)
REMOVED_CLOUD_CREDENTIALS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
)


class ProofFailure(RuntimeError):
    """Raised when an observable P6B.2 invariant is not satisfied."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def _run(
    arguments: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    check: bool = True,
    cwd: Path = ROOT,
) -> CommandResult:
    completed = subprocess.run(
        list(arguments),
        cwd=cwd,
        env=None if environment is None else dict(environment),
        text=True,
        capture_output=True,
        check=False,
    )
    result = CommandResult(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    if check and result.returncode != 0:
        command_name = " ".join(arguments[:3])
        raise ProofFailure(
            f"required command failed with exit {result.returncode}: {command_name}"
        )
    return result


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProofFailure(message)


def _git_source_revision(environment: Mapping[str, str]) -> str:
    configured = environment.get("P6B2_SOURCE_REVISION", "").strip()
    if configured:
        revision = configured
    else:
        revision = _run(["git", "rev-parse", "HEAD"]).stdout.strip()
    _require(
        SOURCE_REVISION_PATTERN.fullmatch(revision) is not None,
        "source revision must be one exact lowercase Git commit",
    )
    return revision


def credential_clean_environment(environment: Mapping[str, str]) -> dict[str, str]:
    values = dict(environment)
    for name in REMOVED_CLOUD_CREDENTIALS:
        values.pop(name, None)
    values["AWS_EC2_METADATA_DISABLED"] = "true"
    return values


def image_manifest_digest(metadata: Mapping[str, Any]) -> str:
    """Return Buildx's manifest digest, never its different config digest."""

    value = metadata.get("containerimage.digest")
    if not isinstance(value, str):
        descriptor = metadata.get("containerimage.descriptor")
        value = descriptor.get("digest") if isinstance(descriptor, dict) else None
    _require(
        isinstance(value, str) and DIGEST_PATTERN.fullmatch(value) is not None,
        "Buildx metadata did not contain a valid image manifest digest",
    )
    return cast(str, value)


def _generate_tls_material(directory: Path) -> None:
    extension_file = directory / "server.ext"
    extension_file.write_text(
        "\n".join(
            (
                "basicConstraints=CA:FALSE",
                "keyUsage=digitalSignature,keyEncipherment",
                "extendedKeyUsage=serverAuth",
                "subjectAltName=DNS:postgres-tls",
                "",
            )
        ),
        encoding="utf-8",
    )
    _run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-days",
            "2",
            "-subj",
            "/CN=Agent Runtime P6B2 proof CA",
            "-keyout",
            str(directory / "ca.key"),
            "-out",
            str(directory / "ca.crt"),
        ]
    )
    _run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-subj",
            "/CN=postgres-tls",
            "-keyout",
            str(directory / "server.key"),
            "-out",
            str(directory / "server.csr"),
        ]
    )
    _run(
        [
            "openssl",
            "x509",
            "-req",
            "-sha256",
            "-days",
            "2",
            "-set_serial",
            "1",
            "-in",
            str(directory / "server.csr"),
            "-CA",
            str(directory / "ca.crt"),
            "-CAkey",
            str(directory / "ca.key"),
            "-extfile",
            str(extension_file),
            "-out",
            str(directory / "server.crt"),
        ]
    )


def _build_image(source_revision: str, directory: Path) -> tuple[str, str, str]:
    image_ref = f"agent-runtime-platform:p6b2-{source_revision[:12]}"
    metadata_path = directory / "build-metadata.json"
    _run(
        [
            "docker",
            "buildx",
            "build",
            "--load",
            "--platform",
            "linux/amd64",
            "--provenance=false",
            "--metadata-file",
            str(metadata_path),
            "--tag",
            image_ref,
            ".",
        ]
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    manifest_digest = image_manifest_digest(metadata)
    config_digest = _run(
        ["docker", "image", "inspect", "--format={{.Id}}", image_ref]
    ).stdout.strip()
    _require(
        DIGEST_PATTERN.fullmatch(config_digest) is not None,
        "Docker did not return a valid loaded-image config digest",
    )
    return image_ref, manifest_digest, config_digest


def _select_image(
    environment: Mapping[str, str],
    source_revision: str,
    directory: Path,
) -> tuple[str, str, str]:
    """Build locally, or consume one already-pinned registry artifact for P6C."""

    external_ref = environment.get("P6B2_IMAGE_REF", "").strip()
    external_digest = environment.get("P6B2_IMAGE_DIGEST", "").strip()
    if not external_ref and not external_digest:
        return _build_image(source_revision, directory)
    _require(
        bool(external_ref) and bool(external_digest),
        "P6B2_IMAGE_REF and P6B2_IMAGE_DIGEST must be supplied together",
    )
    _require(
        DIGEST_PATTERN.fullmatch(external_digest) is not None,
        "external image digest must be one lowercase sha256 digest",
    )
    _require(
        external_ref.endswith(f"@{external_digest}"),
        "external image reference must be pinned to P6B2_IMAGE_DIGEST",
    )
    _run(["docker", "pull", external_ref])
    config_digest = _run(
        ["docker", "image", "inspect", "--format={{.Id}}", external_ref]
    ).stdout.strip()
    _require(
        DIGEST_PATTERN.fullmatch(config_digest) is not None,
        "Docker did not return a valid pulled-image config digest",
    )
    return external_ref, external_digest, config_digest


def _compose_command(project_name: str, *arguments: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-name",
        project_name,
        "--file",
        str(COMPOSE_FILE),
        *arguments,
    ]


def _image_platform(image_ref: str) -> str:
    value = _run(
        [
            "docker",
            "image",
            "inspect",
            "--format={{.Os}}/{{.Architecture}}",
            image_ref,
        ]
    ).stdout.strip()
    _require(value == "linux/amd64", "proof image platform is not linux/amd64")
    return value


def _json_object(text: str) -> dict[str, Any]:
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ProofFailure("expected command output to contain one JSON object")


def _http_json(
    method: str,
    url: str,
    *,
    api_key: str | None = None,
    payload: Mapping[str, Any] | None = None,
    timeout: float = 5.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if api_key is not None:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            decoded = json.loads(response.read().decode("utf-8"))
            _require(isinstance(decoded, dict), "HTTP response must be a JSON object")
            return response.status, decoded
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = {"error": "non-json-response"}
        return exc.code, decoded if isinstance(decoded, dict) else {"error": decoded}


def _wait_for_json(
    url: str,
    *,
    api_key: str | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            status, body = _http_json("GET", url, api_key=api_key)
            if status == 200:
                return body
            last_error = f"HTTP {status}"
        except (OSError, ProofFailure, json.JSONDecodeError) as exc:
            last_error = type(exc).__name__
        time.sleep(0.5)
    raise ProofFailure(f"HTTP proof endpoint did not become ready: {last_error}")


def _host_base_url(
    project_name: str,
    service: str,
    environment: Mapping[str, str],
) -> str:
    value = _run(
        _compose_command(project_name, "port", service, "8000"),
        environment=environment,
    ).stdout.strip()
    _require(bool(value), f"Compose did not publish a port for {service}")
    host, separator, port = value.rpartition(":")
    _require(bool(separator) and port.isdigit(), f"invalid Compose port output for {service}")
    host = host.strip("[]")
    return f"http://{host}:{port}"


def validate_readiness(
    body: Mapping[str, Any],
    *,
    source_revision: str,
    image_digest: str,
) -> None:
    _require(body.get("status") == "ready", "Runtime did not report ready")
    storage = body.get("storage")
    _require(isinstance(storage, dict), "readiness storage metadata is missing")
    storage = cast(dict[str, Any], storage)
    _require(storage.get("backend") == "postgres", "Runtime authority is not PostgreSQL")
    _require(storage.get("schema") == "agent_runtime", "unexpected Runtime schema")
    _require(
        storage.get("schema_versions") == {"execution-plane": 1, "memory": 1},
        "readiness did not report both accepted schema versions",
    )
    _require(
        body.get("release")
        == {"source_revision": source_revision, "image_digest": image_digest},
        "readiness release identity does not match the exact image evidence",
    )


def _release_manifest(release_id: str) -> dict[str, Any]:
    return {
        "release_id": release_id,
        "application_name": "p6b2-proof-app",
        "release_version": "1.0.0",
        "required_artifacts": ["runtime-image"],
        "available_artifacts": [
            {"name": "runtime-image", "checksum": "a" * 64}
        ],
        "required_test_suite": "p6b2-proof-suite",
        "executed_test_suite": "p6b2-proof-suite",
        "tests_passed": True,
        "required_python_versions": ["3.12"],
        "tested_python_versions": ["3.12"],
        "deployment_environment": "portable-proof",
        "configuration_requirements": ["POSTGRES_DSN"],
        "actual_configuration_keys": ["POSTGRES_DSN"],
    }


def _semantic_transition(
    submit_base_url: str,
    observe_base_url: str,
    *,
    sequence: int,
) -> dict[str, Any]:
    status, submitted = _http_json(
        "POST",
        f"{submit_base_url}/runs",
        api_key=API_KEY_CANARY,
        payload={
            "thread_id": f"p6b2-proof-thread-{sequence}",
            "agent_id": "release-validation",
            "agent_version": "1.1.0",
            "client_request_id": f"p6b2-proof-request-{sequence}",
            "input": {"manifest": _release_manifest(f"p6b2-release-{sequence}")},
        },
    )
    _require(status == 202, "semantic smoke submission was not accepted")
    run_id = submitted.get("run_id")
    _require(isinstance(run_id, str) and bool(run_id), "submission omitted run_id")
    run_id = cast(str, run_id)

    deadline = time.monotonic() + 60.0
    terminal: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        observed_status, observed = _http_json(
            "GET",
            f"{observe_base_url}/runs/{run_id}",
            api_key=API_KEY_CANARY,
        )
        if observed_status == 200 and observed.get("status") in {
            "completed",
            "failed",
            "cancelled",
        }:
            terminal = observed
            break
        time.sleep(0.25)
    _require(terminal is not None, "semantic smoke did not become terminal")
    terminal = cast(dict[str, Any], terminal)
    _require(terminal.get("status") == "completed", "semantic smoke did not complete")
    state = terminal.get("state")
    _require(isinstance(state, dict), "semantic smoke omitted durable state")
    state = cast(dict[str, Any], state)
    result = state.get("result")
    _require(
        state.get("current_stage") == "ready"
        and isinstance(result, dict)
        and result.get("status") == "ready",
        "release-validation durable transition did not reach ready",
    )
    # This endpoint returns a JSON list rather than the object shape used by the
    # other proof endpoints, so read and validate it explicitly.
    request = urllib.request.Request(
        f"{observe_base_url}/runs/{run_id}/events",
        headers={"Authorization": f"Bearer {API_KEY_CANARY}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        events = json.loads(response.read().decode("utf-8"))
    _require(isinstance(events, list) and bool(events), "semantic smoke events are empty")
    events = cast(list[Any], events)
    final_event = events[-1]
    _require(isinstance(final_event, dict), "final semantic smoke event is not an object")
    final_event = cast(dict[str, Any], final_event)
    _require(
        final_event.get("event_type") == "run.completed",
        "final event is not run.completed",
    )
    return {
        "run_id": run_id,
        "status": terminal["status"],
        "final_stage": state["current_stage"],
        "final_event": final_event["event_type"],
    }


def parse_json_log_lines(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ProofFailure("Runtime emitted a non-JSON log line") from exc
        _require(isinstance(value, dict), "Runtime JSON log line is not an object")
        _require(
            all(name in value for name in ("timestamp", "level", "logger", "message")),
            "Runtime JSON log line omitted a bounded field",
        )
        records.append(value)
    _require(bool(records), "Runtime emitted no JSON logs")
    return records


def shutdown_evidence(text: str, exit_code: int) -> dict[str, Any]:
    records = parse_json_log_lines(text)
    messages = [str(record["message"]) for record in records]
    for marker in SHUTDOWN_MARKERS:
        _require(
            any(marker in message for message in messages),
            f"Runtime shutdown log sequence omitted: {marker}",
        )
    _require(exit_code == 143, "Docker SIGTERM stop did not produce expected exit code 143")
    return {
        "exit_code": exit_code,
        "log_records": len(records),
        "markers": list(SHUTDOWN_MARKERS),
    }


def _container_id(
    project_name: str,
    service: str,
    environment: Mapping[str, str],
) -> str:
    value = _run(
        _compose_command(project_name, "ps", "-q", service),
        environment=environment,
    ).stdout.strip()
    _require(bool(value), f"Compose did not return a container for {service}")
    return value


def _container_config_digest(container_id: str) -> str:
    value = _run(
        ["docker", "inspect", "--format={{.Image}}", container_id]
    ).stdout.strip()
    _require(DIGEST_PATTERN.fullmatch(value) is not None, "invalid container image digest")
    return value


def _container_exit_code(container_id: str) -> int:
    value = _run(
        ["docker", "inspect", "--format={{.State.ExitCode}}", container_id]
    ).stdout.strip()
    _require(value.isdigit(), "container exit code was not numeric")
    return int(value)


def _container_logs(container_id: str) -> str:
    result = _run(["docker", "logs", container_id])
    return result.stdout + result.stderr


def _assert_no_canaries(texts: Iterable[str]) -> None:
    combined = "\n".join(texts)
    for canary in (DB_PASSWORD_CANARY, API_KEY_CANARY):
        _require(canary not in combined, "secret canary escaped into proof output")


def _tls_log_evidence(postgres_logs: str) -> list[str]:
    applications = ("p6b2-bootstrap", "p6b2-runtime-a", "p6b2-runtime-b")
    for application in applications:
        _require(
            any(
                application in line and "SSL enabled" in line
                for line in postgres_logs.splitlines()
            ),
            f"PostgreSQL did not record a verified TLS session for {application}",
        )
    return list(applications)


def run_proof(environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    values = credential_clean_environment(
        os.environ if environment is None else environment
    )
    source_revision = _git_source_revision(values)
    project_name = f"arp-p6b2-{os.getpid()}"
    image_ref = ""
    compose_environment: dict[str, str] | None = None

    with tempfile.TemporaryDirectory(prefix="arp-p6b2-") as temporary:
        proof_directory = Path(temporary)
        _generate_tls_material(proof_directory)
        image_ref, manifest_digest, config_digest = _select_image(
            values,
            source_revision,
            proof_directory,
        )
        image_platform = _image_platform(image_ref)
        compose_environment = {
            **values,
            "P6B2_API_KEY": API_KEY_CANARY,
            "P6B2_DB_PASSWORD": DB_PASSWORD_CANARY,
            "P6B2_IMAGE_DIGEST": manifest_digest,
            "P6B2_IMAGE_REF": image_ref,
            "P6B2_SOURCE_REVISION": source_revision,
            "P6B2_TLS_DIR": str(proof_directory),
        }
        try:
            _run(
                _compose_command(project_name, "config", "--quiet"),
                environment=compose_environment,
            )
            _run(
                _compose_command(project_name, "up", "--detach", "--wait", "postgres-tls"),
                environment=compose_environment,
            )
            negative = _run(
                _compose_command(
                    project_name,
                    "--profile",
                    "negative-control",
                    "run",
                    "--rm",
                    "tls-negative-control",
                ),
                environment=compose_environment,
                check=False,
            )
            negative_text = negative.stdout + negative.stderr
            negative_result = _json_object(negative.stdout)
            _require(
                negative.returncode == 0
                and negative_result.get("status") == "ok"
                and negative_result.get("result") == "tls_hostname_rejected"
                and negative_result.get("sslmode") == "verify-full"
                and negative_result.get("target") == "postgres-wrong-host",
                "wrong-host control did not prove a TLS hostname rejection",
            )

            _run(
                _compose_command(
                    project_name,
                    "up",
                    "--detach",
                    "--wait",
                    "runtime-a",
                    "runtime-b",
                ),
                environment=compose_environment,
            )
            bootstrap_logs = _run(
                _compose_command(
                    project_name,
                    "logs",
                    "--no-color",
                    "--no-log-prefix",
                    "bootstrap",
                ),
                environment=compose_environment,
            ).stdout
            bootstrap = _json_object(bootstrap_logs)
            _require(bootstrap.get("status") == "ok", "schema bootstrap did not succeed")
            _require(
                bootstrap.get("components") == {"execution-plane": 1, "memory": 1},
                "schema bootstrap did not validate both components",
            )

            container_a = _container_id(
                project_name,
                "runtime-a",
                compose_environment,
            )
            container_b = _container_id(
                project_name,
                "runtime-b",
                compose_environment,
            )
            _require(container_a != container_b, "Runtime containers are not independent")
            image_ids = {
                "runtime-a": _container_config_digest(container_a),
                "runtime-b": _container_config_digest(container_b),
            }
            _require(
                set(image_ids.values()) == {config_digest},
                "Runtime containers did not use the one loaded image",
            )

            base_a = _host_base_url(project_name, "runtime-a", compose_environment)
            base_b = _host_base_url(project_name, "runtime-b", compose_environment)
            ready_a = _wait_for_json(f"{base_a}/ready")
            ready_b = _wait_for_json(f"{base_b}/ready")
            validate_readiness(
                ready_a,
                source_revision=source_revision,
                image_digest=manifest_digest,
            )
            validate_readiness(
                ready_b,
                source_revision=source_revision,
                image_digest=manifest_digest,
            )

            cross_container_transition = _semantic_transition(
                base_a,
                base_b,
                sequence=1,
            )

            stop_started = time.monotonic()
            _run(
                _compose_command(project_name, "stop", "--timeout", "30", "runtime-a"),
                environment=compose_environment,
            )
            stop_elapsed = time.monotonic() - stop_started
            _require(stop_elapsed < 30, "Runtime A exceeded the Docker SIGTERM budget")
            shutdown_a_logs = _container_logs(container_a)
            shutdown_a = shutdown_evidence(
                shutdown_a_logs,
                _container_exit_code(container_a),
            )
            shutdown_a["elapsed_seconds"] = round(stop_elapsed, 3)
            validate_readiness(
                _wait_for_json(f"{base_b}/ready"),
                source_revision=source_revision,
                image_digest=manifest_digest,
            )
            surviving_transition = _semantic_transition(
                base_b,
                base_b,
                sequence=2,
            )

            stop_started = time.monotonic()
            _run(
                _compose_command(project_name, "stop", "--timeout", "30", "runtime-b"),
                environment=compose_environment,
            )
            stop_elapsed = time.monotonic() - stop_started
            _require(stop_elapsed < 30, "Runtime B exceeded the Docker SIGTERM budget")
            shutdown_b_logs = _container_logs(container_b)
            shutdown_b = shutdown_evidence(
                shutdown_b_logs,
                _container_exit_code(container_b),
            )
            shutdown_b["elapsed_seconds"] = round(stop_elapsed, 3)
            postgres_logs = _run(
                _compose_command(project_name, "logs", "--no-color", "postgres-tls"),
                environment=compose_environment,
            ).stdout
            tls_applications = _tls_log_evidence(postgres_logs)

            _assert_no_canaries(
                (
                    negative_text,
                    bootstrap_logs,
                    shutdown_a_logs,
                    shutdown_b_logs,
                    postgres_logs,
                )
            )
            result: dict[str, Any] = {
                "proof": PROOF_ID,
                "source_revision": source_revision,
                "image": {
                    "manifest_digest": manifest_digest,
                    "config_digest": config_digest,
                    "platform": image_platform,
                    "container_config_digests": image_ids,
                },
                "tls": {
                    "mode": "verify-full",
                    "wrong_hostname_rejected": True,
                    "verified_applications": tls_applications,
                },
                "bootstrap": {
                    "action": bootstrap.get("action"),
                    "schema": bootstrap.get("schema"),
                    "components": bootstrap.get("components"),
                },
                "runtime": {
                    "containers": 2,
                    "cross_container_transition": cross_container_transition,
                    "surviving_container_transition": surviving_transition,
                },
                "shutdown": {
                    "runtime-a": shutdown_a,
                    "runtime-b": shutdown_b,
                },
                "logs": {
                    "format": "json-lines",
                    "secret_canary_occurrences": 0,
                },
                "summary": {"failed": 0, "passed": 8},
            }
            rendered = json.dumps(result, sort_keys=True, separators=(",", ":"))
            _assert_no_canaries((rendered,))
            ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
            ARTIFACT_PATH.write_text(rendered + "\n", encoding="utf-8")
            return result
        finally:
            if compose_environment is not None:
                _run(
                    _compose_command(
                        project_name,
                        "down",
                        "--volumes",
                        "--remove-orphans",
                        "--timeout",
                        "10",
                    ),
                    environment=compose_environment,
                    check=False,
                )
            if image_ref:
                _run(["docker", "image", "rm", image_ref], check=False)


def main() -> int:
    missing = [name for name in ("docker", "openssl") if shutil.which(name) is None]
    if missing:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": f"required executable is unavailable: {', '.join(missing)}",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    try:
        result = run_proof()
    except (OSError, ProofFailure, ValueError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {"status": "failed", "error": str(exc)},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
