from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast


RuntimeLogLevel = Literal["debug", "info", "warning", "error", "critical"]
ExternalActionMode = Literal["enabled", "disabled"]

MAX_RUNTIME_WORKERS = 16
MAX_HTTP_CONCURRENCY = 256
MAX_SHUTDOWN_SECONDS = 120

_SOURCE_REVISION = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_EXTERNAL_ACTION_PROVIDER_VARIABLES = (
    "RUNTIME_ACTION_PROVIDERS_JSON",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_URL",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_ALLOW_INSECURE_LOCALHOST",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY",
)


class RuntimeDeploymentConfigurationError(ValueError):
    """Raised before process startup when deployment inputs are incoherent."""


@dataclass(frozen=True, slots=True)
class RuntimeReleaseIdentity:
    """Deployment-supplied identity that is later cross-checked by the substrate."""

    source_revision: str
    image_digest: str

    def public_dict(self) -> dict[str, str]:
        return {
            "source_revision": self.source_revision,
            "image_digest": self.image_digest,
        }


@dataclass(frozen=True, slots=True)
class RuntimeDeploymentConfig:
    """Portable process limits and release metadata for one Runtime container."""

    worker_count: int
    manager_shutdown_grace_seconds: float
    http_concurrency_limit: int
    server_graceful_shutdown_seconds: int
    log_level: RuntimeLogLevel
    external_action_mode: ExternalActionMode
    release_identity: RuntimeReleaseIdentity | None

    def public_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "worker_count": self.worker_count,
            "manager_shutdown_grace_seconds": self.manager_shutdown_grace_seconds,
            "http_concurrency_limit": self.http_concurrency_limit,
            "server_graceful_shutdown_seconds": self.server_graceful_shutdown_seconds,
            "log_level": self.log_level,
            "external_action_mode": self.external_action_mode,
        }
        if self.release_identity is not None:
            result["release"] = self.release_identity.public_dict()
        return result


def _environment_value(environment: Mapping[str, str], name: str) -> str | None:
    value = environment.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _integer(
    environment: Mapping[str, str],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = _environment_value(environment, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeDeploymentConfigurationError(
            f"{name} must be an integer"
        ) from exc
    if not minimum <= value <= maximum:
        raise RuntimeDeploymentConfigurationError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _seconds(
    environment: Mapping[str, str],
    name: str,
    *,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = _environment_value(environment, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeDeploymentConfigurationError(
            f"{name} must be a number of seconds"
        ) from exc
    if not minimum <= value <= maximum:
        raise RuntimeDeploymentConfigurationError(
            f"{name} must be between {minimum:g} and {maximum:g} seconds"
        )
    return value


def _boolean(
    environment: Mapping[str, str],
    name: str,
    *,
    default: bool,
) -> bool:
    raw = _environment_value(environment, name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeDeploymentConfigurationError(
        f"{name} must be one of true, false, 1, 0, yes, no, on, or off"
    )


def validate_runtime_worker_count(worker_count: int) -> int:
    if isinstance(worker_count, bool) or not isinstance(worker_count, int):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_WORKER_COUNT must be an integer"
        )
    if not 1 <= worker_count <= MAX_RUNTIME_WORKERS:
        raise RuntimeDeploymentConfigurationError(
            f"RUNTIME_WORKER_COUNT must be between 1 and {MAX_RUNTIME_WORKERS}"
        )
    return worker_count


def validate_manager_shutdown_grace_seconds(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS must be a number of seconds"
        )
    resolved = float(value)
    if not 0 <= resolved <= MAX_SHUTDOWN_SECONDS:
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS must be between 0 and 120 seconds"
        )
    return resolved


def _release_identity(
    environment: Mapping[str, str],
) -> RuntimeReleaseIdentity | None:
    required = _boolean(
        environment,
        "RUNTIME_RELEASE_IDENTITY_REQUIRED",
        default=False,
    )
    source_revision = _environment_value(environment, "RUNTIME_SOURCE_REVISION")
    image_digest = _environment_value(environment, "RUNTIME_IMAGE_DIGEST")
    if (source_revision is None) != (image_digest is None):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_SOURCE_REVISION and RUNTIME_IMAGE_DIGEST must be configured together"
        )
    if source_revision is None or image_digest is None:
        if required:
            raise RuntimeDeploymentConfigurationError(
                "release identity is required but RUNTIME_SOURCE_REVISION and "
                "RUNTIME_IMAGE_DIGEST are missing"
            )
        return None
    if not _SOURCE_REVISION.fullmatch(source_revision):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_SOURCE_REVISION must be a lowercase 40- or 64-character hex commit"
        )
    if not _IMAGE_DIGEST.fullmatch(image_digest):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_IMAGE_DIGEST must be a lowercase sha256 OCI digest"
        )
    return RuntimeReleaseIdentity(
        source_revision=source_revision,
        image_digest=image_digest,
    )


def resolve_runtime_deployment_config(
    environment: Mapping[str, str] | None = None,
) -> RuntimeDeploymentConfig:
    """Resolve bounded process configuration without consulting cloud metadata."""

    values = os.environ if environment is None else environment
    worker_count = _integer(
        values,
        "RUNTIME_WORKER_COUNT",
        default=1,
        minimum=1,
        maximum=MAX_RUNTIME_WORKERS,
    )
    manager_shutdown_grace_seconds = _seconds(
        values,
        "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS",
        default=5.0,
        minimum=0,
        maximum=MAX_SHUTDOWN_SECONDS,
    )
    http_concurrency_limit = _integer(
        values,
        "RUNTIME_HTTP_CONCURRENCY_LIMIT",
        default=32,
        minimum=1,
        maximum=MAX_HTTP_CONCURRENCY,
    )
    server_graceful_shutdown_seconds = _integer(
        values,
        "RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS",
        default=15,
        minimum=1,
        maximum=MAX_SHUTDOWN_SECONDS,
    )
    if server_graceful_shutdown_seconds <= manager_shutdown_grace_seconds:
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS must be greater than "
            "RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS"
        )

    raw_log_level = _environment_value(values, "RUNTIME_LOG_LEVEL") or "info"
    log_level = raw_log_level.lower()
    if log_level not in {"debug", "info", "warning", "error", "critical"}:
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_LOG_LEVEL must be debug, info, warning, error, or critical"
        )

    raw_action_mode = _environment_value(values, "RUNTIME_EXTERNAL_ACTION_MODE") or "enabled"
    action_mode = raw_action_mode.lower()
    if action_mode not in {"enabled", "disabled"}:
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_EXTERNAL_ACTION_MODE must be 'enabled' or 'disabled'"
        )
    if action_mode == "disabled" and any(
        _environment_value(values, name) is not None
        for name in _EXTERNAL_ACTION_PROVIDER_VARIABLES
    ):
        raise RuntimeDeploymentConfigurationError(
            "RUNTIME_EXTERNAL_ACTION_MODE=disabled cannot be combined with "
            "external Action provider configuration"
        )

    return RuntimeDeploymentConfig(
        worker_count=worker_count,
        manager_shutdown_grace_seconds=manager_shutdown_grace_seconds,
        http_concurrency_limit=http_concurrency_limit,
        server_graceful_shutdown_seconds=server_graceful_shutdown_seconds,
        log_level=cast(RuntimeLogLevel, log_level),
        external_action_mode=cast(ExternalActionMode, action_mode),
        release_identity=_release_identity(values),
    )
