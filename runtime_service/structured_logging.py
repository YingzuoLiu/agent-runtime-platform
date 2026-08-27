from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlsplit


_DIRECT_SECRET_VARIABLES = (
    "OPENAI_API_KEY",
    "RUNTIME_POSTGRES_DSN",
    "RUNTIME_API_KEYS_JSON",
    "RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN",
    "RUNTIME_ACTION_PROVIDERS_JSON",
)
_SECRET_KEY_PARTS = ("api_key", "bearer_token", "password", "secret", "token")
_KEYWORD_DSN_PASSWORD = re.compile(r"(?:^|\s)password=(?:'([^']*)'|\"([^\"]*)\"|([^\s]+))")


def _json_secret_values(value: Any, *, key: str | None = None) -> set[str]:
    secrets: set[str] = set()
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            secrets.update(_json_secret_values(child_value, key=str(child_key).lower()))
    elif isinstance(value, list):
        for child in value:
            secrets.update(_json_secret_values(child, key=key))
    elif isinstance(value, str) and key is not None:
        if any(part in key for part in _SECRET_KEY_PARTS) and value:
            secrets.add(value)
    return secrets


def runtime_secret_redactions(environment: Mapping[str, str]) -> tuple[str, ...]:
    """Collect configured secret material without returning variable names or values in logs."""

    secrets: set[str] = set()
    for name in _DIRECT_SECRET_VARIABLES:
        raw = environment.get(name)
        if raw:
            secrets.add(raw)
        if not raw or name not in {"RUNTIME_API_KEYS_JSON", "RUNTIME_ACTION_PROVIDERS_JSON"}:
            continue
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            continue
        secrets.update(_json_secret_values(decoded))

    dsn = environment.get("RUNTIME_POSTGRES_DSN")
    if dsn:
        try:
            parsed = urlsplit(dsn)
        except ValueError:
            parsed = None
        if parsed is not None and parsed.password:
            secrets.add(parsed.password)
            secrets.add(unquote(parsed.password))
        keyword_password = _KEYWORD_DSN_PASSWORD.search(dsn)
        if keyword_password is not None:
            password = next(
                (item for item in keyword_password.groups() if item is not None),
                None,
            )
            if password:
                secrets.add(password)

    return tuple(sorted((value for value in secrets if value), key=len, reverse=True))


class JsonLogFormatter(logging.Formatter):
    """One-line JSON formatter with bounded fields and configured-secret redaction."""

    def __init__(self, *, redacted_values: Sequence[str] = ()) -> None:
        super().__init__()
        self._redacted_values = tuple(
            sorted({value for value in redacted_values if value}, key=len, reverse=True)
        )

    def _redact(self, value: str) -> str:
        redacted = value
        for secret in self._redacted_values:
            redacted = redacted.replace(secret, "[REDACTED]")
        return redacted

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc)
        payload: dict[str, object] = {
            "timestamp": timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": self._redact(record.getMessage()),
        }
        for name in ("event", "trace_id", "request_id", "run_id"):
            value = getattr(record, name, None)
            if isinstance(value, str) and value:
                payload[name] = self._redact(value)
        if record.exc_info:
            payload["exception"] = self._redact(self.formatException(record.exc_info))
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def uvicorn_json_log_config(
    environment: Mapping[str, str],
    *,
    log_level: str = "info",
) -> dict[str, object]:
    configured_level = log_level.upper()
    formatter = {
        "()": "runtime_service.structured_logging.JsonLogFormatter",
        "redacted_values": runtime_secret_redactions(environment),
    }
    handler = {
        "class": "logging.StreamHandler",
        "formatter": "json",
        "stream": "ext://sys.stdout",
    }
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"json": formatter},
        "handlers": {"default": handler},
        "root": {"handlers": ["default"], "level": configured_level},
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": configured_level,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": configured_level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": configured_level,
                "propagate": False,
            },
        },
    }
