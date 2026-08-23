from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Mapping
from enum import Enum
from hashlib import sha256
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from .external_actions import ExternalActionProvider
from .http_external_action import HttpExternalActionProvider
from .models import RunRecord


ACTION_AGENT_ID = "durable-action-gateway"
ACTION_AGENT_VERSION = "1.0.0"
ACTION_CONTRACT = "durable-action:1"
ACTION_DOMAIN_ID = "durable-action"
ACTION_SCHEMA_VERSION = "1"
ACTION_WORKFLOW_TYPE = "durable-action:webhook.send:1"
ACTION_STEP_ID = "dispatch"
ACTION_REQUEST_NAMESPACE_PREFIX = "action-request:"
ACTION_DESTINATION_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$"

MAX_ACTION_PAYLOAD_BYTES = 32_768
MAX_ACTION_PAYLOAD_DEPTH = 16


class ActionStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    OUTCOME_UNKNOWN = "outcome_unknown"

    @property
    def is_terminal(self) -> bool:
        return self in {
            self.SUCCEEDED,
            self.FAILED,
            self.CANCELLED,
            self.OUTCOME_UNKNOWN,
        }


class ActionEvidenceStatus(str, Enum):
    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    OUTCOME_UNKNOWN = "outcome_unknown"


def _validate_json_value(value: Any, *, depth: int) -> None:
    if depth > MAX_ACTION_PAYLOAD_DEPTH:
        raise ValueError(
            f"payload must not exceed {MAX_ACTION_PAYLOAD_DEPTH} levels of nesting"
        )
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("payload must not contain NaN or Infinity")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("payload object keys must be strings")
            _validate_json_value(item, depth=depth + 1)
        return
    raise ValueError("payload must contain only JSON values")


def canonical_action_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


class WebhookSendInput(BaseModel):
    """The only caller-controlled arguments exposed by ``webhook.send``."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            raise ValueError("payload must be a JSON object")
        _validate_json_value(value, depth=1)
        try:
            encoded = canonical_action_json(value).encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("payload must contain valid Unicode") from None
        if len(encoded) > MAX_ACTION_PAYLOAD_BYTES:
            raise ValueError(
                f"payload must contain at most {MAX_ACTION_PAYLOAD_BYTES} UTF-8 bytes"
            )
        return value


class WebhookSendOutput(BaseModel):
    """The complete allowlist for provider data exposed by the Action API."""

    model_config = ConfigDict(extra="forbid", strict=True)

    provider_reference: str = Field(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$",
    )


class ActionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    action_type: str = Field(min_length=1, max_length=100)
    destination: str = Field(
        min_length=1,
        max_length=200,
        pattern=ACTION_DESTINATION_PATTERN,
    )
    idempotency_key: str = Field(min_length=1, max_length=200)
    input: WebhookSendInput


class DurableActionInput(BaseModel):
    """Private, persisted input for the built-in single-step Action domain."""

    model_config = ConfigDict(extra="forbid", strict=True)

    contract: Literal["durable-action:1"] = "durable-action:1"
    action_type: Literal["webhook.send"] = "webhook.send"
    destination: str = Field(
        min_length=1,
        max_length=200,
        pattern=ACTION_DESTINATION_PATTERN,
    )
    idempotency_key: str = Field(min_length=1, max_length=200)
    input: WebhookSendInput


class ActionResource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    action_type: Literal["webhook.send"]
    destination: str
    idempotency_key: str
    status: ActionStatus
    result: WebhookSendOutput | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: str
    updated_at: str
    completed_at: str | None = None


class ActionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1)
    event_type: str
    status: ActionEvidenceStatus
    destination: str
    dispatch_count: int = Field(ge=0)
    retry_mode: Literal["provider_idempotent", "unsafe"]
    provider_reference: str | None = None
    error_code: str | None = None
    created_at: str


class ActionApiErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    details: dict[str, Any] | None = None


class ActionApiErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ActionApiErrorBody


class HttpActionProviderConfig(BaseModel):
    """Deployment-owned configuration for one HTTP destination alias."""

    model_config = ConfigDict(extra="forbid", strict=True)

    endpoint: str
    provider_identity: str = Field(min_length=1, max_length=200)
    bearer_token: SecretStr | None = None
    allow_insecure_localhost: bool = False
    supports_idempotency: bool = False
    definitive_status_codes: tuple[int, ...] = ()
    timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    max_response_bytes: int = Field(default=65_536, ge=256, le=1_048_576)

    def build(self) -> HttpExternalActionProvider:
        return HttpExternalActionProvider(
            endpoint=self.endpoint,
            provider_identity=self.provider_identity,
            bearer_token=(
                self.bearer_token.get_secret_value()
                if self.bearer_token is not None
                else None
            ),
            allow_insecure_localhost=self.allow_insecure_localhost,
            supports_idempotency=self.supports_idempotency,
            definitive_status_codes=self.definitive_status_codes,
            timeout_seconds=self.timeout_seconds,
            max_response_bytes=self.max_response_bytes,
        )


def to_durable_action_input(request: ActionCreateRequest) -> DurableActionInput:
    return DurableActionInput(
        action_type="webhook.send",
        destination=request.destination,
        idempotency_key=request.idempotency_key,
        input=request.input,
    )


def action_fingerprint(runtime_input: DurableActionInput) -> str:
    material = {
        "contract": runtime_input.contract,
        "action_type": runtime_input.action_type,
        "destination": runtime_input.destination,
        "input": runtime_input.input.model_dump(mode="json"),
    }
    return sha256(canonical_action_json(material).encode("utf-8")).hexdigest()


def action_client_request_id(idempotency_key: str) -> str:
    digest = sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"action-request:v1:{digest}"


def action_thread_id(
    tenant_id: str,
    action_type: str,
    idempotency_key: str,
) -> str:
    material = [
        "action-thread-v1",
        tenant_id,
        action_type,
        idempotency_key,
    ]
    digest = sha256(canonical_action_json(material).encode("utf-8")).hexdigest()
    return f"action_thread_v1_{digest}"


def persisted_action_input(run: RunRecord) -> DurableActionInput:
    if run.input is None:
        raise ValueError("Persisted Action input is missing")
    return DurableActionInput.model_validate(run.input)


def is_action_run(run: RunRecord) -> bool:
    return bool(
        run.agent_id == ACTION_AGENT_ID
        and run.agent_version == ACTION_AGENT_VERSION
        and run.domain_id == ACTION_DOMAIN_ID
        and run.schema_version == ACTION_SCHEMA_VERSION
    )


def load_action_providers_from_environment(
    variable_name: str = "RUNTIME_ACTION_PROVIDERS_JSON",
) -> Mapping[str, ExternalActionProvider]:
    raw_value = os.getenv(variable_name)
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        configs = TypeAdapter(dict[str, HttpActionProviderConfig]).validate_json(
            raw_value
        )
    except ValidationError:
        raise ValueError(
            f"{variable_name} must be a JSON object of HTTP destination configurations"
        ) from None
    if any(re.fullmatch(ACTION_DESTINATION_PATTERN, alias) is None for alias in configs):
        raise ValueError(f"{variable_name} contains an invalid destination alias")
    return {alias: config.build() for alias, config in configs.items()}
