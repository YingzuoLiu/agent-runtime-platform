from __future__ import annotations

import os
import secrets
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from runtime_service import (
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)


DEMO_AGENT_ID = "travel-agent"
DEMO_AGENT_VERSION = "1.2.0"
DEMO_DEFAULT_MESSAGE = (
    "Plan a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
)
DEMO_TENANT_ID = "local-demo-tenant"
DEMO_SUBJECT_ID = "local-demo-user"


class DemoSession(BaseModel):
    """Ephemeral browser bootstrap available only in explicit local demo mode."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    api_key: str = Field(min_length=1)
    agent_id: str = DEMO_AGENT_ID
    agent_version: str = DEMO_AGENT_VERSION
    default_message: str = DEMO_DEFAULT_MESSAGE
    requested_action: str = "plan_only"


def resolve_demo_mode(explicit_value: bool | None) -> bool:
    if explicit_value is not None:
        return explicit_value
    raw_value = os.getenv("RUNTIME_DEMO_MODE")
    if raw_value is None or not raw_value.strip():
        return False
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        "RUNTIME_DEMO_MODE must be one of true, false, 1, 0, yes, no, on, or off"
    )


def create_demo_session(api_key: str | None = None) -> DemoSession:
    return DemoSession(api_key=api_key or secrets.token_urlsafe(32))


def build_demo_authenticator(session: DemoSession) -> StaticApiKeyAuthenticator:
    return StaticApiKeyAuthenticator(
        [
            ApiKeyCredential(
                credential_id="local-demo-operator",
                api_key=session.api_key,
                tenant_id=DEMO_TENANT_ID,
                subject_id=DEMO_SUBJECT_ID,
                role=RuntimeRole.OPERATOR,
            )
        ]
    )


def demo_assets_path() -> Path:
    return Path(__file__).resolve().parent / "demo_assets"
