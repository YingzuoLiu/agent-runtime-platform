from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, model_validator

from agent.contracts import BaseRuntimeState, utc_now


LEGACY_TENANT_ID = "legacy"


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED, self.CANCELLED}


class RunCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thread_id: str = Field(..., min_length=1)
    agent_id: str = "travel-agent"
    agent_version: str = "0.3.0"
    input: dict[str, Any] | None = None
    state: dict[str, Any] | SerializeAsAny[BaseRuntimeState] | None = None
    client_request_id: str | None = Field(default=None, min_length=1, max_length=200)
    user_message: str | None = Field(default=None, min_length=1, exclude=True)

    @model_validator(mode="after")
    def normalize_legacy_message(self) -> "RunCreateRequest":
        if self.input is None and self.user_message is None:
            raise ValueError("input is required")
        if self.input is not None and self.user_message is not None:
            raise ValueError("provide input or user_message, not both")
        if self.input is None:
            self.input = {"user_message": self.user_message}
        return self


class RunRecord(BaseModel):
    run_id: str
    tenant_id: str = Field(default=LEGACY_TENANT_ID, min_length=1, max_length=200)
    thread_id: str
    agent_id: str
    agent_version: str
    domain_id: str = "travel"
    schema_version: str = "1"
    status: RunStatus
    input: dict[str, Any] | None = None
    input_message: str | None = Field(default=None, exclude=True)
    state: SerializeAsAny[BaseRuntimeState] | None = None
    output_message: str | None = None
    validation_errors: list[str] = Field(default_factory=list)
    error: str | None = None
    attempt: int = 0
    cancel_requested: bool = False
    client_request_id: str | None = None
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    started_at: str | None = None
    completed_at: str | None = None

    @model_validator(mode="after")
    def normalize_legacy_message(self) -> "RunRecord":
        if self.input is None and self.input_message is None:
            raise ValueError("input is required")
        if self.input is None:
            self.input = {"user_message": self.input_message}
        return self


class RunEvent(BaseModel):
    event_id: int | None = None
    run_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)


class AgentDescriptor(BaseModel):
    agent_id: str
    version: str
    description: str
    domain_id: str
    schema_version: str
    input_schema: dict[str, Any]
