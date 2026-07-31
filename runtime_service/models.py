from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from agent.contracts import utc_now
from domains.travel.state import AgentState


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
    thread_id: str = Field(..., min_length=1)
    user_message: str = Field(..., min_length=1)
    agent_id: str = "travel-agent"
    agent_version: str = "0.3.0"
    state: AgentState | None = None
    client_request_id: str | None = Field(default=None, min_length=1, max_length=200)


class RunRecord(BaseModel):
    run_id: str
    thread_id: str
    agent_id: str
    agent_version: str
    status: RunStatus
    input_message: str
    state: AgentState | None = None
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
