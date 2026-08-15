from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import BaseRuntimeState

IncidentSeverity = Literal["warning", "critical"]
IncidentRisk = Literal["low", "elevated", "high"]
IncidentAction = Literal["observe", "investigate", "prepare_rollback_review"]


class SupportedIncidentService(str, Enum):
    CATALOG_API = "catalog-api"
    CHECKOUT_API = "checkout-api"


class IncidentTriageInput(BaseModel):
    """Structured input owned by the optional incident-triage domain."""

    model_config = ConfigDict(extra="forbid")

    alert_id: str = Field(min_length=1, max_length=200)
    service: str | None = Field(default=None, min_length=1, max_length=200)
    severity: IncidentSeverity
    error_rate_percent: float = Field(ge=0, le=100)
    recent_deployment: bool


class InspectIncidentSignalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_id: str = Field(min_length=1, max_length=200)
    service: SupportedIncidentService


class IncidentSignalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["synthetic_incident_fixture"]
    alert_id: str
    service: SupportedIncidentService
    severity: IncidentSeverity
    error_rate_percent: float
    recent_deployment: bool
    risk_level: IncidentRisk


class IncidentFinishPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_id: str = Field(min_length=1, max_length=200)
    recommended_action: IncidentAction


class IncidentTriageResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_id: str
    service: SupportedIncidentService
    risk_level: IncidentRisk
    recommended_action: IncidentAction
    evidence_source: Literal["synthetic_incident_fixture"]
    action_executed: Literal[False] = False


class IncidentTriageState(BaseRuntimeState):
    domain_id: ClassVar[str] = "incident-triage"
    schema_version: ClassVar[str] = "1"

    alert_id: str | None = None
    service: str | None = None
    claimed_severity: IncidentSeverity | None = None
    claimed_error_rate_percent: float | None = None
    claimed_recent_deployment: bool | None = None
    result: IncidentTriageResult | None = None
    tool_outputs: dict[str, Any] = Field(default_factory=dict)
    blockers: list[str] = Field(default_factory=list)
    current_stage: str = "initialized"
