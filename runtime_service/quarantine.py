from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import RunStatus


class QuarantineResolutionError(RuntimeError):
    pass


class QuarantineTargetNotFoundError(QuarantineResolutionError):
    pass


class QuarantineResolutionStalePlanError(QuarantineResolutionError):
    def __init__(self, current_plan: "QuarantineResolutionPlan") -> None:
        self.current_plan = current_plan
        super().__init__("Quarantine resolution plan is stale")


class QuarantineResolutionEvidenceIncompleteError(QuarantineResolutionError):
    pass


class QuarantineResolutionKind(str, Enum):
    TERMINALIZE_FAILED_PRESERVING_CHECKPOINT = (
        "terminalize_failed_preserving_checkpoint"
    )


class QuarantineTargetKind(str, Enum):
    RUN = "run"
    ACTION = "action"


class QuarantineResolutionTarget(BaseModel):
    """One public target identity without exposing a private Action-owned Run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str | None = Field(default=None, min_length=1, max_length=200)
    action_id: str | None = Field(default=None, min_length=1, max_length=200)

    @model_validator(mode="after")
    def require_exactly_one_target(self) -> "QuarantineResolutionTarget":
        if (self.run_id is None) == (self.action_id is None):
            raise ValueError("provide exactly one of run_id or action_id")
        return self

    @property
    def kind(self) -> QuarantineTargetKind:
        return (
            QuarantineTargetKind.RUN
            if self.run_id is not None
            else QuarantineTargetKind.ACTION
        )

    @property
    def identifier(self) -> str:
        identifier = self.run_id if self.run_id is not None else self.action_id
        assert identifier is not None
        return identifier


class QuarantineResolutionCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: QuarantineResolutionTarget
    resolution: QuarantineResolutionKind
    dry_run: bool = True
    expected_plan_id: str | None = Field(
        default=None,
        pattern=r"^qrp_[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def validate_plan_reference(self) -> "QuarantineResolutionCommand":
        if self.dry_run and self.expected_plan_id is not None:
            raise ValueError("dry-run must not include expected_plan_id")
        if not self.dry_run and self.expected_plan_id is None:
            raise ValueError("apply requires expected_plan_id")
        return self


class ExternalActionStatusSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    total: int = Field(ge=0)
    prepared: int = Field(ge=0)
    dispatching: int = Field(ge=0)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)
    outcome_unknown: int = Field(ge=0)
    unrecognized: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_total(self) -> "ExternalActionStatusSummary":
        counted = (
            self.prepared
            + self.dispatching
            + self.succeeded
            + self.failed
            + self.outcome_unknown
            + self.unrecognized
        )
        if self.total != counted:
            raise ValueError("external action status counts must equal total")
        return self


class QuarantineThreadReference(BaseModel):
    """Tenant-qualified public reference for the blocked execution slot.

    Public Runs expose their Thread ID. Private Action-owned Runs keep that
    implementation detail hidden and expose the public Action ID instead.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    reference_kind: Literal["thread_id", "action_id"]
    reference: str


class QuarantineResolutionPlan(BaseModel):
    """Sanitized deterministic plan derived from one durable SQLite snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target: QuarantineResolutionTarget
    resolution: QuarantineResolutionKind
    eligible: bool
    plan_id: str | None = Field(default=None, pattern=r"^qrp_[0-9a-f]{64}$")
    thread: QuarantineThreadReference
    current_run_status: RunStatus
    current_quarantine_code: str | None = None
    cancel_requested: bool
    checkpoint_base_revision: int | None = Field(default=None, ge=0)
    observed_checkpoint_revision: int = Field(ge=0)
    external_actions: ExternalActionStatusSummary
    workflow_reconciliation_required: bool
    planned_run_transition: str
    checkpoint_disposition: Literal["preserved"] = "preserved"
    external_evidence_disposition: Literal["preserved"] = "preserved"
    provider_calls: Literal[0] = 0
    new_audit_events: tuple[str, ...] = ()
    thread_disposition: str
    ineligibility_reasons: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_eligibility_shape(self) -> "QuarantineResolutionPlan":
        if self.eligible:
            if self.plan_id is None or self.ineligibility_reasons:
                raise ValueError("eligible plan requires plan_id and no reasons")
        elif self.plan_id is not None:
            raise ValueError("ineligible plan must not include plan_id")
        return self


class QuarantineResolutionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal["dry_run", "applied", "reused"]
    plan: QuarantineResolutionPlan
    reused: bool = False
    verified: bool = False


class QuarantineResolutionCommit(BaseModel):
    """Internal store outcome carried into post-commit verification."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    plan: QuarantineResolutionPlan
    reused: bool
    workflow_evidence_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_evidence_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
