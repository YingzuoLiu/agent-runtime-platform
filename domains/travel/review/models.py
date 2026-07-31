from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from agent.contracts import utc_now


class ReviewerRole(str, Enum):
    BUDGET = "budget"
    PREFERENCE = "preference"


class EvidenceSourceType(str, Enum):
    CONSTRAINT = "constraint"
    PLAN_ITEM = "plan_item"
    MEMORY = "memory"
    TOOL_RESULT = "tool_result"


class CostLedgerStatus(str, Enum):
    """Whether the cost ledger can independently verify the plan total."""

    COMPLETE = "complete"
    FALLBACK = "fallback"
    INCOMPLETE = "incomplete"
    UNAVAILABLE = "unavailable"


class FindingBasis(str, Enum):
    DETERMINISTIC = "deterministic"
    LLM_SEMANTIC = "llm_semantic"
    HYBRID = "hybrid"


class FindingVerdict(str, Enum):
    CONFIRMED = "confirmed"
    PLAUSIBLE = "plausible"
    REFUTED = "refuted"


class FindingSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TaskStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    BLOCKED = "blocked"


class WorkflowStatus(str, Enum):
    COMPLETED = "completed"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FindingType(str, Enum):
    BUDGET_OVERRUN = "budget_overrun"
    RED_EYE_PREFERENCE_VIOLATION = "red_eye_preference_violation"
    HOTEL_LOCATION_PREFERENCE_VIOLATION = "hotel_location_preference_violation"
    TRAVEL_STYLE_PREFERENCE_VIOLATION = "travel_style_preference_violation"
    MEMORY_PREFERENCE_VIOLATION = "memory_preference_violation"


class ReplanAction(str, Enum):
    REDUCE_COST = "reduce_cost"
    CHANGE_FLIGHT = "change_flight"
    CHANGE_ACCOMMODATION = "change_accommodation"
    ADJUST_ITINERARY_STYLE = "adjust_itinerary_style"
    REQUEST_USER_INPUT = "request_user_input"


class EvidenceRef(BaseModel):
    """Machine-readable evidence with provenance, not a free-text assertion."""

    source_type: EvidenceSourceType
    source_id: str = Field(..., min_length=1)
    field: str | None = None
    observed: Any = None
    expected: Any = None
    unit: str | None = None
    tool_call_id: str | None = None


class CostItem(BaseModel):
    item_id: str
    category: str
    amount: float
    unit: str = "currency_units"
    source_id: str


class MemoryEvidence(BaseModel):
    memory_id: str
    key: str
    value: Any


class CandidatePlanEvidence(BaseModel):
    plan_item_id: str = "itinerary"
    destination: str
    days: int
    flight_type: str
    hotel_tier: str
    poi_style: str
    total_cost: float


class PlanEvidence(BaseModel):
    """Canonical, read-only facts shared by all role-specific projections."""

    thread_id: str
    candidate_plan: CandidatePlanEvidence | None = None
    budget_limit: float | None = None
    explicit_preferences: dict[str, Any] = Field(default_factory=dict)
    relevant_memory: list[MemoryEvidence] = Field(default_factory=list)
    cost_ledger: list[CostItem] = Field(default_factory=list)
    cost_ledger_status: CostLedgerStatus = CostLedgerStatus.UNAVAILABLE
    evidence_issues: list[str] = Field(default_factory=list)


class BudgetReviewContext(BaseModel):
    workflow_run_id: str
    task_id: str
    reviewer: ReviewerRole = ReviewerRole.BUDGET
    candidate_plan: CandidatePlanEvidence | None
    budget_limit: float | None
    cost_ledger: list[CostItem] = Field(default_factory=list)
    cost_ledger_status: CostLedgerStatus = CostLedgerStatus.UNAVAILABLE


class PreferenceReviewContext(BaseModel):
    workflow_run_id: str
    task_id: str
    reviewer: ReviewerRole = ReviewerRole.PREFERENCE
    candidate_plan: CandidatePlanEvidence | None
    explicit_preferences: dict[str, Any] = Field(default_factory=dict)
    relevant_memory: list[MemoryEvidence] = Field(default_factory=list)


ReviewContext = BudgetReviewContext | PreferenceReviewContext


class ReviewFinding(BaseModel):
    finding_id: str
    task_id: str
    reviewer: ReviewerRole
    rule_id: str
    finding_type: FindingType
    basis: FindingBasis
    verdict: FindingVerdict
    severity: FindingSeverity
    summary: str
    evidence: list[EvidenceRef] = Field(min_length=1)
    affected_plan_item_ids: list[str] = Field(default_factory=list)


class SkippedCheck(BaseModel):
    rule_id: str
    reason: str
    required: bool = True
    missing_evidence: list[str] = Field(default_factory=list)


class ReviewerReport(BaseModel):
    reviewer: ReviewerRole
    task_id: str
    checked_rule_ids: list[str] = Field(default_factory=list)
    skipped_checks: list[SkippedCheck] = Field(default_factory=list)
    findings: list[ReviewFinding] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_report_identity_and_coverage(self) -> "ReviewerReport":
        for finding in self.findings:
            if finding.task_id != self.task_id or finding.reviewer != self.reviewer:
                raise ValueError("Finding identity must match its reviewer report")
        overlap = set(self.checked_rule_ids) & {
            check.rule_id for check in self.skipped_checks
        }
        if overlap:
            raise ValueError(
                "A rule cannot be both checked and skipped: " + ", ".join(sorted(overlap))
            )
        return self

    @property
    def has_required_coverage_gap(self) -> bool:
        return any(check.required for check in self.skipped_checks)


class SubtaskResult(BaseModel):
    task_id: str
    reviewer: ReviewerRole
    status: TaskStatus
    attempts: int
    output: ReviewerReport | None = None
    error_code: str | None = None
    error_message: str | None = None
    started_at: str = Field(default_factory=utc_now)
    finished_at: str = Field(default_factory=utc_now)
    duration_ms: float = 0.0


class ReplanDirective(BaseModel):
    directive_id: str
    action_type: ReplanAction
    target_item_ids: list[str]
    finding_ids: list[str]
    preserve_item_ids: list[str] = Field(default_factory=list)
    reason: str


class ReducerOutput(BaseModel):
    findings: list[ReviewFinding] = Field(default_factory=list)
    directives: list[ReplanDirective] = Field(default_factory=list)


class WorkflowReviewResult(BaseModel):
    workflow_run_id: str
    status: WorkflowStatus
    tasks: list[SubtaskResult]
    evidence_issues: list[str] = Field(default_factory=list)
    findings: list[ReviewFinding] = Field(default_factory=list)
    directives: list[ReplanDirective] = Field(default_factory=list)
    started_at: str = Field(default_factory=utc_now)
    finished_at: str = Field(default_factory=utc_now)
    duration_ms: float = 0.0
