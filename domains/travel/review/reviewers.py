from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import uuid4

from .models import (
    BudgetReviewContext,
    CostLedgerStatus,
    EvidenceRef,
    EvidenceSourceType,
    FindingBasis,
    FindingSeverity,
    FindingType,
    FindingVerdict,
    PreferenceReviewContext,
    ReviewerReport,
    ReviewerRole,
    ReviewFinding,
    ReviewContext,
    SkippedCheck,
)


class ReviewerBlocked(RuntimeError):
    """The reviewer cannot run because required evidence is unavailable."""

    def __init__(self, message: str, *, missing_evidence: list[str] | None = None):
        super().__init__(message)
        self.missing_evidence = missing_evidence or []


class RetryableReviewerError(RuntimeError):
    """An explicitly transient reviewer failure that may be retried."""


class Reviewer(Protocol):
    role: ReviewerRole

    async def run(self, context: ReviewContext) -> ReviewerReport:
        ...


SemanticPreferenceAnalyzer = Callable[
    [PreferenceReviewContext],
    Awaitable[list[ReviewFinding]],
]


class BudgetChecker:
    role = ReviewerRole.BUDGET
    rule_id = "budget.total_cost_within_limit"

    async def run(self, context: ReviewContext) -> ReviewerReport:
        if not isinstance(context, BudgetReviewContext):
            raise TypeError("BudgetChecker requires BudgetReviewContext")
        if context.candidate_plan is None:
            raise ReviewerBlocked(
                "Candidate plan is required for budget review",
                missing_evidence=["candidate_plan"],
            )
        if context.budget_limit is None:
            raise ReviewerBlocked(
                "Budget limit is required for budget review",
                missing_evidence=["budget_limit"],
            )

        findings: list[ReviewFinding] = []
        plan_total = context.candidate_plan.total_cost
        ledger_is_authoritative = (
            context.cost_ledger_status == CostLedgerStatus.COMPLETE
        )
        total = (
            sum(item.amount for item in context.cost_ledger)
            if ledger_is_authoritative
            else plan_total
        )
        limit = context.budget_limit
        if total > limit:
            overrun = total - limit
            ratio = overrun / limit if limit > 0 else 1.0
            severity = FindingSeverity.CRITICAL if ratio >= 0.2 else FindingSeverity.HIGH
            evidence = [
                EvidenceRef(
                    source_type=EvidenceSourceType.PLAN_ITEM,
                    source_id=context.candidate_plan.plan_item_id,
                    field="total_cost",
                    observed=plan_total,
                    expected=limit,
                    unit="currency_units",
                ),
                EvidenceRef(
                    source_type=EvidenceSourceType.CONSTRAINT,
                    source_id="agent_state.budget",
                    field="budget_limit",
                    observed=limit,
                    expected=f"total_cost <= {limit}",
                    unit="currency_units",
                ),
            ]
            if ledger_is_authoritative:
                evidence.append(
                    EvidenceRef(
                        source_type=EvidenceSourceType.TOOL_RESULT,
                        source_id="tool_outputs.cost_breakdown",
                        field="computed_total",
                        observed=total,
                        expected=limit,
                        unit="currency_units",
                    )
                )
            evidence.extend(
                EvidenceRef(
                    source_type=EvidenceSourceType.TOOL_RESULT,
                    source_id=item.source_id,
                    field=item.item_id,
                    observed=item.amount,
                    unit=item.unit,
                )
                for item in context.cost_ledger
            )
            findings.append(
                ReviewFinding(
                    finding_id=f"finding_{uuid4().hex}",
                    task_id=context.task_id,
                    reviewer=self.role,
                    rule_id=self.rule_id,
                    finding_type=FindingType.BUDGET_OVERRUN,
                    basis=FindingBasis.DETERMINISTIC,
                    verdict=FindingVerdict.CONFIRMED,
                    severity=severity,
                    summary=f"Candidate plan exceeds the budget by {overrun:g} currency units.",
                    evidence=evidence,
                    affected_plan_item_ids=[context.candidate_plan.plan_item_id],
                )
            )

        return ReviewerReport(
            reviewer=self.role,
            task_id=context.task_id,
            checked_rule_ids=[self.rule_id],
            findings=findings,
        )


class PreferenceReviewer:
    """
    Review structured preferences offline and optionally call a semantic analyzer.

    The injectable analyzer is the boundary for an LLM-backed implementation. The
    default path remains deterministic and API-key-free so the repository and CI
    stay reproducible.
    """

    role = ReviewerRole.PREFERENCE
    red_eye_rule = "preference.avoid_red_eye"
    hotel_rule = "preference.hotel_near_subway"
    style_rule = "preference.travel_style"
    memory_rule = "preference.long_term_memory_alignment"
    semantic_rule = "preference.semantic_alignment"

    def __init__(
        self,
        semantic_analyzer: SemanticPreferenceAnalyzer | None = None,
    ) -> None:
        self.semantic_analyzer = semantic_analyzer

    async def run(self, context: ReviewContext) -> ReviewerReport:
        if not isinstance(context, PreferenceReviewContext):
            raise TypeError("PreferenceReviewer requires PreferenceReviewContext")
        if context.candidate_plan is None:
            raise ReviewerBlocked(
                "Candidate plan is required for preference review",
                missing_evidence=["candidate_plan"],
            )

        checked = [self.red_eye_rule, self.hotel_rule, self.style_rule]
        skipped: list[SkippedCheck] = []
        findings = self._review_structured_preferences(context)

        if context.relevant_memory:
            checked.append(self.memory_rule)
            findings.extend(self._review_memory_preferences(context))
        else:
            skipped.append(
                SkippedCheck(
                    rule_id=self.memory_rule,
                    reason="No relevant long-term memory was loaded for this workflow.",
                    required=False,
                    missing_evidence=["relevant_memory"],
                )
            )

        if self.semantic_analyzer is None:
            skipped.append(
                SkippedCheck(
                    rule_id=self.semantic_rule,
                    reason="No semantic preference analyzer is configured.",
                    required=False,
                    missing_evidence=["semantic_analyzer"],
                )
            )
        else:
            checked.append(self.semantic_rule)
            semantic_findings = await self.semantic_analyzer(context)
            for finding in semantic_findings:
                if finding.reviewer != self.role or finding.task_id != context.task_id:
                    raise ValueError(
                        "Semantic preference findings must preserve reviewer and task identity"
                    )
                if finding.basis not in {
                    FindingBasis.LLM_SEMANTIC,
                    FindingBasis.HYBRID,
                }:
                    raise ValueError(
                        "Semantic preference findings must use llm_semantic or hybrid basis"
                    )
            findings.extend(semantic_findings)

        return ReviewerReport(
            reviewer=self.role,
            task_id=context.task_id,
            checked_rule_ids=checked,
            skipped_checks=skipped,
            findings=findings,
        )

    def _review_structured_preferences(
        self,
        context: PreferenceReviewContext,
    ) -> list[ReviewFinding]:
        plan = context.candidate_plan
        assert plan is not None
        findings: list[ReviewFinding] = []

        if bool(context.explicit_preferences.get("avoid_red_eye")) and plan.flight_type == "red_eye":
            findings.append(
                self._finding(
                    context,
                    rule_id=self.red_eye_rule,
                    finding_type=FindingType.RED_EYE_PREFERENCE_VIOLATION,
                    severity=FindingSeverity.HIGH,
                    summary="Candidate plan uses a red-eye flight despite the explicit preference.",
                    preference_key="avoid_red_eye",
                    expected=True,
                    plan_field="flight_type",
                    observed=plan.flight_type,
                )
            )

        if bool(context.explicit_preferences.get("hotel_near_subway")) and (
            "near-subway" not in plan.hotel_tier.lower()
        ):
            findings.append(
                self._finding(
                    context,
                    rule_id=self.hotel_rule,
                    finding_type=FindingType.HOTEL_LOCATION_PREFERENCE_VIOLATION,
                    severity=FindingSeverity.MEDIUM,
                    summary="Candidate hotel does not satisfy the near-subway preference.",
                    preference_key="hotel_near_subway",
                    expected=True,
                    plan_field="hotel_tier",
                    observed=plan.hotel_tier,
                )
            )

        expected_style = context.explicit_preferences.get("travel_style")
        if expected_style and str(expected_style).lower() not in plan.poi_style.lower():
            findings.append(
                self._finding(
                    context,
                    rule_id=self.style_rule,
                    finding_type=FindingType.TRAVEL_STYLE_PREFERENCE_VIOLATION,
                    severity=FindingSeverity.MEDIUM,
                    summary="Candidate itinerary style conflicts with the explicit travel style.",
                    preference_key="travel_style",
                    expected=expected_style,
                    plan_field="poi_style",
                    observed=plan.poi_style,
                )
            )

        return findings

    def _review_memory_preferences(
        self,
        context: PreferenceReviewContext,
    ) -> list[ReviewFinding]:
        """Use memory only when the current request did not override that key."""
        plan = context.candidate_plan
        assert plan is not None
        findings: list[ReviewFinding] = []
        for memory in context.relevant_memory:
            if memory.key in context.explicit_preferences:
                continue
            violated = False
            plan_field = ""
            observed: object = None
            if memory.key == "avoid_red_eye" and bool(memory.value):
                violated = plan.flight_type == "red_eye"
                plan_field = "flight_type"
                observed = plan.flight_type
            elif memory.key == "hotel_near_subway" and bool(memory.value):
                violated = "near-subway" not in plan.hotel_tier.lower()
                plan_field = "hotel_tier"
                observed = plan.hotel_tier
            elif memory.key == "travel_style" and memory.value:
                violated = str(memory.value).lower() not in plan.poi_style.lower()
                plan_field = "poi_style"
                observed = plan.poi_style

            if violated:
                findings.append(
                    ReviewFinding(
                        finding_id=f"finding_{uuid4().hex}",
                        task_id=context.task_id,
                        reviewer=self.role,
                        rule_id=self.memory_rule,
                        finding_type=FindingType.MEMORY_PREFERENCE_VIOLATION,
                        basis=FindingBasis.DETERMINISTIC,
                        verdict=FindingVerdict.CONFIRMED,
                        severity=FindingSeverity.LOW,
                        summary=(
                            f"Candidate plan does not use the relevant long-term preference "
                            f"'{memory.key}'."
                        ),
                        evidence=[
                            EvidenceRef(
                                source_type=EvidenceSourceType.MEMORY,
                                source_id=memory.memory_id,
                                field=memory.key,
                                observed=memory.value,
                            ),
                            EvidenceRef(
                                source_type=EvidenceSourceType.PLAN_ITEM,
                                source_id=plan.plan_item_id,
                                field=plan_field,
                                observed=observed,
                                expected=memory.value,
                            ),
                        ],
                        affected_plan_item_ids=[plan.plan_item_id],
                    )
                )
        return findings

    def _finding(
        self,
        context: PreferenceReviewContext,
        *,
        rule_id: str,
        finding_type: FindingType,
        severity: FindingSeverity,
        summary: str,
        preference_key: str,
        expected: object,
        plan_field: str,
        observed: object,
    ) -> ReviewFinding:
        assert context.candidate_plan is not None
        return ReviewFinding(
            finding_id=f"finding_{uuid4().hex}",
            task_id=context.task_id,
            reviewer=self.role,
            rule_id=rule_id,
            finding_type=finding_type,
            basis=FindingBasis.DETERMINISTIC,
            verdict=FindingVerdict.CONFIRMED,
            severity=severity,
            summary=summary,
            evidence=[
                EvidenceRef(
                    source_type=EvidenceSourceType.CONSTRAINT,
                    source_id="agent_state.preferences",
                    field=preference_key,
                    observed=expected,
                ),
                EvidenceRef(
                    source_type=EvidenceSourceType.PLAN_ITEM,
                    source_id=context.candidate_plan.plan_item_id,
                    field=plan_field,
                    observed=observed,
                    expected=expected,
                ),
            ],
            affected_plan_item_ids=[context.candidate_plan.plan_item_id],
        )
