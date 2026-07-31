from __future__ import annotations

from typing import Any

from ..state import AgentState

from .models import (
    BudgetReviewContext,
    CandidatePlanEvidence,
    CostLedgerStatus,
    CostItem,
    MemoryEvidence,
    PlanEvidence,
    PreferenceReviewContext,
    ReviewerRole,
    ReviewContext,
)


class PlanEvidenceBuilder:
    """Build one canonical fact snapshot before reviewers run in parallel."""

    COST_TOTAL_TOLERANCE = 0.01

    _COST_KEYS = {
        "flight_cost": ("flight", False),
        "hotel_cost_per_day": ("hotel", True),
        "activity_cost_per_day": ("activities", True),
    }

    def build(self, state: AgentState) -> PlanEvidence:
        candidate = None
        if state.itinerary is not None:
            candidate = CandidatePlanEvidence(
                destination=state.itinerary.destination,
                days=state.itinerary.days,
                flight_type=state.itinerary.flight_type,
                hotel_tier=state.itinerary.hotel_tier,
                poi_style=state.itinerary.poi_style,
                total_cost=state.itinerary.total_cost,
            )

        issues: list[str] = []
        cost_ledger, cost_ledger_status = self._build_cost_ledger(state, issues)
        self._check_cost_total_consistency(
            candidate,
            cost_ledger,
            cost_ledger_status,
            issues,
        )
        relevant_memory = self._build_memory_evidence(state, issues)

        return PlanEvidence(
            thread_id=state.thread_id,
            candidate_plan=candidate,
            budget_limit=state.budget,
            explicit_preferences=dict(state.preferences),
            relevant_memory=relevant_memory,
            cost_ledger=cost_ledger,
            cost_ledger_status=cost_ledger_status,
            evidence_issues=issues,
        )

    def _build_cost_ledger(
        self,
        state: AgentState,
        issues: list[str],
    ) -> tuple[list[CostItem], CostLedgerStatus]:
        raw_breakdown = state.tool_outputs.get("cost_breakdown")
        if raw_breakdown is None:
            if state.itinerary is None:
                return [], CostLedgerStatus.UNAVAILABLE
            issues.append("cost_breakdown_missing_using_plan_total")
            return (
                [
                    CostItem(
                        item_id="plan-total",
                        category="plan_total",
                        amount=float(state.itinerary.total_cost),
                        source_id="itinerary",
                    )
                ],
                CostLedgerStatus.FALLBACK,
            )
        if not isinstance(raw_breakdown, dict):
            issues.append("cost_breakdown_invalid")
            return [], CostLedgerStatus.INCOMPLETE

        ledger: list[CostItem] = []
        for key, (category, per_day) in self._COST_KEYS.items():
            raw_amount = raw_breakdown.get(key)
            if not isinstance(raw_amount, (int, float)):
                issues.append(f"cost_breakdown_missing_or_invalid:{key}")
                continue
            amount = float(raw_amount)
            if per_day:
                if state.days is None:
                    issues.append(f"cost_breakdown_cannot_expand_without_days:{key}")
                    continue
                amount *= state.days
            ledger.append(
                CostItem(
                    item_id=key,
                    category=category,
                    amount=amount,
                    source_id="tool_outputs.cost_breakdown",
                )
            )
        status = (
            CostLedgerStatus.COMPLETE
            if len(ledger) == len(self._COST_KEYS)
            else CostLedgerStatus.INCOMPLETE
        )
        return ledger, status

    def _check_cost_total_consistency(
        self,
        candidate: CandidatePlanEvidence | None,
        ledger: list[CostItem],
        status: CostLedgerStatus,
        issues: list[str],
    ) -> None:
        if candidate is None or status != CostLedgerStatus.COMPLETE:
            return
        ledger_total = sum(item.amount for item in ledger)
        if abs(ledger_total - candidate.total_cost) <= self.COST_TOTAL_TOLERANCE:
            return
        issues.append(
            "cost_total_mismatch:"
            f"plan_total={candidate.total_cost:g},ledger_total={ledger_total:g}"
        )

    @staticmethod
    def _build_memory_evidence(
        state: AgentState,
        issues: list[str],
    ) -> list[MemoryEvidence]:
        raw_memories = state.tool_outputs.get("memory_refs", [])
        if not isinstance(raw_memories, list):
            issues.append("memory_refs_invalid")
            return []

        memories: list[MemoryEvidence] = []
        for index, item in enumerate(raw_memories):
            if not isinstance(item, dict) or "key" not in item:
                issues.append(f"memory_ref_invalid:{index}")
                continue
            memory_id = str(item.get("memory_id") or item.get("id") or f"memory-{index}")
            memories.append(
                MemoryEvidence(
                    memory_id=memory_id,
                    key=str(item["key"]),
                    value=item.get("value"),
                )
            )
        return memories


class ContextProjector:
    """Project role-specific typed inputs from PlanEvidence without recomputing facts."""

    def project(
        self,
        evidence: PlanEvidence,
        reviewer: ReviewerRole,
        *,
        workflow_run_id: str,
        task_id: str,
    ) -> ReviewContext:
        if reviewer == ReviewerRole.BUDGET:
            return BudgetReviewContext(
                workflow_run_id=workflow_run_id,
                task_id=task_id,
                candidate_plan=evidence.candidate_plan,
                budget_limit=evidence.budget_limit,
                cost_ledger=evidence.cost_ledger,
                cost_ledger_status=evidence.cost_ledger_status,
            )
        if reviewer == ReviewerRole.PREFERENCE:
            return PreferenceReviewContext(
                workflow_run_id=workflow_run_id,
                task_id=task_id,
                candidate_plan=evidence.candidate_plan,
                explicit_preferences=evidence.explicit_preferences,
                relevant_memory=evidence.relevant_memory,
            )
        raise ValueError(f"Unsupported reviewer role: {reviewer}")


def has_truthy_preference(preferences: dict[str, Any], key: str) -> bool:
    return bool(preferences.get(key, False))
