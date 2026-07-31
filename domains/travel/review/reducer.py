from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

from .models import (
    FindingSeverity,
    FindingType,
    FindingVerdict,
    ReducerOutput,
    ReplanAction,
    ReplanDirective,
    ReviewFinding,
    ReviewerReport,
)


_SEVERITY_ORDER = {
    FindingSeverity.CRITICAL: 0,
    FindingSeverity.HIGH: 1,
    FindingSeverity.MEDIUM: 2,
    FindingSeverity.LOW: 3,
}

_ACTIONS = {
    FindingType.BUDGET_OVERRUN: ReplanAction.REDUCE_COST,
    FindingType.RED_EYE_PREFERENCE_VIOLATION: ReplanAction.CHANGE_FLIGHT,
    FindingType.HOTEL_LOCATION_PREFERENCE_VIOLATION: ReplanAction.CHANGE_ACCOMMODATION,
    FindingType.TRAVEL_STYLE_PREFERENCE_VIOLATION: ReplanAction.ADJUST_ITINERARY_STYLE,
    FindingType.MEMORY_PREFERENCE_VIOLATION: ReplanAction.ADJUST_ITINERARY_STYLE,
}


class FindingReducer:
    """Deterministically filter, deduplicate, rank and route reviewer findings."""

    def reduce(self, reports: list[ReviewerReport]) -> ReducerOutput:
        deduplicated: dict[tuple[object, ...], ReviewFinding] = {}
        for report in reports:
            for finding in report.findings:
                if finding.verdict == FindingVerdict.REFUTED:
                    continue
                key = (
                    finding.rule_id,
                    finding.finding_type,
                    tuple(sorted(finding.affected_plan_item_ids)),
                )
                existing = deduplicated.get(key)
                if existing is None or (
                    _SEVERITY_ORDER[finding.severity]
                    < _SEVERITY_ORDER[existing.severity]
                ):
                    deduplicated[key] = finding

        findings = sorted(
            deduplicated.values(),
            key=lambda finding: (
                _SEVERITY_ORDER[finding.severity],
                finding.rule_id,
                finding.finding_id,
            ),
        )

        grouped: dict[tuple[ReplanAction, tuple[str, ...]], list[ReviewFinding]] = defaultdict(list)
        for finding in findings:
            if finding.verdict != FindingVerdict.CONFIRMED:
                # Plausible semantic findings remain visible evidence, but they
                # cannot trigger an automatic state-changing replan.
                continue
            action = _ACTIONS.get(finding.finding_type)
            if action is None:
                continue
            targets = tuple(sorted(set(finding.affected_plan_item_ids)))
            grouped[(action, targets)].append(finding)

        directives = [
            ReplanDirective(
                directive_id=f"directive_{uuid4().hex}",
                action_type=action,
                target_item_ids=list(targets),
                finding_ids=[finding.finding_id for finding in grouped_findings],
                reason="; ".join(finding.summary for finding in grouped_findings),
            )
            for (action, targets), grouped_findings in sorted(
                grouped.items(),
                key=lambda item: (item[0][0].value, item[0][1]),
            )
        ]
        return ReducerOutput(findings=findings, directives=directives)
