from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from domains.travel.review.orchestrator import WorkflowOrchestrator
from domains.travel.state import AgentState, TravelPlan


DEFAULT_CASES_PATH = Path(__file__).with_name("review_cases.json")


def load_cases(path: Path = DEFAULT_CASES_PATH) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def state_from_case(case: dict[str, Any]) -> AgentState:
    return AgentState(
        thread_id=f"review-eval-{case['case_id']}",
        destination="Tokyo",
        days=5,
        budget=case["budget"],
        preferences=case["preferences"],
        itinerary=TravelPlan(
            destination="Tokyo",
            days=5,
            flight_type="red_eye",
            hotel_tier="standard hotel",
            poi_style="balanced itinerary",
            total_cost=case["total_cost"],
        ),
        tool_outputs={"memory_refs": case["memory_refs"]},
    )


def evaluate_review_cases(
    cases: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    orchestrator = WorkflowOrchestrator()
    results: list[dict[str, Any]] = []
    true_positive = 0
    false_positive = 0
    false_negative = 0

    for case in cases or load_cases():
        workflow = orchestrator.run_sync(state_from_case(case))
        predicted = {finding.finding_type.value for finding in workflow.findings}
        expected = set(case["expected_finding_types"])
        true_positive += len(predicted & expected)
        false_positive += len(predicted - expected)
        false_negative += len(expected - predicted)
        results.append(
            {
                "case_id": case["case_id"],
                "expected": sorted(expected),
                "predicted": sorted(predicted),
                "workflow_status": workflow.status.value,
                "matched": predicted == expected,
            }
        )

    passed = sum(result["matched"] for result in results)
    return {
        "fixture_count": len(results),
        "fixtures_passed": passed,
        "all_fixtures_passed": passed == len(results),
        "expected_findings_matched": true_positive,
        "unexpected_findings": false_positive,
        "missing_expected_findings": false_negative,
        "results": results,
    }


if __name__ == "__main__":
    print(json.dumps(evaluate_review_cases(), indent=2))
