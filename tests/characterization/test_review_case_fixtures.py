"""Golden characterization of the labeled review-workflow fixtures.

Wraps `eval/review_cases.json` (via `eval/review_runner.py`'s loader) and
locks the exact `WorkflowReviewResult` shape the orchestrator currently
produces for each case, after normalizing non-deterministic ids/timestamps/
durations with `canonicalize()`. This is a superset of what
`eval/review_runner.py` itself checks (expected finding *types* only): here
we lock the full structured findings/directives/evidence, not just their
type set, so a later refactor (e.g. ValidationFinding/ReviewFinding shape
changes) cannot silently change severity, evidence, or rule ids.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from domains.travel.review.orchestrator import WorkflowOrchestrator
from eval.review_runner import load_cases, state_from_case

from canonicalize import canonicalize

FIXTURES_DIR = Path(__file__).with_name("fixtures")
FIXTURE_PATH = FIXTURES_DIR / "review_case_golden.json"


def _run_cases() -> list[dict]:
    orchestrator = WorkflowOrchestrator()
    results = []
    for case in load_cases():
        workflow = orchestrator.run_sync(state_from_case(case))
        results.append(
            {
                "case_id": case["case_id"],
                "workflow": canonicalize(workflow.model_dump(mode="json")),
            }
        )
    return results


def test_review_case_golden_matches_fixture():
    if not FIXTURE_PATH.exists():
        pytest.fail(
            f"Missing golden fixture: {FIXTURE_PATH}. Regenerate with "
            "`python -m tests.characterization.test_review_case_fixtures` "
            "from the repo root."
        )
    expected = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert _run_cases() == expected


def test_review_case_golden_is_reproducible_across_consecutive_runs():
    first = _run_cases()
    second = _run_cases()
    assert first == second


if __name__ == "__main__":
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(
        json.dumps(_run_cases(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {FIXTURE_PATH}")
