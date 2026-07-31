"""Golden-trace characterization tests for TravelAgentRuntime.

These tests lock the current, observable behavior of `travel-agent:0.3.0`
(baseline) and `travel-agent:0.5.0` (evidence-review workflow enabled)
across the four multi-turn scenarios already used by the ablation eval
harness (`eval/runner.py`) and documented in `FINDINGS.md`.

Non-deterministic fields (timestamps, workflow/task/finding/directive uuids,
duration_ms) are normalized by `canonicalize()` before comparison -- see
`tests/characterization/canonicalize.py` for why. The fixture must still
reflect real, current output: `python -m tests.characterization.test_travel_golden_traces`
(run from the repo root) regenerates `fixtures/travel_golden_traces.json` from
the actual runtime.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from domains.travel.state import AgentState
from domains.travel.runtime import TravelAgentRuntime
from eval.runner import SCENARIOS

from canonicalize import canonicalize

FIXTURES_DIR = Path(__file__).with_name("fixtures")
FIXTURE_PATH = FIXTURES_DIR / "travel_golden_traces.json"

# (config_name, enable_review_workflow) -- mirrors the two registered agent
# versions in runtime_service/registry.py::build_default_registry.
CONFIGS = [
    ("travel-agent-0.3.0", False),
    ("travel-agent-0.5.0", True),
]


def _run_scenario(scenario, enable_review_workflow: bool) -> dict:
    """Run one scenario end to end.

    Per-turn output is kept lightweight (message/validation/summary fields)
    since `execution_trace` accumulates every turn; capturing the *full*
    state at every turn would repeatedly re-store the same growing prefix.
    The full canonicalized state (including the complete `execution_trace`)
    is captured once at the end of the scenario, which already contains an
    audit trail of every intermediate change.
    """
    runtime = TravelAgentRuntime(retry_limit=2, enable_review_workflow=enable_review_workflow)
    state = AgentState(thread_id=f"golden-{scenario.name}")
    turns = []
    for turn in scenario.turns:
        result = runtime.handle_user_message(state, turn)
        state = result.state
        turns.append(
            {
                "user_message": turn,
                "assistant_message": result.message,
                "validation_errors": result.validation_errors,
                "current_stage": state.current_stage,
                "blockers": state.blockers,
                "destination": state.destination,
                "days": state.days,
                "budget": state.budget,
                "preferences": state.preferences,
                "itinerary": (
                    state.itinerary.model_dump(mode="json")
                    if state.itinerary is not None
                    else None
                ),
            }
        )
    return {
        "turns": turns,
        "final_state": canonicalize(state.model_dump(mode="json")),
    }


def _run_matrix() -> dict:
    matrix: dict = {}
    for scenario in SCENARIOS:
        matrix[scenario.name] = {}
        for config_name, enable_review_workflow in CONFIGS:
            matrix[scenario.name][config_name] = _run_scenario(scenario, enable_review_workflow)
    return matrix


def test_golden_traces_match_fixture():
    if not FIXTURE_PATH.exists():
        pytest.fail(
            f"Missing golden fixture: {FIXTURE_PATH}. Regenerate with "
            "`python -m tests.characterization.test_travel_golden_traces` "
            "from the repo root."
        )
    expected = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    actual = _run_matrix()
    assert actual == expected


def test_golden_traces_are_reproducible_across_consecutive_runs():
    """Same requirement the fixture itself must satisfy: run twice, get the
    same canonicalized result, even though raw uuids/timestamps differ."""
    first = _run_matrix()
    second = _run_matrix()
    assert first == second


if __name__ == "__main__":
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(
        json.dumps(_run_matrix(), indent=2, ensure_ascii=False, sort_keys=False),
        encoding="utf-8",
    )
    print(f"Wrote {FIXTURE_PATH}")
