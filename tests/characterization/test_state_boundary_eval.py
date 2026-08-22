from __future__ import annotations

import json
from pathlib import Path

import pytest

from eval.state_boundary import build_report


REPORT_PATH = (
    Path(__file__).resolve().parents[2] / "eval" / "results" / "state_boundary_latest.json"
)


@pytest.fixture(scope="module")
def report(tmp_path_factory: pytest.TempPathFactory) -> dict:
    return build_report(tmp_path_factory.mktemp("state-boundary-eval"))


def test_state_boundary_report_is_reproducible(report: dict) -> None:
    expected = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    assert report == expected


def test_memory_supersession_and_sealed_snapshot_are_characterized(report: dict) -> None:
    supersession = report["memory_supersession"]
    assert [item["version"] for item in supersession["version_history"]] == [1, 2]
    assert [item["status"] for item in supersession["version_history"]] == [
        "superseded",
        "active",
    ]
    assert supersession["new_run_snapshot"] == [
        {"key": "flight.avoid_red_eye", "version": 2, "value": False}
    ]
    assert supersession["new_run_behavior"]["checkpoint_preference_keys"] == []

    sealed = report["sealed_snapshot"]
    assert sealed["sealed_view_unchanged"] is True
    assert sealed["sealed_run_after_update"][0]["version"] == 1
    assert sealed["new_run_after_update"][0]["version"] == 2


def test_budget_and_negation_use_current_production_parsers(report: dict) -> None:
    replacement = report["budget_replacement"]
    assert replacement["checkpoint"]["before"]["budget"] == 9000
    assert replacement["checkpoint"]["after"]["budget"] == 12000
    assert replacement["memory_changed"] is False

    negation = report["negation_safety"]
    assert all(
        item["parser_output"] == {} and item["memory_mutation_events"] == 0
        for item in negation["ambiguous_mentions"]
    )
    assert {item["key"]: item["value"] for item in negation["persisted_active_memories"]} == {
        "flight.avoid_red_eye": False,
        "hotel.near_subway": False,
        "travel.style": "balanced",
    }


def test_confirm_plan_reads_current_plan_evidence_from_checkpoint(report: dict) -> None:
    confirmation = report["confirm_plan_dependency"]
    assert confirmation["confirmation_turn"]["checkpoint_loaded"] == {
        "source": "thread_store",
        "revision": 1,
    }
    assert confirmation["confirmation_turn"]["intent_observed"] is True
    assert confirmation["confirmation_turn"]["review_evidence"] == {
        "candidate_plan_present": True,
        "budget_limit": 9000.0,
        "cost_ledger_status": "complete",
        "cost_ledger_total": 7300.0,
        "cost_source_ids": ["tool_outputs.cost_breakdown"],
        "evidence_issues": [],
    }
    assert confirmation["confirmation_turn"]["validation_errors"] == []


def test_checkpoint_growth_is_observational_not_threshold_gated(report: dict) -> None:
    growth = report["checkpoint_growth"]
    turns = growth["turns"]
    assert growth["summary"]["strictly_monotonic_total"] is True
    assert growth["summary"]["strictly_monotonic_execution_trace"] is True
    assert all(
        item["total_checkpoint_bytes"]
        == item["execution_trace_value_bytes"]
        + item["tool_outputs_value_bytes"]
        + item["other_state_and_json_structure_bytes"]
        for item in turns
    )
    assert {item["tool_outputs_value_bytes"] for item in turns} == {92}
    assert "maximum_bytes" not in growth


def test_retry_count_is_observed_across_independent_runs(report: dict) -> None:
    turns = report["retry_count_scope"]["turns"]
    assert [item["run_attempt"] for item in turns] == [1, 1, 1, 1, 1]
    assert [item["retry_count"] for item in turns] == [1, 1, 2, 2, 2]
    assert [item["current_stage"] for item in turns] == [
        "needs_repair",
        "planned",
        "needs_repair",
        "planned",
        "blocked",
    ]
