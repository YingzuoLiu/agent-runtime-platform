from __future__ import annotations
import json
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from domains.travel.state import AgentState  # noqa: E402
from domains.travel.runtime import TravelAgentRuntime  # noqa: E402
from domains.travel.validator import TravelValidator  # noqa: E402

@dataclass
class Scenario:
    name: str
    description: str
    turns: List[str]

SCENARIOS = [
    Scenario("happy_path_5turn", "Normal 5-turn planning, no conflicts.", [
        "I want a 5-day Tokyo trip under 7000 SGD.",
        "Make it a relaxed style.",
        "I prefer to avoid red-eye flights.",
        "I want a hotel near subway.",
        "Change the budget to 9000.",
    ]),
    Scenario("budget_conflict_recovery", "Budget set below cost, then recovered.", [
        "I want a 7-day Tokyo trip under 15000 SGD.",
        "Avoid red-eye. Hotel near subway.",
        "Actually cut budget to 4000.",
        "OK raise budget to 6000.",
        "Keep everything else the same.",
    ]),
    Scenario("conflicting_preferences_stall", "Contradictory signals, blocker propagation.", [
        "I want a 10-day Tokyo trip under 5000 SGD.",
        "Avoid red-eye. Hotel near subway.",
        "Keep budget at 5000.",
        "I insist the budget stays at 5000.",
        "Fine, raise budget to 8000.",
    ]),
    Scenario("memory_drift_8turn", "8-turn; early prefs must survive later updates.", [
        "Plan a 6-day Tokyo trip, budget 10000 SGD.",
        "I want a relaxed travel style.",
        "Avoid red-eye flights.",
        "Hotel near subway please.",
        "Change the trip to 8 days.",
        "Raise budget to 12000.",
        "Keep the relaxed style and no red-eye.",
        "Confirm everything looks good.",
    ]),
]

@dataclass
class ScenarioResult:
    scenario_name: str
    config_name: str
    task_completed: bool
    final_stage: str
    blocker_count: int
    replan_count: int
    recovery_success: bool
    validation_fail_count: int
    total_turns: int
    turn_outcomes: List[str] = field(default_factory=list)

def _count_trace_events(state, event_name, reason_substring=""):
    return sum(1 for e in state.execution_trace
               if e.event == event_name and (not reason_substring or reason_substring in e.reason))

def extract_metrics(scenario, config_name, final_state, turn_stages, turn_val_passed):
    entered_repair = "needs_repair" in turn_stages
    return ScenarioResult(
        scenario_name=scenario.name,
        config_name=config_name,
        task_completed=final_state.current_stage == "planned" and not final_state.blockers,
        final_stage=final_state.current_stage,
        blocker_count=turn_stages.count("blocked"),
        replan_count=_count_trace_events(final_state, "state_patch_applied", "partial_replan_completed"),
        recovery_success=entered_repair and final_state.current_stage == "planned",
        validation_fail_count=sum(1 for p in turn_val_passed if not p),
        total_turns=len(scenario.turns),
        turn_outcomes=turn_stages,
    )

@dataclass
class RuntimeConfig:
    name: str
    retry_limit: int
    disable_validator: bool = False

CONFIGS = [
    RuntimeConfig("full_runtime",           retry_limit=2, disable_validator=False),
    RuntimeConfig("no_validator",           retry_limit=2, disable_validator=True),
    RuntimeConfig("no_retry",               retry_limit=0, disable_validator=False),
    RuntimeConfig("no_validator_no_retry",  retry_limit=0, disable_validator=True),
]

def run_scenario(scenario, config):
    runtime = TravelAgentRuntime(retry_limit=config.retry_limit)
    if config.disable_validator:
        class _AlwaysPass(TravelValidator):
            def validate(self, state):
                from domains.travel.validator import ValidationResult
                return ValidationResult(passed=True, errors=[])
        runtime.validator = _AlwaysPass()
    state = AgentState(thread_id=f"{scenario.name}_{config.name}")
    turn_stages, turn_val_passed = [], []
    for turn in scenario.turns:
        result = runtime.handle_user_message(state, turn)
        state = result.state
        turn_stages.append(state.current_stage)
        turn_val_passed.append(len(result.validation_errors) == 0)
    return extract_metrics(scenario, config.name, state, turn_stages, turn_val_passed)

def run_all():
    return [run_scenario(s, c) for s in SCENARIOS for c in CONFIGS]

def print_summary(results):
    print("\n" + "=" * 72)
    print(f"{'Scenario':<35} {'Config':<25} {'Done':>4} {'Blk':>4} {'Rpl':>4} {'VFail':>6}")
    print("=" * 72)
    for r in results:
        print(f"{r.scenario_name:<35} {r.config_name:<25} {'V' if r.task_completed else 'X':>4} "
              f"{r.blocker_count:>4} {r.replan_count:>4} {r.validation_fail_count:>6}")
    print("=" * 72)
    print("\nCompletion rate by config:")
    for cfg in [c.name for c in CONFIGS]:
        cr = [r for r in results if r.config_name == cfg]
        rate = sum(r.task_completed for r in cr) / len(cr) * 100
        print(f"  {cfg:<30} {rate:.0f}%  ({sum(r.task_completed for r in cr)}/{len(cr)})")

def save_results(results, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8")
    print(f"\nSaved to: {path}")

if __name__ == "__main__":
    print("Running ablation eval harness...")
    results = run_all()
    print_summary(results)
    save_results(results, ROOT / "eval" / "results" / "ablation_latest.json")
