"""
rl/reward.py — Reward function for intent policy training.

Given a user message, a predicted intent, and current agent state,
runs one step of TravelAgentRuntime and returns a scalar reward.
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domains.travel.state import AgentState
from domains.travel.runtime import TravelAgentRuntime

INTENT_LABELS = [
    "update_budget",
    "update_days",
    "avoid_redeye",
    "hotel_preference",
    "travel_style",
    "confirm_plan",
    "ask_clarification",
]

_runtime = TravelAgentRuntime(retry_limit=2)


def reward_for_intent(
    state: AgentState,
    user_message: str,
    predicted_intent: str,
) -> tuple[float, AgentState]:
    """
    Run one runtime step and return (reward, next_state).

    Reward shaping:
      +1.0  task moves to / stays at 'planned' with no blockers
      +0.3  partial_replan succeeds (needs_repair → planned)
      -0.5  blocker added to state
      -0.3  validation error
       0.0  ask_clarification (neutral — sometimes correct)
    """
    result = _runtime.handle_user_message(state, user_message)
    next_state = result.state

    reward = 0.0
    if next_state.current_stage == "planned" and not next_state.blockers:
        reward += 1.0
    if next_state.current_stage == "needs_repair":
        reward -= 0.3
    if next_state.blockers:
        reward -= 0.5
    if result.validation_errors:
        reward -= 0.3
    # Penalise unnecessary clarification on simple messages
    if predicted_intent == "ask_clarification" and next_state.current_stage == "planned":
        reward -= 0.4

    return round(reward, 3), next_state
