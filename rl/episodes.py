"""
rl/episodes.py — Generate (context, intent_label, reward) training episodes
from the eval scenarios by running them through the runtime and recording
what each turn's ground-truth intent should have been.
"""
from __future__ import annotations
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domains.travel.state import AgentState
from rl.reward import reward_for_intent, INTENT_LABELS

# Hand-labelled ground-truth intents for each scenario turn
LABELLED_SCENARIOS = [
    {
        "name": "happy_path_5turn",
        "turns": [
            ("I want a 5-day Tokyo trip under 7000 SGD.", "update_budget"),
            ("Make it a relaxed style.", "travel_style"),
            ("I prefer to avoid red-eye flights.", "avoid_redeye"),
            ("I want a hotel near subway.", "hotel_preference"),
            ("Change the budget to 9000.", "update_budget"),
        ],
    },
    {
        "name": "budget_conflict_recovery",
        "turns": [
            ("I want a 7-day Tokyo trip under 15000 SGD.", "update_budget"),
            ("Avoid red-eye. Hotel near subway.", "avoid_redeye"),
            ("Actually cut budget to 4000.", "update_budget"),
            ("OK raise budget to 6000.", "update_budget"),
            ("Keep everything else the same.", "confirm_plan"),
        ],
    },
    {
        "name": "conflicting_preferences_stall",
        "turns": [
            ("I want a 10-day Tokyo trip under 5000 SGD.", "update_budget"),
            ("Avoid red-eye. Hotel near subway.", "avoid_redeye"),
            ("Keep budget at 5000.", "update_budget"),
            ("I insist the budget stays at 5000.", "update_budget"),
            ("Fine, raise budget to 8000.", "update_budget"),
        ],
    },
    {
        "name": "memory_drift_8turn",
        "turns": [
            ("Plan a 6-day Tokyo trip, budget 10000 SGD.", "update_budget"),
            ("I want a relaxed travel style.", "travel_style"),
            ("Avoid red-eye flights.", "avoid_redeye"),
            ("Hotel near subway please.", "hotel_preference"),
            ("Change the trip to 8 days.", "update_days"),
            ("Raise budget to 12000.", "update_budget"),
            ("Keep the relaxed style and no red-eye.", "travel_style"),
            ("Confirm everything looks good.", "confirm_plan"),
        ],
    },
]


def build_prompt(history: list[str], user_message: str) -> str:
    """Format conversation history + current message as model input."""
    labels_str = ", ".join(INTENT_LABELS)
    history_str = "\n".join(f"Turn {i+1}: {t}" for i, t in enumerate(history))
    return (
        f"You are an intent classifier for a travel planning agent.\n"
        f"Conversation so far:\n{history_str}\n"
        f"New user message: {user_message}\n"
        f"Classify the intent. Choose exactly one from: {labels_str}\n"
        f"Intent:"
    )


def generate_episodes() -> list[dict]:
    """Run all scenarios and collect (prompt, label, reward) episodes."""
    episodes = []

    for scenario in LABELLED_SCENARIOS:
        state = AgentState(thread_id=scenario["name"])
        history = []

        for user_msg, true_label in scenario["turns"]:
            prompt = build_prompt(history, user_msg)
            reward, state = reward_for_intent(state, user_msg, true_label)
            episodes.append({
                "prompt": prompt,
                "label": true_label,
                "reward": reward,
                "scenario": scenario["name"],
            })
            history.append(user_msg)

    return episodes


if __name__ == "__main__":
    episodes = generate_episodes()
    out = Path(__file__).parent / "episodes.json"
    out.write_text(json.dumps(episodes, indent=2), encoding="utf-8")
    print(f"Generated {len(episodes)} episodes → {out}")
    print("\nReward distribution:")
    from collections import Counter
    dist = Counter(e["reward"] for e in episodes)
    for r, count in sorted(dist.items()):
        print(f"  reward={r:+.1f}  count={count}")
