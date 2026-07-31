"""
rl/tabular_reinforce.py

Tabular REINFORCE over intent labels.
No GPU, no LLM training — just a probability table over intents
updated by the reward signal from the eval harness.

This demonstrates the core RL loop:
  observe state → sample action → get reward → update policy
"""
from __future__ import annotations
import sys
import json
import random
from pathlib import Path
from collections import defaultdict

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from domains.travel.state import AgentState  # noqa: E402
from rl.reward import reward_for_intent, INTENT_LABELS  # noqa: E402
from rl.episodes import LABELLED_SCENARIOS, build_prompt  # noqa: E402

N_LABELS  = len(INTENT_LABELS)
LR        = 0.1
EPOCHS    = 20
SEED      = 42
random.seed(SEED)
np.random.seed(SEED)

# ── Policy: one softmax distribution per unique prompt ─────────────────────
logits: dict[str, np.ndarray] = defaultdict(lambda: np.zeros(N_LABELS))

def sample_action(prompt: str) -> tuple[str, int]:
    probs = np.exp(logits[prompt])
    probs /= probs.sum()
    idx   = np.random.choice(N_LABELS, p=probs)
    return INTENT_LABELS[idx], idx

def update(prompt: str, idx: int, reward: float):
    probs = np.exp(logits[prompt])
    probs /= probs.sum()
    grad  = -reward * (1 - probs[idx])          # REINFORCE gradient
    logits[prompt][idx] += LR * grad

# ── Episode pool ──────────────────────────────────────────────────────────
episodes = []
for scenario in LABELLED_SCENARIOS:
    history = []
    for user_msg, true_label in scenario["turns"]:
        episodes.append({
            "prompt":      build_prompt(history, user_msg),
            "user_msg":    user_msg,
            "true_label":  true_label,
        })
        history.append(user_msg)

# ── Training loop ─────────────────────────────────────────────────────────
print(f"Tabular REINFORCE | {len(episodes)} episodes | {EPOCHS} epochs\n")
print(f"{'Epoch':>5}  {'Avg Reward':>10}  {'Accuracy':>9}  {'Correct/Total':>13}")
print("-" * 46)

history_log = []

for epoch in range(EPOCHS):
    random.shuffle(episodes)
    rewards, correct = [], 0

    for ep in episodes:
        predicted, idx = sample_action(ep["prompt"])

        r, _ = reward_for_intent(
            AgentState(thread_id="rl_train"),
            ep["user_msg"],
            predicted,
        )
        if predicted == ep["true_label"]:
            r += 0.5          # bonus for exact match

        update(ep["prompt"], idx, r)
        rewards.append(r)
        correct += int(predicted == ep["true_label"])

    avg_r = np.mean(rewards)
    acc   = correct / len(episodes) * 100
    history_log.append({"epoch": epoch+1, "avg_reward": round(float(avg_r),3), "accuracy": round(acc,1)})
    print(f"{epoch+1:>5}  {avg_r:>10.3f}  {acc:>8.1f}%  {correct:>6}/{len(episodes)}")

# ── Save ──────────────────────────────────────────────────────────────────
out = ROOT / "rl" / "reinforce_results.json"
out.write_text(json.dumps(history_log, indent=2), encoding="utf-8")
print(f"\nSaved to: {out}")

# Summary
first, last = history_log[0], history_log[-1]
print(f"\nEpoch 1  → reward={first['avg_reward']:+.3f}  acc={first['accuracy']:.1f}%")
print(f"Epoch {EPOCHS} → reward={last['avg_reward']:+.3f}  acc={last['accuracy']:.1f}%")
print(f"Reward improvement: {last['avg_reward']-first['avg_reward']:+.3f}")
print(f"Accuracy improvement: {last['accuracy']-first['accuracy']:+.1f}pp")
