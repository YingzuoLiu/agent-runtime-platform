"""
rl/train.py — GRPO intent policy training using TRL.

Uses Qwen2.5-0.5B-Instruct as the base policy.
Reward signal comes from the eval harness (rl/reward.py).
"""
from __future__ import annotations
import sys
import json
from pathlib import Path

from trl import GRPOConfig, GRPOTrainer
from datasets import Dataset

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from domains.travel.state import AgentState  # noqa: E402
from rl.reward import reward_for_intent, INTENT_LABELS  # noqa: E402
from rl.episodes import LABELLED_SCENARIOS, build_prompt  # noqa: E402

# ── Dataset ───────────────────────────────────────────────────────────────────
print("Building dataset...")
records = []
for scenario in LABELLED_SCENARIOS:
    history = []
    for user_msg, true_label in scenario["turns"]:
        records.append({
            "prompt": build_prompt(history, user_msg),
            "user_msg": user_msg,
            "true_label": true_label,
        })
        history.append(user_msg)

print(f"Dataset size: {len(records)} episodes")

# TRL GRPOTrainer expects a datasets.Dataset
dataset = Dataset.from_list([{"prompt": r["prompt"]} for r in records])

# Keep a lookup so reward_fn can find user_msg by prompt
prompt_to_meta = {r["prompt"]: r for r in records}

# ── Reward function ───────────────────────────────────────────────────────────
def extract_label(text: str) -> str:
    text = text.lower().strip()
    for label in INTENT_LABELS:
        if label in text:
            return label
    return "ask_clarification"

def reward_fn(completions: list[str], prompts: list[str], **kwargs) -> list[float]:
    rewards = []
    for prompt, completion in zip(prompts, completions):
        meta = prompt_to_meta.get(prompt)
        if meta is None:
            rewards.append(0.0)
            continue
        predicted = extract_label(completion)
        r, _ = reward_for_intent(
            AgentState(thread_id="reward_eval"),
            meta["user_msg"],
            predicted,
        )
        # Bonus for exact label match
        if predicted == meta["true_label"]:
            r += 0.5
        rewards.append(float(r))
    return rewards

# ── Training ──────────────────────────────────────────────────────────────────
MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"

config = GRPOConfig(
    output_dir=str(ROOT / "rl" / "checkpoints" / "intent_policy"),
    num_train_epochs=3,
    per_device_train_batch_size=1,
    num_generations=4,
    max_prompt_length=512,
    max_completion_length=16,
    learning_rate=5e-6,
    logging_steps=5,
    save_strategy="no",
    report_to="none",
    bf16=False,
    fp16=True,
)

print(f"\nStarting GRPO training on {MODEL_ID}...")
print(f"Epochs: {config.num_train_epochs}  G: {config.num_generations}  LR: {config.learning_rate}\n")

trainer = GRPOTrainer(
    model=MODEL_ID,
    reward_funcs=reward_fn,
    args=config,
    train_dataset=dataset,
)

trainer.train()

# ── Save results ──────────────────────────────────────────────────────────────
trainer.save_model()

# Extract loss/reward history from trainer logs
history = trainer.state.log_history
results = {
    "loss":   [h["loss"]          for h in history if "loss"          in h],
    "reward": [h["reward"]        for h in history if "reward"        in h],
}

out = ROOT / "rl" / "training_results.json"
out.write_text(json.dumps(results, indent=2), encoding="utf-8")
print(f"\nResults saved to: {out}")
print("Training complete.")
