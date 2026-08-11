from __future__ import annotations

import json
from hashlib import sha256
from typing import Any

from agent.contracts import RuntimeExecutionError

from .workflow_store import ToolCallRecord


def canonical_json(payload: dict[str, Any]) -> str:
    """Encode a payload so byte equality implies durable identity equality."""

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def stable_hash(payload: dict[str, Any]) -> str:
    """Hash a payload through the one canonical encoding used for identity."""

    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def decode_tool_result(step: ToolCallRecord) -> dict[str, Any]:
    """Decode a persisted tool result, refusing anything but a JSON object."""

    if step.result_json is None:
        raise RuntimeExecutionError(
            "tool_execution_failed",
            f"Completed tool step {step.step_id} is missing its result.",
        )
    try:
        value = json.loads(step.result_json)
    except json.JSONDecodeError as exc:
        raise RuntimeExecutionError(
            "tool_execution_failed",
            f"Tool step {step.step_id} persisted invalid JSON.",
        ) from exc
    if not isinstance(value, dict):
        raise RuntimeExecutionError(
            "tool_execution_failed",
            f"Tool step {step.step_id} did not persist an object result.",
        )
    return value
