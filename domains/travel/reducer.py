from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

from agent.contracts import TraceEvent
from .state import AgentState, StatePatch


def _merge_dict(original: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow recursive merge for nested state fields such as preferences."""
    merged = deepcopy(original)

    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value

    return merged


def apply_patch(state: AgentState, patch: StatePatch) -> AgentState:
    """
    Apply a StatePatch to AgentState and append a trace event.

    Locked fields are skipped. This is useful when certain constraints are fixed
    by the user or by a business rule.
    """
    update_payload: Dict[str, Any] = {}
    applied_updates: Dict[str, Any] = {}
    skipped_updates: Dict[str, Any] = {}

    for field_name, value in patch.updates.items():
        if field_name in patch.locked_fields:
            skipped_updates[field_name] = value
            continue

        current_value = getattr(state, field_name, None)

        if isinstance(current_value, dict) and isinstance(value, dict):
            update_payload[field_name] = _merge_dict(current_value, value)
        else:
            update_payload[field_name] = value

        applied_updates[field_name] = value

    trace = TraceEvent(
        event="state_patch_applied",
        reason=patch.reason,
        payload={
            "applied_updates": applied_updates,
            "skipped_updates": skipped_updates,
            "affected_fields": patch.affected_fields,
            "trigger_replan": patch.trigger_replan,
            "metadata": patch.metadata,
        },
    )

    update_payload["execution_trace"] = [*state.execution_trace, trace]
    return state.model_copy(update=update_payload, deep=True)


def append_trace(
    state: AgentState,
    event: str,
    reason: str,
    payload: Dict[str, Any] | None = None,
) -> AgentState:
    """Return a copied state with an additional trace event."""
    trace = TraceEvent(event=event, reason=reason, payload=payload or {})
    return state.model_copy(
        update={"execution_trace": [*state.execution_trace, trace]},
        deep=True,
    )
