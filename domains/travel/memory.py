from __future__ import annotations

from typing import Any

from agent.contracts import RuntimeExecutionError
from runtime_service.memory import MemoryKind, MemoryWrite, RetrievedMemory

from .state import AgentState


class TravelMemoryPolicy:
    """Narrow allowlist for explicit, stable Travel preferences."""

    DOMAIN_ID = "travel"
    AVOID_RED_EYE_KEY = "flight.avoid_red_eye"
    HOTEL_NEAR_SUBWAY_KEY = "hotel.near_subway"
    TRAVEL_STYLE_KEY = "travel.style"
    SUPPORTED_KEYS = (
        AVOID_RED_EYE_KEY,
        HOTEL_NEAR_SUBWAY_KEY,
        TRAVEL_STYLE_KEY,
    )

    _STATE_KEYS = {
        AVOID_RED_EYE_KEY: "avoid_red_eye",
        HOTEL_NEAR_SUBWAY_KEY: "hotel_near_subway",
        TRAVEL_STYLE_KEY: "travel_style",
    }

    def apply(
        self,
        state: AgentState,
        memories: tuple[RetrievedMemory, ...],
    ) -> AgentState:
        preferences = dict(state.preferences)
        for memory in memories:
            if memory.kind != MemoryKind.PREFERENCE or memory.key not in self._STATE_KEYS:
                raise RuntimeExecutionError(
                    "invalid_memory_record",
                    "Retrieved memory is not valid for the Travel memory policy.",
                )
            state_key = self._STATE_KEYS[memory.key]
            self._validate_value(memory.key, memory.value)
            preferences[state_key] = memory.value
        return state.model_copy(update={"preferences": preferences}, deep=True)

    def restore_checkpoint_preferences(
        self,
        state: AgentState,
        original_preferences: dict[str, Any],
    ) -> AgentState:
        """Keep retrieved memory as an execution overlay, not stale thread state."""

        preferences = dict(state.preferences)
        for state_key in self._STATE_KEYS.values():
            if state_key in original_preferences:
                preferences[state_key] = original_preferences[state_key]
            else:
                preferences.pop(state_key, None)
        return state.model_copy(update={"preferences": preferences}, deep=True)

    def extract(self, user_message: str) -> tuple[MemoryWrite, ...]:
        text = user_message.lower()
        candidates: dict[str, Any] = {}
        if "red-eye" in text or "red eye" in text or "红眼" in user_message:
            candidates[self.AVOID_RED_EYE_KEY] = not any(
                phrase in text for phrase in ("allow red-eye", "allow red eye")
            )
        if "near subway" in text or "靠近地铁" in user_message:
            candidates[self.HOTEL_NEAR_SUBWAY_KEY] = True
        if "relaxed" in text or "轻松" in user_message:
            candidates[self.TRAVEL_STYLE_KEY] = "relaxed"
        return tuple(
            MemoryWrite(
                kind=MemoryKind.PREFERENCE,
                key=key,
                value=value,
                confidence=1.0,
            )
            for key, value in sorted(candidates.items())
        )

    @classmethod
    def _validate_value(cls, key: str, value: Any) -> None:
        if key in {cls.AVOID_RED_EYE_KEY, cls.HOTEL_NEAR_SUBWAY_KEY}:
            valid = isinstance(value, bool)
        elif key == cls.TRAVEL_STYLE_KEY:
            valid = isinstance(value, str) and value in {"balanced", "relaxed"}
        else:  # pragma: no cover - guarded by the allowlist above
            valid = False
        if not valid:
            raise RuntimeExecutionError(
                "invalid_memory_record",
                "Retrieved memory value failed Travel policy validation.",
            )
