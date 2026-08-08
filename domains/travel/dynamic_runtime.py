from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import RuntimeExecutionContext, RuntimeResponse
from runtime_service.dynamic_loop import (
    DynamicLoopOutcome,
    DynamicToolLoop,
    FinishEvaluation,
)
from runtime_service.planner import FinishDecision, ToolObservation

from .reducer import append_trace
from .runtime import TravelMessageInput
from .state import AgentState, TravelPlan
from .validator import TravelValidator


class TravelFinishPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    destination: str = Field(min_length=1, max_length=200)
    days: int = Field(gt=0, le=365)
    selected_option_name: str = Field(min_length=1, max_length=200)


class DynamicTravelRuntime:
    """Travel adapter for the domain-neutral dynamic tool loop."""

    _KNOWN_DESTINATIONS = {
        "tokyo": "Tokyo",
        "东京": "Tokyo",
        "kyoto": "Kyoto",
        "京都": "Kyoto",
        "singapore": "Singapore",
        "新加坡": "Singapore",
        "paris": "Paris",
        "巴黎": "Paris",
        "bali": "Bali",
        "巴厘岛": "Bali",
        "seoul": "Seoul",
        "首尔": "Seoul",
    }

    def __init__(self, loop: DynamicToolLoop) -> None:
        self.loop = loop
        self.validator = TravelValidator()

    def initial_state(self, thread_id: str) -> AgentState:
        return AgentState(thread_id=thread_id)

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        prepared = self._prepare_state(state, runtime_input.user_message)
        prepared = append_trace(
            prepared,
            event="dynamic_input_prepared",
            reason="travel_constraints_parsed",
            payload={
                "destination": prepared.destination,
                "days": prepared.days,
                "budget": prepared.budget,
                "preferences": prepared.preferences,
            },
        )

        result = self.loop.execute(
            runtime_input=runtime_input.model_dump(mode="json"),
            state=prepared.model_dump(mode="json"),
            context=context,
            finish_evaluator=lambda decision, observations: self._evaluate_finish(
                prepared,
                decision,
                observations,
            ),
        )
        tool_outputs = {
            observation.step_id: {
                "tool_name": observation.tool_name,
                "arguments": observation.arguments,
                "result": observation.result,
                "cached": observation.cached,
            }
            for observation in result.observations
        }

        if result.outcome == DynamicLoopOutcome.CLARIFICATION:
            updated = prepared.model_copy(
                update={
                    "current_stage": "needs_clarification",
                    "blockers": [result.message],
                    "tool_outputs": tool_outputs,
                    "itinerary": None,
                },
                deep=True,
            )
        else:
            itinerary_payload = result.output.get("itinerary")
            itinerary = (
                TravelPlan.model_validate(itinerary_payload)
                if isinstance(itinerary_payload, dict)
                else None
            )
            updated = prepared.model_copy(
                update={
                    "current_stage": (
                        "planned"
                        if result.outcome == DynamicLoopOutcome.FINISHED
                        else "blocked"
                    ),
                    "blockers": result.validation_errors,
                    "tool_outputs": tool_outputs,
                    "itinerary": itinerary,
                },
                deep=True,
            )

        updated = append_trace(
            updated,
            event="dynamic_loop_finished",
            reason=result.outcome.value,
            payload={
                "tool_call_count": len(result.observations),
                "validation_errors": result.validation_errors,
            },
        )
        return RuntimeResponse[AgentState](
            message=result.message,
            state=updated,
            validation_errors=result.validation_errors,
        )

    def _evaluate_finish(
        self,
        state: AgentState,
        decision: FinishDecision,
        observations: list[ToolObservation],
    ) -> FinishEvaluation:
        payload = TravelFinishPayload.model_validate(decision.output)
        search = self._observation(observations, "search_trip_options")
        ranking = self._observation(observations, "rank_trip_options")
        route = self._observation(observations, "route_cost_summary")

        errors: list[str] = []
        if payload.destination != state.destination:
            errors.append("Planner finish destination does not match requested destination")
        if payload.days != state.days:
            errors.append("Planner finish day count does not match requested day count")

        ranking_values = ranking.result.get("ranking")
        if not isinstance(ranking_values, list) or not ranking_values:
            raise ValueError("rank_trip_options result has no ranking")
        winner = ranking_values[0]
        if not isinstance(winner, dict) or winner.get("name") != payload.selected_option_name:
            errors.append("Planner finish selection does not match ranking evidence")

        options = search.result.get("options")
        if not isinstance(options, list):
            raise ValueError("search_trip_options result has no options")
        selected = next(
            (
                option
                for option in options
                if isinstance(option, dict)
                and option.get("name") == payload.selected_option_name
            ),
            None,
        )
        if selected is None:
            errors.append("Planner finish selection is absent from search evidence")
            selected = {}

        expected_total = sum(
            self._integer_cost(selected, field_name)
            for field_name in ("transport_cost", "hotel_cost", "activity_cost")
        )
        if route.result.get("total_cost") != expected_total:
            errors.append("Cost summary does not match searched option components")
        if route.result.get("budget") != state.budget:
            errors.append("Cost summary budget does not match requested budget")

        itinerary = TravelPlan(
            destination=state.destination or payload.destination,
            days=state.days or payload.days,
            flight_type=str(selected.get("flight_type", "unknown")),
            hotel_tier=str(selected.get("hotel_tier", "unknown")),
            poi_style=str(selected.get("poi_style", "unknown")),
            total_cost=expected_total,
            notes=[
                "Built from synthetic reference tool evidence.",
                f"Selected option: {payload.selected_option_name}.",
            ],
        )
        candidate = state.model_copy(
            update={"itinerary": itinerary, "blockers": [], "current_stage": "planned"},
            deep=True,
        )
        errors.extend(self.validator.validate(candidate).errors)

        output = {
            "selected_option_name": payload.selected_option_name,
            "itinerary": itinerary.model_dump(mode="json"),
            "evidence_source": "synthetic_reference_catalog",
        }
        if errors:
            return FinishEvaluation(
                outcome=DynamicLoopOutcome.BLOCKED,
                message="The proposed plan was blocked by deterministic travel validation.",
                output=output,
                validation_errors=errors,
            )
        return FinishEvaluation(
            outcome=DynamicLoopOutcome.FINISHED,
            message=(
                f"Planned {itinerary.days}-day synthetic reference trip to "
                f"{itinerary.destination} within budget."
            ),
            output=output,
        )

    def _prepare_state(self, state: AgentState, user_message: str) -> AgentState:
        text = user_message.lower()
        updates: dict[str, Any] = {
            "itinerary": None,
            "tool_outputs": {},
            "blockers": [],
            "current_stage": "planning",
        }
        for token, destination in self._KNOWN_DESTINATIONS.items():
            if token in text or token in user_message:
                updates["destination"] = destination
                break

        destination_match = re.search(
            r"(?:travel|trip|go|visit)\s+(?:to\s+)?([A-Za-z][A-Za-z .'-]{1,40}?)"
            r"(?=\s+(?:for|under|with|within|on)|[,.]|$)",
            user_message,
            flags=re.IGNORECASE,
        )
        if "destination" not in updates and destination_match:
            updates["destination"] = destination_match.group(1).strip().title()

        days_match = re.search(r"(\d{1,3})\s*[- ]?day", text)
        if days_match is None:
            days_match = re.search(r"(?:玩\s*)?(\d{1,3})\s*天", user_message)
        if days_match:
            updates["days"] = int(days_match.group(1))

        budget_patterns = (
            r"(?:budget|under|within|预算|控制在|改成|调整到|提高到)\D{0,12}(\d{2,7})",
            r"(\d{2,7})\s*(?:sgd|rmb|usd|新币|人民币|预算)",
        )
        for pattern in budget_patterns:
            budget_match = re.search(pattern, user_message, flags=re.IGNORECASE)
            if budget_match:
                updates["budget"] = int(budget_match.group(1))
                break

        preferences = dict(state.preferences)
        if "red-eye" in text or "red eye" in text or "红眼" in user_message:
            preferences["avoid_red_eye"] = not any(
                phrase in text for phrase in ("allow red-eye", "allow red eye")
            )
        if "near subway" in text or "靠近地铁" in user_message:
            preferences["hotel_near_subway"] = True
        if "relaxed" in text or "轻松" in user_message:
            preferences["travel_style"] = "relaxed"
        if preferences:
            updates["preferences"] = preferences
        return state.model_copy(update=updates, deep=True)

    @staticmethod
    def _observation(
        observations: list[ToolObservation],
        tool_name: str,
    ) -> ToolObservation:
        for observation in observations:
            if observation.tool_name == tool_name:
                return observation
        raise ValueError(f"FINISH requires {tool_name} evidence")

    @staticmethod
    def _integer_cost(option: dict[str, Any], field_name: str) -> int:
        value = option.get(field_name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"searched option is missing integer {field_name}")
        return value
