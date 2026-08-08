from __future__ import annotations

import os
from typing import Any

from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    Planner,
    PlannerContext,
    RequestClarificationDecision,
)

from .state import AgentState


TRAVEL_PLANNER_INSTRUCTIONS = """You are the planner for a synthetic travel reference
domain. Choose exactly one registered function per turn. Use tool observations as facts,
ask for clarification when required constraints are missing or the selected option exceeds
the budget, and call finish only after search, ranking, and cost-summary evidence exists.
Never claim that the synthetic catalog represents live inventory or make a booking.
"""


class ScriptedTravelPlanner:
    """Stateless, observation-driven planner used by default and in CI."""

    def decide(self, context: PlannerContext):
        state = AgentState.model_validate(context.state)
        missing = [
            field_name
            for field_name, value in (
                ("destination", state.destination),
                ("days", state.days),
                ("budget", state.budget),
            )
            if not value
        ]
        if missing:
            return RequestClarificationDecision(
                question=(
                    "Please provide the missing travel details: "
                    + ", ".join(missing)
                    + "."
                ),
                reason="required_travel_constraints_missing",
            )

        by_tool = {observation.tool_name: observation for observation in context.observations}
        search = by_tool.get("search_trip_options")
        if search is None:
            return CallToolDecision(
                tool_name="search_trip_options",
                arguments={
                    "destination": state.destination,
                    "days": state.days,
                    "avoid_red_eye": bool(state.preferences.get("avoid_red_eye", False)),
                    "hotel_near_subway": bool(
                        state.preferences.get("hotel_near_subway", False)
                    ),
                    "travel_style": str(
                        state.preferences.get("travel_style", "balanced")
                    ),
                },
                reason="search_requires_structured_travel_constraints",
            )

        options = search.result.get("options")
        if not isinstance(options, list) or not options:
            return RequestClarificationDecision(
                question=(
                    "No synthetic reference options were found. Would you like to change "
                    "the destination or relax a preference?"
                ),
                reason="search_returned_no_options",
            )

        ranking = by_tool.get("rank_trip_options")
        if ranking is None:
            rankable_options = [self._rankable_option(option) for option in options]
            return CallToolDecision(
                tool_name="rank_trip_options",
                arguments={
                    "options": rankable_options,
                    "cost_weight": 0.7,
                    "duration_weight": 0.3,
                },
                reason="rank_arguments_derived_from_search_result",
            )

        route = by_tool.get("route_cost_summary")
        winner = self._ranking_winner(ranking.result)
        selected = self._option_named(options, winner)
        if route is None:
            return CallToolDecision(
                tool_name="route_cost_summary",
                arguments={
                    "transport_cost": self._required_int(selected, "transport_cost"),
                    "hotel_cost": self._required_int(selected, "hotel_cost"),
                    "activity_cost": self._required_int(selected, "activity_cost"),
                    "budget": state.budget,
                },
                reason="cost_arguments_derived_from_ranked_search_winner",
            )

        within_budget = route.result.get("within_budget")
        if not isinstance(within_budget, bool):
            raise InvalidPlannerDecisionError(
                "route_cost_summary observation is missing within_budget"
            )
        if not within_budget:
            total = route.result.get("total_cost")
            return RequestClarificationDecision(
                question=(
                    f"The selected synthetic option costs {total}, above the budget of "
                    f"{state.budget}. Would you like to raise the budget or relax a preference?"
                ),
                reason="cost_summary_exceeds_budget",
            )

        return FinishDecision(
            message=(
                f"A policy-validated synthetic plan for {state.destination} is ready."
            ),
            output={
                "destination": state.destination,
                "days": state.days,
                "selected_option_name": winner,
            },
            reason="search_ranking_and_cost_evidence_satisfy_constraints",
        )

    @staticmethod
    def _rankable_option(option: Any) -> dict[str, Any]:
        if not isinstance(option, dict):
            raise InvalidPlannerDecisionError("search option must be an object")
        name = option.get("name")
        cost = option.get("cost")
        duration = option.get("duration_hours")
        if not isinstance(name, str) or not name:
            raise InvalidPlannerDecisionError("search option is missing name")
        if not isinstance(cost, (int, float)) or not isinstance(duration, (int, float)):
            raise InvalidPlannerDecisionError(
                "search option is missing numeric cost or duration_hours"
            )
        return {
            "name": name,
            "cost": float(cost),
            "duration_hours": float(duration),
        }

    @staticmethod
    def _ranking_winner(result: dict[str, Any]) -> str:
        ranking = result.get("ranking")
        if not isinstance(ranking, list) or not ranking:
            raise InvalidPlannerDecisionError("ranking observation has no winner")
        first = ranking[0]
        if not isinstance(first, dict) or not isinstance(first.get("name"), str):
            raise InvalidPlannerDecisionError("ranking winner is invalid")
        return first["name"]

    @staticmethod
    def _option_named(options: list[Any], name: str) -> dict[str, Any]:
        for option in options:
            if isinstance(option, dict) and option.get("name") == name:
                return option
        raise InvalidPlannerDecisionError(
            "ranking winner does not reference a searched option"
        )

    @staticmethod
    def _required_int(option: dict[str, Any], field_name: str) -> int:
        value = option.get(field_name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise InvalidPlannerDecisionError(
                f"selected option is missing integer {field_name}"
            )
        return value


def build_travel_planner_from_environment() -> Planner:
    provider = os.getenv("RUNTIME_PLANNER_PROVIDER", "scripted").strip().lower()
    if provider == "scripted":
        return ScriptedTravelPlanner()
    if provider != "openai":
        raise ValueError(
            "RUNTIME_PLANNER_PROVIDER must be either 'scripted' or 'openai'"
        )

    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL")
    if api_key is None or not api_key.strip():
        raise ValueError("OPENAI_API_KEY is required for the OpenAI planner")
    if model is None or not model.strip():
        raise ValueError("OPENAI_MODEL is required for the OpenAI planner")

    from runtime_service.openai_planner import OpenAIResponsesPlanner

    from .dynamic_runtime import TravelFinishPayload

    return OpenAIResponsesPlanner(
        model=model,
        api_key=api_key,
        system_instructions=TRAVEL_PLANNER_INSTRUCTIONS,
        finish_output_schema=TravelFinishPayload.model_json_schema(),
    )
