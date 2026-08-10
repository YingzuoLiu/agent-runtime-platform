from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, ValidationError

from agent.contracts import RuntimeExecutionContext, RuntimeResponse
from runtime_service.dynamic_loop import FinishEvaluation
from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    Planner,
    PlannerContext,
    ToolObservation,
)

from .dynamic_runtime import DynamicTravelRuntime, TravelFinishPayload
from .runtime import TravelMessageInput
from .state import AgentState, TravelPlan
from .tools.models import CreateTripHoldInput
from .validator import TravelValidator


class TravelExternalActionInput(TravelMessageInput):
    """Opt-in Travel 1.2 input; older Travel input schemas remain unchanged."""

    requested_action: Literal["plan_only", "create_hold"] = Field(
        default="plan_only"
    )


class DurableActionTravelPlanner:
    """Add one explicit Travel action after the base Planner has enough evidence."""

    ACTION_TOOL_NAME = "create_trip_hold"

    def __init__(self, base_planner: Planner) -> None:
        self.base_planner = base_planner

    def decide(self, context: PlannerContext):
        # The base/model Planner is plan-only: it never sees the write tool.
        # This wrapper alone may insert the action after deterministic FINISH
        # validation, so even a compromised or drifting provider cannot select
        # an external write early or during a plan-only run.
        base_context = context.model_copy(
            update={
                "tools": [
                    descriptor
                    for descriptor in context.tools
                    if descriptor.name != self.ACTION_TOOL_NAME
                ]
            },
            deep=True,
        )
        base_decision = self.base_planner.decide(base_context)
        if (
            isinstance(base_decision, CallToolDecision)
            and base_decision.tool_name == self.ACTION_TOOL_NAME
        ):
            raise InvalidPlannerDecisionError(
                "The base Travel Planner cannot select external actions"
            )
        requested_action = context.runtime_input.get("requested_action", "plan_only")
        if requested_action == "plan_only":
            return base_decision
        if requested_action != "create_hold":
            raise InvalidPlannerDecisionError("Unsupported Travel external action")
        if not isinstance(base_decision, FinishDecision):
            return base_decision

        by_tool = {observation.tool_name: observation for observation in context.observations}
        existing_hold = by_tool.get(self.ACTION_TOOL_NAME)
        expected_arguments = self._hold_arguments(
            base_decision,
            by_tool,
            AgentState.model_validate(context.state),
        )
        if existing_hold is None:
            return CallToolDecision(
                tool_name=self.ACTION_TOOL_NAME,
                arguments=expected_arguments,
                reason="explicit_hold_requested_after_validated_plan_evidence",
            )

        self._validate_hold_observation(existing_hold, expected_arguments)
        return base_decision

    @staticmethod
    def _hold_arguments(
        base_decision: FinishDecision,
        by_tool: dict[str, ToolObservation],
        state: AgentState,
    ) -> dict[str, Any]:
        try:
            finish_payload = TravelFinishPayload.model_validate(base_decision.output)
        except ValidationError:
            raise InvalidPlannerDecisionError(
                "Trip hold base finish payload is invalid"
            ) from None
        if finish_payload.destination != state.destination:
            raise InvalidPlannerDecisionError(
                "Trip hold destination does not match requested destination"
            )
        if finish_payload.days != state.days:
            raise InvalidPlannerDecisionError(
                "Trip hold day count does not match requested day count"
            )

        try:
            search = by_tool["search_trip_options"]
            ranking = by_tool["rank_trip_options"]
            route = by_tool["route_cost_summary"]
        except KeyError as exc:
            raise InvalidPlannerDecisionError(
                "Trip hold requires search, ranking, and cost evidence"
            ) from exc

        ranking_values = ranking.result.get("ranking")
        if not isinstance(ranking_values, list) or not ranking_values:
            raise InvalidPlannerDecisionError("Trip hold ranking evidence has no winner")
        winner = ranking_values[0]
        if not isinstance(winner, dict) or not isinstance(winner.get("name"), str):
            raise InvalidPlannerDecisionError("Trip hold ranking winner is invalid")
        selected_option_name = winner["name"]
        if finish_payload.selected_option_name != selected_option_name:
            raise InvalidPlannerDecisionError(
                "Trip hold selection does not match the base Planner finish"
            )

        destination = search.result.get("destination")
        if not isinstance(destination, str) or not destination:
            raise InvalidPlannerDecisionError("Trip hold search destination is invalid")
        if destination != state.destination:
            raise InvalidPlannerDecisionError(
                "Trip hold search destination does not match requested destination"
            )
        options = search.result.get("options")
        if not isinstance(options, list):
            raise InvalidPlannerDecisionError("Trip hold search evidence has no options")
        selected = next(
            (
                option
                for option in options
                if isinstance(option, dict)
                and option.get("name") == selected_option_name
            ),
            None,
        )
        if selected is None:
            raise InvalidPlannerDecisionError(
                "Trip hold selection is absent from search evidence"
            )

        quoted_total = route.result.get("total_cost")
        if not isinstance(quoted_total, int) or isinstance(quoted_total, bool):
            raise InvalidPlannerDecisionError("Trip hold quoted total is invalid")
        if route.result.get("within_budget") is not True:
            raise InvalidPlannerDecisionError(
                "Trip hold cannot be created for an over-budget option"
            )
        selected_total = sum(
            DurableActionTravelPlanner._required_cost(selected, field_name)
            for field_name in ("transport_cost", "hotel_cost", "activity_cost")
        )
        if quoted_total != selected_total:
            raise InvalidPlannerDecisionError(
                "Trip hold quoted total does not match search evidence"
            )
        if route.result.get("budget") != state.budget:
            raise InvalidPlannerDecisionError(
                "Trip hold cost evidence does not match requested budget"
            )

        itinerary = TravelPlan(
            destination=state.destination or finish_payload.destination,
            days=state.days or finish_payload.days,
            flight_type=str(selected.get("flight_type", "unknown")),
            hotel_tier=str(selected.get("hotel_tier", "unknown")),
            poi_style=str(selected.get("poi_style", "unknown")),
            total_cost=selected_total,
            notes=[],
        )
        candidate = state.model_copy(
            update={
                "itinerary": itinerary,
                "blockers": [],
                "current_stage": "planned",
            },
            deep=True,
        )
        validation_errors = TravelValidator().validate(candidate).errors
        if validation_errors:
            raise InvalidPlannerDecisionError(
                "Trip hold candidate failed deterministic Travel validation"
            )

        return CreateTripHoldInput(
            destination=destination,
            selected_option_name=selected_option_name,
            quoted_total=quoted_total,
        ).model_dump(mode="json")

    @staticmethod
    def _required_cost(option: dict[str, Any], field_name: str) -> int:
        value = option.get(field_name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise InvalidPlannerDecisionError(
                f"Trip hold option has invalid {field_name}"
            )
        return value

    @staticmethod
    def _validate_hold_observation(
        observation: ToolObservation,
        expected_arguments: dict[str, Any],
    ) -> None:
        try:
            normalized = CreateTripHoldInput.model_validate(
                observation.arguments
            ).model_dump(mode="json")
        except ValidationError:
            raise InvalidPlannerDecisionError(
                "Trip hold observation arguments are invalid"
            ) from None
        if normalized != expected_arguments:
            raise InvalidPlannerDecisionError(
                "Trip hold observation does not match the selected option"
            )
        provider_reference = observation.result.get("provider_reference")
        if observation.result.get("status") != "held" or not isinstance(
            provider_reference, str
        ) or not provider_reference:
            raise InvalidPlannerDecisionError("Trip hold observation is not successful")
        for key, value in expected_arguments.items():
            if observation.result.get(key) != value:
                raise InvalidPlannerDecisionError(
                    "Trip hold result does not match its normalized arguments"
                )


class DurableActionTravelRuntime(DynamicTravelRuntime):
    """Travel 1.2 adapter that validates the explicitly requested external action."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._requested_action: Literal["plan_only", "create_hold"] = "plan_only"

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelExternalActionInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        self._requested_action = runtime_input.requested_action
        try:
            return super().execute(state, runtime_input, context)
        finally:
            self._requested_action = "plan_only"

    def _evaluate_finish(
        self,
        state: AgentState,
        decision: FinishDecision,
        observations: list[ToolObservation],
    ) -> FinishEvaluation:
        evaluation = super()._evaluate_finish(state, decision, observations)
        if self._requested_action == "plan_only":
            return evaluation

        hold = self._observation(observations, "create_trip_hold")
        expected_arguments = DurableActionTravelPlanner._hold_arguments(
            decision,
            {observation.tool_name: observation for observation in observations},
            state,
        )
        DurableActionTravelPlanner._validate_hold_observation(
            hold,
            expected_arguments,
        )
        provider_reference = str(hold.result["provider_reference"])
        return evaluation.model_copy(
            update={
                "output": {
                    **evaluation.output,
                    "external_action": {
                        "tool_name": "create_trip_hold",
                        "status": "held",
                        "provider_reference": provider_reference,
                    },
                }
            },
            deep=True,
        )
