from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent.contracts import RuntimeExecutionAuthority, RuntimeExecutionContext
from domains.travel.dynamic_runtime import DynamicTravelRuntime
from domains.travel.external_action_runtime import (
    DurableActionTravelPlanner,
    DurableActionTravelRuntime,
    TravelExternalActionInput,
)
from domains.travel.planner import ScriptedTravelPlanner
from domains.travel.preferences import parse_explicit_travel_preferences
from domains.travel.runtime import TravelMessageInput
from domains.travel.state import AgentState
from domains.travel.tools import (
    SQLiteTripHoldProvider,
    build_travel_external_action_tool_registry,
    build_travel_tool_registry,
)
from domains.travel.tools.handlers import (
    rank_trip_options,
    route_cost_summary,
    search_trip_options,
)
from runtime_service.dynamic_loop import DynamicLoopOutcome
from runtime_service.external_actions import (
    DefinitiveExternalActionError,
    ExternalActionRequest,
)
from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    PlannerContext,
    ToolObservation,
)
from runtime_service.sandbox import ToolEffect, ToolRetryMode


def _state() -> AgentState:
    return AgentState(
        thread_id="travel-action-thread",
        destination="Tokyo",
        days=5,
        budget=9000,
        preferences={"avoid_red_eye": True},
    )


def _planner_context(
    requested_action: str,
    observations: list[ToolObservation],
) -> PlannerContext:
    return PlannerContext(
        run_id="run-travel-action",
        thread_id="travel-action-thread",
        runtime_input={
            "user_message": "Plan a five-day Tokyo trip and hold the selected option.",
            "requested_action": requested_action,
        },
        state=_state().model_dump(mode="json"),
        tools=build_travel_external_action_tool_registry().list_tools(),
        observations=observations,
        tool_call_count=len(observations),
        max_tool_calls=4,
    )


def _travel_observations() -> tuple[list[ToolObservation], FinishDecision]:
    base = ScriptedTravelPlanner()
    observations: list[ToolObservation] = []

    search_decision = base.decide(_planner_context("plan_only", observations))
    assert isinstance(search_decision, CallToolDecision)
    search_result = search_trip_options(search_decision.arguments)
    observations.append(
        ToolObservation(
            step_id="call-0001",
            tool_name=search_decision.tool_name,
            arguments=search_decision.arguments,
            result=search_result,
        )
    )

    rank_decision = base.decide(_planner_context("plan_only", observations))
    assert isinstance(rank_decision, CallToolDecision)
    rank_result = rank_trip_options(rank_decision.arguments)
    observations.append(
        ToolObservation(
            step_id="call-0002",
            tool_name=rank_decision.tool_name,
            arguments=rank_decision.arguments,
            result=rank_result,
        )
    )

    cost_decision = base.decide(_planner_context("plan_only", observations))
    assert isinstance(cost_decision, CallToolDecision)
    cost_result = route_cost_summary(cost_decision.arguments)
    observations.append(
        ToolObservation(
            step_id="call-0003",
            tool_name=cost_decision.tool_name,
            arguments=cost_decision.arguments,
            result=cost_result,
        )
    )

    finish = base.decide(_planner_context("plan_only", observations))
    assert isinstance(finish, FinishDecision)
    return observations, finish


def _hold_observation(decision: CallToolDecision) -> ToolObservation:
    provider_reference = "hold_test_reference"
    return ToolObservation(
        step_id="call-0004",
        tool_name=decision.tool_name,
        arguments=decision.arguments,
        result={
            "status": "held",
            "provider_reference": provider_reference,
            **decision.arguments,
        },
    )


def _provider_request(
    *,
    idempotency_key: str = "travel-action-idempotency-key",
    quoted_total: int = 7800,
) -> ExternalActionRequest:
    return ExternalActionRequest(
        action_id="action-travel-hold",
        run_id="run-travel-action",
        step_id="call-0004",
        tenant_id="tenant-travel",
        subject_id="subject-travel",
        workflow_type="dynamic-tool-loop:travel-agent:1.2.0",
        tool_name="create_trip_hold",
        arguments={
            "destination": "Tokyo",
            "selected_option_name": "Tokyo value itinerary",
            "quoted_total": quoted_total,
            "hold_minutes": 15,
        },
        idempotency_key=idempotency_key,
    )


def test_external_action_input_is_version_isolated_and_defaults_to_plan_only():
    parsed = TravelExternalActionInput(user_message="Plan Tokyo")

    assert parsed.requested_action == "plan_only"
    with pytest.raises(ValidationError):
        TravelMessageInput.model_validate(
            {
                "user_message": "Plan Tokyo",
                "requested_action": "create_hold",
            }
        )


def test_travel_external_action_registry_does_not_change_legacy_tool_set():
    legacy = build_travel_tool_registry()
    external_action = build_travel_external_action_tool_registry()

    assert {descriptor.name for descriptor in legacy.list_tools()} == {
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
    }
    assert {descriptor.name for descriptor in external_action.list_tools()} == {
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
        "create_trip_hold",
    }
    hold = external_action.resolve("create_trip_hold")
    assert hold is not None
    assert hold.effect == ToolEffect.EXTERNAL_WRITE
    assert hold.retry_mode == ToolRetryMode.PROVIDER_IDEMPOTENT
    assert hold.provider_name == "travel-trip-hold"
    assert hold.handler_entrypoint is None
    assert hold.runtime_input_gate is not None
    assert hold.runtime_input_gate({"requested_action": "create_hold"}) is True
    assert hold.runtime_input_gate({"requested_action": "plan_only"}) is False
    for tool_name in (
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
    ):
        spec = legacy.resolve(tool_name)
        assert spec is not None
        assert spec.effect == ToolEffect.READ_ONLY


def test_durable_action_planner_preserves_plan_only_finish_and_inserts_explicit_hold():
    observations, base_finish = _travel_observations()
    planner = DurableActionTravelPlanner(ScriptedTravelPlanner())

    plan_only = planner.decide(_planner_context("plan_only", observations))
    create_hold = planner.decide(_planner_context("create_hold", observations))

    assert plan_only == base_finish
    assert isinstance(create_hold, CallToolDecision)
    assert create_hold.tool_name == "create_trip_hold"
    assert create_hold.arguments == {
        "destination": "Tokyo",
        "selected_option_name": base_finish.output["selected_option_name"],
        "quoted_total": observations[-1].result["total_cost"],
        "hold_minutes": 15,
    }

    after_hold = planner.decide(
        _planner_context(
            "create_hold",
            [*observations, _hold_observation(create_hold)],
        )
    )
    assert after_hold == base_finish


@pytest.mark.parametrize("requested_action", ["plan_only", "create_hold"])
def test_base_planner_never_sees_or_selects_the_external_write_tool(
    requested_action: str,
):
    class MaliciousBasePlanner:
        def __init__(self) -> None:
            self.context: PlannerContext | None = None

        def decide(self, context: PlannerContext):
            self.context = context
            return CallToolDecision(
                tool_name="create_trip_hold",
                arguments={
                    "destination": "Tokyo",
                    "selected_option_name": "unvalidated",
                    "quoted_total": 1,
                },
                reason="attempt an early external write",
            )

    base = MaliciousBasePlanner()
    planner = DurableActionTravelPlanner(base)

    with pytest.raises(InvalidPlannerDecisionError, match="cannot select"):
        planner.decide(_planner_context(requested_action, []))

    assert base.context is not None
    assert "create_trip_hold" not in {
        descriptor.name for descriptor in base.context.tools
    }


def test_durable_action_planner_rejects_invalid_finish_before_action_decision():
    observations, base_finish = _travel_observations()

    class InvalidFinishPlanner:
        def decide(self, _context):
            return base_finish.model_copy(
                update={
                    "output": {
                        **base_finish.output,
                        "destination": "Paris",
                    }
                }
            )

    planner = DurableActionTravelPlanner(InvalidFinishPlanner())

    with pytest.raises(InvalidPlannerDecisionError, match="destination"):
        planner.decide(_planner_context("create_hold", observations))


def test_durable_action_planner_runs_travel_validator_before_action_decision():
    observations, _ = _travel_observations()
    tampered_search = observations[0].model_copy(deep=True)
    winner = observations[1].result["ranking"][0]["name"]
    for option in tampered_search.result["options"]:
        if option["name"] == winner:
            option["flight_type"] = "red_eye"
    observations[0] = tampered_search
    planner = DurableActionTravelPlanner(ScriptedTravelPlanner())

    with pytest.raises(InvalidPlannerDecisionError, match="Travel validation"):
        planner.decide(_planner_context("create_hold", observations))


def test_durable_action_runtime_keeps_plan_only_output_and_adds_hold_evidence():
    observations, finish = _travel_observations()
    base_runtime = DynamicTravelRuntime(
        None,  # type: ignore[arg-type]
        preference_parser=parse_explicit_travel_preferences,
    )
    runtime = DurableActionTravelRuntime(
        None,  # type: ignore[arg-type]
        preference_parser=parse_explicit_travel_preferences,
    )

    base_evaluation = base_runtime._evaluate_finish(_state(), finish, observations)
    plan_only = runtime._evaluate_finish(_state(), finish, observations)
    assert plan_only == base_evaluation
    assert plan_only.outcome == DynamicLoopOutcome.FINISHED

    planner = DurableActionTravelPlanner(ScriptedTravelPlanner())
    hold_decision = planner.decide(_planner_context("create_hold", observations))
    assert isinstance(hold_decision, CallToolDecision)
    hold_observation = _hold_observation(hold_decision)
    runtime._requested_action = "create_hold"
    with_hold = runtime._evaluate_finish(
        _state(),
        finish,
        [*observations, hold_observation],
    )

    assert with_hold.output["itinerary"] == base_evaluation.output["itinerary"]
    assert with_hold.output["external_action"] == {
        "tool_name": "create_trip_hold",
        "status": "held",
        "provider_reference": "hold_test_reference",
    }


def test_sqlite_trip_hold_provider_deduplicates_same_key_and_payload(tmp_path):
    database_path = tmp_path / "provider.db"
    provider = SQLiteTripHoldProvider(database_path)
    request = _provider_request()

    first = provider.execute(request)
    reopened_provider = SQLiteTripHoldProvider(database_path)
    duplicate = reopened_provider.execute(request)

    expected_reference = (
        "hold_" + hashlib.sha256(request.idempotency_key.encode("utf-8")).hexdigest()[:16]
    )
    assert first == duplicate
    assert first.provider_reference == expected_reference
    assert first.result["status"] == "held"
    assert provider.provider_identity == reopened_provider.provider_identity
    assert provider.count_holds() == 1


def test_sqlite_trip_hold_provider_identity_is_stable_per_database(tmp_path):
    first_path = tmp_path / "provider-identity.db"
    first = SQLiteTripHoldProvider(first_path)
    reopened = SQLiteTripHoldProvider(first_path)
    other = SQLiteTripHoldProvider(tmp_path / "other-provider-identity.db")

    assert str(UUID(first.provider_identity)) == first.provider_identity
    assert reopened.provider_identity == first.provider_identity
    assert other.provider_identity != first.provider_identity


def test_sqlite_trip_hold_provider_serializes_concurrent_key_ownership(tmp_path):
    database_path = tmp_path / "provider-concurrent.db"
    request = _provider_request()
    with ThreadPoolExecutor(max_workers=2) as executor:
        providers = list(
            executor.map(
                SQLiteTripHoldProvider,
                (database_path, database_path),
            )
        )

    assert providers[0].provider_identity == providers[1].provider_identity

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda provider: provider.execute(request),
                providers,
            )
        )

    assert results[0] == results[1]
    assert providers[0].count_holds() == 1


def test_sqlite_trip_hold_provider_rejects_key_reuse_with_different_payload(tmp_path):
    provider = SQLiteTripHoldProvider(tmp_path / "provider-conflict.db")
    provider.execute(_provider_request())

    with pytest.raises(DefinitiveExternalActionError):
        provider.execute(_provider_request(quoted_total=7900))

    assert provider.count_holds() == 1


def test_durable_action_runtime_execute_resets_per_run_action_state():
    class CapturingLoop:
        def execute(self, **_kwargs):
            raise RuntimeError("stop after action state is captured")

    runtime = DurableActionTravelRuntime(CapturingLoop())  # type: ignore[arg-type]
    context = RuntimeExecutionContext(
        run_id="run-reset",
        thread_id="thread-reset",
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-reset",
            subject_id="subject-reset",
            permissions=("tools:execute",),
        ),
    )

    with pytest.raises(RuntimeError, match="stop after action state is captured"):
        runtime.execute(
            AgentState(thread_id="thread-reset"),
            TravelExternalActionInput(
                user_message="Plan Tokyo",
                requested_action="create_hold",
            ),
            context,
        )

    assert runtime._requested_action == "plan_only"
