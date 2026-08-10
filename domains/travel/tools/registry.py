from __future__ import annotations

from runtime_service.sandbox import (
    ToolEffect,
    ToolPolicy,
    ToolRegistry,
    ToolRetryMode,
    ToolSpec,
)

from .models import (
    CreateTripHoldInput,
    CreateTripHoldResult,
    RankOptionsInput,
    RouteCostInput,
    SearchTripOptionsInput,
)


def _trip_hold_was_explicitly_requested(runtime_input: dict[str, object]) -> bool:
    return runtime_input.get("requested_action") == "create_hold"


def build_travel_tool_registry() -> ToolRegistry:
    """Build the domain-owned allowlist used by the Travel reference runtime."""

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="search_trip_options",
            description=(
                "Search a deterministic offline reference catalog for Travel options."
            ),
            input_model=SearchTripOptionsInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint="domains.travel.tools.handlers:search_trip_options",
        )
    )
    registry.register(
        ToolSpec(
            name="rank_trip_options",
            description="Rank up to 50 trip options by normalized cost and duration.",
            input_model=RankOptionsInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint="domains.travel.tools.handlers:rank_trip_options",
        )
    )
    registry.register(
        ToolSpec(
            name="route_cost_summary",
            description="Calculate a deterministic trip-cost summary and budget delta.",
            input_model=RouteCostInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint="domains.travel.tools.handlers:route_cost_summary",
        )
    )
    return registry


def build_travel_external_action_tool_registry() -> ToolRegistry:
    """Build the opt-in Travel 1.2 registry without changing older versions."""

    registry = build_travel_tool_registry()
    registry.register(
        ToolSpec(
            name="create_trip_hold",
            description=(
                "Create an idempotent temporary hold through the configured Travel provider."
            ),
            input_model=CreateTripHoldInput,
            output_model=CreateTripHoldResult,
            policy=ToolPolicy(timeout_seconds=2.0),
            effect=ToolEffect.EXTERNAL_WRITE,
            retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
            # This is a logical, server-controlled provider route. The API
            # may bind it to the bundled SQLite test double or to the
            # configured HTTP adapter without changing Planner-visible tools.
            provider_name="travel-trip-hold",
            runtime_input_gate=_trip_hold_was_explicitly_requested,
        )
    )
    return registry
