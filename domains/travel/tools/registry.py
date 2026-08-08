from __future__ import annotations

from runtime_service.sandbox import ToolPolicy, ToolRegistry, ToolSpec

from .models import RankOptionsInput, RouteCostInput, SearchTripOptionsInput


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
