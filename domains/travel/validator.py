from __future__ import annotations

import math
from typing import List, Protocol

from pydantic import BaseModel

from .state import AgentState
from .tools.geocode_tool import GeoPoint


class GeocodeToolProtocol(Protocol):
    def search(
        self,
        query: str,
        country: str | None = None,
        limit: int = 3,
    ) -> List[GeoPoint]:
        ...


class ValidationResult(BaseModel):
    passed: bool
    errors: List[str]


class TravelValidator:
    """
    Deterministic validation layer.

    The goal is to keep hard business constraints outside the LLM prompt.
    """

    def __init__(
        self,
        geocode_tool: GeocodeToolProtocol | None = None,
        enable_geography_validation: bool = False,
        default_country: str | None = None,
    ):
        self.geocode_tool = geocode_tool
        self.enable_geography_validation = enable_geography_validation
        self.default_country = default_country

    def validate_required_fields(self, state: AgentState) -> List[str]:
        errors: List[str] = []

        if not state.destination:
            errors.append("Missing required field: destination")

        if not state.days:
            errors.append("Missing required field: days")

        if not state.budget:
            errors.append("Missing required field: budget")

        return errors

    def validate_budget(self, state: AgentState) -> List[str]:
        errors: List[str] = []

        if state.itinerary is None or state.budget is None:
            return errors

        if state.itinerary.total_cost > state.budget:
            errors.append(
                f"Budget exceeded: total_cost={state.itinerary.total_cost}, budget={state.budget}"
            )

        return errors

    def validate_no_red_eye(self, state: AgentState) -> List[str]:
        errors: List[str] = []

        if state.itinerary is None:
            return errors

        avoid_red_eye = bool(state.preferences.get("avoid_red_eye", False))

        if avoid_red_eye and state.itinerary.flight_type == "red_eye":
            errors.append("Flight constraint violated: red-eye flight is not allowed")

        return errors

    def validate_itinerary_days(self, state: AgentState) -> List[str]:
        errors: List[str] = []

        if state.itinerary is None or state.days is None:
            return errors

        if state.itinerary.days != state.days:
            errors.append(
                f"Day count mismatch: itinerary_days={state.itinerary.days}, requested_days={state.days}"
            )

        return errors

    def validate_destination_exists(self, state: AgentState) -> List[str]:
        errors: List[str] = []

        if not self.enable_geography_validation:
            return errors

        if self.geocode_tool is None:
            return errors

        if not state.destination:
            return errors

        try:
            results = self.geocode_tool.search(
                state.destination,
                country=self.default_country,
                limit=3,
            )
        except Exception as exc:
            errors.append(
                f"Geography validation failed: geocode tool error={type(exc).__name__}"
            )
            return errors

        if not results:
            errors.append(f"Destination could not be geocoded: {state.destination}")

        return errors

    @staticmethod
    def haversine_km(point_a: GeoPoint, point_b: GeoPoint) -> float:
        radius_km = 6371.0

        lat1 = math.radians(point_a.lat)
        lon1 = math.radians(point_a.lon)
        lat2 = math.radians(point_b.lat)
        lon2 = math.radians(point_b.lon)

        delta_lat = lat2 - lat1
        delta_lon = lon2 - lon1

        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
        )

        return 2 * radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def validate(self, state: AgentState) -> ValidationResult:
        errors: List[str] = []

        errors.extend(self.validate_required_fields(state))
        errors.extend(self.validate_budget(state))
        errors.extend(self.validate_no_red_eye(state))
        errors.extend(self.validate_itinerary_days(state))
        errors.extend(self.validate_destination_exists(state))

        return ValidationResult(passed=len(errors) == 0, errors=errors)
