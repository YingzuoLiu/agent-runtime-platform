"""Characterization of TravelValidator's exact error strings.

`tests/test_validator.py` and `tests/test_geography_validator.py` already
check these with substring assertions (`"Budget exceeded" in error`). This
module locks the *exact* text so a later refactor into structured
`ValidationFinding` objects (see the phased plan, requirement 4) can prove
it reproduces byte-identical rendered messages, not just "contains the
right keyword".
"""

from __future__ import annotations

from domains.travel.state import AgentState, TravelPlan
from domains.travel.tools.geocode_tool import GeoPoint
from domains.travel.validator import TravelValidator


class FakeGeocodeTool:
    def __init__(self, results=None, raises: Exception | None = None):
        self.results = results or []
        self.raises = raises

    def search(self, query, country=None, limit=3):
        if self.raises is not None:
            raise self.raises
        return self.results


def test_missing_required_field_messages_are_exact():
    state = AgentState(thread_id="t")

    errors = TravelValidator().validate_required_fields(state)

    assert errors == [
        "Missing required field: destination",
        "Missing required field: days",
        "Missing required field: budget",
    ]


def test_budget_exceeded_message_is_exact():
    state = AgentState(
        thread_id="t",
        destination="Tokyo",
        days=5,
        budget=7000,
        itinerary=TravelPlan(
            destination="Tokyo",
            days=5,
            flight_type="daytime",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=9000,
        ),
    )

    errors = TravelValidator().validate_budget(state)

    assert errors == ["Budget exceeded: total_cost=9000, budget=7000"]


def test_red_eye_violation_message_is_exact():
    state = AgentState(
        thread_id="t",
        destination="Tokyo",
        days=5,
        budget=9000,
        preferences={"avoid_red_eye": True},
        itinerary=TravelPlan(
            destination="Tokyo",
            days=5,
            flight_type="red_eye",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=6000,
        ),
    )

    errors = TravelValidator().validate_no_red_eye(state)

    assert errors == ["Flight constraint violated: red-eye flight is not allowed"]


def test_day_count_mismatch_message_is_exact():
    state = AgentState(
        thread_id="t",
        destination="Tokyo",
        days=5,
        budget=9000,
        itinerary=TravelPlan(
            destination="Tokyo",
            days=7,
            flight_type="daytime",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=6000,
        ),
    )

    errors = TravelValidator().validate_itinerary_days(state)

    assert errors == ["Day count mismatch: itinerary_days=7, requested_days=5"]


def test_geocode_tool_exception_message_is_exact():
    state = AgentState(thread_id="t", destination="Tokyo")
    validator = TravelValidator(
        geocode_tool=FakeGeocodeTool(raises=RuntimeError("boom")),
        enable_geography_validation=True,
    )

    errors = validator.validate_destination_exists(state)

    assert errors == ["Geography validation failed: geocode tool error=RuntimeError"]


def test_unknown_destination_message_is_exact():
    state = AgentState(thread_id="t", destination="FakeCityXYZ")
    validator = TravelValidator(
        geocode_tool=FakeGeocodeTool(results=[]),
        enable_geography_validation=True,
    )

    errors = validator.validate_destination_exists(state)

    assert errors == ["Destination could not be geocoded: FakeCityXYZ"]


def test_destination_exists_no_errors_when_geocode_succeeds():
    state = AgentState(thread_id="t", destination="Hangzhou")
    validator = TravelValidator(
        geocode_tool=FakeGeocodeTool(
            results=[
                GeoPoint(
                    name="Hangzhou",
                    display_name="Hangzhou, Zhejiang, China",
                    lat=30.2741,
                    lon=120.1551,
                )
            ]
        ),
        enable_geography_validation=True,
    )

    assert validator.validate_destination_exists(state) == []


def test_geography_validation_disabled_by_default_produces_no_errors():
    state = AgentState(thread_id="t", destination="FakeCityXYZ")

    assert TravelValidator().validate_destination_exists(state) == []


def test_aggregate_validate_combines_all_rule_errors_in_order():
    state = AgentState(
        thread_id="t",
        destination="Tokyo",
        days=5,
        budget=5000,
        preferences={"avoid_red_eye": True},
        itinerary=TravelPlan(
            destination="Tokyo",
            days=7,
            flight_type="red_eye",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=9000,
        ),
    )

    result = TravelValidator().validate(state)

    assert not result.passed
    assert result.errors == [
        "Budget exceeded: total_cost=9000, budget=5000",
        "Flight constraint violated: red-eye flight is not allowed",
        "Day count mismatch: itinerary_days=7, requested_days=5",
    ]
