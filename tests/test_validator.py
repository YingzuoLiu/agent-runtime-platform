from domains.travel.state import AgentState, TravelPlan
from domains.travel.validator import TravelValidator


def test_validator_catches_budget_violation():
    state = AgentState(
        thread_id="test_thread",
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

    result = TravelValidator().validate(state)

    assert not result.passed
    assert any("Budget exceeded" in error for error in result.errors)


def test_validator_catches_red_eye_violation():
    state = AgentState(
        thread_id="test_thread",
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

    result = TravelValidator().validate(state)

    assert not result.passed
    assert any("red-eye" in error for error in result.errors)
