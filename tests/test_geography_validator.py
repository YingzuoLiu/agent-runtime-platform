from domains.travel.state import AgentState, TravelPlan
from domains.travel.tools.geocode_tool import GeoPoint
from domains.travel.validator import TravelValidator

"""
这个测试证明三件事：
1. Validator 可以接入 geocode tool。
2. 目的地不存在时能生成 validation error。
3. Haversine distance 能作为地理合理性信号。
"""

class FakeGeocodeTool:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def search(self, query, country=None, limit=3):
        self.calls.append(
            {
                "query": query,
                "country": country,
                "limit": limit,
            }
        )
        return self.results


def test_validator_passes_when_destination_can_be_geocoded():
    fake_tool = FakeGeocodeTool(
        results=[
            GeoPoint(
                name="Hangzhou",
                display_name="Hangzhou, Zhejiang, China",
                lat=30.2741,
                lon=120.1551,
                place_type="city",
                place_class="place",
                country="China",
                city="Hangzhou",
            )
        ]
    )

    state = AgentState(
        thread_id="geo_test",
        destination="Hangzhou",
        days=3,
        budget=5000,
        itinerary=TravelPlan(
            destination="Hangzhou",
            days=3,
            flight_type="daytime",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=3000,
        ),
    )

    result = TravelValidator(
        geocode_tool=fake_tool,
        enable_geography_validation=True,
        default_country="China",
    ).validate(state)

    assert result.passed
    assert fake_tool.calls[0]["query"] == "Hangzhou"


def test_validator_catches_unknown_destination():
    fake_tool = FakeGeocodeTool(results=[])

    state = AgentState(
        thread_id="geo_test",
        destination="FakeCityXYZ",
        days=3,
        budget=5000,
        itinerary=TravelPlan(
            destination="FakeCityXYZ",
            days=3,
            flight_type="daytime",
            hotel_tier="standard",
            poi_style="balanced",
            total_cost=3000,
        ),
    )

    result = TravelValidator(
        geocode_tool=fake_tool,
        enable_geography_validation=True,
    ).validate(state)

    assert not result.passed
    assert any("could not be geocoded" in error for error in result.errors)


def test_haversine_distance_detects_far_locations():
    hangzhou = GeoPoint(
        name="Hangzhou",
        display_name="Hangzhou, Zhejiang, China",
        lat=30.2741,
        lon=120.1551,
    )

    shanghai = GeoPoint(
        name="Shanghai",
        display_name="Shanghai, China",
        lat=31.2304,
        lon=121.4737,
    )

    distance = TravelValidator.haversine_km(hangzhou, shanghai)

    assert distance > 100
