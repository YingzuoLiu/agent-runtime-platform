from __future__ import annotations

from typing import Any


def search_trip_options(payload: dict[str, Any]) -> dict[str, Any]:
    """Return an offline, deterministic catalog for the reference domain.

    The results deliberately do not represent live flight or hotel inventory.
    They provide stable observations for CI while still allowing a planner to
    choose its next call from actual tool output.
    """

    destination = str(payload["destination"])
    days = int(payload["days"])
    avoid_red_eye = bool(payload["avoid_red_eye"])
    hotel_near_subway = bool(payload["hotel_near_subway"])
    travel_style = str(payload["travel_style"])

    value_transport = 2300 if avoid_red_eye else 1800
    value_hotel_per_day = 850 if hotel_near_subway else 650
    value_activity_per_day = 300 if travel_style == "relaxed" else 450
    value_hotel = days * value_hotel_per_day
    value_activity = days * value_activity_per_day

    comfort_transport = 2700
    comfort_hotel_per_day = 1000 if hotel_near_subway else 800
    comfort_activity_per_day = 350 if travel_style == "relaxed" else 500
    comfort_hotel = days * comfort_hotel_per_day
    comfort_activity = days * comfort_activity_per_day

    options = [
        {
            "index": 0,
            "name": f"{destination} value itinerary",
            "cost": float(value_transport + value_hotel + value_activity),
            "duration_hours": 10.5 if avoid_red_eye else 13.0,
            "transport_cost": value_transport,
            "hotel_cost": value_hotel,
            "activity_cost": value_activity,
            "flight_type": "daytime" if avoid_red_eye else "red_eye",
            "hotel_tier": (
                "near-subway comfort hotel" if hotel_near_subway else "standard hotel"
            ),
            "poi_style": (
                "relaxed itinerary" if travel_style == "relaxed" else "balanced itinerary"
            ),
        },
        {
            "index": 1,
            "name": f"{destination} comfort itinerary",
            "cost": float(comfort_transport + comfort_hotel + comfort_activity),
            "duration_hours": 8.5,
            "transport_cost": comfort_transport,
            "hotel_cost": comfort_hotel,
            "activity_cost": comfort_activity,
            "flight_type": "daytime",
            "hotel_tier": (
                "near-subway premium hotel" if hotel_near_subway else "comfort hotel"
            ),
            "poi_style": (
                "relaxed highlights" if travel_style == "relaxed" else "balanced highlights"
            ),
        },
    ]
    return {
        "source": "synthetic_reference_catalog",
        "destination": destination,
        "options": options,
    }


def route_cost_summary(payload: dict[str, Any]) -> dict[str, Any]:
    total = int(payload["transport_cost"]) + int(payload["hotel_cost"]) + int(
        payload["activity_cost"]
    )
    budget = int(payload["budget"])
    return {
        "total_cost": total,
        "budget": budget,
        "remaining_budget": budget - total,
        "within_budget": total <= budget,
    }


def rank_trip_options(payload: dict[str, Any]) -> dict[str, Any]:
    options = payload["options"]
    cost_weight = float(payload["cost_weight"])
    duration_weight = float(payload["duration_weight"])

    normalized: list[dict[str, Any]] = []
    max_cost = max(float(item.get("cost", 0)) for item in options) or 1.0
    max_duration = max(float(item.get("duration_hours", 0)) for item in options) or 1.0

    for index, item in enumerate(options):
        cost = float(item.get("cost", 0))
        duration = float(item.get("duration_hours", 0))
        score = cost_weight * (1 - cost / max_cost) + duration_weight * (
            1 - duration / max_duration
        )
        normalized.append(
            {
                "index": index,
                "name": str(item.get("name", f"option-{index}")),
                "score": round(score, 6),
            }
        )

    normalized.sort(key=lambda item: (-float(item["score"]), int(item["index"])))
    return {"ranking": normalized}
