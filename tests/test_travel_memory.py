import pytest

from domains.travel.memory import TravelMemoryPolicy
from domains.travel.preferences import (
    parse_explicit_travel_preferences,
    parse_legacy_travel_preferences,
)
from domains.travel.state import AgentState
from runtime_service.memory import RetrievedMemory


def test_travel_memory_allowlist_extracts_applies_and_removes_execution_overlay():
    policy = TravelMemoryPolicy()
    writes = policy.extract(
        "I avoid red-eye flights, prefer a hotel near subway, and like relaxed travel."
    )
    assert {write.key: write.value for write in writes} == {
        "flight.avoid_red_eye": True,
        "hotel.near_subway": True,
        "travel.style": "relaxed",
    }

    memories = tuple(
        RetrievedMemory(
            memory_id=f"memory-{index}",
            kind=write.kind,
            key=write.key,
            value=write.value,
            confidence=write.confidence,
            version=1,
            created_at="2026-08-09T00:00:00+00:00",
        )
        for index, write in enumerate(writes)
    )
    original = AgentState(
        thread_id="travel-memory-policy",
        preferences={"meal_preference": "vegetarian"},
    )
    applied = policy.apply(original, memories)
    assert applied.preferences == {
        "meal_preference": "vegetarian",
        "avoid_red_eye": True,
        "hotel_near_subway": True,
        "travel_style": "relaxed",
    }

    restored = policy.restore_checkpoint_preferences(
        applied,
        original_preferences=original.preferences,
    )
    assert restored.preferences == {"meal_preference": "vegetarian"}


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("I do not mind red-eye flights.", {"avoid_red_eye": False}),
        ("I do not want red-eye flights.", {"avoid_red_eye": True}),
        (
            "I do not want a hotel near subway.",
            {"hotel_near_subway": False},
        ),
        (
            "I prefer NOT a relaxed travel style.",
            {"travel_style": "balanced"},
        ),
        ("I allow red-eye flights.", {"avoid_red_eye": False}),
        ("I avoid red-eye flights.", {"avoid_red_eye": True}),
        ("I prefer a hotel near subway.", {"hotel_near_subway": True}),
        ("I like relaxed travel.", {"travel_style": "relaxed"}),
    ],
)
def test_explicit_travel_preference_parser_handles_intent_direction(message, expected):
    assert parse_explicit_travel_preferences(message) == expected


@pytest.mark.parametrize(
    "message",
    [
        "Do you offer red-eye flights?",
        "Tell me about a hotel near subway.",
        "What does a relaxed travel style mean?",
    ],
)
def test_explicit_travel_preference_parser_fails_closed_on_ambiguous_mentions(message):
    assert parse_explicit_travel_preferences(message) == {}


def test_legacy_travel_preference_parser_preserves_phase5a_substring_behavior():
    assert parse_legacy_travel_preferences(
        "I heard red-eye flights might be cheap, any details?"
    ) == {"avoid_red_eye": True}
    assert parse_legacy_travel_preferences(
        "I do not want a hotel near subway."
    ) == {"hotel_near_subway": True}
    assert parse_legacy_travel_preferences(
        "I prefer NOT a relaxed travel style."
    ) == {"travel_style": "relaxed"}
