from domains.travel.memory import TravelMemoryPolicy
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
