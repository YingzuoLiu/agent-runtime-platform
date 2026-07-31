from domains.travel.state import AgentState
from domains.travel.runtime import TravelAgentRuntime


def test_runtime_partially_replans_after_budget_and_preference_change():
    runtime = TravelAgentRuntime()
    state = AgentState(thread_id="test_thread")

    first = runtime.handle_user_message(
        state,
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )

    second = runtime.handle_user_message(
        first.state,
        "Change the budget to 9000 and avoid red-eye flights.",
    )

    assert second.state.budget == 9000
    assert second.state.preferences["avoid_red_eye"] is True
    assert second.state.itinerary is not None
    assert second.state.itinerary.flight_type == "daytime"
    assert any(event.event == "state_patch_applied" for event in second.state.execution_trace)
