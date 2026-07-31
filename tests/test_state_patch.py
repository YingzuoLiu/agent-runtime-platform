from domains.travel.reducer import apply_patch
from domains.travel.state import AgentState, StatePatch


def test_apply_patch_updates_budget_and_trace():
    state = AgentState(thread_id="test_thread", budget=7000)

    patch = StatePatch(
        updates={"budget": 9000},
        reason="user_updated_budget",
        affected_fields=["budget", "itinerary"],
        trigger_replan=True,
    )

    updated = apply_patch(state, patch)

    assert updated.budget == 9000
    assert len(updated.execution_trace) == 1
    assert updated.execution_trace[0].event == "state_patch_applied"


def test_apply_patch_respects_locked_fields():
    state = AgentState(thread_id="test_thread", budget=7000)

    patch = StatePatch(
        updates={"budget": 9000},
        reason="budget_locked_by_user_policy",
        affected_fields=["budget"],
        locked_fields=["budget"],
    )

    updated = apply_patch(state, patch)

    assert updated.budget == 7000
    assert updated.execution_trace[0].payload["skipped_updates"] == {"budget": 9000}
