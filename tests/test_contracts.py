"""Contract tests for `agent/contracts.py`.

Pydantic v2 does not preserve subclass-only fields when a field is merely
annotated with a base class and handed a subclass instance at runtime (see
`agent/contracts.py`'s `RuntimeResponse` docstring). These tests prove, for
this repository's installed Pydantic version, that the chosen fix --
parametrizing the generic `RuntimeResponse` with the concrete `AgentState`
type -- actually avoids that trap, instead of relying on manual inspection.
They also exercise the real production chain (RuntimeManager -> SQLite ->
API) to prove no Travel-specific field is lost along the way.
"""

from __future__ import annotations

import time
import typing
from typing import TypeVar

from fastapi.testclient import TestClient
from pydantic import BaseModel

from agent import RuntimeResponse as PackageRuntimeResponse
from agent.contracts import BaseRuntimeState
from agent.contracts import RuntimeResponse as GenericRuntimeResponse
from domains.travel.runtime import TravelAgentRuntime
from domains.travel.state import AgentState, TravelPlan
from api.main import create_app
from runtime_service import (
    ApiKeyCredential,
    RunCreateRequest,
    RuntimeManager,
    RuntimeRole,
    SQLiteRunStore,
    StaticApiKeyAuthenticator,
    TenantContext,
    build_default_registry,
)


TENANT_CONTEXT = TenantContext(tenant_id="legacy", subject_id="contract-tester")
CONTRACT_API_KEY = "contract-test-key"
CONTRACT_AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="contract-credential",
            api_key=CONTRACT_API_KEY,
            tenant_id="tenant-contract",
            subject_id="contract-tester",
            role=RuntimeRole.OPERATOR,
        )
    ]
)


def _sample_state(thread_id: str = "contract-test") -> AgentState:
    return AgentState(
        thread_id=thread_id,
        destination="Tokyo",
        days=5,
        budget=9000,
        preferences={"avoid_red_eye": True, "travel_style": "relaxed"},
        itinerary=TravelPlan(
            destination="Tokyo",
            days=5,
            flight_type="daytime",
            hotel_tier="near-subway comfort hotel",
            poi_style="relaxed itinerary",
            total_cost=8000,
        ),
        tool_outputs={"cost_breakdown": {"flight_cost": 2300}},
        blockers=["placeholder blocker"],
        retry_count=1,
        current_stage="planned",
    )


TRAVEL_FIELD_CHECKS = {
    "destination": "Tokyo",
    "days": 5,
    "budget": 9000,
    "preferences": {"avoid_red_eye": True, "travel_style": "relaxed"},
    "blockers": ["placeholder blocker"],
    "retry_count": 1,
    "current_stage": "planned",
}


def _assert_travel_fields_intact(dumped_state: dict) -> None:
    for field, expected in TRAVEL_FIELD_CHECKS.items():
        assert dumped_state[field] == expected, field
    assert dumped_state["itinerary"]["flight_type"] == "daytime"
    assert dumped_state["itinerary"]["hotel_tier"] == "near-subway comfort hotel"
    assert dumped_state["itinerary"]["total_cost"] == 8000
    assert dumped_state["tool_outputs"] == {"cost_breakdown": {"flight_cost": 2300}}


# --- BaseRuntimeState / ClassVar routing metadata ---------------------------


def test_agent_state_is_a_base_runtime_state():
    state = _sample_state()
    assert isinstance(state, BaseRuntimeState)
    assert AgentState.domain_id == "travel"
    assert AgentState.schema_version == "1"


def test_domain_id_and_schema_version_are_classvars_not_pydantic_fields():
    assert "domain_id" not in AgentState.model_fields
    assert "schema_version" not in AgentState.model_fields

    state = _sample_state()
    dumped = state.model_dump()
    assert "domain_id" not in dumped
    assert "schema_version" not in dumped
    assert "domain_id" not in state.model_dump_json()

    # Round-tripping through JSON must not require domain_id/schema_version
    # to be present in the payload, and the class attribute must still be
    # reachable afterwards -- it was never part of the instance's own data.
    round_tripped = AgentState.model_validate_json(state.model_dump_json())
    assert round_tripped.domain_id == "travel"
    assert round_tripped.schema_version == "1"


def test_naive_base_class_annotation_would_lose_subclass_fields():
    """Empirical proof of the trap `RuntimeResponse`'s design avoids.

    This does not exercise any production code path -- it is a minimal
    reproduction, on this repo's installed Pydantic version, of the
    failure mode a field annotated with the abstract `BaseRuntimeState`
    (instead of the concrete `AgentState`) would hit.
    """

    class NaiveResponse(BaseModel):
        state: BaseRuntimeState

    naive = NaiveResponse(state=_sample_state())
    dumped = naive.model_dump()

    assert "destination" not in dumped["state"]
    assert dumped["state"] == {"thread_id": "contract-test", "execution_trace": []}


# --- RuntimeResponse: Core stays generic, Travel binds it explicitly -------


def test_agent_package_exports_the_generic_unbound_runtime_response():
    """`from agent import RuntimeResponse` must resolve to the Core, unbound
    Generic contract -- not something silently pinned to `AgentState` just
    because `agent/runtime.py` (a Travel-specific module slated to move to
    `domains/travel/` in a later phase) also imports and parametrizes it.
    Core must not depend on, or leak, a Travel-bound type under a Core name.
    """
    assert PackageRuntimeResponse is GenericRuntimeResponse
    assert PackageRuntimeResponse is not GenericRuntimeResponse[AgentState]

    # The `state` field annotation is still the bare TypeVar: nothing has
    # bound this export to any concrete state type.
    state_annotation = PackageRuntimeResponse.model_fields["state"].annotation
    assert isinstance(state_annotation, TypeVar)


def test_travel_agent_runtime_handle_user_message_returns_bound_runtime_response():
    """`TravelAgentRuntime.handle_user_message`'s declared return type is
    `RuntimeResponse[AgentState]` -- resolved via `typing.get_type_hints`,
    not just inferred from one instance's runtime type -- and the actual
    returned object matches that contract.
    """
    hints = typing.get_type_hints(TravelAgentRuntime.handle_user_message)
    assert hints["return"] == GenericRuntimeResponse[AgentState]

    runtime = TravelAgentRuntime()
    result = runtime.handle_user_message(
        AgentState(thread_id="contract-response-test"),
        "I want a 5-day Tokyo trip under 7000 SGD.",
    )
    assert type(result.state) is AgentState
    assert result.state.destination == "Tokyo"


def test_runtime_response_whole_object_serialization_keeps_travel_fields():
    """No production code path currently calls `.model_dump()` /
    `.model_dump_json()` on a *whole* `RuntimeResponse`: `RuntimeManager`
    only destructures `.state` / `.message` / `.validation_errors`
    individually (see `runtime_service/manager.py::_execute_run`), and the
    FastAPI layer uses its own `AgentMessageResponse` / `RunRecord` models,
    both already concretely typed with `AgentState` rather than
    `RuntimeResponse`. This test exists anyway, per explicit request, to
    prove the contract holds even if a future caller serializes the whole
    envelope -- not because a real call site needs it today.
    """
    state = _sample_state(thread_id="contract-response-test-2")
    response = GenericRuntimeResponse[AgentState](
        message="Planned.", state=state, validation_errors=[]
    )

    dumped = response.model_dump()
    _assert_travel_fields_intact(dumped["state"])

    dumped_json = response.model_dump_json()
    assert '"destination":"Tokyo"' in dumped_json
    assert '"budget":9000' in dumped_json

    round_tripped_state = AgentState.model_validate(dumped["state"])
    assert round_tripped_state.destination == "Tokyo"
    assert round_tripped_state.itinerary is not None
    assert round_tripped_state.itinerary.total_cost == 8000


def test_generic_run_request_keeps_concrete_client_state_until_registry_validation():
    state = _sample_state(thread_id="request-state-thread")
    request = RunCreateRequest(
        thread_id="request-state-thread",
        input={"user_message": "Change the budget to 9500."},
        state=state,
    )

    assert type(request.state) is AgentState
    assert request.state.destination == "Tokyo"
    assert request.state.itinerary is not None
    assert request.state.itinerary.total_cost == 8000


# --- Real production chain: RuntimeManager -> SQLite -> reload -------------


def test_sqlite_round_trip_preserves_all_travel_specific_fields(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    manager = RuntimeManager(store, build_default_registry())
    manager.start()
    try:
        submitted = manager.submit(
            RunCreateRequest(
                thread_id="contract-sqlite-thread",
                user_message="I want a 5-day Tokyo trip under 9000 SGD.",
            ),
            tenant_context=TENANT_CONTEXT,
        )
        deadline = time.monotonic() + 10.0
        result = None
        while time.monotonic() < deadline:
            result = manager.get_run(submitted.run_id, tenant_context=TENANT_CONTEXT)
            if result is not None and result.status.is_terminal:
                break
            time.sleep(0.02)
        assert result is not None and result.status.is_terminal
    finally:
        manager.stop()

    # Reload from a brand-new SQLiteRunStore instance against the same
    # database file, so this genuinely exercises persistence and not just
    # the in-memory RunRecord already held by the manager.
    reopened_store = SQLiteRunStore(
        database_path,
        state_registry=build_default_registry(),
    )
    persisted_run = reopened_store.get_run_internal(submitted.run_id)
    assert persisted_run is not None
    assert persisted_run.state is not None
    assert type(persisted_run.state) is AgentState
    assert persisted_run.state.destination == "Tokyo"
    assert persisted_run.state.budget == 9000
    assert persisted_run.state.itinerary is not None
    assert persisted_run.state.itinerary.destination == "Tokyo"

    persisted_thread_state = reopened_store.load_thread_state(
        "contract-sqlite-thread",
        tenant_id=TENANT_CONTEXT.tenant_id,
    )
    assert persisted_thread_state is not None
    assert type(persisted_thread_state) is AgentState
    assert persisted_thread_state.destination == "Tokyo"
    assert persisted_thread_state.budget == 9000
    assert persisted_thread_state.itinerary is not None


def test_api_response_preserves_travel_specific_fields(tmp_path):
    app = create_app(
        database_path=tmp_path / "runtime.db",
        authenticator=CONTRACT_AUTHENTICATOR,
    )
    with TestClient(
        app,
        headers={"Authorization": f"Bearer {CONTRACT_API_KEY}"},
    ) as client:
        response = client.post(
            "/agent/message",
            json={
                "thread_id": "contract-api-thread",
                "user_message": "I want a 5-day Tokyo trip under 9000 SGD. Avoid red-eye flights.",
            },
        )
    assert response.status_code == 200
    body = response.json()
    state = body["updated_state"]

    assert state["destination"] == "Tokyo"
    assert state["days"] == 5
    assert state["budget"] == 9000
    assert state["preferences"] == {"avoid_red_eye": True}
    assert state["itinerary"] is not None
    assert state["itinerary"]["flight_type"] == "daytime"
    assert state["current_stage"] == "planned"
    assert "execution_trace" in state
    assert "domain_id" not in state
    assert "schema_version" not in state
