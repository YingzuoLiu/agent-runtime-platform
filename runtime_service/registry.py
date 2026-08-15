from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from agent.contracts import BaseRuntimeState, ManagedRuntimeProtocol

from .models import AgentDescriptor

RuntimeFactory = Callable[[], ManagedRuntimeProtocol[Any, Any]]
RunReferenceResolver = Callable[[BaseModel], tuple[str, ...]]


@dataclass(frozen=True)
class RuntimeRegistration:
    agent_id: str
    version: str
    description: str
    domain_id: str
    schema_version: str
    input_model: type[BaseModel]
    state_model: type[BaseRuntimeState]
    factory: RuntimeFactory
    run_reference_resolver: RunReferenceResolver | None = None

    def parse_input(self, payload: dict[str, Any]) -> BaseModel:
        return self.input_model.model_validate(payload)

    def parse_state(self, payload: dict[str, Any] | BaseRuntimeState) -> BaseRuntimeState:
        if isinstance(payload, BaseRuntimeState):
            payload = payload.model_dump(mode="python", warnings="error")
        return self.state_model.model_validate(payload)

    def referenced_run_ids(self, runtime_input: BaseModel) -> tuple[str, ...]:
        if self.run_reference_resolver is None:
            return ()
        return self.run_reference_resolver(runtime_input)


class AgentRegistry:
    """Typed registry that pins execution and serialization to one schema."""

    def __init__(self) -> None:
        self._registrations: dict[tuple[str, str], RuntimeRegistration] = {}
        self._state_schemas: dict[tuple[str, str], type[BaseRuntimeState]] = {}

    def register(
        self,
        agent_id: str,
        version: str,
        factory: RuntimeFactory,
        *,
        description: str,
        input_model: type[BaseModel],
        state_model: type[BaseRuntimeState],
        run_reference_resolver: RunReferenceResolver | None = None,
    ) -> None:
        key = (agent_id, version)
        if key in self._registrations:
            raise ValueError(f"Agent already registered: {agent_id}:{version}")
        schema_key = (state_model.domain_id, state_model.schema_version)
        existing_model = self._state_schemas.get(schema_key)
        if existing_model is not None and existing_model is not state_model:
            raise ValueError(
                f"State schema already registered: {schema_key[0]}:{schema_key[1]}"
            )
        self._state_schemas[schema_key] = state_model
        self._registrations[key] = RuntimeRegistration(
            agent_id=agent_id,
            version=version,
            description=description,
            domain_id=state_model.domain_id,
            schema_version=state_model.schema_version,
            input_model=input_model,
            state_model=state_model,
            factory=factory,
            run_reference_resolver=run_reference_resolver,
        )

    def registration(self, agent_id: str, version: str) -> RuntimeRegistration:
        try:
            return self._registrations[(agent_id, version)]
        except KeyError as exc:
            raise KeyError(f"Unknown agent version: {agent_id}:{version}") from exc

    def resolve(self, agent_id: str, version: str) -> ManagedRuntimeProtocol[Any, Any]:
        return self.registration(agent_id, version).factory()

    def parse_state(
        self,
        domain_id: str,
        schema_version: str,
        payload: dict[str, Any],
    ) -> BaseRuntimeState:
        try:
            state_model = self._state_schemas[(domain_id, schema_version)]
        except KeyError as exc:
            raise KeyError(f"Unknown state schema: {domain_id}:{schema_version}") from exc
        return state_model.model_validate(payload)

    def list_agents(self) -> list[AgentDescriptor]:
        return [
            AgentDescriptor(
                agent_id=registration.agent_id,
                version=registration.version,
                description=registration.description,
                domain_id=registration.domain_id,
                schema_version=registration.schema_version,
                input_schema=registration.input_model.model_json_schema(),
            )
            for _, registration in sorted(self._registrations.items())
        ]


def build_default_registry(
    *,
    release_validation_workflow: Any | None = None,
    dynamic_travel_runtime_factory: RuntimeFactory | None = None,
    governed_memory_travel_runtime_factory: RuntimeFactory | None = None,
    external_action_travel_runtime_factory: RuntimeFactory | None = None,
) -> AgentRegistry:
    from domains.travel.runtime import TravelAgentRuntime, TravelMessageInput
    from domains.travel.state import AgentState

    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "0.3.0",
        lambda: TravelAgentRuntime(retry_limit=2),
        description="Rule-based travel planning runtime with typed state and deterministic validation.",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    if dynamic_travel_runtime_factory is not None:
        registry.register(
            "travel-agent",
            "1.0.0",
            dynamic_travel_runtime_factory,
            description=(
                "Policy-governed dynamic travel reference runtime with typed planner "
                "decisions, durable tool evidence and deterministic final validation."
            ),
            input_model=TravelMessageInput,
            state_model=AgentState,
        )
    if governed_memory_travel_runtime_factory is not None:
        registry.register(
            "travel-agent",
            "1.1.0",
            governed_memory_travel_runtime_factory,
            description=(
                "Governed cross-thread Travel memory with subject-scoped retrieval, "
                "sealed run snapshots and deterministic preference application."
            ),
            input_model=TravelMessageInput,
            state_model=AgentState,
        )
    if external_action_travel_runtime_factory is not None:
        from domains.travel.external_action_runtime import TravelExternalActionInput

        registry.register(
            "travel-agent",
            "1.2.0",
            external_action_travel_runtime_factory,
            description=(
                "Durable external Travel actions with server-derived idempotency, "
                "provider evidence and explicit uncertain-outcome recovery."
            ),
            input_model=TravelExternalActionInput,
            state_model=AgentState,
        )
    registry.register(
        "travel-agent",
        "0.5.0",
        lambda: TravelAgentRuntime(
            retry_limit=2,
            enable_review_workflow=True,
        ),
        description=(
            "Evidence-review travel runtime with typed Budget and Preference reviewers, "
            "deadline-aware orchestration, deterministic reduction and validator-gated replanning."
        ),
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    if release_validation_workflow is not None:
        from domains.release_validation.models import (
            ReleaseValidationInput,
            ReleaseValidationInputV1,
            ReleaseValidationState,
        )
        from domains.release_validation.runtime import ManagedReleaseValidationRuntime

        def release_run_references(runtime_input: BaseModel) -> tuple[str, ...]:
            if not isinstance(runtime_input, ReleaseValidationInput):
                raise TypeError("release-validation:1.1.0 received an unexpected input model")
            if runtime_input.replay is None:
                return ()
            return (runtime_input.replay.source_run_id,)

        registry.register(
            "release-validation",
            "1.0.0",
            lambda: ManagedReleaseValidationRuntime(
                release_validation_workflow,
                legacy_fixed_order=True,
            ),
            description=(
                "Legacy fixed-order durable release validation with registered tools and "
                "deterministic readiness findings."
            ),
            input_model=ReleaseValidationInputV1,
            state_model=ReleaseValidationState,
        )
        registry.register(
            "release-validation",
            "1.1.0",
            lambda: ManagedReleaseValidationRuntime(release_validation_workflow),
            description=(
                "DAG-based durable release validation with selective replay, registered "
                "tools and deterministic readiness findings."
            ),
            input_model=ReleaseValidationInput,
            state_model=ReleaseValidationState,
            run_reference_resolver=release_run_references,
        )
    return registry
