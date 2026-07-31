from __future__ import annotations

from collections.abc import Callable

from domains.travel.runtime import TravelAgentRuntime

from .models import AgentDescriptor

RuntimeFactory = Callable[[], TravelAgentRuntime]


class AgentRegistry:
    """Small in-process registry used to pin each run to an exact agent version."""

    def __init__(self) -> None:
        self._factories: dict[tuple[str, str], RuntimeFactory] = {}
        self._descriptions: dict[tuple[str, str], str] = {}

    def register(
        self,
        agent_id: str,
        version: str,
        factory: RuntimeFactory,
        *,
        description: str,
    ) -> None:
        key = (agent_id, version)
        if key in self._factories:
            raise ValueError(f"Agent already registered: {agent_id}:{version}")
        self._factories[key] = factory
        self._descriptions[key] = description

    def resolve(self, agent_id: str, version: str) -> TravelAgentRuntime:
        try:
            factory = self._factories[(agent_id, version)]
        except KeyError as exc:
            raise KeyError(f"Unknown agent version: {agent_id}:{version}") from exc
        return factory()

    def list_agents(self) -> list[AgentDescriptor]:
        return [
            AgentDescriptor(
                agent_id=agent_id,
                version=version,
                description=self._descriptions[(agent_id, version)],
            )
            for agent_id, version in sorted(self._factories)
        ]


def build_default_registry() -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "0.3.0",
        lambda: TravelAgentRuntime(retry_limit=2),
        description="Rule-based travel planning runtime with typed state and deterministic validation.",
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
    )
    return registry
