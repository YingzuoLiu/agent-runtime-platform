from .contracts import (
    BaseRuntimeState,
    ManagedRuntimeProtocol,
    RuntimeExecutionContext,
    RuntimeProtocol,
    RuntimeResponse,
    TraceEvent,
    project_thread_checkpoint_state,
    utc_now,
)

# `agent/__init__.py` imports only from `.contracts`: the domain-agnostic
# base state, response envelope, and runtime protocol. Phase 1B moved
# Travel's concrete state (`AgentState`/`StatePatch`/`TravelPlan`) to
# `domains/travel/state.py` and its runtime (`TravelAgentRuntime`) to
# `domains/travel/runtime.py`. Neither is re-exported here, for the same
# reason `TravelAgentRuntime` was never re-exported before the move:
# re-exporting a concrete domain's types would make this Core package
# depend on that domain. Callers import Travel symbols directly from
# `domains.travel.state` / `domains.travel.runtime`.

__all__ = [
    "BaseRuntimeState",
    "ManagedRuntimeProtocol",
    "RuntimeExecutionContext",
    "RuntimeProtocol",
    "RuntimeResponse",
    "TraceEvent",
    "project_thread_checkpoint_state",
    "utc_now",
]
