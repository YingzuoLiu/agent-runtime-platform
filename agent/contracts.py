"""Core, domain-agnostic runtime contracts.

Every domain runtime (Travel today, others in later phases) is expected to
build on this shared layer: a base state shape, a generic response
envelope, and a structural protocol describing what a runtime must be able
to do. This module must never import from a concrete domain module
(currently `AgentState`/`TravelPlan` in `agent/state.py`) -- domain modules
import from here, not the other way around. See `agent/state.py`'s module
docstring for why `TraceEvent`/`utc_now` live here instead of there.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Generic, List, Protocol, TypeVar

from pydantic import BaseModel, Field


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TraceEvent(BaseModel):
    """A small execution log entry for observability and debugging."""

    event: str
    reason: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=utc_now)


class BaseRuntimeState(BaseModel):
    """Fields every domain's runtime state carries, regardless of domain.

    `domain_id` and `schema_version` are `ClassVar`s, not Pydantic fields:
    they identify which concrete state type a serialized blob belongs to,
    but they are routing metadata about the *type*, not part of the
    state's own data, so they must not be duplicated into every row's
    `state_json`. The registry/store persists them as separate routing
    columns instead. Concrete subclasses (e.g. `AgentState`) set
    both as real class attributes.
    """

    domain_id: ClassVar[str]
    schema_version: ClassVar[str]

    thread_id: str
    execution_trace: List[TraceEvent] = Field(default_factory=list)


StateT = TypeVar("StateT", bound=BaseRuntimeState)
InputT = TypeVar("InputT", bound=BaseModel, contravariant=True)


class RuntimeResponse(BaseModel, Generic[StateT]):
    """What a runtime step returns: a message, the resulting state, and any
    validation errors surfaced along the way.

    This is generic in the concrete state type rather than annotated with
    the abstract `BaseRuntimeState`, precisely so a concrete instantiation
    such as `RuntimeResponse[AgentState]` keeps `state`'s static type,
    JSON schema, and serialized output all pinned to `AgentState`. Pydantic
    v2 does not preserve subclass-only fields when a field is merely
    annotated with a base class and handed a subclass instance at runtime;
    binding the type parameter to the concrete class sidesteps that
    entirely instead of asking Pydantic to serialize "as seen at runtime".
    See `tests/test_contracts.py` for the empirical proof on this
    repository's installed Pydantic version.
    """

    message: str
    state: StateT
    validation_errors: List[str]


class RuntimeExecutionContext(BaseModel):
    """Stable identifiers supplied by ``RuntimeManager`` to one execution."""

    run_id: str
    thread_id: str


class RuntimeProtocol(Protocol[StateT]):
    """Legacy message-oriented contract retained for application callers.

    Durable multi-domain execution uses ``ManagedRuntimeProtocol`` below.
    Approval, disposition, and memory-provider hooks remain out of scope.
    """

    def initial_state(self, thread_id: str) -> StateT:
        ...

    def handle_user_message(
        self, state: StateT, user_message: str
    ) -> RuntimeResponse[StateT]:
        ...


class ManagedRuntimeProtocol(Protocol[StateT, InputT]):
    """Domain-neutral contract used by the durable runtime manager.

    Domain runtimes own their concrete input and state models. The manager
    supplies only execution identity and never branches on a domain name.
    """

    def initial_state(self, thread_id: str) -> StateT:
        ...

    def execute(
        self,
        state: StateT,
        runtime_input: InputT,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[StateT]:
        ...
