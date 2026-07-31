from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field

from agent.contracts import BaseRuntimeState

# `TraceEvent` and `utc_now` moved to `agent/contracts.py`: they are fully
# domain-agnostic (an event/reason/payload/timestamp log entry has nothing
# Travel-specific about it) and `BaseRuntimeState.execution_trace` needs
# `TraceEvent` as its field type. Keeping them here would force
# `agent/contracts.py` to import from this module for `TraceEvent` while
# this module imports `BaseRuntimeState` from `agent/contracts.py` -- a
# circular import. Every direct importer of `TraceEvent`/`utc_now` from
# this module has been repointed to `agent.contracts`; nothing is
# re-exported from here.


class TravelPlan(BaseModel):
    """A simplified travel plan produced by the runtime."""

    destination: str
    days: int
    flight_type: str
    hotel_tier: str
    poi_style: str
    total_cost: int
    notes: List[str] = Field(default_factory=list)


class AgentState(BaseRuntimeState):
    """
    Typed task state for a multi-turn travel planning agent.

    Extends the domain-agnostic `BaseRuntimeState` (`thread_id`,
    `execution_trace`) with Travel-specific fields. In production, this
    state could be persisted in Redis or a database. This demo keeps it
    in memory so the repository can run without external services.
    """

    domain_id: ClassVar[str] = "travel"
    schema_version: ClassVar[str] = "1"

    destination: Optional[str] = None
    days: Optional[int] = None
    budget: Optional[int] = None

    preferences: Dict[str, Any] = Field(default_factory=dict)
    itinerary: Optional[TravelPlan] = None
    tool_outputs: Dict[str, Any] = Field(default_factory=dict)

    blockers: List[str] = Field(default_factory=list)
    retry_count: int = 0
    current_stage: str = "initialized"


class StatePatch(BaseModel):
    """
    A structured state update generated from user intent or runtime events.

    The patch makes state transitions explicit instead of directly mutating the
    whole AgentState.
    """

    updates: Dict[str, Any]
    reason: str
    affected_fields: List[str]
    trigger_replan: bool = False
    locked_fields: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
