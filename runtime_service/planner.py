from __future__ import annotations

from typing import Annotated, Any, Literal, Protocol, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from .sandbox import ToolDescriptor


class CallToolDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_type: Literal["CALL_TOOL"] = "CALL_TOOL"
    tool_name: str = Field(min_length=1, max_length=200)
    arguments: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(min_length=1, max_length=1_000)


class RequestClarificationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_type: Literal["REQUEST_CLARIFICATION"] = "REQUEST_CLARIFICATION"
    question: str = Field(min_length=1, max_length=2_000)
    reason: str = Field(min_length=1, max_length=1_000)


class FinishDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_type: Literal["FINISH"] = "FINISH"
    message: str = Field(min_length=1, max_length=4_000)
    output: dict[str, Any]
    reason: str = Field(min_length=1, max_length=1_000)


PlannerDecision = Annotated[
    Union[CallToolDecision, RequestClarificationDecision, FinishDecision],
    Field(discriminator="decision_type"),
]
PLANNER_DECISION_ADAPTER = TypeAdapter(PlannerDecision)


class ToolObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_id: str
    tool_name: str
    arguments: dict[str, Any]
    result: dict[str, Any]
    cached: bool = False


class PlannerContext(BaseModel):
    """One domain-neutral decision snapshot supplied to a planner."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    thread_id: str
    runtime_input: dict[str, Any]
    state: dict[str, Any]
    tools: list[ToolDescriptor]
    observations: list[ToolObservation] = Field(default_factory=list)
    tool_call_count: int = Field(ge=0)
    max_tool_calls: int = Field(ge=1)


class Planner(Protocol):
    def decide(self, context: PlannerContext) -> PlannerDecision | dict[str, Any]:
        ...


class PlannerProviderError(RuntimeError):
    """The configured model/provider could not produce a response."""


class InvalidPlannerDecisionError(ValueError):
    """A provider response could not be represented by the typed contract."""
