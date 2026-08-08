from __future__ import annotations

import builtins
import json
from types import SimpleNamespace

import pytest

from runtime_service.openai_planner import OpenAIResponsesPlanner
from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    PlannerContext,
    PlannerProviderError,
    RequestClarificationDecision,
    ToolObservation,
)
from runtime_service.sandbox import ToolDescriptor, ToolPolicy


class FakeResponsesResource:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


class FakeClient:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.responses = FakeResponsesResource(response=response, error=error)


def function_response(name: str, arguments) -> SimpleNamespace:
    return SimpleNamespace(
        output=[
            SimpleNamespace(type="reasoning", summary=[]),
            SimpleNamespace(
                type="function_call",
                name=name,
                arguments=(arguments if isinstance(arguments, str) else json.dumps(arguments)),
            ),
        ]
    )


def tool_descriptor(name: str = "search_options") -> ToolDescriptor:
    return ToolDescriptor(
        name=name,
        description="Search deterministic options.",
        input_schema={
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "filters": {
                    "type": "object",
                    "properties": {
                        "nonstop": {"type": "boolean", "default": False},
                    },
                },
                "limit": {"type": "integer", "default": 3},
            },
            "required": ["destination"],
        },
        policy=ToolPolicy(),
    )


def planner_context(*, tools: list[ToolDescriptor] | None = None) -> PlannerContext:
    return PlannerContext(
        run_id="run-openai-1",
        thread_id="thread-openai-1",
        runtime_input={"user_message": "Plan a trip to Tokyo."},
        state={"current_stage": "planning"},
        tools=tools or [tool_descriptor()],
        observations=[
            ToolObservation(
                step_id="tool-call-1",
                tool_name="seed_context",
                arguments={"query": "Tokyo"},
                result={"available": True},
            )
        ],
        tool_call_count=1,
        max_tool_calls=5,
    )


def make_planner(client: FakeClient) -> OpenAIResponsesPlanner:
    return OpenAIResponsesPlanner(
        model="test-model",
        system_instructions="Choose exactly one typed next action.",
        finish_output_schema={
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "notes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": [],
                },
            },
            "required": ["destination"],
        },
        client=client,
    )


def test_registered_tool_call_uses_strict_single_call_responses_request():
    client = FakeClient(
        function_response(
            "search_options",
            {"destination": "Tokyo", "filters": {"nonstop": True}, "limit": 3},
        )
    )
    planner = make_planner(client)
    context = planner_context()

    decision = planner.decide(context)

    assert decision == CallToolDecision(
        tool_name="search_options",
        arguments={"destination": "Tokyo", "filters": {"nonstop": True}, "limit": 3},
        reason="Model selected registered tool 'search_options'.",
    )
    assert len(client.responses.calls) == 1
    request = client.responses.calls[0]
    assert request["model"] == "test-model"
    assert request["instructions"] == "Choose exactly one typed next action."
    assert request["tool_choice"] == "required"
    assert request["parallel_tool_calls"] is False
    assert request["store"] is False
    assert json.loads(request["input"]) == context.model_dump(mode="json")

    tools = {tool["name"]: tool for tool in request["tools"]}
    assert set(tools) == {"search_options", "request_clarification", "finish"}
    search_schema = tools["search_options"]["parameters"]
    assert tools["search_options"]["strict"] is True
    assert search_schema["additionalProperties"] is False
    assert search_schema["required"] == ["destination", "filters", "limit"]
    assert search_schema["properties"]["filters"]["additionalProperties"] is False
    assert search_schema["properties"]["filters"]["required"] == ["nonstop"]
    assert "default" not in search_schema["properties"]["limit"]
    clarification_schema = tools["request_clarification"]["parameters"]
    assert set(clarification_schema["properties"]) == {"question"}
    assert clarification_schema["required"] == ["question"]
    assert clarification_schema["additionalProperties"] is False
    finish_schema = tools["finish"]["parameters"]
    assert set(finish_schema["properties"]) == {"message", "output"}
    assert finish_schema["required"] == ["message", "output"]
    assert finish_schema["additionalProperties"] is False
    finish_output = finish_schema["properties"]["output"]
    assert finish_output["required"] == ["destination", "notes"]
    assert finish_output["additionalProperties"] is False
    assert "default" not in finish_output["properties"]["notes"]


def test_request_clarification_function_maps_to_typed_decision():
    client = FakeClient(
        function_response(
            "request_clarification",
            {
                "question": "What is your maximum budget?",
            },
        )
    )

    decision = make_planner(client).decide(planner_context())

    assert decision == RequestClarificationDecision(
        question="What is your maximum budget?",
        reason="model_requested_clarification",
    )


def test_finish_function_maps_to_typed_decision():
    client = FakeClient(
        function_response(
            "finish",
            {
                "message": "Here is the candidate plan.",
                "output": {"destination": "Tokyo", "notes": ["Review before booking."]},
            },
        )
    )

    decision = make_planner(client).decide(planner_context())

    assert decision == FinishDecision(
        message="Here is the candidate plan.",
        output={"destination": "Tokyo", "notes": ["Review before booking."]},
        reason="model_requested_finish",
    )


def test_provider_transport_error_is_sanitized_and_not_chained():
    secret = "sk-provider-error-must-not-leak"
    client = FakeClient(error=RuntimeError(f"request failed with {secret}"))

    with pytest.raises(PlannerProviderError, match="OpenAI Responses request failed") as caught:
        make_planner(client).decide(planner_context())

    assert caught.value.__cause__ is None
    assert secret not in str(caught.value)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (SimpleNamespace(output="not-a-list"), "output must be a list"),
        (SimpleNamespace(output=[]), "exactly one function call"),
        (
            SimpleNamespace(
                output=[
                    SimpleNamespace(type="function_call", name="a", arguments="{}"),
                    SimpleNamespace(type="function_call", name="b", arguments="{}"),
                ]
            ),
            "exactly one function call",
        ),
        (function_response("search_options", "{bad json"), "not valid JSON"),
        (function_response("search_options", "[]"), "decode to an object"),
        (function_response("unknown_tool", {}), "selected unknown function"),
        (function_response("request_clarification", {}), "typed decision schema"),
    ],
)
def test_invalid_response_shapes_and_arguments_fail_closed(response, message):
    with pytest.raises(InvalidPlannerDecisionError, match=message):
        make_planner(FakeClient(response)).decide(planner_context())


def test_mapping_response_items_are_supported_for_fake_clients():
    response = {
        "output": [
            {
                "type": "function_call",
                "name": "search_options",
                "arguments": '{"destination":"Tokyo"}',
            }
        ]
    }

    decision = make_planner(FakeClient(response)).decide(planner_context())

    assert isinstance(decision, CallToolDecision)
    assert decision.arguments == {"destination": "Tokyo"}


@pytest.mark.parametrize(
    ("function_name", "arguments"),
    [
        (
            "request_clarification",
            {
                "question": "What is your budget?",
                "reason": "sensitive-marker-must-not-enter-decision",
            },
        ),
        (
            "finish",
            {
                "message": "Candidate plan.",
                "output": {"destination": "Tokyo", "notes": []},
                "reason": "sensitive-marker-must-not-enter-decision",
            },
        ),
    ],
)
def test_model_supplied_reason_is_rejected_at_the_pseudo_function_boundary(
    function_name,
    arguments,
):
    marker = "sensitive-marker-must-not-enter-decision"

    with pytest.raises(InvalidPlannerDecisionError, match="typed decision schema") as caught:
        make_planner(FakeClient(function_response(function_name, arguments))).decide(
            planner_context()
        )

    assert marker not in str(caught.value)


def test_injected_client_path_does_not_import_optional_sdk(monkeypatch):
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "openai":
            raise AssertionError("optional SDK import should not occur")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    planner = make_planner(
        FakeClient(function_response("search_options", {"destination": "Tokyo"}))
    )

    assert isinstance(planner.decide(planner_context()), CallToolDecision)


def test_missing_optional_sdk_has_a_clear_configuration_failure(monkeypatch):
    original_import = builtins.__import__

    def missing_openai(name, *args, **kwargs):
        if name == "openai":
            raise ImportError("not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_openai)

    with pytest.raises(PlannerProviderError, match="optional model-demo dependencies"):
        OpenAIResponsesPlanner(
            model="test-model",
            system_instructions="Choose one action.",
            finish_output_schema={"type": "object", "properties": {}},
        )


@pytest.mark.parametrize(
    ("finish_schema", "message"),
    [
        ({"type": "array", "items": {"type": "string"}}, "must describe an object"),
        ({"type": "object"}, "must describe an object"),
    ],
)
def test_finish_output_schema_must_be_an_object_schema(finish_schema, message):
    with pytest.raises(ValueError, match=message):
        OpenAIResponsesPlanner(
            model="test-model",
            system_instructions="Choose one action.",
            finish_output_schema=finish_schema,
            client=FakeClient(),
        )


def test_registered_tools_cannot_shadow_terminal_decision_functions():
    planner = make_planner(FakeClient())

    with pytest.raises(ValueError, match="conflict with reserved decisions"):
        planner.decide(planner_context(tools=[tool_descriptor("finish")]))
