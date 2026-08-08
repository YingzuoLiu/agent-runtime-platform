from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    PlannerContext,
    PlannerDecision,
    PlannerProviderError,
    RequestClarificationDecision,
)
from .sandbox import ToolDescriptor


REQUEST_CLARIFICATION_TOOL_NAME = "request_clarification"
FINISH_TOOL_NAME = "finish"
_RESERVED_DECISION_TOOL_NAMES = frozenset(
    {REQUEST_CLARIFICATION_TOOL_NAME, FINISH_TOOL_NAME}
)


class _RequestClarificationArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=2_000)


class _FinishArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, max_length=4_000)
    output: dict[str, Any]


class _ResponsesResource(Protocol):
    def create(self, **kwargs: Any) -> Any:
        ...


class _OpenAIClient(Protocol):
    responses: _ResponsesResource


class OpenAIResponsesPlanner:
    """Generic planner adapter backed by OpenAI Responses function calling.

    The optional OpenAI SDK is imported only when no client is injected. This
    keeps the default runtime and deterministic CI path API-key-free while a
    fake Responses client can exercise the complete adapter contract.
    """

    def __init__(
        self,
        *,
        model: str,
        system_instructions: str,
        finish_output_schema: dict[str, Any],
        client: _OpenAIClient | None = None,
        api_key: str | None = None,
    ) -> None:
        if not model.strip():
            raise ValueError("model must not be empty")
        if not system_instructions.strip():
            raise ValueError("system_instructions must not be empty")

        self.model = model.strip()
        self.system_instructions = system_instructions.strip()
        self._finish_output_schema = _strict_object_schema(
            finish_output_schema,
            schema_name="finish_output_schema",
        )
        self._client = client if client is not None else self._load_client(api_key)

    def decide(self, context: PlannerContext) -> PlannerDecision:
        tools = self._response_tools(context.tools)
        try:
            response = self._client.responses.create(
                model=self.model,
                instructions=self.system_instructions,
                input=json.dumps(
                    context.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                tools=tools,
                tool_choice="required",
                parallel_tool_calls=False,
                store=False,
            )
        except Exception:
            # Provider exceptions can contain request details. Keep durable
            # runtime failures stable and avoid chaining provider internals.
            raise PlannerProviderError("OpenAI Responses request failed") from None

        return self._parse_response(response, context.tools)

    @staticmethod
    def _load_client(api_key: str | None) -> _OpenAIClient:
        try:
            from openai import OpenAI
        except ImportError:
            raise PlannerProviderError(
                "OpenAI planner requires the optional model-demo dependencies"
            ) from None

        try:
            if api_key is None:
                return OpenAI()
            return OpenAI(api_key=api_key)
        except Exception:
            raise PlannerProviderError("OpenAI client configuration failed") from None

    def _response_tools(self, descriptors: list[ToolDescriptor]) -> list[dict[str, Any]]:
        names = [descriptor.name for descriptor in descriptors]
        if len(names) != len(set(names)):
            raise ValueError("planner tool names must be unique")
        conflicts = _RESERVED_DECISION_TOOL_NAMES.intersection(names)
        if conflicts:
            rendered = ", ".join(sorted(conflicts))
            raise ValueError(f"planner tool names conflict with reserved decisions: {rendered}")

        finish_output_schema = copy.deepcopy(self._finish_output_schema)
        _rebase_local_schema_refs(
            finish_output_schema,
            embedding_pointer="#/properties/output",
        )
        tools = [self._function_tool(descriptor) for descriptor in descriptors]
        tools.extend(
            [
                {
                    "type": "function",
                    "name": REQUEST_CLARIFICATION_TOOL_NAME,
                    "description": (
                        "Ask the user one focused question when required information is missing."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 2_000,
                            },
                        },
                        "required": ["question"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                {
                    "type": "function",
                    "name": FINISH_TOOL_NAME,
                    "description": (
                        "Return the final candidate output for deterministic domain validation."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "message": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 4_000,
                            },
                            "output": finish_output_schema,
                        },
                        "required": ["message", "output"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
            ]
        )
        return tools

    @staticmethod
    def _function_tool(descriptor: ToolDescriptor) -> dict[str, Any]:
        return {
            "type": "function",
            "name": descriptor.name,
            "description": descriptor.description,
            "parameters": _strict_object_schema(
                descriptor.input_schema,
                schema_name=f"tool {descriptor.name!r} input schema",
            ),
            "strict": True,
        }

    @staticmethod
    def _parse_response(
        response: Any,
        descriptors: list[ToolDescriptor],
    ) -> PlannerDecision:
        output = _field(response, "output")
        if not isinstance(output, (list, tuple)):
            raise InvalidPlannerDecisionError(
                "OpenAI response output must be a list of response items"
            )

        function_calls = [item for item in output if _field(item, "type") == "function_call"]
        if len(function_calls) != 1:
            raise InvalidPlannerDecisionError(
                "OpenAI response must contain exactly one function call"
            )

        function_call = function_calls[0]
        name = _field(function_call, "name")
        raw_arguments = _field(function_call, "arguments")
        if not isinstance(name, str) or not name:
            raise InvalidPlannerDecisionError("OpenAI function call name is missing")
        if not isinstance(raw_arguments, str):
            raise InvalidPlannerDecisionError(
                "OpenAI function call arguments must be JSON text"
            )

        try:
            arguments = json.loads(raw_arguments)
        except json.JSONDecodeError:
            raise InvalidPlannerDecisionError(
                "OpenAI function call arguments are not valid JSON"
            ) from None
        if not isinstance(arguments, dict):
            raise InvalidPlannerDecisionError(
                "OpenAI function call arguments must decode to an object"
            )

        registered_tool_names = {descriptor.name for descriptor in descriptors}
        try:
            if name in registered_tool_names:
                return CallToolDecision(
                    tool_name=name,
                    arguments=arguments,
                    reason=f"Model selected registered tool {name!r}.",
                )
            if name == REQUEST_CLARIFICATION_TOOL_NAME:
                parsed = _RequestClarificationArguments.model_validate(arguments)
                return RequestClarificationDecision(
                    question=parsed.question,
                    reason="model_requested_clarification",
                )
            if name == FINISH_TOOL_NAME:
                parsed = _FinishArguments.model_validate(arguments)
                return FinishDecision(
                    message=parsed.message,
                    output=parsed.output,
                    reason="model_requested_finish",
                )
        except ValidationError:
            raise InvalidPlannerDecisionError(
                f"OpenAI function call {name!r} does not match its typed decision schema"
            ) from None

        raise InvalidPlannerDecisionError(f"OpenAI selected unknown function {name!r}")


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _strict_object_schema(
    schema: dict[str, Any],
    *,
    schema_name: str,
) -> dict[str, Any]:
    if not isinstance(schema, dict):
        raise ValueError(f"{schema_name} must be a JSON Schema object")
    normalized = copy.deepcopy(schema)
    if normalized.get("type") != "object" or not isinstance(
        normalized.get("properties"), dict
    ):
        raise ValueError(f"{schema_name} must describe an object with properties")
    _normalize_strict_schema_node(normalized)
    return normalized


def _normalize_strict_schema_node(node: Any) -> None:
    if not isinstance(node, dict):
        return

    node.pop("default", None)

    for keyword in (
        "$defs",
        "definitions",
        "dependencies",
        "dependentSchemas",
        "patternProperties",
        "properties",
    ):
        children = node.get(keyword)
        if isinstance(children, dict):
            for child in children.values():
                _normalize_strict_schema_node(child)

    for keyword in ("allOf", "anyOf", "oneOf", "prefixItems"):
        children = node.get(keyword)
        if isinstance(children, list):
            for child in children:
                _normalize_strict_schema_node(child)

    for keyword in (
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    ):
        _normalize_strict_schema_node(node.get(keyword))

    properties = node.get("properties")
    if node.get("type") == "object" or isinstance(properties, dict):
        if not isinstance(properties, dict):
            raise ValueError("strict object schemas must define properties")
        node["required"] = list(properties)
        node["additionalProperties"] = False


def _rebase_local_schema_refs(node: Any, *, embedding_pointer: str) -> None:
    if isinstance(node, list):
        for item in node:
            _rebase_local_schema_refs(item, embedding_pointer=embedding_pointer)
        return
    if not isinstance(node, dict):
        return

    reference = node.get("$ref")
    if reference == "#":
        node["$ref"] = embedding_pointer
    elif isinstance(reference, str) and reference.startswith("#/"):
        node["$ref"] = f"{embedding_pointer}{reference[1:]}"

    for value in node.values():
        _rebase_local_schema_refs(value, embedding_pointer=embedding_pointer)
