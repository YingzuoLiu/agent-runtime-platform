from __future__ import annotations

import pytest
from pydantic import ValidationError

from runtime_service.planner import (
    PLANNER_DECISION_ADAPTER,
    CallToolDecision,
    FinishDecision,
    RequestClarificationDecision,
)


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (
            CallToolDecision,
            {
                "decision_type": "CALL_TOOL",
                "tool_name": "increment",
                "arguments": {"value": 2},
                "reason": "Need a grounded value.",
            },
        ),
        (
            RequestClarificationDecision,
            {
                "decision_type": "REQUEST_CLARIFICATION",
                "question": "Which value should I use?",
                "reason": "The required value is missing.",
            },
        ),
        (
            FinishDecision,
            {
                "decision_type": "FINISH",
                "message": "The calculation is complete.",
                "output": {"value": 3},
                "reason": "The tool result is sufficient.",
            },
        ),
    ],
)
def test_planner_decision_models_accept_only_their_strict_shape(model, payload):
    decision = model.model_validate(payload)

    assert decision.model_dump(mode="json") == payload

    with pytest.raises(ValidationError, match="extra_forbidden"):
        model.model_validate({**payload, "unexpected": True})


@pytest.mark.parametrize(
    ("payload", "expected_type"),
    [
        (
            {
                "decision_type": "CALL_TOOL",
                "tool_name": "increment",
                "arguments": {"value": 2},
                "reason": "Need a grounded value.",
            },
            CallToolDecision,
        ),
        (
            {
                "decision_type": "REQUEST_CLARIFICATION",
                "question": "Which value should I use?",
                "reason": "The required value is missing.",
            },
            RequestClarificationDecision,
        ),
        (
            {
                "decision_type": "FINISH",
                "message": "The calculation is complete.",
                "output": {"value": 3},
                "reason": "The tool result is sufficient.",
            },
            FinishDecision,
        ),
    ],
)
def test_discriminated_union_returns_the_exact_typed_decision(payload, expected_type):
    decision = PLANNER_DECISION_ADAPTER.validate_python(payload)

    assert type(decision) is expected_type


@pytest.mark.parametrize(
    "payload",
    [
        {
            "decision_type": "CALL_TOOL",
            "tool_name": "increment",
            "arguments": {"value": 2},
            "reason": "Need a grounded value.",
            "unexpected": True,
        },
        {
            "decision_type": "REQUEST_CLARIFICATION",
            "question": "Which value should I use?",
            "reason": "The required value is missing.",
            "unexpected": True,
        },
        {
            "decision_type": "FINISH",
            "message": "The calculation is complete.",
            "output": {"value": 3},
            "reason": "The tool result is sufficient.",
            "unexpected": True,
        },
    ],
)
def test_discriminated_union_rejects_extra_fields_for_every_decision(payload):
    with pytest.raises(ValidationError, match="extra_forbidden"):
        PLANNER_DECISION_ADAPTER.validate_python(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "decision_type": "USE_TOOL",
            "tool_name": "increment",
            "arguments": {"value": 2},
            "reason": "Unknown decision type.",
        },
        {
            "tool_name": "increment",
            "arguments": {"value": 2},
            "reason": "Missing discriminator.",
        },
    ],
)
def test_discriminated_union_rejects_unknown_or_missing_decision_type(payload):
    with pytest.raises(ValidationError):
        PLANNER_DECISION_ADAPTER.validate_python(payload)
