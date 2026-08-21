from __future__ import annotations

import pytest

from examples.action_recovery_proof import (
    ProofFailure,
    SCENARIOS,
    _verify_scenario,
)


def runtime_events(*, retry_mode: str, attempts: int, terminal_status: str) -> list[dict]:
    events = [
        {
            "sequence": 1,
            "event_type": "external_action.prepared",
            "status": "prepared",
            "dispatch_count": 0,
            "retry_mode": retry_mode,
        }
    ]
    for attempt in range(1, attempts + 1):
        events.append(
            {
                "sequence": len(events) + 1,
                "event_type": "external_action.dispatch_started",
                "status": "dispatching",
                "dispatch_count": attempt,
                "retry_mode": retry_mode,
            }
        )
    events.append(
        {
            "sequence": len(events) + 1,
            "event_type": f"external_action.{terminal_status}",
            "status": terminal_status,
            "dispatch_count": attempts,
            "retry_mode": retry_mode,
        }
    )
    return events


def provider_state(*, scenario: str, attempts: int, include_receipt: bool) -> dict:
    event_types = [
        "attempt.received",
        "effect.committed",
        "fault.release_requested",
        "response.ambiguous",
    ]
    if include_receipt:
        event_types.extend(["attempt.received", "receipt.replayed"])
    return {
        "scenario": scenario,
        "attempt_count": attempts,
        "effect_count": 1,
        "provider_reference": "delivery_reference",
        "waiting_for_release": False,
        "events": [
            {"event_sequence": index, "event_type": event_type}
            for index, event_type in enumerate(event_types, start=1)
        ],
    }


def test_proof_verifier_accepts_both_capability_paths():
    idempotent, unsafe = SCENARIOS
    _verify_scenario(
        idempotent,
        action={
            "action_id": "action-idempotent",
            "status": "succeeded",
            "result": {"provider_reference": "delivery_reference"},
        },
        action_events=runtime_events(
            retry_mode="provider_idempotent",
            attempts=2,
            terminal_status="succeeded",
        ),
        provider_state=provider_state(
            scenario="idempotent",
            attempts=2,
            include_receipt=True,
        ),
        repeated={"action_id": "action-idempotent", "status": "succeeded"},
        runtime_started_before="first",
        runtime_started_after="second",
    )
    _verify_scenario(
        unsafe,
        action={
            "action_id": "action-unsafe",
            "status": "outcome_unknown",
            "result": None,
            "error_code": "external_action_outcome_unknown",
        },
        action_events=runtime_events(
            retry_mode="unsafe",
            attempts=1,
            terminal_status="outcome_unknown",
        ),
        provider_state=provider_state(
            scenario="unsafe",
            attempts=1,
            include_receipt=False,
        ),
        repeated={"action_id": "action-unsafe", "status": "outcome_unknown"},
        runtime_started_before="second",
        runtime_started_after="third",
    )


def test_proof_verifier_rejects_a_second_unsafe_dispatch():
    unsafe = SCENARIOS[1]
    with pytest.raises(ProofFailure, match="attempt count"):
        _verify_scenario(
            unsafe,
            action={
                "action_id": "action-unsafe",
                "status": "outcome_unknown",
                "error_code": "external_action_outcome_unknown",
            },
            action_events=runtime_events(
                retry_mode="unsafe",
                attempts=2,
                terminal_status="outcome_unknown",
            ),
            provider_state=provider_state(
                scenario="unsafe",
                attempts=2,
                include_receipt=False,
            ),
            repeated={"action_id": "action-unsafe", "status": "outcome_unknown"},
            runtime_started_before="first",
            runtime_started_after="second",
        )
