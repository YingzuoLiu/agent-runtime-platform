from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from agent.contracts import RuntimeExecutionError
from runtime_service.evidence import EvidenceProjector
from runtime_service.workflow_store import SQLiteWorkflowStore


RUN_ID = "run-evidence-projector"


@dataclass
class RecordedEvent:
    event_type: str
    payload: dict[str, Any]


class RecordingRunEventSink:
    def __init__(self) -> None:
        self.events: list[RecordedEvent] = []
        self.fail_before_append = False
        self.raise_after_append = False

    def append_event(
        self,
        _run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RecordedEvent:
        if self.fail_before_append:
            self.fail_before_append = False
            raise OSError("injected run evidence outage")
        event = RecordedEvent(event_type=event_type, payload=payload or {})
        self.events.append(event)
        if self.raise_after_append:
            self.raise_after_append = False
            raise OSError("injected post-append failure")
        return event

    def list_events(
        self,
        _run_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RecordedEvent]:
        del after_sequence
        return list(self.events)


def build_projector(
    tmp_path: Path,
) -> tuple[SQLiteWorkflowStore, RecordingRunEventSink, EvidenceProjector]:
    workflow_store = SQLiteWorkflowStore(tmp_path / "evidence.db")
    workflow_store.create_or_get_execution(
        RUN_ID,
        "evidence-projector-test:1.0.0",
        "input-hash",
    )
    sink = RecordingRunEventSink()
    return (
        workflow_store,
        sink,
        EvidenceProjector(
            workflow_store=workflow_store,
            run_event_sink=sink,
        ),
    )


def test_record_is_workflow_first_and_mirror_repairs_public_gap(tmp_path: Path):
    workflow_store, sink, projector = build_projector(tmp_path)
    payload = {
        "evidence_id": "planner:1",
        "decision_index": 1,
        "outcome": "accepted",
    }
    sink.fail_before_append = True

    with pytest.raises(OSError, match="run evidence outage"):
        projector.record(RUN_ID, "planner.decision", payload)

    assert [event.payload for event in workflow_store.list_events(RUN_ID)] == [payload]
    assert sink.events == []

    projector.mirror(RUN_ID)

    assert [(event.event_type, event.payload) for event in sink.events] == [
        ("planner.decision", payload)
    ]


def test_record_is_idempotent_and_rejects_durable_payload_drift(tmp_path: Path):
    _workflow_store, sink, projector = build_projector(tmp_path)
    payload = {
        "evidence_id": "policy:call-0001",
        "step_id": "call-0001",
        "outcome": "allowed",
    }

    projector.record(RUN_ID, "policy.decision", payload)
    projector.record(RUN_ID, "policy.decision", payload)

    assert len(sink.events) == 1
    with pytest.raises(RuntimeExecutionError) as raised:
        projector.record(
            RUN_ID,
            "policy.decision",
            {**payload, "outcome": "denied"},
        )
    assert raised.value.code == "invalid_planner_decision"
    assert len(sink.events) == 1


def test_record_retry_does_not_duplicate_run_event_after_post_append_error(
    tmp_path: Path,
):
    _workflow_store, sink, projector = build_projector(tmp_path)
    payload = {
        "evidence_id": "loop:outcome",
        "outcome": "finished",
        "message": "done",
    }
    sink.raise_after_append = True

    with pytest.raises(OSError, match="post-append failure"):
        projector.record(RUN_ID, "loop.outcome", payload)

    projector.record(RUN_ID, "loop.outcome", payload)

    assert [(event.event_type, event.payload) for event in sink.events] == [
        ("loop.outcome", payload)
    ]


def test_mirror_preserves_source_order_and_filters_non_public_events(
    tmp_path: Path,
):
    workflow_store, sink, projector = build_projector(tmp_path)
    planner_payload = {"evidence_id": "planner:1", "outcome": "accepted"}
    action_payload = {
        "evidence_id": "external-action:action-1:prepared",
        "action_id": "action-1",
    }
    loop_payload = {"evidence_id": "loop:outcome", "outcome": "finished"}
    workflow_store.append_event(RUN_ID, "planner.decision", planner_payload)
    workflow_store.append_event(
        RUN_ID,
        "step.started",
        {"evidence_id": "step:call-0001", "step_id": "call-0001"},
    )
    workflow_store.append_event(RUN_ID, "external_action.prepared", action_payload)
    workflow_store.append_event(
        RUN_ID,
        "tool.result",
        {"status": "completed"},
    )
    workflow_store.append_event(RUN_ID, "loop.outcome", loop_payload)

    projector.mirror(RUN_ID)

    assert [(event.event_type, event.payload) for event in sink.events] == [
        ("planner.decision", planner_payload),
        ("external_action.prepared", action_payload),
        ("loop.outcome", loop_payload),
    ]
