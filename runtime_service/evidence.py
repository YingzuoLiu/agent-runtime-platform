from __future__ import annotations

from typing import Any, Protocol

from agent.contracts import RuntimeExecutionError

from .workflow_store import SQLiteWorkflowStore


class RunEventSink(Protocol):
    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> Any: ...

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[Any]: ...


class EvidenceProjector:
    """Projects durable workflow evidence into the public Run event stream.

    Workflow events remain authoritative and are always written first. Callers
    retain responsibility for retry and failure semantics around the public
    mirror boundary.
    """

    _MIRRORED_EVENT_TYPES = {
        "planner.decision",
        "policy.decision",
        "tool.result",
        "loop.outcome",
    }

    def __init__(
        self,
        *,
        workflow_store: SQLiteWorkflowStore,
        run_event_sink: RunEventSink,
    ) -> None:
        self.workflow_store = workflow_store
        self.run_event_sink = run_event_sink

    def record(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        evidence_id = str(payload["evidence_id"])
        existing = self.workflow_evidence(run_id, event_type, evidence_id)
        if existing is None:
            self.workflow_store.append_event(run_id, event_type, payload)
            existing = payload
        elif existing != payload:
            raise RuntimeExecutionError(
                "invalid_planner_decision",
                f"Durable evidence mismatch for {evidence_id}.",
            )
        self.ensure_run_evidence(run_id, event_type, existing)

    def mirror(self, run_id: str) -> None:
        for event in self.workflow_store.list_events(run_id):
            if not (
                event.event_type.startswith("external_action.")
                or event.event_type in self._MIRRORED_EVENT_TYPES
            ):
                continue
            if "evidence_id" not in event.payload:
                continue
            self.ensure_run_evidence(run_id, event.event_type, event.payload)

    def ensure_run_evidence(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        evidence_id = payload.get("evidence_id")
        for event in self.run_event_sink.list_events(run_id):
            if event.event_type == event_type and event.payload.get("evidence_id") == evidence_id:
                return
        self.run_event_sink.append_event(run_id, event_type, payload)

    def workflow_evidence(
        self,
        run_id: str,
        event_type: str,
        evidence_id: str,
    ) -> dict[str, Any] | None:
        for event in self.workflow_store.list_events(run_id):
            if event.event_type == event_type and event.payload.get("evidence_id") == evidence_id:
                return event.payload
        return None
