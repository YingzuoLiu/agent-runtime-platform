"""Deterministic characterization of persisted Travel state boundaries.

This evaluator observes the current production implementations without changing
their schemas or mutation paths. It uses the scripted Travel planner, synthetic
offline tools, the managed Run lifecycle, and SQLite stores. Random identifiers
and wall-clock values are deliberately excluded from the report.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from domains.travel.dynamic_runtime import DynamicTravelRuntime  # noqa: E402
from domains.travel.memory import TravelMemoryPolicy  # noqa: E402
from domains.travel.planner import ScriptedTravelPlanner  # noqa: E402
from domains.travel.preferences import (  # noqa: E402
    parse_explicit_travel_preferences,
)
from domains.travel.review.evidence import PlanEvidenceBuilder  # noqa: E402
from domains.travel.review.models import (  # noqa: E402
    WorkflowReviewResult,
    WorkflowStatus as ReviewWorkflowStatus,
)
from domains.travel.review.orchestrator import WorkflowOrchestrator  # noqa: E402
from domains.travel.runtime import TravelAgentRuntime, TravelMessageInput  # noqa: E402
from domains.travel.state import AgentState  # noqa: E402
from domains.travel.tools import build_travel_tool_registry  # noqa: E402
from domains.travel.tools.handlers import (  # noqa: E402
    rank_trip_options,
    route_cost_summary,
    search_trip_options,
)
from runtime_service import (  # noqa: E402
    AgentRegistry,
    GovernedMemory,
    RunCreateRequest,
    RunRecord,
    RunStatus,
    RuntimeManager,
    SQLiteMemoryStore,
    SQLiteRunStore,
    TenantContext,
    build_default_registry,
)
from runtime_service.dynamic_loop import DynamicToolLoop  # noqa: E402
from runtime_service.sandbox import (  # noqa: E402
    ToolExecutionResult,
    ToolExecutionStatus,
)
from runtime_service.workflow_store import SQLiteWorkflowStore  # noqa: E402


TENANT_ID = "state-boundary-tenant"
SUBJECT_ID = "state-boundary-subject"
TRAVEL_PERMISSIONS = ("memory:read", "memory:write", "tools:execute")
FIXED_TIME = "2026-08-22T00:00:00+00:00"


@dataclass(frozen=True)
class CheckpointObservation:
    revision: int
    raw_json: str
    state: dict[str, Any]


@dataclass(frozen=True)
class RunStateObservation:
    raw_json: str
    state: dict[str, Any]


class ManagedHarness:
    """Small deterministic driver around the real managed Run lifecycle."""

    def __init__(
        self,
        *,
        database_path: Path,
        store: SQLiteRunStore,
        registry: AgentRegistry,
        permissions: tuple[str, ...] = (),
    ) -> None:
        self.database_path = database_path
        self.store = store
        self.manager = RuntimeManager(
            store,
            registry,
            owner_id="state-boundary-evaluator",
            lease_duration_seconds=10,
            heartbeat_interval_seconds=2,
            poll_interval_seconds=0.01,
        )
        self.tenant_context = TenantContext(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            permissions=permissions,
        )

    def __enter__(self) -> "ManagedHarness":
        self.manager.start()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.manager.stop()

    def submit(
        self,
        *,
        thread_id: str,
        agent_version: str,
        message: str,
    ) -> RunRecord:
        submitted = self.manager.submit(
            RunCreateRequest(
                thread_id=thread_id,
                agent_id="travel-agent",
                agent_version=agent_version,
                user_message=message,
            ),
            tenant_context=self.tenant_context,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            persisted = self.store.get_run_internal(submitted.run_id)
            if persisted is not None and persisted.status.is_terminal:
                if persisted.status != RunStatus.COMPLETED:
                    raise AssertionError(
                        f"characterization Run failed: {persisted.error_code}: {persisted.error}"
                    )
                return persisted
            time.sleep(0.01)
        raise AssertionError(f"characterization Run did not finish: {submitted.run_id}")

    def checkpoint(self, thread_id: str) -> CheckpointObservation:
        with sqlite3.connect(self.database_path) as connection:
            row = connection.execute(
                """
                SELECT revision, state_json FROM thread_states
                WHERE tenant_id = ? AND thread_id = ?
                """,
                (TENANT_ID, thread_id),
            ).fetchone()
        if row is None:
            raise AssertionError(f"missing checkpoint for {thread_id}")
        raw_json = str(row[1])
        return CheckpointObservation(
            revision=int(row[0]),
            raw_json=raw_json,
            state=json.loads(raw_json),
        )

    def run_state(self, run_id: str) -> RunStateObservation:
        with sqlite3.connect(self.database_path) as connection:
            row = connection.execute(
                "SELECT state_json FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None or row[0] is None:
            raise AssertionError(f"missing completed Run state for {run_id}")
        raw_json = str(row[0])
        return RunStateObservation(
            raw_json=raw_json,
            state=json.loads(raw_json),
        )


class RecordingDeterministicReview(WorkflowOrchestrator):
    """Use the real evidence builder while removing UUID/time noise from the eval."""

    def __init__(self) -> None:
        super().__init__()
        self.observations: list[dict[str, Any]] = []

    def run_sync(self, state: AgentState) -> WorkflowReviewResult:
        evidence = PlanEvidenceBuilder().build(state)
        self.observations.append(
            {
                "candidate_plan_present": evidence.candidate_plan is not None,
                "budget_limit": evidence.budget_limit,
                "cost_ledger_status": evidence.cost_ledger_status.value,
                "cost_ledger_total": sum(item.amount for item in evidence.cost_ledger),
                "cost_source_ids": sorted({item.source_id for item in evidence.cost_ledger}),
                "evidence_issues": list(evidence.evidence_issues),
            }
        )
        invocation = len(self.observations)
        return WorkflowReviewResult(
            workflow_run_id=f"workflow_state_boundary_{invocation:02d}",
            status=ReviewWorkflowStatus.COMPLETED,
            tasks=[],
            evidence_issues=evidence.evidence_issues,
            findings=[],
            directives=[],
            started_at=FIXED_TIME,
            finished_at=FIXED_TIME,
            duration_ms=0,
        )


class DirectSyntheticSandbox:
    """Call repository-owned offline handlers without subprocess timing noise."""

    _HANDLERS = {
        "search_trip_options": search_trip_options,
        "rank_trip_options": rank_trip_options,
        "route_cost_summary": route_cost_summary,
    }

    def execute(self, tool_name: str, arguments: dict[str, Any]) -> ToolExecutionResult:
        result = self._HANDLERS[tool_name](arguments)
        return ToolExecutionResult(
            execution_id=f"state-boundary-{tool_name}",
            tool_name=tool_name,
            status=ToolExecutionStatus.COMPLETED,
            result=result,
            duration_ms=0,
            exit_code=0,
        )


def _build_dynamic_harness(
    database_path: Path,
) -> tuple[ManagedHarness, SQLiteMemoryStore]:
    store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    workflow_store = SQLiteWorkflowStore(database_path)
    tool_registry = build_travel_tool_registry()
    loop = DynamicToolLoop(
        planner=ScriptedTravelPlanner(),
        tool_registry=tool_registry,
        tool_sandbox=DirectSyntheticSandbox(),  # type: ignore[arg-type]
        workflow_store=workflow_store,
        run_event_sink=store,
        workflow_type="state-boundary:travel-agent:1.1.0",
        max_tool_calls=3,
    )
    governed_memory = GovernedMemory(memory_store, store)
    registry = build_default_registry(
        governed_memory_travel_runtime_factory=lambda: DynamicTravelRuntime(
            loop,
            governed_memory=governed_memory,
            memory_policy=TravelMemoryPolicy(),
            preference_parser=parse_explicit_travel_preferences,
        )
    )
    return (
        ManagedHarness(
            database_path=database_path,
            store=store,
            registry=registry,
            permissions=TRAVEL_PERMISSIONS,
        ),
        memory_store,
    )


def _build_review_harness(
    database_path: Path,
) -> tuple[ManagedHarness, RecordingDeterministicReview]:
    store = SQLiteRunStore(database_path)
    review = RecordingDeterministicReview()
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "0.5.0",
        lambda: TravelAgentRuntime(
            retry_limit=2,
            enable_review_workflow=True,
            review_orchestrator=review,
        ),
        description="Deterministic state-boundary characterization runtime.",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return (
        ManagedHarness(
            database_path=database_path,
            store=store,
            registry=registry,
        ),
        review,
    )


def _build_baseline_harness(database_path: Path) -> ManagedHarness:
    store = SQLiteRunStore(database_path)
    return ManagedHarness(
        database_path=database_path,
        store=store,
        registry=build_default_registry(),
    )


def _memory_history(memory_store: SQLiteMemoryStore) -> list[dict[str, Any]]:
    records = memory_store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id="travel",
        include_inactive=True,
    )
    return [
        {
            "key": record.key,
            "version": record.version,
            "value": record.value,
            "status": record.status.value,
        }
        for record in sorted(records, key=lambda item: (item.key, item.version))
    ]


def _snapshot_values(
    memory_store: SQLiteMemoryStore,
    run_id: str,
) -> list[dict[str, Any]]:
    snapshot = memory_store.get_run_snapshot(run_id)
    if snapshot is None:
        raise AssertionError(f"missing memory snapshot for {run_id}")
    return [
        {
            "key": memory.key,
            "version": memory.version,
            "value": memory.value,
        }
        for memory in snapshot.memories
    ]


def characterize_memory_supersession(workspace: Path) -> dict[str, Any]:
    harness, memory_store = _build_dynamic_harness(workspace / "memory-supersession.db")
    with harness:
        harness.submit(
            thread_id="memory-create",
            agent_version="1.1.0",
            message=("Plan a 5-day Tokyo trip under 9000 SGD. I avoid red-eye flights."),
        )
        harness.submit(
            thread_id="memory-replace",
            agent_version="1.1.0",
            message=("Plan a 5-day Tokyo trip under 9000 SGD. I allow red-eye flights."),
        )
        observed = harness.submit(
            thread_id="memory-observe",
            agent_version="1.1.0",
            message="Plan a 5-day Tokyo trip under 9000 SGD.",
        )
        checkpoint = harness.checkpoint("memory-observe")

    observed_state = AgentState.model_validate(observed.state)
    return {
        "logical_key": TravelMemoryPolicy.AVOID_RED_EYE_KEY,
        "version_history": _memory_history(memory_store),
        "new_run_snapshot": _snapshot_values(memory_store, observed.run_id),
        "new_run_behavior": {
            "itinerary_flight_type": (
                observed_state.itinerary.flight_type
                if observed_state.itinerary is not None
                else None
            ),
            "checkpoint_preference_keys": sorted(checkpoint.state["preferences"].keys()),
        },
    }


def characterize_sealed_snapshot(workspace: Path) -> dict[str, Any]:
    harness, memory_store = _build_dynamic_harness(workspace / "sealed-snapshot.db")
    with harness:
        harness.submit(
            thread_id="sealed-create",
            agent_version="1.1.0",
            message=("Plan a 5-day Tokyo trip under 9000 SGD. I avoid red-eye flights."),
        )
        sealed_run = harness.submit(
            thread_id="sealed-view",
            agent_version="1.1.0",
            message="Plan a 5-day Tokyo trip under 9000 SGD.",
        )
        before_update = _snapshot_values(memory_store, sealed_run.run_id)
        harness.submit(
            thread_id="sealed-update",
            agent_version="1.1.0",
            message=("Plan a 5-day Tokyo trip under 9000 SGD. I allow red-eye flights."),
        )
        after_update = _snapshot_values(memory_store, sealed_run.run_id)
        new_run = harness.submit(
            thread_id="sealed-new-run",
            agent_version="1.1.0",
            message="Plan a 5-day Tokyo trip under 9000 SGD.",
        )

    return {
        "sealed_run_before_update": before_update,
        "sealed_run_after_update": after_update,
        "sealed_view_unchanged": before_update == after_update,
        "new_run_after_update": _snapshot_values(memory_store, new_run.run_id),
    }


def characterize_budget_replacement(workspace: Path) -> dict[str, Any]:
    harness, memory_store = _build_dynamic_harness(workspace / "budget-replacement.db")
    with harness:
        harness.submit(
            thread_id="budget-thread",
            agent_version="1.1.0",
            message=("Plan a 5-day Tokyo trip under 9000 SGD. I avoid red-eye flights."),
        )
        before = harness.checkpoint("budget-thread")
        memory_before = _memory_history(memory_store)
        replacement_run = harness.submit(
            thread_id="budget-thread",
            agent_version="1.1.0",
            message="Replace the budget with 12000 SGD.",
        )
        after = harness.checkpoint("budget-thread")
        memory_after = _memory_history(memory_store)
        loaded = next(
            event
            for event in harness.store.list_events(replacement_run.run_id)
            if event.event_type == "checkpoint.loaded"
        )

    return {
        "checkpoint": {
            "before": {
                "revision": before.revision,
                "budget": before.state["budget"],
                "preferences": before.state["preferences"],
            },
            "after": {
                "revision": after.revision,
                "budget": after.state["budget"],
                "preferences": after.state["preferences"],
            },
            "replacement_run_loaded": loaded.payload,
        },
        "memory_before": memory_before,
        "memory_after": memory_after,
        "memory_changed": memory_before != memory_after,
    }


def characterize_negation_safety(workspace: Path) -> dict[str, Any]:
    harness, memory_store = _build_dynamic_harness(workspace / "negation-safety.db")
    ambiguous_messages = (
        "Do you offer red-eye flights?",
        "Tell me about a hotel near subway.",
        "What does a relaxed travel style mean?",
    )
    explicit_negative_messages = (
        "I do not mind red-eye flights.",
        "I do not want a hotel near subway.",
        "I prefer NOT a relaxed travel style.",
    )
    ambiguous_results: list[dict[str, Any]] = []
    explicit_results: list[dict[str, Any]] = []
    with harness:
        for index, message in enumerate(ambiguous_messages, start=1):
            before_count = len(
                memory_store.list_events_for_subject(
                    tenant_id=TENANT_ID,
                    subject_id=SUBJECT_ID,
                )
            )
            harness.submit(
                thread_id=f"ambiguous-{index}",
                agent_version="1.1.0",
                message=message,
            )
            after_count = len(
                memory_store.list_events_for_subject(
                    tenant_id=TENANT_ID,
                    subject_id=SUBJECT_ID,
                )
            )
            ambiguous_results.append(
                {
                    "message": message,
                    "parser_output": parse_explicit_travel_preferences(message),
                    "memory_mutation_events": after_count - before_count,
                }
            )
        for index, message in enumerate(explicit_negative_messages, start=1):
            harness.submit(
                thread_id=f"explicit-negative-{index}",
                agent_version="1.1.0",
                message=message,
            )
            explicit_results.append(
                {
                    "message": message,
                    "parser_output": parse_explicit_travel_preferences(message),
                }
            )

    return {
        "ambiguous_mentions": ambiguous_results,
        "explicit_negative_intent": explicit_results,
        "persisted_active_memories": [
            record for record in _memory_history(memory_store) if record["status"] == "active"
        ],
    }


def characterize_confirm_plan(workspace: Path) -> dict[str, Any]:
    harness, review = _build_review_harness(workspace / "confirm-plan.db")
    with harness:
        first_run = harness.submit(
            thread_id="confirm-plan-thread",
            agent_version="0.5.0",
            message="Plan a 5-day Tokyo trip under 9000 SGD.",
        )
        first_checkpoint = harness.checkpoint("confirm-plan-thread")
        confirmed_run = harness.submit(
            thread_id="confirm-plan-thread",
            agent_version="0.5.0",
            message="Confirm everything looks good.",
        )
        confirmed_checkpoint = harness.checkpoint("confirm-plan-thread")
        loaded = next(
            event
            for event in harness.store.list_events(confirmed_run.run_id)
            if event.event_type == "checkpoint.loaded"
        )

    first_state = AgentState.model_validate(first_run.state)
    confirmed_state = AgentState.model_validate(confirmed_run.state)
    return {
        "previous_turn": {
            "run_status": first_run.status.value,
            "checkpoint_revision": first_checkpoint.revision,
            "checkpoint_execution_trace_events": len(
                first_checkpoint.state["execution_trace"]
            ),
            "run_result_execution_trace_events": len(first_state.execution_trace),
            "itinerary_total_cost": first_checkpoint.state["itinerary"]["total_cost"],
            "cost_breakdown": first_checkpoint.state["tool_outputs"]["cost_breakdown"],
        },
        "confirmation_turn": {
            "run_status": confirmed_run.status.value,
            "checkpoint_loaded": loaded.payload,
            "intent_observed": any(
                event.event == "intent_detected" and event.reason == "confirm_plan"
                for event in confirmed_state.execution_trace
            ),
            "review_invocation_count": len(review.observations),
            "review_evidence": review.observations[-1],
            "validation_errors": confirmed_run.validation_errors,
            "checkpoint_revision": confirmed_checkpoint.revision,
            "checkpoint_execution_trace_events": len(
                confirmed_checkpoint.state["execution_trace"]
            ),
            "run_result_execution_trace_events": len(confirmed_state.execution_trace),
        },
        "observed_dependency": (
            "confirm_plan reuses checkpoint itinerary/budget and the review evidence "
            "builder reads tool_outputs.cost_breakdown as a complete ledger"
        ),
        "authority_note": (
            "TravelValidator gates itinerary.total_cost against budget; the review builder "
            "has an existing plan-total fallback when cost_breakdown is absent"
        ),
    }


def _serialized_value_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _checkpoint_size(
    checkpoint: CheckpointObservation,
    run_state: RunStateObservation,
    *,
    turn: int,
    message: str,
    previous_total: int,
    checkpoint_event: dict[str, Any],
) -> dict[str, Any]:
    total = len(checkpoint.raw_json.encode("utf-8"))
    checkpoint_trace = _serialized_value_bytes(checkpoint.state["execution_trace"])
    run_trace = _serialized_value_bytes(run_state.state["execution_trace"])
    tool_outputs = _serialized_value_bytes(checkpoint.state["tool_outputs"])
    return {
        "turn": turn,
        "message": message,
        "checkpoint_revision": checkpoint.revision,
        "total_checkpoint_bytes": total,
        "checkpoint_execution_trace_value_bytes": checkpoint_trace,
        "checkpoint_execution_trace_events": len(checkpoint.state["execution_trace"]),
        "run_result_total_bytes": len(run_state.raw_json.encode("utf-8")),
        "run_result_execution_trace_value_bytes": run_trace,
        "run_result_execution_trace_events": len(run_state.state["execution_trace"]),
        "tool_outputs_value_bytes": tool_outputs,
        "other_state_and_json_structure_bytes": total - checkpoint_trace - tool_outputs,
        "growth_from_previous_checkpoint_bytes": total - previous_total,
        "checkpoint_event_trace_events": checkpoint_event["trace_events"],
        "checkpoint_event_run_trace_events": checkpoint_event["run_trace_events"],
        "checkpoint_projection": checkpoint_event["projection"],
        "retry_count": checkpoint.state["retry_count"],
    }


def characterize_checkpoint_growth(workspace: Path) -> dict[str, Any]:
    turns = (
        "Plan a 6-day Tokyo trip, budget 10000 SGD.",
        "I want a relaxed travel style.",
        "Avoid red-eye flights.",
        "Hotel near subway please.",
        "Change the trip to 8 days.",
        "Raise budget to 12000.",
        "Keep the relaxed style and no red-eye.",
        "Confirm everything looks good.",
    )
    harness, _review = _build_review_harness(workspace / "checkpoint-growth.db")
    measurements: list[dict[str, Any]] = []
    previous_total = 0
    with harness:
        for index, message in enumerate(turns, start=1):
            run = harness.submit(
                thread_id="growth-thread",
                agent_version="0.5.0",
                message=message,
            )
            checkpoint = harness.checkpoint("growth-thread")
            run_state = harness.run_state(run.run_id)
            checkpoint_event = next(
                event.payload
                for event in harness.store.list_events(run.run_id)
                if event.event_type == "checkpoint.saved"
            )
            measurement = _checkpoint_size(
                checkpoint,
                run_state,
                turn=index,
                message=message,
                previous_total=previous_total,
                checkpoint_event=checkpoint_event,
            )
            measurements.append(measurement)
            previous_total = measurement["total_checkpoint_bytes"]

    totals = [item["total_checkpoint_bytes"] for item in measurements]
    checkpoint_traces = [
        item["checkpoint_execution_trace_value_bytes"] for item in measurements
    ]
    return {
        "measurement_definition": (
            "UTF-8 bytes of persisted compact state_json; field contributions are the "
            "compact serialized field values, and other includes all keys/punctuation"
        ),
        "turns": measurements,
        "summary": {
            "total_bytes_first": totals[0],
            "total_bytes_last": totals[-1],
            "total_net_growth_bytes": totals[-1] - totals[0],
            "checkpoint_execution_trace_net_growth_bytes": (
                checkpoint_traces[-1] - checkpoint_traces[0]
            ),
            "checkpoint_execution_trace_empty_all": all(
                item["checkpoint_execution_trace_events"] == 0
                for item in measurements
            ),
            "checkpoint_execution_trace_size_stable": len(set(checkpoint_traces)) == 1,
            "run_result_execution_trace_nonempty_all": all(
                item["run_result_execution_trace_events"] > 0
                for item in measurements
            ),
            "checkpoint_event_counts_truthful": all(
                item["checkpoint_event_trace_events"]
                == item["checkpoint_execution_trace_events"]
                and item["checkpoint_event_run_trace_events"]
                == item["run_result_execution_trace_events"]
                and item["checkpoint_projection"] == "execution_trace_reset"
                for item in measurements
            ),
        },
    }


def characterize_retry_scope(workspace: Path) -> dict[str, Any]:
    turns = (
        "Plan a 5-day Tokyo trip under 1000 SGD.",
        "Raise budget to 9000 SGD.",
        "Cut budget to 1000 SGD.",
        "Raise budget to 9000 SGD.",
        "Cut budget to 1000 SGD.",
    )
    harness = _build_baseline_harness(workspace / "retry-scope.db")
    observations: list[dict[str, Any]] = []
    with harness:
        for index, message in enumerate(turns, start=1):
            run = harness.submit(
                thread_id="retry-thread",
                agent_version="0.3.0",
                message=message,
            )
            checkpoint = harness.checkpoint("retry-thread")
            observations.append(
                {
                    "turn": index,
                    "message": message,
                    "run_attempt": run.attempt,
                    "checkpoint_revision": checkpoint.revision,
                    "retry_count": checkpoint.state["retry_count"],
                    "current_stage": checkpoint.state["current_stage"],
                    "validation_errors": run.validation_errors,
                }
            )
    return {
        "turns": observations,
        "observed_scope": (
            "thread-scoped in current behavior: successful repair Runs do not reset "
            "retry_count, while each durable Run has its own attempt counter"
        ),
    }


def build_report(workspace: Path) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    return {
        "report_schema": "state-boundary-characterization-v2",
        "deterministic_boundary": {
            "planner": "ScriptedTravelPlanner",
            "tools": "offline synthetic Travel handlers via direct eval adapter",
            "network_calls": False,
            "volatile_identifiers_in_report": False,
            "checkpoint_size_is_observational": True,
        },
        "memory_supersession": characterize_memory_supersession(workspace),
        "sealed_snapshot": characterize_sealed_snapshot(workspace),
        "budget_replacement": characterize_budget_replacement(workspace),
        "negation_safety": characterize_negation_safety(workspace),
        "confirm_plan_dependency": characterize_confirm_plan(workspace),
        "checkpoint_growth": characterize_checkpoint_growth(workspace),
        "retry_count_scope": characterize_retry_scope(workspace),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the stable JSON report to this path instead of stdout.",
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="state-boundary-eval-") as temp_dir:
        report = build_report(Path(temp_dir))
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output is None:
        print(payload, end="")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(payload, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
