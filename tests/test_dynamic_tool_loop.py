from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import (
    RuntimeExecutionAuthority,
    RuntimeExecutionContext,
    RuntimeExecutionError,
)
from runtime_service.dynamic_loop import (
    DynamicLoopOutcome,
    DynamicToolLoop,
    FinishEvaluation,
)
from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    InvalidPlannerDecisionError,
    PlannerContext,
    PlannerProviderError,
)
from runtime_service.sandbox import (
    ToolExecutionResult,
    ToolExecutionStatus,
    ToolPolicy,
    ToolRegistry,
    ToolSpec,
)
from runtime_service.workflow_store import (
    SQLiteWorkflowStore,
    ToolCallStatus,
    WorkflowStatus,
)


class IncrementInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: int = Field(ge=0, le=1_000)
    increment: int = Field(default=1, ge=1, le=10)


@dataclass
class RecordedEvent:
    event_type: str
    payload: dict[str, Any]


class MemoryRunEventSink:
    def __init__(self) -> None:
        self.events: list[RecordedEvent] = []

    def append_event(
        self,
        _run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RecordedEvent:
        event = RecordedEvent(event_type=event_type, payload=payload or {})
        self.events.append(event)
        return event

    def list_events(
        self,
        _run_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RecordedEvent]:
        del after_sequence
        return list(self.events)


class ScriptedPlanner:
    def __init__(self, *decisions: Any) -> None:
        self.decisions = list(decisions)
        self.contexts: list[PlannerContext] = []

    def decide(self, context: PlannerContext) -> Any:
        self.contexts.append(context)
        if not self.decisions:
            raise AssertionError("Planner was called more times than expected")
        decision = self.decisions.pop(0)
        if isinstance(decision, BaseException):
            raise decision
        return decision


class ObservationDrivenPlanner:
    def __init__(self) -> None:
        self.contexts: list[PlannerContext] = []

    def decide(self, context: PlannerContext) -> CallToolDecision | FinishDecision:
        self.contexts.append(context)
        if not context.observations:
            return CallToolDecision(
                tool_name="increment",
                arguments={"value": context.state["seed"]},
                reason="Ground the calculation in a registered tool.",
            )
        observed = context.observations[-1].result["value"]
        return FinishDecision(
            message=f"Computed {observed}.",
            output={"computed": observed},
            reason="The observed value is sufficient.",
        )


class FakeSandbox:
    def __init__(self, *outcomes: ToolExecutionResult | BaseException) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, tool_name: str, arguments: dict[str, Any]) -> ToolExecutionResult:
        self.calls.append((tool_name, arguments))
        if not self.outcomes:
            outcome: ToolExecutionResult | BaseException = completed_result(
                tool_name,
                {"value": arguments["value"] + arguments.get("increment", 1)},
            )
        else:
            outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class SimulatedProcessCrash(BaseException):
    pass


def completed_result(tool_name: str, result: dict[str, Any]) -> ToolExecutionResult:
    return ToolExecutionResult(
        execution_id="exec-test",
        tool_name=tool_name,
        status=ToolExecutionStatus.COMPLETED,
        result=result,
        duration_ms=1,
        exit_code=0,
    )


def failed_result(status: ToolExecutionStatus) -> ToolExecutionResult:
    return ToolExecutionResult(
        execution_id="exec-test-failure",
        tool_name="increment",
        status=status,
        error=f"simulated {status.value}",
        duration_ms=1,
        exit_code=1,
    )


def build_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="increment",
            description="Increment a generic integer value.",
            input_model=IncrementInput,
            policy=ToolPolicy(),
            handler_entrypoint="tests.sandbox_handlers:echo_payload",
        )
    )
    return registry


def execution_context(
    run_id: str,
    *,
    permissions: tuple[str, ...] = ("tools:execute",),
    recovered_after_restart: bool = False,
) -> RuntimeExecutionContext:
    return RuntimeExecutionContext(
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        recovered_after_restart=recovered_after_restart,
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-generic",
            subject_id="subject-generic",
            permissions=permissions,
        ),
    )


def finish_evaluator(
    decision: FinishDecision,
    _observations,
) -> FinishEvaluation:
    return FinishEvaluation(
        outcome=DynamicLoopOutcome.FINISHED,
        message=decision.message,
        output=decision.output,
    )


def build_loop(
    *,
    planner,
    sandbox: FakeSandbox,
    workflow_store: SQLiteWorkflowStore,
    event_sink: MemoryRunEventSink,
    registry: ToolRegistry | None = None,
    max_tool_calls: int = 4,
) -> DynamicToolLoop:
    return DynamicToolLoop(
        planner=planner,
        tool_registry=registry or build_registry(),
        tool_sandbox=sandbox,  # type: ignore[arg-type]
        workflow_store=workflow_store,
        run_event_sink=event_sink,
        workflow_type="generic-calculation:1.0.0",
        max_tool_calls=max_tool_calls,
    )


def execute_loop(
    loop: DynamicToolLoop,
    context: RuntimeExecutionContext,
    *,
    state: dict[str, Any] | None = None,
):
    return loop.execute(
        runtime_input={"request": "increment the seed"},
        state=state or {"seed": 2, "status": "new"},
        context=context,
        finish_evaluator=finish_evaluator,
    )


def assert_failure_code(expected_code: str, call) -> RuntimeExecutionError:
    with pytest.raises(RuntimeExecutionError) as raised:
        call()
    assert raised.value.code == expected_code
    return raised.value


def seed_completed_step_before_crash(
    database_path: Path,
    run_id: str,
    event_sink: MemoryRunEventSink,
) -> None:
    loop = build_loop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="increment",
                arguments={"value": 2},
                reason="Persist a completed step for recovery validation.",
            ),
            SimulatedProcessCrash(),
        ),
        sandbox=FakeSandbox(),
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    with pytest.raises(SimulatedProcessCrash):
        execute_loop(loop, execution_context(run_id))

    step = SQLiteWorkflowStore(database_path).get_step(run_id, "call-0001")
    assert step is not None and step.status == ToolCallStatus.COMPLETED


def seed_running_execution(
    loop: DynamicToolLoop,
    workflow_store: SQLiteWorkflowStore,
    context: RuntimeExecutionContext,
) -> None:
    input_hash = loop._stable_hash(
        {
            "runtime_input": {"request": "increment the seed"},
            "state": {"seed": 2, "status": "new"},
            "workflow_type": "generic-calculation:1.0.0",
            "max_tool_calls": loop.max_tool_calls,
        }
    )
    workflow_store.create_or_get_execution(
        context.run_id,
        "generic-calculation:1.0.0",
        input_hash,
    )
    execution = workflow_store.mark_running(context.run_id)
    assert execution.status == WorkflowStatus.RUNNING


def test_generic_non_travel_loop_uses_observation_to_finish(tmp_path):
    planner = ObservationDrivenPlanner()
    sandbox = FakeSandbox()
    event_sink = MemoryRunEventSink()
    workflow_store = SQLiteWorkflowStore(tmp_path / "generic.db")
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=event_sink,
    )

    result = execute_loop(loop, execution_context("generic-happy"))

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert result.output == {"computed": 3}
    assert result.observations[0].tool_name == "increment"
    assert result.observations[0].result == {"value": 3}
    assert sandbox.calls == [("increment", {"value": 2, "increment": 1})]
    assert len(planner.contexts) == 2
    assert planner.contexts[1].observations[0].result == {"value": 3}


def test_happy_path_evidence_orders_decision_policy_tool_decision_outcome(tmp_path):
    event_sink = MemoryRunEventSink()
    workflow_store = SQLiteWorkflowStore(tmp_path / "evidence.db")
    loop = build_loop(
        planner=ObservationDrivenPlanner(),
        sandbox=FakeSandbox(),
        workflow_store=workflow_store,
        event_sink=event_sink,
    )

    execute_loop(loop, execution_context("evidence-order"))

    expected = [
        "planner.decision",
        "policy.decision",
        "tool.result",
        "planner.decision",
        "loop.outcome",
    ]
    assert [event.event_type for event in event_sink.events] == expected
    assert [
        event.event_type
        for event in workflow_store.list_events("evidence-order")
        if event.event_type in {
            "planner.decision",
            "policy.decision",
            "tool.result",
            "loop.outcome",
        }
    ] == expected
    evidence_ids = [event.payload["evidence_id"] for event in event_sink.events]
    assert evidence_ids == [
        "planner:1",
        "policy:call-0001",
        "tool-result:call-0001",
        "planner:2",
        "loop:outcome",
    ]


@pytest.mark.parametrize(
    ("run_id", "planner", "permissions", "expected_code"),
    [
        (
            "unknown-tool",
            ScriptedPlanner(
                CallToolDecision(
                    tool_name="not-registered",
                    arguments={"value": -1, "unexpected": True},
                    reason="Exercise allowlist denial.",
                )
            ),
            (),
            "unknown_tool",
        ),
        (
            "permission-denied",
            ScriptedPlanner(
                CallToolDecision(
                    tool_name="increment",
                    arguments={"value": 2},
                    reason="Exercise authority denial.",
                )
            ),
            (),
            "tool_permission_denied",
        ),
        (
            "invalid-arguments",
            ScriptedPlanner(
                CallToolDecision(
                    tool_name="increment",
                    arguments={"value": -1, "unexpected": True},
                    reason="Exercise schema denial.",
                )
            ),
            ("tools:execute",),
            "invalid_tool_arguments",
        ),
    ],
)
def test_policy_denials_do_not_call_sandbox_or_claim_step(
    tmp_path,
    run_id,
    planner,
    permissions,
    expected_code,
):
    sandbox = FakeSandbox()
    workflow_store = SQLiteWorkflowStore(tmp_path / f"{run_id}.db")
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=MemoryRunEventSink(),
    )

    assert_failure_code(
        expected_code,
        lambda: execute_loop(
            loop,
            execution_context(run_id, permissions=permissions),
        ),
    )

    assert sandbox.calls == []
    assert workflow_store.list_steps(run_id) == []
    execution = workflow_store.get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.FAILED
    assert execution.error_code == expected_code


def test_step_limit_wins_before_unknown_tool_and_does_not_claim_an_extra_step(tmp_path):
    planner = ScriptedPlanner(
        CallToolDecision(
            tool_name="increment",
            arguments={"value": 2},
            reason="Use the one allowed step.",
        ),
        CallToolDecision(
            tool_name="not-registered",
            arguments={"value": -1, "unexpected": True},
            reason="This must be rejected by the step limit first.",
        ),
    )
    sandbox = FakeSandbox()
    workflow_store = SQLiteWorkflowStore(tmp_path / "step-limit.db")
    event_sink = MemoryRunEventSink()
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=event_sink,
        max_tool_calls=1,
    )

    assert_failure_code(
        "step_limit_exceeded",
        lambda: execute_loop(loop, execution_context("step-limit")),
    )

    assert len(sandbox.calls) == 1
    assert [step.step_id for step in workflow_store.list_steps("step-limit")] == [
        "call-0001"
    ]
    denials = [
        event.payload
        for event in event_sink.events
        if event.event_type == "policy.decision" and event.payload["outcome"] == "denied"
    ]
    assert denials == [
        {
            "evidence_id": "policy:call-0002",
            "step_id": "call-0002",
            "tool_name": "not-registered",
            "outcome": "denied",
            "error_code": "step_limit_exceeded",
        }
    ]


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [
        (ToolExecutionStatus.TIMED_OUT, "tool_timed_out"),
        (ToolExecutionStatus.FAILED, "tool_execution_failed"),
    ],
)
def test_tool_terminal_failures_have_stable_codes_and_durable_failed_steps(
    tmp_path,
    status,
    expected_code,
):
    run_id = f"tool-{status.value}"
    sandbox = FakeSandbox(failed_result(status))
    workflow_store = SQLiteWorkflowStore(tmp_path / f"{run_id}.db")
    event_sink = MemoryRunEventSink()
    loop = build_loop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="increment",
                arguments={"value": 2},
                reason="Exercise a terminal tool result.",
            )
        ),
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=event_sink,
    )

    assert_failure_code(
        expected_code,
        lambda: execute_loop(loop, execution_context(run_id)),
    )

    step = workflow_store.get_step(run_id, "call-0001")
    assert step is not None
    assert step.status == ToolCallStatus.FAILED
    assert step.error_code == expected_code
    assert [
        event.payload["error_code"]
        for event in event_sink.events
        if event.event_type == "tool.result"
    ] == [expected_code]


def test_sandbox_exception_maps_to_tool_execution_failed(tmp_path):
    workflow_store = SQLiteWorkflowStore(tmp_path / "sandbox-exception.db")
    sandbox = FakeSandbox(RuntimeError("executor broke"))
    loop = build_loop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="increment",
                arguments={"value": 2},
                reason="Exercise an executor exception.",
            )
        ),
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=MemoryRunEventSink(),
    )

    assert_failure_code(
        "tool_execution_failed",
        lambda: execute_loop(loop, execution_context("sandbox-exception")),
    )


@pytest.mark.parametrize(
    ("run_id", "planner", "expected_code"),
    [
        (
            "provider-failed",
            ScriptedPlanner(PlannerProviderError("provider unavailable")),
            "planner_provider_failed",
        ),
        (
            "provider-unexpected",
            ScriptedPlanner(RuntimeError("unexpected provider error")),
            "planner_provider_failed",
        ),
        (
            "invalid-decision-error",
            ScriptedPlanner(InvalidPlannerDecisionError("bad provider payload")),
            "invalid_planner_decision",
        ),
        (
            "invalid-decision-shape",
            ScriptedPlanner(
                {
                    "decision_type": "CALL_TOOL",
                    "tool_name": "increment",
                    "arguments": {"value": 2},
                    "reason": "Has a forbidden field.",
                    "unexpected": True,
                }
            ),
            "invalid_planner_decision",
        ),
    ],
)
def test_planner_failures_have_stable_codes_without_tool_claims(
    tmp_path,
    run_id,
    planner,
    expected_code,
):
    sandbox = FakeSandbox()
    workflow_store = SQLiteWorkflowStore(tmp_path / f"{run_id}.db")
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=MemoryRunEventSink(),
    )

    assert_failure_code(
        expected_code,
        lambda: execute_loop(loop, execution_context(run_id)),
    )

    assert sandbox.calls == []
    assert workflow_store.list_steps(run_id) == []
    execution = workflow_store.get_execution(run_id)
    assert execution is not None and execution.error_code == expected_code


def test_completed_step_is_restored_after_crash_without_repeating_sandbox(tmp_path):
    database_path = Path(tmp_path) / "completed-restart.db"
    run_id = "completed-restart"
    event_sink = MemoryRunEventSink()
    first_sandbox = FakeSandbox()
    first_loop = build_loop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="increment",
                arguments={"value": 2},
                reason="Produce durable evidence before the crash.",
            ),
            SimulatedProcessCrash(),
        ),
        sandbox=first_sandbox,
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    with pytest.raises(SimulatedProcessCrash):
        execute_loop(first_loop, execution_context(run_id))

    assert len(first_sandbox.calls) == 1
    persisted = SQLiteWorkflowStore(database_path).get_step(run_id, "call-0001")
    assert persisted is not None and persisted.status == ToolCallStatus.COMPLETED

    recovery_planner = ScriptedPlanner(
        FinishDecision(
            message="Recovered the completed observation.",
            output={"computed": 3},
            reason="The cached observation is sufficient.",
        )
    )
    recovery_sandbox = FakeSandbox()
    recovery_loop = build_loop(
        planner=recovery_planner,
        sandbox=recovery_sandbox,
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    result = execute_loop(
        recovery_loop,
        execution_context(run_id, recovered_after_restart=True),
    )

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert recovery_sandbox.calls == []
    assert len(recovery_planner.contexts) == 1
    assert recovery_planner.contexts[0].observations[0].cached is True
    assert recovery_planner.contexts[0].observations[0].result == {"value": 3}
    evidence_ids = [event.payload.get("evidence_id") for event in event_sink.events]
    assert len(evidence_ids) == len(set(evidence_ids))


def test_running_step_recovery_replays_persisted_decision_before_new_planner_call(tmp_path):
    database_path = Path(tmp_path) / "running-restart.db"
    run_id = "running-restart"
    event_sink = MemoryRunEventSink()
    first_loop = build_loop(
        planner=ScriptedPlanner(
            CallToolDecision(
                tool_name="increment",
                arguments={"value": 2},
                reason="Persist this decision before process loss.",
            )
        ),
        sandbox=FakeSandbox(SimulatedProcessCrash()),
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    with pytest.raises(SimulatedProcessCrash):
        execute_loop(first_loop, execution_context(run_id))

    interrupted = SQLiteWorkflowStore(database_path).get_step(run_id, "call-0001")
    assert interrupted is not None
    assert interrupted.status == ToolCallStatus.RUNNING
    assert interrupted.attempt_count == 1

    recovery_planner = ScriptedPlanner(
        FinishDecision(
            message="Recovered and finished.",
            output={"computed": 3},
            reason="The recovered call supplied the required observation.",
        )
    )
    recovery_sandbox = FakeSandbox()
    recovery_store = SQLiteWorkflowStore(database_path)
    recovery_loop = build_loop(
        planner=recovery_planner,
        sandbox=recovery_sandbox,
        workflow_store=recovery_store,
        event_sink=event_sink,
    )

    result = execute_loop(
        recovery_loop,
        execution_context(run_id, recovered_after_restart=True),
    )

    assert result.outcome == DynamicLoopOutcome.FINISHED
    assert recovery_sandbox.calls == [("increment", {"value": 2, "increment": 1})]
    assert len(recovery_planner.contexts) == 1
    assert recovery_planner.contexts[0].tool_call_count == 1
    assert recovery_planner.contexts[0].observations[0].result == {"value": 3}
    recovered_step = recovery_store.get_step(run_id, "call-0001")
    assert recovered_step is not None
    assert recovered_step.status == ToolCallStatus.COMPLETED
    assert recovered_step.attempt_count == 2
    assert "step.interrupted_recovery" in [
        event.event_type for event in recovery_store.list_events(run_id)
    ]
    planner_events = [
        event
        for event in recovery_store.list_events(run_id)
        if event.event_type == "planner.decision"
    ]
    assert [event.payload["decision_index"] for event in planner_events] == [1, 2]


def test_corrupt_terminal_workflow_result_maps_to_invalid_planner_decision(tmp_path):
    database_path = Path(tmp_path) / "corrupt-terminal-result.db"
    run_id = "corrupt-terminal-result"
    event_sink = MemoryRunEventSink()
    initial_loop = build_loop(
        planner=ObservationDrivenPlanner(),
        sandbox=FakeSandbox(),
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )
    execute_loop(initial_loop, execution_context(run_id))

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE workflow_executions SET result_json = ? WHERE run_id = ?",
            ('{"outcome":', run_id),
        )

    terminal_planner = ScriptedPlanner()
    terminal_sandbox = FakeSandbox()
    recovery_loop = build_loop(
        planner=terminal_planner,
        sandbox=terminal_sandbox,
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    assert_failure_code(
        "invalid_planner_decision",
        lambda: execute_loop(
            recovery_loop,
            execution_context(run_id, recovered_after_restart=True),
        ),
    )
    assert terminal_planner.contexts == []
    assert terminal_sandbox.calls == []


def test_completed_tool_result_with_invalid_json_maps_to_tool_execution_failed(tmp_path):
    database_path = Path(tmp_path) / "corrupt-tool-result.db"
    run_id = "corrupt-tool-result"
    event_sink = MemoryRunEventSink()
    seed_completed_step_before_crash(database_path, run_id, event_sink)

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE tool_calls SET result_json = ? WHERE run_id = ? AND step_id = ?",
            ('{"value":', run_id, "call-0001"),
        )

    recovery_planner = ScriptedPlanner()
    recovery_sandbox = FakeSandbox()
    recovery_loop = build_loop(
        planner=recovery_planner,
        sandbox=recovery_sandbox,
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    assert_failure_code(
        "tool_execution_failed",
        lambda: execute_loop(
            recovery_loop,
            execution_context(run_id, recovered_after_restart=True),
        ),
    )
    assert recovery_planner.contexts == []
    assert recovery_sandbox.calls == []


@pytest.mark.parametrize("corruption", ["missing", "invalid"])
def test_completed_step_with_missing_or_invalid_planner_evidence_fails_closed(
    tmp_path,
    corruption,
):
    database_path = Path(tmp_path) / f"planner-evidence-{corruption}.db"
    run_id = f"planner-evidence-{corruption}"
    event_sink = MemoryRunEventSink()
    seed_completed_step_before_crash(database_path, run_id, event_sink)

    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT event_id FROM workflow_events "
            "WHERE run_id = ? AND event_type = ? ORDER BY sequence LIMIT 1",
            (run_id, "planner.decision"),
        ).fetchone()
        assert row is not None
        if corruption == "missing":
            connection.execute(
                "DELETE FROM workflow_events WHERE event_id = ?",
                (row[0],),
            )
        else:
            invalid_payload = {
                "evidence_id": "planner:1",
                "decision_index": 1,
                "outcome": "accepted",
                "step_id": "call-0001",
                "decision": {
                    "decision_type": "CALL_TOOL",
                    "tool_name": "increment",
                    "arguments": {"value": 2},
                    "reason": "Corrupted by an unexpected field.",
                    "unexpected": True,
                },
            }
            connection.execute(
                "UPDATE workflow_events SET payload_json = ? WHERE event_id = ?",
                (json.dumps(invalid_payload), row[0]),
            )

    recovery_planner = ScriptedPlanner()
    recovery_sandbox = FakeSandbox()
    recovery_loop = build_loop(
        planner=recovery_planner,
        sandbox=recovery_sandbox,
        workflow_store=SQLiteWorkflowStore(database_path),
        event_sink=event_sink,
    )

    assert_failure_code(
        "invalid_planner_decision",
        lambda: execute_loop(
            recovery_loop,
            execution_context(run_id, recovered_after_restart=True),
        ),
    )
    assert recovery_planner.contexts == []
    assert recovery_sandbox.calls == []


def test_recovery_finalizes_original_planner_rejection_after_outcome_crash_window(
    tmp_path,
):
    run_id = "planner-outcome-crash-window"
    workflow_store = SQLiteWorkflowStore(tmp_path / "planner-outcome-crash-window.db")
    event_sink = MemoryRunEventSink()
    planner = ScriptedPlanner()
    sandbox = FakeSandbox()
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=event_sink,
    )
    context = execution_context(run_id, recovered_after_restart=True)
    seed_running_execution(loop, workflow_store, context)
    workflow_store.append_event(
        run_id,
        "planner.decision",
        {
            "evidence_id": "planner:1",
            "decision_index": 1,
            "outcome": "rejected",
            "error_code": "planner_provider_failed",
        },
    )
    workflow_store.append_event(
        run_id,
        "loop.outcome",
        {
            "evidence_id": "loop:outcome",
            "outcome": "failed",
            "error_code": "planner_provider_failed",
            "message": "Planner provider failed: upstream unavailable",
        },
    )

    assert_failure_code(
        "planner_provider_failed",
        lambda: execute_loop(loop, context),
    )

    execution = workflow_store.get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.FAILED
    assert execution.error_code == "planner_provider_failed"
    assert planner.contexts == []
    assert sandbox.calls == []


def test_recovery_finalizes_original_tool_failure_after_outcome_crash_window(tmp_path):
    run_id = "tool-outcome-crash-window"
    workflow_store = SQLiteWorkflowStore(tmp_path / "tool-outcome-crash-window.db")
    event_sink = MemoryRunEventSink()
    planner = ScriptedPlanner()
    sandbox = FakeSandbox()
    loop = build_loop(
        planner=planner,
        sandbox=sandbox,
        workflow_store=workflow_store,
        event_sink=event_sink,
    )
    context = execution_context(run_id, recovered_after_restart=True)
    seed_running_execution(loop, workflow_store, context)
    decision = CallToolDecision(
        tool_name="increment",
        arguments={"value": 2},
        reason="Persist the tool call before its timeout is finalized.",
    )
    workflow_store.append_event(
        run_id,
        "planner.decision",
        {
            "evidence_id": "planner:1",
            "decision_index": 1,
            "outcome": "accepted",
            "decision": decision.model_dump(mode="json"),
            "step_id": "call-0001",
        },
    )
    workflow_store.append_event(
        run_id,
        "policy.decision",
        {
            "evidence_id": "policy:call-0001",
            "step_id": "call-0001",
            "tool_name": "increment",
            "outcome": "allowed",
            "error_code": None,
        },
    )
    normalized_arguments = {"value": 2, "increment": 1}
    claim = workflow_store.claim_step(
        run_id,
        "call-0001",
        "increment",
        loop._stable_hash(normalized_arguments),
        max_attempts=2,
    )
    assert claim.attempt_token is not None
    workflow_store.fail_step(
        run_id,
        "call-0001",
        claim.attempt_token,
        "tool_timed_out",
    )
    workflow_store.append_event(
        run_id,
        "tool.result",
        {
            "evidence_id": "tool-result:call-0001",
            "step_id": "call-0001",
            "tool_name": "increment",
            "status": "failed",
            "result": None,
            "error_code": "tool_timed_out",
        },
    )
    workflow_store.append_event(
        run_id,
        "loop.outcome",
        {
            "evidence_id": "loop:outcome",
            "outcome": "failed",
            "error_code": "tool_timed_out",
            "message": "Tool execution did not complete successfully.",
        },
    )

    assert_failure_code(
        "tool_timed_out",
        lambda: execute_loop(loop, context),
    )

    execution = workflow_store.get_execution(run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.FAILED
    assert execution.error_code == "tool_timed_out"
    assert planner.contexts == []
    assert sandbox.calls == []
