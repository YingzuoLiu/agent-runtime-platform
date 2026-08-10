import threading
import time

from agent.contracts import (
    RuntimeExecutionContext,
    RuntimeExecutionError,
    RuntimeResponse,
)
from domains.release_validation.models import (
    ReleaseManifest,
    ReleaseValidationInput,
    ReleaseValidationInputV1,
    SelectiveReplayRequest,
)
from domains.release_validation.runtime import (
    MAX_ATTEMPTS,
    STEP_SEQUENCE,
    WORKFLOW_TYPE,
    ReleaseValidationWorkflow,
    _canonicalize_manifest,
    _stable_hash,
)
from domains.release_validation.tools import build_release_validation_tool_registry
from domains.travel.runtime import TravelAgentRuntime, TravelMessageInput
from domains.travel.state import AgentState
from runtime_service import (
    AgentRegistry,
    RunCreateRequest,
    RunRecord,
    RunStatus,
    RuntimeManager,
    SQLiteRunStore,
    TenantContext,
    build_default_registry,
)
from runtime_service.sandbox import ToolSandbox
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStatus


TENANT_CONTEXT = TenantContext(tenant_id="legacy", subject_id="runtime-tester")


def submit_run(manager: RuntimeManager, request: RunCreateRequest):
    return manager.submit(request, tenant_context=TENANT_CONTEXT)


def wait_for_terminal(manager: RuntimeManager, run_id: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = manager.get_run(run_id, tenant_context=TENANT_CONTEXT)
        if run is not None and run.status.is_terminal:
            return run
        time.sleep(0.02)
    raise AssertionError(f"Run did not finish: {run_id}")


class BlockingRuntime(TravelAgentRuntime):
    def __init__(self, started: threading.Event, release: threading.Event):
        super().__init__()
        self.started = started
        self.release = release

    def handle_user_message(self, state: AgentState, user_message: str) -> RuntimeResponse[AgentState]:
        self.started.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("test release event was not set")
        return super().handle_user_message(state, user_message)


class CountingBlockingRuntime(TravelAgentRuntime):
    def __init__(
        self,
        first_started: threading.Event,
        second_started: threading.Event,
        release: threading.Event,
        call_count: list[int],
        count_lock: threading.Lock,
    ):
        super().__init__()
        self.first_started = first_started
        self.second_started = second_started
        self.release = release
        self.call_count = call_count
        self.count_lock = count_lock

    def handle_user_message(
        self,
        state: AgentState,
        user_message: str,
    ) -> RuntimeResponse[AgentState]:
        with self.count_lock:
            self.call_count[0] += 1
            current_count = self.call_count[0]
        if current_count == 1:
            self.first_started.set()
        else:
            self.second_started.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("test release event was not set")
        return super().handle_user_message(state, user_message)


class BlockingUncertainActionRuntime(TravelAgentRuntime):
    def __init__(self, started: threading.Event, release: threading.Event):
        super().__init__()
        self.started = started
        self.release = release

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        del state, runtime_input, context
        self.started.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("test release event was not set")
        raise RuntimeExecutionError(
            "external_action_outcome_unknown",
            "External action provider outcome is unknown.",
        )


def blocking_registry(started: threading.Event, release: threading.Event) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "0.3.0",
        lambda: BlockingRuntime(started, release),
        description="Blocking test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def uncertain_action_registry(
    started: threading.Event,
    release: threading.Event,
) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "1.2.0",
        lambda: BlockingUncertainActionRuntime(started, release),
        description="Uncertain external-action test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def release_manifest() -> ReleaseManifest:
    return ReleaseManifest(
        release_id="rel-manager-recovery",
        application_name="aurora-notes",
        release_version="2.4.0",
        required_artifacts=["aurora-notes-server", "aurora-notes-cli"],
        available_artifacts=[
            {"name": "aurora-notes-server", "checksum": "a" * 64},
            {"name": "aurora-notes-cli", "checksum": "b" * 64},
        ],
        required_test_suite="aurora-notes-full-suite",
        executed_test_suite="aurora-notes-full-suite",
        tests_passed=True,
        required_python_versions=["3.11", "3.12"],
        tested_python_versions=["3.11", "3.12"],
        deployment_environment="staging",
        configuration_requirements=["DATABASE_URL", "FEATURE_FLAGS_ENDPOINT"],
        actual_configuration_keys=[
            "DATABASE_URL",
            "FEATURE_FLAGS_ENDPOINT",
            "LOG_LEVEL",
        ],
    )


def test_manager_persists_state_and_events(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry())
    manager.start()
    try:
        first = submit_run(manager, RunCreateRequest(thread_id="trip-001", user_message="I want a 5-day Tokyo trip under 7000 SGD."))
        first_result = wait_for_terminal(manager, first.run_id)
        assert first_result.status == RunStatus.COMPLETED
        assert first_result.state is not None
        assert first_result.state.destination == "Tokyo"
        second = submit_run(manager, RunCreateRequest(thread_id="trip-001", user_message="Change the budget to 9000 and avoid red-eye flights."))
        second_result = wait_for_terminal(manager, second.run_id)
        assert second_result.state is not None
        assert second_result.state.destination == "Tokyo"
        assert second_result.state.budget == 9000
        assert second_result.state.itinerary is not None
        assert second_result.state.itinerary.flight_type == "daytime"
    finally:
        manager.stop()


def test_cancelled_before_start(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry())
    submitted = submit_run(manager, RunCreateRequest(thread_id="cancel-before", user_message="I want a 5-day Tokyo trip under 9000 SGD."))
    manager.request_cancel(submitted.run_id, tenant_context=TENANT_CONTEXT)
    manager.start()
    try:
        result = wait_for_terminal(manager, submitted.run_id)
        assert result.status == RunStatus.CANCELLED
        events = [event.event_type for event in store.list_events(submitted.run_id)]
        assert "run.cancel_requested" in events
        assert "run.cancelled" in events
        # A QUEUED -> CANCELLED transition never actually started the run:
        # no run.started, and therefore no checkpoint/completion events either.
        assert "run.started" not in events
        assert "checkpoint.saved" not in events
        assert "run.completed" not in events
        assert events.index("run.cancel_requested") < events.index("run.cancelled")
    finally:
        manager.stop()


def test_cancelled_after_execution_boundary(tmp_path):
    started = threading.Event()
    release = threading.Event()
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, blocking_registry(started, release))
    manager.start()
    try:
        submitted = submit_run(manager, RunCreateRequest(thread_id="cancel-running", user_message="I want a 5-day Tokyo trip under 9000 SGD."))
        assert started.wait(timeout=5)
        manager.request_cancel(submitted.run_id, tenant_context=TENANT_CONTEXT)
        release.set()
        result = wait_for_terminal(manager, submitted.run_id)
        assert result.status == RunStatus.CANCELLED
        assert store.load_thread_state("cancel-running", tenant_id="legacy") is None
        reasons = [event.payload.get("reason") for event in store.list_events(submitted.run_id) if event.event_type == "run.cancelled"]
        assert reasons == ["cancelled_after_execution_boundary"]
        # A cancel that commits before finalize_completed_run's compare-and-set
        # UPDATE must win: no checkpoint or completion event may appear, even
        # though the runtime step itself ran to completion.
        event_types = [event.event_type for event in store.list_events(submitted.run_id)]
        assert "checkpoint.saved" not in event_types
        assert "run.completed" not in event_types
        assert event_types.index("run.cancel_requested") < event_types.index("run.cancelled")
        # after-execution-boundary cancellation still carries over the
        # just-computed result state onto the run record itself (only the
        # thread_states checkpoint used by the *next* run is withheld).
        assert result.state is not None
        assert result.state.destination == "Tokyo"
    finally:
        release.set()
        manager.stop()


def test_outcome_unknown_is_not_hidden_by_concurrent_cancellation(tmp_path):
    started = threading.Event()
    release = threading.Event()
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, uncertain_action_registry(started, release))
    manager.start()
    try:
        submitted = submit_run(
            manager,
            RunCreateRequest(
                thread_id="uncertain-action-cancel",
                agent_version="1.2.0",
                user_message="Create the test action.",
            ),
        )
        assert started.wait(timeout=5)
        manager.request_cancel(submitted.run_id, tenant_context=TENANT_CONTEXT)
        release.set()
        result = wait_for_terminal(manager, submitted.run_id)
    finally:
        release.set()
        manager.stop()

    assert result.status == RunStatus.FAILED
    assert result.error_code == "external_action_outcome_unknown"
    event_types = [event.event_type for event in store.list_events(submitted.run_id)]
    assert "run.cancel_requested" in event_types
    assert "run.failed" in event_types
    assert "run.cancelled" not in event_types


def test_recovered_inflight_action_is_reconciled_before_persisted_cancel(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    store.create_run(
        RunRecord(
            run_id="run_reconcile_before_cancel",
            thread_id="reconcile-before-cancel",
            agent_id="travel-agent",
            agent_version="1.2.0",
            domain_id="travel",
            schema_version="1",
            status=RunStatus.RUNNING,
            input=TravelMessageInput(
                user_message="Create the test action."
            ).model_dump(mode="json"),
            cancel_requested=True,
            attempt=1,
        )
    )
    started = threading.Event()
    release = threading.Event()
    release.set()
    manager = RuntimeManager(
        SQLiteRunStore(database_path),
        uncertain_action_registry(started, release),
        recovery_reconciliation_required=(
            lambda run_id: run_id == "run_reconcile_before_cancel"
        ),
    )

    manager.start()
    try:
        result = wait_for_terminal(manager, "run_reconcile_before_cancel")
    finally:
        manager.stop()

    assert started.is_set()
    assert result.status == RunStatus.FAILED
    assert result.error_code == "external_action_outcome_unknown"
    event_types = [
        event.event_type
        for event in manager.store.list_events("run_reconcile_before_cancel")
    ]
    assert "run.recovered" in event_types
    assert "run.started" in event_types
    assert "run.failed" in event_types
    assert "run.cancelled" not in event_types


def test_running_run_is_recovered_after_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    run = RunRecord(run_id="run_recovery_test", thread_id="recovery-thread", agent_id="travel-agent", agent_version="0.3.0", status=RunStatus.RUNNING, input_message="I want a 5-day Tokyo trip under 9000 SGD.", attempt=1)
    store.create_run(run)
    store.append_event(run.run_id, "run.started", {"attempt": 1})
    manager = RuntimeManager(SQLiteRunStore(database_path), build_default_registry())
    manager.start()
    try:
        result = wait_for_terminal(manager, run.run_id)
        # No extra wait beyond wait_for_terminal's own polling: the moment
        # `result.status` is observed as terminal, the checkpoint and its
        # describing events must already be committed alongside it.
        assert result.status == RunStatus.COMPLETED
        assert result.attempt == 2
        events = [event.event_type for event in manager.store.list_events(run.run_id)]
        assert "run.recovered" in events
        assert "checkpoint.saved" in events
        assert events[-2] == "checkpoint.saved"
        assert events[-1] == "run.completed"
        assert manager.store.load_thread_state("recovery-thread", tenant_id="legacy") is not None
    finally:
        manager.stop()


def test_release_validation_running_step_is_resumed_after_manager_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    manifest = release_manifest()
    runtime_input = ReleaseValidationInputV1(manifest=manifest)
    run = RunRecord(
        run_id="run_release_recovery",
        thread_id="release-recovery-thread",
        agent_id="release-validation",
        agent_version="1.0.0",
        domain_id="release-validation",
        schema_version="1",
        status=RunStatus.RUNNING,
        input=runtime_input.model_dump(mode="json"),
        attempt=1,
    )
    SQLiteRunStore(database_path).create_run(run)

    workflow_store = SQLiteWorkflowStore(database_path)
    canonical_manifest = _canonicalize_manifest(manifest)
    manifest_hash = _stable_hash(canonical_manifest.model_dump(mode="json"))
    workflow_store.create_or_get_execution(run.run_id, WORKFLOW_TYPE, manifest_hash)
    workflow_store.mark_running(run.run_id)
    first_step = STEP_SEQUENCE[0]
    step_hash = _stable_hash(first_step.build_arguments(canonical_manifest, {}))
    workflow_store.claim_step(
        run.run_id,
        first_step.step_id,
        first_step.tool_name,
        step_hash,
        max_attempts=MAX_ATTEMPTS,
    )

    reopened_workflow_store = SQLiteWorkflowStore(database_path)
    reopened_workflow = ReleaseValidationWorkflow(
        reopened_workflow_store,
        ToolSandbox(build_release_validation_tool_registry()),
    )
    registry = build_default_registry(release_validation_workflow=reopened_workflow)
    manager = RuntimeManager(SQLiteRunStore(database_path), registry)
    manager.start()
    try:
        result = wait_for_terminal(manager, run.run_id)
    finally:
        manager.stop()

    assert result.status == RunStatus.COMPLETED
    assert result.run_id == run.run_id
    assert result.attempt == 2
    assert result.input is not None
    assert result.input["resume_interrupted"] is False
    execution = reopened_workflow_store.get_execution(run.run_id)
    assert execution is not None
    assert execution.status == WorkflowStatus.READY
    steps = {step.step_id: step for step in reopened_workflow_store.list_steps(run.run_id)}
    assert steps[first_step.step_id].attempt_count == 2
    event_types = [event.event_type for event in manager.store.list_events(run.run_id)]
    assert "run.recovered" in event_types
    assert "run.failed" not in event_types


def test_selective_replay_child_resumes_end_to_end_after_manager_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    manifest = _canonicalize_manifest(release_manifest())
    workflow_store = SQLiteWorkflowStore(database_path)
    source_workflow = ReleaseValidationWorkflow(
        workflow_store,
        ToolSandbox(build_release_validation_tool_registry()),
    )
    source_workflow.run("source", manifest)
    run_store = SQLiteRunStore(database_path)
    run_store.create_run(
        RunRecord(
            run_id="source",
            thread_id="replay-source-thread",
            agent_id="release-validation",
            agent_version="1.1.0",
            domain_id="release-validation",
            schema_version="1",
            status=RunStatus.COMPLETED,
            input=ReleaseValidationInput(manifest=manifest).model_dump(mode="json"),
        )
    )
    replay = SelectiveReplayRequest(
        source_run_id="source",
        step_ids=["run_unit_tests"],
    )
    runtime_input = ReleaseValidationInput(manifest=manifest, replay=replay)
    target = RunRecord(
        run_id="run_replay_recovery",
        thread_id="replay-recovery-thread",
        agent_id="release-validation",
        agent_version="1.1.0",
        domain_id="release-validation",
        schema_version="1",
        status=RunStatus.RUNNING,
        input=runtime_input.model_dump(mode="json"),
        attempt=1,
    )
    run_store.create_run(target)
    workflow_store.create_or_get_execution(
        target.run_id,
        WORKFLOW_TYPE,
        _stable_hash(
            {
                "manifest": manifest.model_dump(mode="json"),
                "replay": replay.model_dump(mode="json"),
            }
        ),
    )
    workflow_store.mark_running(target.run_id)
    by_id = {step.step_id: step for step in STEP_SEQUENCE}
    for step_id in ("load_manifest", "inspect_artifacts"):
        step = by_id[step_id]
        arguments = step.build_arguments(manifest, {})
        workflow_store.reuse_completed_step(
            "source",
            target.run_id,
            step_id,
            step.tool_name,
            _stable_hash(arguments),
        )
    interrupted = by_id["run_unit_tests"]
    workflow_store.claim_step(
        target.run_id,
        interrupted.step_id,
        interrupted.tool_name,
        _stable_hash(interrupted.build_arguments(manifest, {})),
        max_attempts=MAX_ATTEMPTS,
    )

    reopened_workflow_store = SQLiteWorkflowStore(database_path)
    reopened_workflow = ReleaseValidationWorkflow(
        reopened_workflow_store,
        ToolSandbox(build_release_validation_tool_registry()),
    )
    manager = RuntimeManager(
        SQLiteRunStore(database_path),
        build_default_registry(release_validation_workflow=reopened_workflow),
    )
    manager.start()
    try:
        result = wait_for_terminal(manager, target.run_id)
    finally:
        manager.stop()

    assert result.status == RunStatus.COMPLETED
    assert result.agent_version == "1.1.0"
    assert result.state.result.replay.source_run_id == "source"
    target_steps = {
        step.step_id: step for step in reopened_workflow_store.list_steps(target.run_id)
    }
    assert target_steps["run_unit_tests"].attempt_count == 2
    assert target_steps["load_manifest"].attempt_count == 0
    assert target_steps["inspect_artifacts"].attempt_count == 0


def test_finalize_completed_run_is_idempotent_on_duplicate_call(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_finalize_test",
        thread_id="finalize-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        attempt=1,
    )
    store.create_run(run)
    run.state = AgentState(thread_id="finalize-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    first = store.finalize_completed_run(run)
    assert first is True
    assert run.status == RunStatus.COMPLETED

    events_after_first = store.list_events(run.run_id)
    event_types_after_first = [event.event_type for event in events_after_first]
    assert event_types_after_first.count("checkpoint.saved") == 1
    assert event_types_after_first.count("run.completed") == 1

    second = store.finalize_completed_run(run)

    assert second is False
    # A duplicate call must not write any new checkpoint or event: the
    # conditional UPDATE affects zero rows because status is no longer
    # RUNNING, so nothing past it in the method body ever runs.
    events_after_second = store.list_events(run.run_id)
    assert events_after_second == events_after_first

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED


def test_finalize_completed_run_rejects_when_cancel_requested_first(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_cancel_race_test",
        thread_id="cancel-race-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        attempt=1,
    )
    store.create_run(run)

    # Simulate a cancel committing to the database first, in the window
    # between a worker starting execution and it finishing.
    cancelled = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert cancelled.cancel_requested is True
    assert cancelled.status == RunStatus.RUNNING

    run.state = AgentState(thread_id="cancel-race-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    result = store.finalize_completed_run(run)

    assert result is False
    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    # RuntimeManager -- not finalize_completed_run -- owns the transition to
    # CANCELLED once this returns False; the row is left RUNNING here.
    assert persisted.status == RunStatus.RUNNING
    assert persisted.state is None
    assert store.load_thread_state("cancel-race-thread", tenant_id="legacy") is None
    events = [event.event_type for event in store.list_events(run.run_id)]
    assert "checkpoint.saved" not in events
    assert "run.completed" not in events


def test_request_cancel_atomically_is_idempotent_on_duplicate_call(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_dup_cancel_request",
        thread_id="dup-cancel-request-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.QUEUED,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
    )
    store.create_run(run)

    first = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert first.cancel_requested is True

    second = store.request_cancel_atomically(run.run_id, tenant_id="legacy")

    assert second.cancel_requested is True
    events = [event.event_type for event in store.list_events(run.run_id)]
    assert events.count("run.cancel_requested") == 1


def test_finalize_cancelled_run_is_idempotent_on_duplicate_call(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_dup_cancel_finalize",
        thread_id="dup-cancel-finalize-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        cancel_requested=True,
    )
    store.create_run(run)

    first = store.finalize_cancelled_run(run, reason="cancelled_after_execution_boundary")
    assert first is True
    assert run.status == RunStatus.CANCELLED

    events_after_first = store.list_events(run.run_id)
    event_types_after_first = [event.event_type for event in events_after_first]
    assert event_types_after_first.count("run.cancelled") == 1

    second = store.finalize_cancelled_run(run, reason="cancelled_after_execution_boundary")

    assert second is False
    # Same guarantee as finalize_completed_run: a duplicate call must not
    # write any new event because the conditional UPDATE affects zero rows
    # once status is no longer QUEUED/RUNNING.
    events_after_second = store.list_events(run.run_id)
    assert events_after_second == events_after_first

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.CANCELLED


def test_cancellation_event_order_is_cancel_requested_before_cancelled(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_cancel_order",
        thread_id="cancel-order-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
    )
    store.create_run(run)

    store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    store.finalize_cancelled_run(run, reason="cancelled_after_execution_boundary")

    events = [event.event_type for event in store.list_events(run.run_id)]
    assert events.index("run.cancel_requested") < events.index("run.cancelled")


def test_completion_and_cancellation_are_mutually_exclusive_when_cancel_wins(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_race_cancel_wins",
        thread_id="race-cancel-wins-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
    )
    store.create_run(run)

    # Cancel commits first, exactly like the real cancelled_after_execution_
    # boundary path: cancel_requested flips to 1 while the run is still
    # RUNNING and the runtime step is still in flight.
    store.request_cancel_atomically(run.run_id, tenant_id="legacy")

    run.state = AgentState(thread_id="race-cancel-wins-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    completed = store.finalize_completed_run(run)
    assert completed is False

    cancelled = store.finalize_cancelled_run(run, reason="cancelled_after_execution_boundary")
    assert cancelled is True

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.CANCELLED

    events = [event.event_type for event in store.list_events(run.run_id)]
    # Exactly one terminal event set must exist: cancellation's, not completion's.
    assert events.count("run.cancelled") == 1
    assert "run.completed" not in events
    assert "checkpoint.saved" not in events
    assert events.index("run.cancel_requested") < events.index("run.cancelled")


def test_completion_and_cancellation_are_mutually_exclusive_when_completion_wins(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = RunRecord(
        run_id="run_race_completion_wins",
        thread_id="race-completion-wins-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
    )
    store.create_run(run)

    run.state = AgentState(thread_id="race-completion-wins-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    completed = store.finalize_completed_run(run)
    assert completed is True

    # A cancel arriving after completion must not be able to flip anything:
    # the row is no longer QUEUED/RUNNING, so the CAS simply does not match.
    cancel_request_result = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert cancel_request_result.cancel_requested is False
    assert cancel_request_result.status == RunStatus.COMPLETED

    finalize_cancel_result = store.finalize_cancelled_run(run, reason="cancelled_after_execution_boundary")
    assert finalize_cancel_result is False

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED

    events = [event.event_type for event in store.list_events(run.run_id)]
    # Exactly one terminal event set must exist: completion's, not cancellation's.
    assert events.count("run.completed") == 1
    assert "run.cancel_requested" not in events
    assert "run.cancelled" not in events


def test_request_cancel_on_already_terminal_run_is_a_no_op(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry())
    manager.start()
    try:
        submitted = submit_run(manager,
            RunCreateRequest(thread_id="cancel-terminal", user_message="I want a 5-day Tokyo trip under 9000 SGD.")
        )
        completed = wait_for_terminal(manager, submitted.run_id)
        assert completed.status == RunStatus.COMPLETED

        events_before = store.list_events(submitted.run_id)

        # This documents the existing, preserved convention: cancelling an
        # already-terminal run is not an error and not a duplicate-cancel
        # code path either -- it is simply a no-op that returns the run
        # exactly as it already was.
        result = manager.request_cancel(submitted.run_id, tenant_context=TENANT_CONTEXT)

        assert result.status == RunStatus.COMPLETED
        assert result.cancel_requested is False
        events_after = store.list_events(submitted.run_id)
        assert events_after == events_before
    finally:
        manager.stop()


def test_two_workers_complete_independent_runs(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry(), worker_count=2)
    manager.start()
    try:
        runs = [submit_run(manager, RunCreateRequest(thread_id=f"parallel-{index}", user_message="I want a 5-day Tokyo trip under 9000 SGD.")) for index in range(8)]
        results = [wait_for_terminal(manager, run.run_id) for run in runs]
        assert all(result.status == RunStatus.COMPLETED for result in results)
        assert len({result.run_id for result in results}) == 8
    finally:
        manager.stop()


def test_submit_before_start_is_enqueued_once_with_two_workers(tmp_path):
    first_started = threading.Event()
    second_started = threading.Event()
    release = threading.Event()
    call_count = [0]
    count_lock = threading.Lock()
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "0.3.0",
        lambda: CountingBlockingRuntime(
            first_started,
            second_started,
            release,
            call_count,
            count_lock,
        ),
        description="Counting blocking test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    manager = RuntimeManager(
        SQLiteRunStore(tmp_path / "runtime.db"),
        registry,
        worker_count=2,
    )
    submitted = submit_run(manager,
        RunCreateRequest(
            thread_id="submit-before-start",
            user_message="I want a 5-day Tokyo trip under 9000 SGD.",
        )
    )

    manager.start()
    try:
        assert first_started.wait(timeout=5)
        assert not second_started.wait(timeout=0.25)
        release.set()
        result = wait_for_terminal(manager, submitted.run_id)
    finally:
        release.set()
        manager.stop()

    assert result.status == RunStatus.COMPLETED
    assert call_count == [1]
    event_types = [event.event_type for event in manager.store.list_events(submitted.run_id)]
    assert event_types.count("run.started") == 1


def test_idempotent_submit_returns_existing_run(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry())
    request = RunCreateRequest(thread_id="idempotent", user_message="I want a 5-day Tokyo trip under 9000 SGD.", client_request_id="request-123")
    first = submit_run(manager, request)
    second = submit_run(manager, request)
    assert first.run_id == second.run_id
    assert len(store.list_events(first.run_id)) == 1


def test_restart_and_idempotent_submit_do_not_revive_failed_run(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    failed = RunRecord(
        run_id="run_failed_terminal",
        thread_id="failed-terminal-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.FAILED,
        input={"user_message": "Plan a five-day trip to Tokyo."},
        client_request_id="failed-request-123",
        attempt=1,
        error="RuntimeError: persisted failure",
    )
    store.create_run(failed)
    store.append_event(failed.run_id, "run.failed", {"error": failed.error})

    manager = RuntimeManager(SQLiteRunStore(database_path), build_default_registry())
    manager.start()
    try:
        duplicate = submit_run(manager,
            RunCreateRequest(
                thread_id="failed-terminal-thread",
                user_message="Plan a five-day trip to Tokyo.",
                client_request_id="failed-request-123",
            )
        )
    finally:
        manager.stop()

    assert duplicate.run_id == failed.run_id
    assert duplicate.status == RunStatus.FAILED
    assert duplicate.attempt == 1
    event_types = [event.event_type for event in manager.store.list_events(failed.run_id)]
    assert event_types == ["run.failed"]


def test_store_survives_new_store_instance(tmp_path):
    database_path = tmp_path / "runtime.db"
    manager = RuntimeManager(SQLiteRunStore(database_path), build_default_registry())
    manager.start()
    try:
        submitted = submit_run(manager, RunCreateRequest(thread_id="persistent-thread", user_message="I want a 5-day Tokyo trip under 9000 SGD."))
        completed = wait_for_terminal(manager, submitted.run_id)
    finally:
        manager.stop()
    reopened = SQLiteRunStore(database_path, state_registry=build_default_registry())
    persisted = reopened.get_run_internal(completed.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert reopened.load_thread_state("persistent-thread", tenant_id="legacy") is not None
