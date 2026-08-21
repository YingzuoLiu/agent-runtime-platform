import sqlite3
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
    RunCommitOutcome,
    RunCreateRequest,
    RunRecord,
    RunStatus,
    RuntimeManager,
    SQLiteRunStore,
    TenantContext,
    build_default_registry,
)
from runtime_service.external_actions import (
    ExternalActionReconciliationPendingError,
)
from runtime_service.sandbox import ToolSandbox
from runtime_service.workflow_store import SQLiteWorkflowStore, WorkflowStatus


TENANT_CONTEXT = TenantContext(tenant_id="legacy", subject_id="runtime-tester")


def submit_run(manager: RuntimeManager, request: RunCreateRequest):
    return manager.submit(request, tenant_context=TENANT_CONTEXT)


def claim_store_run(
    store: SQLiteRunStore,
    run: RunRecord,
) -> tuple[RunRecord, str]:
    store.create_run(run)
    claim = store.claim_next_run(
        owner_id=f"test-owner-{run.run_id}",
        lease_duration_seconds=30,
        reconciliation_pending_code=(
            ExternalActionReconciliationPendingError.CODE
        ),
    )
    assert claim is not None
    assert claim.run.run_id == run.run_id
    return claim.run, claim.lease_token


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


class ReconciliationPendingRuntime(TravelAgentRuntime):
    def __init__(self, attempted: threading.Event):
        super().__init__()
        self.attempted = attempted

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        del state, runtime_input, context
        self.attempted.set()
        raise ExternalActionReconciliationPendingError()


class ReconciliationSetupFailureRuntime(TravelAgentRuntime):
    def __init__(self, attempted: threading.Event):
        super().__init__()
        self.attempted = attempted

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        del state, runtime_input, context
        self.attempted.set()
        raise ValueError("injected recovery setup failure")


class ReconciliationCancellationRuntime(TravelAgentRuntime):
    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        del state, runtime_input, context
        raise RuntimeExecutionError(
            "run_cancel_requested",
            "Cancellation won before external action dispatch.",
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


def reconciliation_pending_registry(attempted: threading.Event) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "1.2.0",
        lambda: ReconciliationPendingRuntime(attempted),
        description="Pending external-action reconciliation test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def reconciliation_setup_failure_registry(
    attempted: threading.Event,
) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "1.2.0",
        lambda: ReconciliationSetupFailureRuntime(attempted),
        description="External-action recovery setup failure test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def reconciliation_cancellation_registry() -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "travel-agent",
        "1.2.0",
        ReconciliationCancellationRuntime,
        description="External-action recovery cancellation test runtime",
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


def test_stop_gate_prevents_claim_after_control_plane_scan(
    tmp_path,
    monkeypatch,
):
    scan_entered = threading.Event()
    release_scan = threading.Event()
    claim_called = threading.Event()
    store = SQLiteRunStore(tmp_path / "runtime.db")
    original_finalize = store.finalize_next_queued_cancellation
    original_claim = store.claim_next_run

    def blocked_finalize():
        scan_entered.set()
        if not release_scan.wait(timeout=5):
            raise TimeoutError("test did not release cancellation scan")
        return original_finalize()

    def observed_claim(**kwargs):
        claim_called.set()
        return original_claim(**kwargs)

    monkeypatch.setattr(store, "finalize_next_queued_cancellation", blocked_finalize)
    monkeypatch.setattr(store, "claim_next_run", observed_claim)
    manager = RuntimeManager(
        store,
        build_default_registry(),
        shutdown_grace_seconds=0,
    )
    manager.start()
    assert scan_entered.wait(timeout=5)
    submitted = submit_run(
        manager,
        RunCreateRequest(
            thread_id="stop-claim-admission",
            user_message="Plan a five-day trip to Tokyo.",
        ),
    )
    workers = list(manager._workers)

    manager.stop()
    release_scan.set()
    for worker in workers:
        worker.join(timeout=5)

    assert all(not worker.is_alive() for worker in workers)
    assert not claim_called.is_set()
    persisted = store.get_run_internal(submitted.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.QUEUED
    assert persisted.attempt == 0


def test_cleanup_read_failure_does_not_kill_worker_or_leak_claim(
    tmp_path,
    monkeypatch,
):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, build_default_registry())
    first = submit_run(
        manager,
        RunCreateRequest(
            thread_id="cleanup-read-first",
            user_message="Plan a five-day trip to Tokyo.",
        ),
    )
    second = submit_run(
        manager,
        RunCreateRequest(
            thread_id="cleanup-read-second",
            user_message="Plan a five-day trip to Seoul.",
        ),
    )
    cleanup_failure_observed = threading.Event()
    original_get = store.get_run_internal

    def fail_first_cleanup_read(run_id: str):
        if not cleanup_failure_observed.is_set():
            cleanup_failure_observed.set()
            raise sqlite3.OperationalError("injected cleanup read failure")
        return original_get(run_id)

    monkeypatch.setattr(store, "get_run_internal", fail_first_cleanup_read)
    manager.start()
    try:
        assert wait_for_terminal(manager, first.run_id).status == RunStatus.COMPLETED
        assert cleanup_failure_observed.wait(timeout=5)
        assert wait_for_terminal(manager, second.run_id).status == RunStatus.COMPLETED

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with manager._heartbeat_lock:
                if not manager._owned_claims:
                    break
            time.sleep(0.01)
        else:
            raise AssertionError("terminal claims leaked from manager bookkeeping")

        assert any(worker.is_alive() for worker in manager._workers)
    finally:
        manager.stop()


def test_cancel_cas_committing_before_atomic_claim_is_not_overwritten(
    tmp_path,
    monkeypatch,
):
    started = threading.Event()
    release = threading.Event()
    release.set()
    store = SQLiteRunStore(tmp_path / "runtime.db")
    manager = RuntimeManager(store, blocking_registry(started, release))
    submitted = submit_run(
        manager,
        RunCreateRequest(
            thread_id="cancel-start-claim-race",
            user_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )
    original_claim = store.claim_next_run

    def cancel_then_claim(**kwargs):
        store.request_cancel_atomically(
            submitted.run_id,
            tenant_id=submitted.tenant_id,
        )
        return original_claim(**kwargs)

    monkeypatch.setattr(store, "claim_next_run", cancel_then_claim)
    manager.start()
    try:
        result = wait_for_terminal(manager, submitted.run_id)
    finally:
        manager.stop()

    assert result.status == RunStatus.CANCELLED
    assert result.cancel_requested
    assert not started.is_set()
    events = store.list_events(submitted.run_id)
    event_types = [event.event_type for event in events]
    assert event_types.count("run.cancel_requested") == 1
    assert event_types.count("run.started") == 0
    assert event_types.count("run.cancelled") == 1
    cancelled_events = [
        event for event in events if event.event_type == "run.cancelled"
    ]
    assert cancelled_events[0].payload == {
        "reason": "cancelled_before_start"
    }


def test_cancel_cas_after_recovery_scan_is_not_overwritten(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "runtime.db"
    started = threading.Event()
    release = threading.Event()
    release.set()
    store = SQLiteRunStore(database_path)
    run = RunRecord(
        run_id="run_recovery_cancel_scan_race",
        thread_id="recovery-cancel-scan-race",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        attempt=1,
    )
    store._seed_historical_run_for_migration(
        run,
        event_type="run.started",
        payload={"attempt": 1},
    )
    original_list = store.list_recoverable_runs

    def list_then_cancel():
        stale_rows = original_list()
        store.request_cancel_atomically(run.run_id, tenant_id=run.tenant_id)
        return stale_rows

    monkeypatch.setattr(store, "list_recoverable_runs", list_then_cancel)
    manager = RuntimeManager(store, blocking_registry(started, release))
    manager.start()
    try:
        result = wait_for_terminal(manager, run.run_id)
    finally:
        manager.stop()

    assert result.status == RunStatus.CANCELLED
    assert result.cancel_requested
    assert result.attempt == 2
    assert not started.is_set()
    events = store.list_events(run.run_id)
    event_types = [event.event_type for event in events]
    assert event_types.count("run.cancel_requested") == 1
    assert event_types.count("run.recovered") == 1
    assert event_types.count("run.started") == 2
    assert event_types.count("run.cancelled") == 1


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
        # A cancel that commits before commit_completed_run's compare-and-set
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
    store._seed_historical_run_for_migration(
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


def test_reconciliation_pending_run_remains_recoverable_across_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    first_attempt = threading.Event()
    first_manager = RuntimeManager(
        SQLiteRunStore(database_path),
        reconciliation_pending_registry(first_attempt),
    )
    first_manager.start()
    try:
        submitted = submit_run(
            first_manager,
            RunCreateRequest(
                thread_id="reconciliation-pending",
                agent_version="1.2.0",
                user_message="Create the test action.",
            ),
        )
        assert first_attempt.wait(timeout=5)
    finally:
        first_manager.stop()

    persisted = first_manager.store.get_run_internal(submitted.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert persisted.attempt == 1
    assert (
        persisted.error_code
        == ExternalActionReconciliationPendingError.CODE
    )
    first_event_types = [
        event.event_type
        for event in first_manager.store.list_events(submitted.run_id)
    ]
    assert "run.failed" not in first_event_types
    assert "run.cancelled" not in first_event_types
    assert "run.completed" not in first_event_types

    cancelled = first_manager.request_cancel(
        submitted.run_id,
        tenant_context=TENANT_CONTEXT,
    )
    assert cancelled.cancel_requested

    recovery_attempt = threading.Event()
    recovered_manager = RuntimeManager(
        SQLiteRunStore(database_path),
        reconciliation_setup_failure_registry(recovery_attempt),
    )
    recovered_manager.start()
    try:
        assert recovery_attempt.wait(timeout=5)
    finally:
        recovered_manager.stop()

    recovered = recovered_manager.store.get_run_internal(submitted.run_id)
    assert recovered is not None
    assert recovered.status == RunStatus.RUNNING
    assert recovered.attempt == 2
    assert recovered.cancel_requested
    assert (
        recovered.error_code
        == ExternalActionReconciliationPendingError.CODE
    )
    event_types = [
        event.event_type
        for event in recovered_manager.store.list_events(submitted.run_id)
    ]
    assert event_types.count("run.recovered") == 1
    assert event_types.count("run.started") == 2
    recovered_events = [
        event
        for event in recovered_manager.store.list_events(submitted.run_id)
        if event.event_type == "run.recovered"
    ]
    assert recovered_events[0].payload == {
        "reason": "external_action_reconciliation_pending"
    }
    assert "run.failed" not in event_types
    assert "run.cancelled" not in event_types
    assert "run.completed" not in event_types


def test_reconciliation_pending_cancel_resolution_clears_marker(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    store._seed_historical_run_for_migration(
        RunRecord(
            run_id="run_pending_cancel_resolution",
            thread_id="pending-cancel-resolution",
            agent_id="travel-agent",
            agent_version="1.2.0",
            domain_id="travel",
            schema_version="1",
            status=RunStatus.RUNNING,
            input=TravelMessageInput(
                user_message="Create the test action."
            ).model_dump(mode="json"),
            error_code=ExternalActionReconciliationPendingError.CODE,
            error="pending reconciliation",
            cancel_requested=True,
            attempt=1,
        )
    )
    manager = RuntimeManager(
        SQLiteRunStore(database_path),
        reconciliation_cancellation_registry(),
    )

    manager.start()
    try:
        result = wait_for_terminal(manager, "run_pending_cancel_resolution")
    finally:
        manager.stop()

    assert result.status == RunStatus.CANCELLED
    assert result.error_code is None
    assert result.error is None
    event_types = [
        event.event_type
        for event in manager.store.list_events("run_pending_cancel_resolution")
    ]
    assert "run.recovered" in event_types
    assert "run.cancelled" in event_types
    assert "run.failed" not in event_types


def test_unmarked_crash_recovery_setup_failure_persists_pending_marker(
    tmp_path,
):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    store._seed_historical_run_for_migration(
        RunRecord(
            run_id="run_unmarked_crash_setup_failure",
            thread_id="unmarked-crash-setup-failure",
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
    attempted = threading.Event()
    manager = RuntimeManager(
        SQLiteRunStore(database_path),
        reconciliation_setup_failure_registry(attempted),
        recovery_reconciliation_required=lambda _run_id: True,
    )

    manager.start()
    try:
        assert attempted.wait(timeout=5)
    finally:
        manager.stop()

    recovered = manager.store.get_run_internal("run_unmarked_crash_setup_failure")
    assert recovered is not None
    assert recovered.status == RunStatus.RUNNING
    assert recovered.cancel_requested
    assert recovered.attempt == 2
    assert (
        recovered.error_code
        == ExternalActionReconciliationPendingError.CODE
    )
    event_types = [
        event.event_type
        for event in manager.store.list_events("run_unmarked_crash_setup_failure")
    ]
    assert "run.recovered" in event_types
    assert "run.cancelled" not in event_types
    assert "run.failed" not in event_types


def test_reconciliation_marker_update_preserves_cancel_cas(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_pending_marker_cancel_race",
            thread_id="pending-marker-cancel-race",
            agent_id="travel-agent",
            agent_version="1.2.0",
            status=RunStatus.QUEUED,
            input_message="Create the test action.",
        ),
    )
    cancelled = store.request_cancel_atomically(run.run_id, tenant_id=run.tenant_id)
    assert cancelled.cancel_requested

    outcome = store.commit_reconciliation_pending(
        run.run_id,
        tenant_id=run.tenant_id,
        lease_token=lease_token,
        error_code=ExternalActionReconciliationPendingError.CODE,
        error="pending reconciliation",
    )
    marked = store.get_run_internal(run.run_id)

    assert outcome == RunCommitOutcome.COMMITTED
    assert marked is not None
    assert marked.status == RunStatus.RUNNING
    assert marked.cancel_requested
    assert marked.error_code == ExternalActionReconciliationPendingError.CODE


def test_running_run_is_recovered_after_restart(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    run = RunRecord(run_id="run_recovery_test", thread_id="recovery-thread", agent_id="travel-agent", agent_version="0.3.0", status=RunStatus.RUNNING, input_message="I want a 5-day Tokyo trip under 9000 SGD.", attempt=1)
    store._seed_historical_run_for_migration(
        run,
        event_type="run.started",
        payload={"attempt": 1},
    )
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
        status=RunStatus.QUEUED,
        input=runtime_input.model_dump(mode="json"),
        attempt=0,
    )
    run_store = SQLiteRunStore(database_path)
    run_store.create_run(run)
    seed_claim = run_store.claim_next_run(
        owner_id="crashed-manager",
        lease_duration_seconds=30,
    )
    assert seed_claim is not None
    lease_token = seed_claim.lease_token

    workflow_store = SQLiteWorkflowStore(database_path)
    canonical_manifest = _canonicalize_manifest(manifest)
    manifest_hash = _stable_hash(canonical_manifest.model_dump(mode="json"))
    workflow_store.create_or_get_execution(
        run.run_id,
        WORKFLOW_TYPE,
        manifest_hash,
        lease_token=lease_token,
    )
    workflow_store.mark_running(run.run_id, lease_token=lease_token)
    first_step = STEP_SEQUENCE[0]
    step_hash = _stable_hash(first_step.build_arguments(canonical_manifest, {}))
    workflow_store.claim_step(
        run.run_id,
        first_step.step_id,
        first_step.tool_name,
        step_hash,
        max_attempts=MAX_ATTEMPTS,
        lease_token=lease_token,
    )
    assert run_store.expire_run_lease(run.run_id, lease_token=lease_token)

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
    run_store._seed_historical_run_for_migration(
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
        status=RunStatus.QUEUED,
        input=runtime_input.model_dump(mode="json"),
        attempt=0,
    )
    run_store.create_run(target)
    seed_claim = run_store.claim_next_run(
        owner_id="crashed-manager",
        lease_duration_seconds=30,
    )
    assert seed_claim is not None
    lease_token = seed_claim.lease_token
    workflow_store.create_or_get_execution(
        target.run_id,
        WORKFLOW_TYPE,
        _stable_hash(
            {
                "manifest": manifest.model_dump(mode="json"),
                "replay": replay.model_dump(mode="json"),
            }
        ),
        lease_token=lease_token,
    )
    workflow_store.mark_running(target.run_id, lease_token=lease_token)
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
            lease_token=lease_token,
        )
    interrupted = by_id["run_unit_tests"]
    workflow_store.claim_step(
        target.run_id,
        interrupted.step_id,
        interrupted.tool_name,
        _stable_hash(interrupted.build_arguments(manifest, {})),
        max_attempts=MAX_ATTEMPTS,
        lease_token=lease_token,
    )
    assert run_store.expire_run_lease(target.run_id, lease_token=lease_token)

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


def test_commit_completed_run_is_idempotent_on_duplicate_call(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_finalize_test",
            thread_id="finalize-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )
    assert (
        store.commit_reconciliation_pending(
            run.run_id,
            tenant_id=run.tenant_id,
            lease_token=lease_token,
            error_code=ExternalActionReconciliationPendingError.CODE,
            error="pending reconciliation",
        )
        == RunCommitOutcome.COMMITTED
    )
    persisted_pending = store.get_run_internal(run.run_id)
    assert persisted_pending is not None
    run = persisted_pending
    run.state = AgentState(thread_id="finalize-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    first = store.commit_completed_run(run, lease_token=lease_token)
    assert first == RunCommitOutcome.COMMITTED
    assert run.status == RunStatus.COMPLETED
    assert run.error_code is None
    assert run.error is None
    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.error_code is None
    assert persisted.error is None

    events_after_first = store.list_events(run.run_id)
    event_types_after_first = [event.event_type for event in events_after_first]
    assert event_types_after_first.count("checkpoint.saved") == 1
    assert event_types_after_first.count("run.completed") == 1

    second = store.commit_completed_run(run, lease_token=lease_token)

    assert second == RunCommitOutcome.ALREADY_TERMINAL
    # A duplicate call must not write any new checkpoint or event: the
    # conditional UPDATE affects zero rows because status is no longer
    # RUNNING, so nothing past it in the method body ever runs.
    events_after_second = store.list_events(run.run_id)
    assert events_after_second == events_after_first

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED


def test_commit_completed_run_rejects_when_cancel_requested_first(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_cancel_race_test",
            thread_id="cancel-race-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )

    # Simulate a cancel committing to the database first, in the window
    # between a worker starting execution and it finishing.
    cancelled = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert cancelled.cancel_requested is True
    assert cancelled.status == RunStatus.RUNNING

    run.state = AgentState(thread_id="cancel-race-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    result = store.commit_completed_run(run, lease_token=lease_token)

    assert result == RunCommitOutcome.CANCEL_REQUESTED
    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    # RuntimeManager -- not commit_completed_run -- owns the transition to
    # CANCELLED after this outcome; the row is left RUNNING here.
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


def test_commit_cancelled_run_is_idempotent_on_duplicate_call(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_dup_cancel_finalize",
            thread_id="dup-cancel-finalize-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )
    run = store.request_cancel_atomically(run.run_id, tenant_id=run.tenant_id)

    first = store.commit_cancelled_run(
        run,
        reason="cancelled_after_execution_boundary",
        lease_token=lease_token,
    )
    assert first == RunCommitOutcome.COMMITTED
    assert run.status == RunStatus.CANCELLED

    events_after_first = store.list_events(run.run_id)
    event_types_after_first = [event.event_type for event in events_after_first]
    assert event_types_after_first.count("run.cancelled") == 1

    second = store.commit_cancelled_run(
        run,
        reason="cancelled_after_execution_boundary",
        lease_token=lease_token,
    )

    assert second == RunCommitOutcome.ALREADY_TERMINAL
    # Same guarantee as commit_completed_run: a duplicate call must not
    # write any new event because the conditional UPDATE affects zero rows
    # once status is no longer QUEUED/RUNNING.
    events_after_second = store.list_events(run.run_id)
    assert events_after_second == events_after_first

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.CANCELLED


def test_cancellation_event_order_is_cancel_requested_before_cancelled(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_cancel_order",
            thread_id="cancel-order-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )

    run = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert (
        store.commit_cancelled_run(
            run,
            reason="cancelled_after_execution_boundary",
            lease_token=lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )

    events = [event.event_type for event in store.list_events(run.run_id)]
    assert events.index("run.cancel_requested") < events.index("run.cancelled")


def test_completion_and_cancellation_are_mutually_exclusive_when_cancel_wins(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_race_cancel_wins",
            thread_id="race-cancel-wins-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )

    # Cancel commits first, exactly like the real cancelled_after_execution_
    # boundary path: cancel_requested flips to 1 while the run is still
    # RUNNING and the runtime step is still in flight.
    store.request_cancel_atomically(run.run_id, tenant_id="legacy")

    run.state = AgentState(thread_id="race-cancel-wins-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    completed = store.commit_completed_run(run, lease_token=lease_token)
    assert completed == RunCommitOutcome.CANCEL_REQUESTED

    cancelled = store.commit_cancelled_run(
        run,
        reason="cancelled_after_execution_boundary",
        lease_token=lease_token,
    )
    assert cancelled == RunCommitOutcome.COMMITTED

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
    run, lease_token = claim_store_run(
        store,
        RunRecord(
            run_id="run_race_completion_wins",
            thread_id="race-completion-wins-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input_message="I want a 5-day Tokyo trip under 9000 SGD.",
        ),
    )

    run.state = AgentState(thread_id="race-completion-wins-thread", destination="Tokyo", days=5, budget=9000)
    run.output_message = "Planned."
    run.validation_errors = []

    completed = store.commit_completed_run(run, lease_token=lease_token)
    assert completed == RunCommitOutcome.COMMITTED

    # A cancel arriving after completion must not be able to flip anything:
    # the row is no longer QUEUED/RUNNING, so the CAS simply does not match.
    cancel_request_result = store.request_cancel_atomically(run.run_id, tenant_id="legacy")
    assert cancel_request_result.cancel_requested is False
    assert cancel_request_result.status == RunStatus.COMPLETED

    finalize_cancel_result = store.commit_cancelled_run(
        run,
        reason="cancelled_after_execution_boundary",
        lease_token=lease_token,
    )
    assert finalize_cancel_result == RunCommitOutcome.ALREADY_TERMINAL

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
    store._seed_historical_run_for_migration(
        failed,
        event_type="run.failed",
        payload={"error": failed.error},
    )

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
