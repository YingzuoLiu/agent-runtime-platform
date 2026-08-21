from __future__ import annotations

import threading

import pytest

from agent.contracts import RuntimeExecutionContext, RuntimeResponse
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
)
from runtime_service.models import (
    RunCommitOutcome,
    RunLeaseRecoveryReason,
)


TENANT_CONTEXT = TenantContext(
    tenant_id="lease-test-tenant",
    subject_id="lease-test-subject",
)
LEASE_DURATION_SECONDS = 10


def test_manager_rejects_heartbeat_configuration_without_renewal_margin(tmp_path):
    store = SQLiteRunStore(
        tmp_path / "runtime.db",
        lease_operation_timeout_seconds=2,
    )

    with pytest.raises(ValueError, match="must leave time before lease expiry"):
        RuntimeManager(
            store,
            AgentRegistry(),
            lease_duration_seconds=10,
            heartbeat_interval_seconds=8,
        )


class ManualLeaseClock:
    """Thread-safe store clock controlled explicitly by each test."""

    def __init__(self, now_ms: int = 1_000_000) -> None:
        self._now_ms = now_ms
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            return self._now_ms

    def advance(self, delta_ms: int) -> int:
        with self._lock:
            self._now_ms += delta_ms
            return self._now_ms


class DroppedWakeSignal:
    """Event-shaped test double that drops every in-process wake hint."""

    def __init__(self) -> None:
        self.wait_started = threading.Event()
        self._never_set = threading.Event()

    def set(self) -> None:
        return

    def clear(self) -> None:
        return

    def wait(self, timeout: float | None = None) -> bool:
        self.wait_started.set()
        return self._never_set.wait(timeout)


class SequencedExecution:
    """Coordinates two attempts without using timing as synchronization."""

    def __init__(self, *, first_attempt_fails: bool = False) -> None:
        self.first_attempt_fails = first_attempt_fails
        self.started = [threading.Event(), threading.Event()]
        self.release = [threading.Event(), threading.Event()]
        self.returned = [threading.Event(), threading.Event()]
        self._next_attempt = 0
        self._lock = threading.Lock()

    def begin_attempt(self) -> int:
        with self._lock:
            attempt_index = self._next_attempt
            self._next_attempt += 1
        if attempt_index >= len(self.started):
            raise AssertionError("unexpected third runtime attempt")
        self.started[attempt_index].set()
        return attempt_index


class SequencedRuntime(TravelAgentRuntime):
    def __init__(self, execution: SequencedExecution) -> None:
        super().__init__()
        self.execution = execution

    def execute(
        self,
        state: AgentState,
        runtime_input: TravelMessageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[AgentState]:
        del runtime_input, context
        attempt_index = self.execution.begin_attempt()
        try:
            if not self.execution.release[attempt_index].wait(timeout=5):
                raise TimeoutError("test release event was not set")
            if attempt_index == 0 and self.execution.first_attempt_fails:
                raise RuntimeError("stale first attempt failed")
            attempt_number = attempt_index + 1
            result_state = state.model_copy(
                deep=True,
                update={
                    "current_stage": f"attempt-{attempt_number}",
                    "tool_outputs": {"winning_attempt": attempt_number},
                },
            )
            return RuntimeResponse[AgentState](
                message=f"result-from-attempt-{attempt_number}",
                state=result_state,
                validation_errors=[],
            )
        finally:
            self.execution.returned[attempt_index].set()


def sequenced_registry(execution: SequencedExecution) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        "lease-test-agent",
        "1.0.0",
        lambda: SequencedRuntime(execution),
        description="Deterministic lease fencing test runtime",
        input_model=TravelMessageInput,
        state_model=AgentState,
    )
    return registry


def submit_test_run(manager: RuntimeManager, *, thread_id: str) -> RunRecord:
    return manager.submit(
        RunCreateRequest(
            thread_id=thread_id,
            agent_id="lease-test-agent",
            agent_version="1.0.0",
            input={"user_message": "exercise lease fencing"},
        ),
        tenant_context=TENANT_CONTEXT,
    )


def test_store_lease_expiry_is_an_exact_fencing_boundary(tmp_path) -> None:
    clock = ManualLeaseClock()
    store = SQLiteRunStore(tmp_path / "runtime.db", lease_clock_ms=clock)
    run = RunRecord(
        run_id="run_store_lease_boundary",
        tenant_id=TENANT_CONTEXT.tenant_id,
        thread_id="store-lease-boundary",
        agent_id="lease-test-agent",
        agent_version="1.0.0",
        status=RunStatus.QUEUED,
        input={"user_message": "exercise lease fencing"},
    )
    store.create_run_with_event(run, event_type="run.queued")

    first_claim = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert first_claim is not None
    assert first_claim.run.attempt == 1
    assert first_claim.run.lease_heartbeat_at == clock()
    assert first_claim.run.lease_expires_at == (
        clock() + LEASE_DURATION_SECONDS * 1000
    )

    clock.advance(LEASE_DURATION_SECONDS * 1000 - 1)
    assert (
        store.claim_next_run(
            owner_id="manager-b",
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )
        is None
    )

    clock.advance(1)
    assert not store.renew_run_lease(
        run.run_id,
        lease_token=first_claim.lease_token,
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert (
        store.commit_completed_run(
            first_claim.run,
            lease_token=first_claim.lease_token,
        )
        == RunCommitOutcome.LEASE_LOST
    )
    assert (
        store.commit_failed_run(
            first_claim.run,
            lease_token=first_claim.lease_token,
            error_code="stale_attempt_failed",
            error="the expired attempt must not persist this failure",
        )
        == RunCommitOutcome.LEASE_LOST
    )

    second_claim = store.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert second_claim is not None
    assert second_claim.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
    assert second_claim.run.attempt == 2
    assert second_claim.lease_token != first_claim.lease_token

    second_claim.run.output_message = "winner"
    assert (
        store.commit_completed_run(
            second_claim.run,
            lease_token=second_claim.lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )

    persisted = store.get_run_internal(run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 2
    assert persisted.output_message == "winner"
    event_types = [event.event_type for event in store.list_events(run.run_id)]
    assert event_types.count("run.started") == 2
    assert event_types.count("run.recovered") == 1
    assert event_types.count("checkpoint.saved") == 1
    assert event_types.count("run.completed") == 1
    assert "run.failed" not in event_types


def test_second_manager_cannot_execute_a_live_unexpired_lease(
    tmp_path,
    monkeypatch,
) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store_a = SQLiteRunStore(database_path, lease_clock_ms=clock)
    store_b = SQLiteRunStore(database_path, lease_clock_ms=clock)
    execution = SequencedExecution()
    registry = sequenced_registry(execution)
    second_manager_polled = threading.Event()
    second_manager_claimed: list[bool] = []
    original_claim = store_b.claim_next_run

    def observe_second_manager_claim(
        *,
        owner_id: str,
        lease_duration_seconds: int,
        reconciliation_pending_code: str | None = None,
    ):
        claim = original_claim(
            owner_id=owner_id,
            lease_duration_seconds=lease_duration_seconds,
            reconciliation_pending_code=reconciliation_pending_code,
        )
        second_manager_claimed.append(claim is not None)
        second_manager_polled.set()
        return claim

    monkeypatch.setattr(store_b, "claim_next_run", observe_second_manager_claim)
    manager_a = RuntimeManager(
        store_a,
        registry,
        owner_id="live-lease-manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    manager_b = RuntimeManager(
        store_b,
        registry,
        owner_id="live-lease-manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )

    manager_a.start()
    manager_b_started = False
    try:
        submitted = submit_test_run(manager_a, thread_id="live-lease-single-owner")
        assert execution.started[0].wait(timeout=5)

        active = store_a.get_run_internal(submitted.run_id)
        assert active is not None
        assert active.status == RunStatus.RUNNING
        assert active.lease_expires_at is not None
        assert active.lease_expires_at > clock()

        manager_b.start()
        manager_b_started = True
        assert second_manager_polled.wait(timeout=5)
        assert second_manager_claimed
        assert not any(second_manager_claimed)
        assert not execution.started[1].is_set()

        execution.release[0].set()
        assert execution.returned[0].wait(timeout=5)
    finally:
        execution.release[0].set()
        manager_a.stop()
        if manager_b_started:
            manager_b.stop()

    persisted = store_a.get_run_internal(submitted.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 1
    event_types = [event.event_type for event in store_a.list_events(submitted.run_id)]
    assert event_types.count("run.started") == 1
    assert "run.recovered" not in event_types


def test_manager_polling_discovers_queued_run_when_wake_hint_is_lost(
    tmp_path,
) -> None:
    store = SQLiteRunStore(tmp_path / "runtime.db")
    execution = SequencedExecution()
    manager = RuntimeManager(
        store,
        sequenced_registry(execution),
        owner_id="poll-only-manager",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    dropped_wake = DroppedWakeSignal()
    manager._wake = dropped_wake

    manager.start()
    try:
        # Wait until the worker is idle so both start() and submit() wake hints
        # are known to be dropped. The durable queue must remain sufficient.
        assert dropped_wake.wait_started.wait(timeout=5)
        submitted = submit_test_run(manager, thread_id="poll-survives-lost-wake")
        assert execution.started[0].wait(timeout=5)
        execution.release[0].set()
        assert execution.returned[0].wait(timeout=5)
    finally:
        execution.release[0].set()
        manager.stop()

    persisted = store.get_run_internal(submitted.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 1


def test_shutdown_fences_claim_paused_before_heartbeat_registration(
    tmp_path,
    monkeypatch,
) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    execution = SequencedExecution()
    manager = RuntimeManager(
        store,
        sequenced_registry(execution),
        owner_id="shutdown-gap-manager",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=0,
    )
    claim_paused = threading.Event()
    release_claim = threading.Event()
    claim_handler_returned = threading.Event()
    original_execute_claim = manager._execute_claim

    def pause_before_heartbeat_registration(claim) -> None:
        claim_paused.set()
        try:
            if not release_claim.wait(timeout=5):
                raise TimeoutError("claim-registration test release was not set")
            original_execute_claim(claim)
        finally:
            claim_handler_returned.set()

    monkeypatch.setattr(manager, "_execute_claim", pause_before_heartbeat_registration)
    manager.start()
    try:
        submitted = submit_test_run(manager, thread_id="shutdown-registration-gap")
        assert claim_paused.wait(timeout=5)
        claimed = store.get_run_internal(submitted.run_id)
        assert claimed is not None
        assert claimed.status == RunStatus.RUNNING
        assert claimed.lease_expires_at is not None
        assert claimed.lease_expires_at > clock()

        manager.stop()
        release_claim.set()
        assert claim_handler_returned.wait(timeout=5)
        for worker in manager._workers:
            worker.join(timeout=5)

        # No Runtime code starts after the shutdown renewal cutoff. The
        # pre-registered claim is relinquished and immediately recoverable.
        assert not execution.started[0].is_set()
        relinquished = store.get_run_internal(submitted.run_id)
        assert relinquished is not None
        assert relinquished.status == RunStatus.RUNNING
        assert relinquished.lease_expires_at == clock()

        replacement = SQLiteRunStore(
            database_path,
            lease_clock_ms=clock,
        ).claim_next_run(
            owner_id="replacement-manager",
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )
        assert replacement is not None
        assert replacement.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
        assert replacement.run.attempt == 2
        assert replacement.lease_token != claimed.lease_token
    finally:
        release_claim.set()
        manager.stop()


def test_concurrent_stop_calls_cannot_corrupt_a_restarted_manager(
    tmp_path,
    monkeypatch,
) -> None:
    store = SQLiteRunStore(tmp_path / "runtime.db")
    execution = SequencedExecution()
    manager = RuntimeManager(
        store,
        sequenced_registry(execution),
        owner_id="stop-generation-manager",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=5,
    )
    expire_calls = 0
    original_expire = manager._expire_drained_claims

    def observe_expire() -> None:
        nonlocal expire_calls
        expire_calls += 1
        original_expire()

    monkeypatch.setattr(manager, "_expire_drained_claims", observe_expire)
    manager.start()
    first = submit_test_run(manager, thread_id="stop-generation-first")
    assert execution.started[0].wait(timeout=5)

    first_stop_returned = threading.Event()
    second_stop_returned = threading.Event()
    first_stop = threading.Thread(
        target=lambda: (manager.stop(), first_stop_returned.set()),
        name="first-concurrent-stop",
    )
    first_stop.start()

    stop_cycle: threading.Event | None = None
    while stop_cycle is None:
        with manager._lock:
            stop_cycle = manager._stop_in_progress
        if stop_cycle is None:
            threading.Event().wait(0.001)

    follower_waiting = threading.Event()
    original_wait = stop_cycle.wait

    def observe_follower_wait(timeout: float | None = None) -> bool:
        follower_waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(stop_cycle, "wait", observe_follower_wait)
    second_stop = threading.Thread(
        target=lambda: (manager.stop(), second_stop_returned.set()),
        name="second-concurrent-stop",
    )
    second_stop.start()
    assert follower_waiting.wait(timeout=5)

    execution.release[0].set()
    assert first_stop_returned.wait(timeout=5)
    assert second_stop_returned.wait(timeout=5)
    first_stop.join(timeout=5)
    second_stop.join(timeout=5)
    assert expire_calls == 1

    completed = store.get_run_internal(first.run_id)
    assert completed is not None
    assert completed.status == RunStatus.COMPLETED

    manager.start()
    try:
        second = submit_test_run(manager, thread_id="stop-generation-second")
        assert execution.started[1].wait(timeout=5)
        assert manager._started
        assert len(manager._workers) == 1
        assert manager._workers[0].is_alive()
        execution.release[1].set()
        assert execution.returned[1].wait(timeout=5)
    finally:
        execution.release[1].set()
        manager.stop()

    restarted_result = store.get_run_internal(second.run_id)
    assert restarted_result is not None
    assert restarted_result.status == RunStatus.COMPLETED


def test_stop_join_error_still_disables_renewal_and_fences_the_attempt(
    tmp_path,
    monkeypatch,
) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    execution = SequencedExecution()
    manager = RuntimeManager(
        store,
        sequenced_registry(execution),
        owner_id="join-error-manager",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=1,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    manager.start()
    submitted = submit_test_run(manager, thread_id="join-error-fencing")
    assert execution.started[0].wait(timeout=5)
    claimed = store.get_run_internal(submitted.run_id)
    assert claimed is not None
    assert claimed.lease_expires_at is not None

    worker = manager._workers[0]
    original_join = worker.join

    def fail_join(*, timeout: float | None = None) -> None:
        del timeout
        raise RuntimeError("synthetic worker join failure")

    monkeypatch.setattr(worker, "join", fail_join)
    with pytest.raises(RuntimeError, match="synthetic worker join failure"):
        manager.stop()

    assert manager._renewals_disabled.is_set()
    assert not manager._started
    execution.release[0].set()
    assert execution.returned[0].wait(timeout=5)
    original_join(timeout=5)

    stale = store.get_run_internal(submitted.run_id)
    assert stale is not None
    assert stale.status == RunStatus.RUNNING
    assert stale.output_message is None
    clock.advance(claimed.lease_expires_at - clock())
    replacement = SQLiteRunStore(
        database_path,
        lease_clock_ms=clock,
    ).claim_next_run(
        owner_id="join-error-replacement",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert replacement is not None
    assert replacement.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
    assert replacement.run.attempt == 2
    assert replacement.lease_token != claimed.lease_token


@pytest.mark.parametrize(
    "first_attempt_fails",
    [False, True],
    ids=["stale-late-success", "stale-late-failure"],
)
def test_two_managers_fence_a_stale_late_attempt(
    tmp_path,
    monkeypatch,
    first_attempt_fails: bool,
) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store_a = SQLiteRunStore(database_path, lease_clock_ms=clock)
    store_b = SQLiteRunStore(database_path, lease_clock_ms=clock)
    execution = SequencedExecution(first_attempt_fails=first_attempt_fails)
    registry = sequenced_registry(execution)
    stale_commit_observed = threading.Event()
    stale_commit_outcomes: list[RunCommitOutcome] = []
    winning_commit_observed = threading.Event()

    if first_attempt_fails:
        original_stale_commit = store_a.commit_failed_run

        def observe_stale_failure(
            run: RunRecord,
            *,
            lease_token: str,
            error_code: str,
            error: str,
            traceback_text: str | None = None,
            allow_cancel_requested: bool = False,
        ) -> RunCommitOutcome:
            outcome = original_stale_commit(
                run,
                lease_token=lease_token,
                error_code=error_code,
                error=error,
                traceback_text=traceback_text,
                allow_cancel_requested=allow_cancel_requested,
            )
            stale_commit_outcomes.append(outcome)
            stale_commit_observed.set()
            return outcome

        monkeypatch.setattr(store_a, "commit_failed_run", observe_stale_failure)
    else:
        original_stale_commit = store_a.commit_completed_run

        def observe_stale_completion(
            run: RunRecord,
            *,
            lease_token: str,
        ) -> RunCommitOutcome:
            outcome = original_stale_commit(run, lease_token=lease_token)
            stale_commit_outcomes.append(outcome)
            stale_commit_observed.set()
            return outcome

        monkeypatch.setattr(store_a, "commit_completed_run", observe_stale_completion)

    original_winning_commit = store_b.commit_completed_run

    def observe_winning_completion(
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        outcome = original_winning_commit(run, lease_token=lease_token)
        if outcome == RunCommitOutcome.COMMITTED:
            winning_commit_observed.set()
        return outcome

    monkeypatch.setattr(store_b, "commit_completed_run", observe_winning_completion)

    # The long real-time heartbeat interval is deliberate: manual store time
    # reaches expiry before either Manager renews, without sleeping for expiry.
    manager_a = RuntimeManager(
        store_a,
        registry,
        owner_id="manager-a",
        lease_duration_seconds=60,
        heartbeat_interval_seconds=50,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    manager_b = RuntimeManager(
        store_b,
        registry,
        owner_id="manager-b",
        lease_duration_seconds=60,
        heartbeat_interval_seconds=50,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )

    manager_a.start()
    manager_b_started = False
    try:
        submitted = submit_test_run(manager_a, thread_id="two-manager-fencing")
        assert execution.started[0].wait(timeout=5)

        first_attempt = store_a.get_run_internal(submitted.run_id)
        assert first_attempt is not None
        assert first_attempt.attempt == 1
        assert first_attempt.lease_expires_at is not None
        clock.advance(first_attempt.lease_expires_at - clock())

        manager_b.start()
        manager_b_started = True
        assert execution.started[1].wait(timeout=5)

        # Let the replacement attempt durably win before the stale attempt
        # returns or raises.
        execution.release[1].set()
        assert winning_commit_observed.wait(timeout=5)
        winner = store_b.get_run_internal(submitted.run_id)
        assert winner is not None
        assert winner.status == RunStatus.COMPLETED

        execution.release[0].set()
        assert execution.returned[0].wait(timeout=5)
        assert stale_commit_observed.wait(timeout=5)
    finally:
        for release in execution.release:
            release.set()
        manager_a.stop()
        if manager_b_started:
            manager_b.stop()

    assert stale_commit_outcomes
    assert stale_commit_outcomes[-1] != RunCommitOutcome.COMMITTED
    persisted = store_a.get_run_internal(submitted.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 2
    assert persisted.output_message == "result-from-attempt-2"
    assert isinstance(persisted.state, AgentState)
    assert persisted.state.current_stage == "attempt-2"
    assert persisted.state.tool_outputs == {"winning_attempt": 2}

    checkpoint = store_a.load_thread_state(
        submitted.thread_id,
        tenant_id=TENANT_CONTEXT.tenant_id,
        domain_id=AgentState.domain_id,
        schema_version=AgentState.schema_version,
    )
    assert isinstance(checkpoint, AgentState)
    assert checkpoint.current_stage == "attempt-2"
    assert checkpoint.tool_outputs == {"winning_attempt": 2}

    events = store_a.list_events(submitted.run_id)
    event_types = [event.event_type for event in events]
    assert event_types.count("run.started") == 2
    assert event_types.count("run.recovered") == 1
    assert event_types.count("checkpoint.loaded") == 2
    assert event_types.count("checkpoint.saved") == 1
    assert event_types.count("run.completed") == 1
    assert "run.failed" not in event_types
    assert "run.cancelled" not in event_types


def test_manager_heartbeat_renews_the_same_attempt_without_public_events(
    tmp_path,
    monkeypatch,
) -> None:
    clock = ManualLeaseClock()
    store = SQLiteRunStore(tmp_path / "runtime.db", lease_clock_ms=clock)
    execution = SequencedExecution()
    registry = sequenced_registry(execution)
    renewal_observed = threading.Event()
    completion_observed = threading.Event()
    renewal_target_ms = clock() + 5_000
    original_renew = store.renew_run_lease
    original_commit = store.commit_completed_run

    def observe_renewal(
        run_id: str,
        *,
        lease_token: str,
        lease_duration_seconds: int,
    ) -> bool:
        renewed = original_renew(
            run_id,
            lease_token=lease_token,
            lease_duration_seconds=lease_duration_seconds,
        )
        if renewed and clock() == renewal_target_ms:
            renewal_observed.set()
        return renewed

    def observe_completion(
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        outcome = original_commit(run, lease_token=lease_token)
        if outcome == RunCommitOutcome.COMMITTED:
            completion_observed.set()
        return outcome

    monkeypatch.setattr(store, "renew_run_lease", observe_renewal)
    monkeypatch.setattr(store, "commit_completed_run", observe_completion)
    manager = RuntimeManager(
        store,
        registry,
        owner_id="heartbeat-manager",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
        heartbeat_interval_seconds=0.01,
        poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )

    manager.start()
    try:
        submitted = submit_test_run(manager, thread_id="heartbeat-renewal")
        assert execution.started[0].wait(timeout=5)
        before = store.get_run_internal(submitted.run_id)
        assert before is not None
        assert before.lease_token is not None
        assert before.lease_heartbeat_at == clock()
        assert before.lease_expires_at == (
            clock() + LEASE_DURATION_SECONDS * 1000
        )
        events_before = [
            (event.event_type, event.payload)
            for event in store.list_events(submitted.run_id)
        ]

        clock.advance(5_000)
        assert renewal_observed.wait(timeout=5)
        renewed = store.get_run_internal(submitted.run_id)
        assert renewed is not None
        assert renewed.status == RunStatus.RUNNING
        assert renewed.attempt == before.attempt
        assert renewed.lease_token == before.lease_token
        assert renewed.lease_heartbeat_at == renewal_target_ms
        assert renewed.lease_expires_at == (
            renewal_target_ms + LEASE_DURATION_SECONDS * 1000
        )
        assert [
            (event.event_type, event.payload)
            for event in store.list_events(submitted.run_id)
        ] == events_before

        execution.release[0].set()
        assert completion_observed.wait(timeout=5)
    finally:
        execution.release[0].set()
        manager.stop()

    completed = store.get_run_internal(submitted.run_id)
    assert completed is not None
    assert completed.status == RunStatus.COMPLETED
    assert completed.attempt == 1
