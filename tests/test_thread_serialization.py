from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from domains.travel.state import AgentState
from runtime_service import (
    RunRecord,
    RunStatus,
    SQLiteRunStore,
    ThreadStateConflictError,
    build_default_registry,
)
from runtime_service.models import RunCommitOutcome, RunLeaseRecoveryReason


TENANT_ID = "thread-serialization-tenant"
LEASE_DURATION_SECONDS = 10


class ManualLeaseClock:
    """Thread-safe store clock advanced explicitly at lease boundaries."""

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


def make_store(database_path, *, clock=None) -> SQLiteRunStore:
    return SQLiteRunStore(
        database_path,
        state_registry=build_default_registry(),
        lease_clock_ms=clock,
    )


def create_queued_run(
    store: SQLiteRunStore,
    run_id: str,
    *,
    thread_id: str,
    tenant_id: str = TENANT_ID,
    order: int,
) -> RunRecord:
    timestamp = f"2026-08-22T00:00:{order:02d}+00:00"
    run = RunRecord(
        run_id=run_id,
        tenant_id=tenant_id,
        thread_id=thread_id,
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.QUEUED,
        input={"user_message": f"execute {run_id}"},
        created_at=timestamp,
        updated_at=timestamp,
    )
    store.create_run_with_event(run, event_type="run.queued")
    return run


def claim_concurrently(
    first_store: SQLiteRunStore,
    second_store: SQLiteRunStore,
):
    barrier = threading.Barrier(3)

    def claim(store: SQLiteRunStore, owner_id: str):
        barrier.wait(timeout=5)
        return store.claim_next_run(
            owner_id=owner_id,
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(claim, first_store, "manager-a")
        second = executor.submit(claim, second_store, "manager-b")
        barrier.wait(timeout=5)
        return first.result(timeout=5), second.result(timeout=5)


def complete_with_budget(store: SQLiteRunStore, claim, *, budget: int) -> None:
    claim.run.state = AgentState(
        thread_id=claim.run.thread_id,
        destination="Tokyo",
        budget=budget,
    )
    claim.run.output_message = f"budget={budget}"
    claim.run.validation_errors = []
    assert (
        store.commit_completed_run(
            claim.run,
            lease_token=claim.lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )


def test_two_store_instances_cannot_double_claim_one_thread(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path)
    store_b = make_store(database_path)
    create_queued_run(store_a, "run-first", thread_id="shared-thread", order=1)
    create_queued_run(store_a, "run-second", thread_id="shared-thread", order=2)

    claims = claim_concurrently(store_a, store_b)

    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0].run.run_id == "run-first"
    assert claimed[0].run.checkpoint_base_revision == 0
    queued = store_a.get_run_internal("run-second")
    assert queued is not None
    assert queued.status == RunStatus.QUEUED
    assert queued.attempt == 0


def test_partial_unique_index_rejects_a_second_running_row(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store = make_store(database_path)
    create_queued_run(store, "run-first", thread_id="index-thread", order=1)
    create_queued_run(store, "run-second", thread_id="index-thread", order=2)
    first = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert first is not None

    with sqlite3.connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?",
                (RunStatus.RUNNING.value, "run-second"),
            )

    second = store.get_run_internal("run-second")
    assert second is not None
    assert second.status == RunStatus.QUEUED


def test_blocked_thread_does_not_prevent_claiming_another_thread(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path)
    store_b = make_store(database_path)
    create_queued_run(store_a, "run-active", thread_id="blocked-thread", order=1)
    create_queued_run(store_a, "run-blocked", thread_id="blocked-thread", order=2)
    create_queued_run(store_a, "run-independent", thread_id="independent-thread", order=3)

    active = store_a.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert active is not None
    assert active.run.run_id == "run-active"

    independent = store_b.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )

    assert independent is not None
    assert independent.run.run_id == "run-independent"
    blocked = store_a.get_run_internal("run-blocked")
    assert blocked is not None
    assert blocked.status == RunStatus.QUEUED
    assert blocked.attempt == 0


@pytest.mark.parametrize(
    ("first_tenant", "first_thread", "second_tenant", "second_thread"),
    [
        (TENANT_ID, "thread-a", TENANT_ID, "thread-b"),
        ("tenant-a", "shared-name", "tenant-b", "shared-name"),
    ],
    ids=["different-threads", "same-thread-name-different-tenants"],
)
def test_thread_scope_allows_independent_claims(
    tmp_path,
    first_tenant: str,
    first_thread: str,
    second_tenant: str,
    second_thread: str,
) -> None:
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path)
    store_b = make_store(database_path)
    create_queued_run(
        store_a,
        "run-a",
        tenant_id=first_tenant,
        thread_id=first_thread,
        order=1,
    )
    create_queued_run(
        store_a,
        "run-b",
        tenant_id=second_tenant,
        thread_id=second_thread,
        order=2,
    )

    claims = claim_concurrently(store_a, store_b)

    assert {claim.run.run_id for claim in claims if claim is not None} == {
        "run-a",
        "run-b",
    }


def test_successor_claim_captures_predecessor_checkpoint_revision_and_state(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path)
    store_b = make_store(database_path)
    create_queued_run(store_a, "run-first", thread_id="revision-thread", order=1)
    create_queued_run(store_a, "run-second", thread_id="revision-thread", order=2)

    first = store_a.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert first is not None
    assert first.run.checkpoint_base_revision == 0
    assert store_b.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    ) is None

    complete_with_budget(store_a, first, budget=7_000)
    first_snapshot = store_b.load_thread_state_snapshot(
        "revision-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert first_snapshot.revision == 1
    assert isinstance(first_snapshot.state, AgentState)
    assert first_snapshot.state.budget == 7_000

    second = store_b.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert second is not None
    assert second.run.run_id == "run-second"
    assert second.run.checkpoint_base_revision == 1
    loaded_for_second = store_b.load_thread_state_snapshot(
        "revision-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert loaded_for_second == first_snapshot

    complete_with_budget(store_b, second, budget=9_000)
    final_snapshot = store_a.load_thread_state_snapshot(
        "revision-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert final_snapshot.revision == 2
    assert isinstance(final_snapshot.state, AgentState)
    assert final_snapshot.state.budget == 9_000
    checkpoint_events = [
        event
        for event in store_a.list_events("run-second")
        if event.event_type == "checkpoint.saved"
    ]
    assert [event.payload for event in checkpoint_events] == [
        {
            "thread_id": "revision-thread",
            "trace_events": 0,
            "base_revision": 1,
            "revision": 2,
        }
    ]


def test_expired_predecessor_is_recovered_before_same_thread_successor(tmp_path) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path, clock=clock)
    store_b = make_store(database_path, clock=clock)
    create_queued_run(store_a, "run-predecessor", thread_id="recovery-thread", order=1)
    create_queued_run(store_a, "run-successor", thread_id="recovery-thread", order=2)

    first_attempt = store_a.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert first_attempt is not None
    assert first_attempt.run.run_id == "run-predecessor"
    assert first_attempt.run.lease_expires_at is not None

    clock.advance(first_attempt.run.lease_expires_at - clock())
    recovered = store_b.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )

    assert recovered is not None
    assert recovered.run.run_id == "run-predecessor"
    assert recovered.run.attempt == 2
    assert recovered.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
    assert recovered.run.checkpoint_base_revision == 0
    assert store_a.claim_next_run(
        owner_id="manager-c",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    ) is None

    complete_with_budget(store_b, recovered, budget=8_000)
    successor = store_a.claim_next_run(
        owner_id="manager-c",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert successor is not None
    assert successor.run.run_id == "run-successor"
    assert successor.run.checkpoint_base_revision == 1
    snapshot = store_a.load_thread_state_snapshot(
        "recovery-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert isinstance(snapshot.state, AgentState)
    assert snapshot.state.budget == 8_000


def test_takeover_conditional_update_rechecks_no_live_running_sibling(
    tmp_path,
    monkeypatch,
) -> None:
    clock = ManualLeaseClock()
    database_path = tmp_path / "runtime.db"
    store = make_store(database_path, clock=clock)
    create_queued_run(store, "run-expired", thread_id="defense-thread", order=1)
    expired_claim = store.claim_next_run(
        owner_id="manager-expired",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert expired_claim is not None
    create_queued_run(store, "run-sibling", thread_id="defense-thread", order=2)
    assert expired_claim.run.lease_expires_at is not None
    clock.advance(expired_claim.run.lease_expires_at - clock())

    # Simulate loss of the unique-index backstop, then inject a live sibling
    # after candidate selection but before the conditional UPDATE. The UPDATE
    # must independently repeat the selector's live-sibling predicate.
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP INDEX idx_runs_one_running_per_thread")

    original_connect = store._lease_connect
    injected = False

    class InjectingConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self._connection = connection

        def execute(self, sql: str, parameters=()):
            nonlocal injected
            if not injected and sql.lstrip().startswith("UPDATE runs SET"):
                injected = True
                self._connection.execute(
                    """
                    UPDATE runs SET
                        status = ?, attempt = 1,
                        checkpoint_base_revision = 0,
                        lease_owner_id = ?, lease_token = ?,
                        lease_heartbeat_at = ?, lease_expires_at = ?
                    WHERE run_id = ?
                    """,
                    (
                        RunStatus.RUNNING.value,
                        "manager-sibling",
                        "lease_sibling",
                        clock(),
                        clock() + LEASE_DURATION_SECONDS * 1000,
                        "run-sibling",
                    ),
                )
            return self._connection.execute(sql, parameters)

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

    @contextmanager
    def injecting_connect():
        with original_connect() as connection:
            yield InjectingConnection(connection)

    monkeypatch.setattr(store, "_lease_connect", injecting_connect)

    assert (
        store.claim_next_run(
            owner_id="manager-takeover",
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )
        is None
    )
    assert injected
    expired = store.get_run_internal("run-expired")
    sibling = store.get_run_internal("run-sibling")
    assert expired is not None and expired.attempt == 1
    assert expired.lease_token == expired_claim.lease_token
    assert sibling is not None and sibling.status == RunStatus.RUNNING
    assert sibling.lease_token == "lease_sibling"


def test_checkpoint_revision_conflict_fails_durably_without_overwrite(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store_a = make_store(database_path)
    store_b = make_store(database_path)
    create_queued_run(store_a, "run-seed", thread_id="conflict-thread", order=1)
    seed = store_a.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert seed is not None
    complete_with_budget(store_a, seed, budget=6_000)

    create_queued_run(store_a, "run-stale", thread_id="conflict-thread", order=2)
    stale = store_a.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert stale is not None
    assert stale.run.checkpoint_base_revision == 1

    injected_state = AgentState(
        thread_id="conflict-thread",
        destination="Tokyo",
        budget=7_777,
    )
    with sqlite3.connect(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE thread_states
            SET state_json = ?, revision = revision + 1
            WHERE tenant_id = ? AND thread_id = ? AND revision = 1
            """,
            (injected_state.model_dump_json(), TENANT_ID, "conflict-thread"),
        )
        assert cursor.rowcount == 1

    stale.run.state = AgentState(
        thread_id="conflict-thread",
        destination="Tokyo",
        budget=9_999,
    )
    stale.run.output_message = "this stale result must not win"
    outcome = store_a.commit_completed_run(
        stale.run,
        lease_token=stale.lease_token,
    )

    assert outcome == RunCommitOutcome.CHECKPOINT_CONFLICT
    persisted = store_b.get_run_internal("run-stale")
    assert persisted is not None
    assert persisted.status == RunStatus.FAILED
    assert persisted.error_code == "thread_checkpoint_conflict"
    assert persisted.lease_token is None
    assert "expected 1, observed 2" in (persisted.error or "")

    snapshot = store_b.load_thread_state_snapshot(
        "conflict-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.revision == 2
    assert isinstance(snapshot.state, AgentState)
    assert snapshot.state.budget == 7_777

    events = store_b.list_events("run-stale")
    event_types = [event.event_type for event in events]
    assert event_types == [
        "run.queued",
        "run.started",
        "checkpoint.conflict",
        "run.failed",
    ]
    conflict = next(event for event in events if event.event_type == "checkpoint.conflict")
    assert conflict.payload == {
        "phase": "completion",
        "expected_revision": 1,
        "observed_revision": 2,
    }
    assert "checkpoint.saved" not in event_types
    assert "run.completed" not in event_types


def test_completion_revision_conflict_precedes_corrupt_checkpoint_decoding(
    tmp_path,
) -> None:
    database_path = tmp_path / "runtime.db"
    store = make_store(database_path)
    create_queued_run(store, "run-corrupt-drift", thread_id="corrupt-drift", order=1)
    claim = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert claim is not None
    assert claim.run.checkpoint_base_revision == 0
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO thread_states (
                tenant_id, thread_id, domain_id, schema_version,
                state_json, updated_at, revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TENANT_ID,
                claim.run.thread_id,
                "corrupt-domain",
                "corrupt-schema",
                "not-json",
                "2026-08-22T00:00:00+00:00",
                1,
            ),
        )
    claim.run.state = AgentState(
        thread_id=claim.run.thread_id,
        destination="Tokyo",
    )

    outcome = store.commit_completed_run(
        claim.run,
        lease_token=claim.lease_token,
    )

    assert outcome == RunCommitOutcome.CHECKPOINT_CONFLICT
    persisted = store.get_run_internal(claim.run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.FAILED
    assert persisted.error_code == "thread_checkpoint_conflict"
    assert [event.event_type for event in store.list_events(claim.run.run_id)] == [
        "run.queued",
        "run.started",
        "checkpoint.conflict",
        "run.failed",
    ]


def test_current_completion_requires_checkpoint_state(tmp_path) -> None:
    store = make_store(tmp_path / "runtime.db")
    create_queued_run(store, "run-no-state", thread_id="no-state-thread", order=1)
    claim = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert claim is not None

    with pytest.raises(ValueError, match="must include checkpoint state"):
        store.commit_completed_run(
            claim.run,
            lease_token=claim.lease_token,
        )

    persisted = store.get_run_internal(claim.run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert persisted.lease_token == claim.lease_token
    snapshot = store.load_thread_state_snapshot(
        "no-state-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.state is None
    assert snapshot.revision == 0
    assert [event.event_type for event in store.list_events(claim.run.run_id)] == [
        "run.queued",
        "run.started",
    ]


def test_completion_cannot_redirect_checkpoint_to_another_thread(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store = make_store(database_path)
    create_queued_run(store, "run-thread-a", thread_id="thread-a", order=1)
    create_queued_run(store, "run-thread-b", thread_id="thread-b", order=2)
    claim_a = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    claim_b = store.claim_next_run(
        owner_id="manager-b",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert claim_a is not None and claim_a.run.run_id == "run-thread-a"
    assert claim_b is not None and claim_b.run.run_id == "run-thread-b"

    claim_a.run.thread_id = "thread-b"
    claim_a.run.state = AgentState(
        thread_id="thread-b",
        destination="Tokyo",
    )
    with pytest.raises(ValueError, match="identity must match the persisted Run"):
        store.commit_completed_run(
            claim_a.run,
            lease_token=claim_a.lease_token,
        )

    persisted_a = store.get_run_internal("run-thread-a")
    persisted_b = store.get_run_internal("run-thread-b")
    assert persisted_a is not None and persisted_a.status == RunStatus.RUNNING
    assert persisted_a.thread_id == "thread-a"
    assert persisted_a.lease_token == claim_a.lease_token
    assert persisted_b is not None and persisted_b.status == RunStatus.RUNNING
    assert persisted_b.lease_token == claim_b.lease_token
    snapshot_a = store.load_thread_state_snapshot(
        "thread-a",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    snapshot_b = store.load_thread_state_snapshot(
        "thread-b",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot_a.revision == 0 and snapshot_a.state is None
    assert snapshot_b.revision == 0 and snapshot_b.state is None


def test_completion_event_failure_rolls_back_checkpoint_run_and_lease(tmp_path) -> None:
    database_path = tmp_path / "runtime.db"
    store = make_store(database_path)
    create_queued_run(store, "run-atomic", thread_id="atomic-thread", order=1)
    claim = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert claim is not None
    claim.run.state = AgentState(
        thread_id=claim.run.thread_id,
        destination="Tokyo",
    )
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TRIGGER fail_run_completed_event
            BEFORE INSERT ON run_events
            WHEN NEW.event_type = 'run.completed'
            BEGIN
                SELECT RAISE(ABORT, 'injected run.completed failure');
            END;
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected run.completed failure"):
        store.commit_completed_run(
            claim.run,
            lease_token=claim.lease_token,
        )

    persisted = store.get_run_internal(claim.run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert persisted.lease_token == claim.lease_token
    snapshot = store.load_thread_state_snapshot(
        "atomic-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.state is None
    assert snapshot.revision == 0
    assert [event.event_type for event in store.list_events(claim.run.run_id)] == [
        "run.queued",
        "run.started",
    ]

    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TRIGGER fail_run_completed_event")
    assert (
        store.commit_completed_run(
            claim.run,
            lease_token=claim.lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )
    committed = store.load_thread_state_snapshot(
        "atomic-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert committed.revision == 1


def test_failed_and_cancelled_runs_do_not_advance_checkpoint_revision(tmp_path) -> None:
    store = make_store(tmp_path / "runtime.db")
    create_queued_run(store, "run-seed", thread_id="terminal-thread", order=1)
    seed = store.claim_next_run(
        owner_id="manager-seed",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert seed is not None
    complete_with_budget(store, seed, budget=6_000)

    create_queued_run(store, "run-failed", thread_id="terminal-thread", order=2)
    failed = store.claim_next_run(
        owner_id="manager-failed",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert failed is not None
    assert (
        store.commit_failed_run(
            failed.run,
            lease_token=failed.lease_token,
            error_code="expected_failure",
            error="expected failure",
        )
        == RunCommitOutcome.COMMITTED
    )

    create_queued_run(store, "run-cancelled", thread_id="terminal-thread", order=3)
    cancelled = store.claim_next_run(
        owner_id="manager-cancelled",
        lease_duration_seconds=LEASE_DURATION_SECONDS,
    )
    assert cancelled is not None
    store.request_cancel_atomically(cancelled.run.run_id, tenant_id=TENANT_ID)
    assert (
        store.commit_cancelled_run(
            cancelled.run,
            reason="test cancellation",
            lease_token=cancelled.lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )

    snapshot = store.load_thread_state_snapshot(
        "terminal-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.revision == 1
    assert isinstance(snapshot.state, AgentState)
    assert snapshot.state.budget == 6_000


def test_client_state_seed_is_rejected_behind_existing_queued_work(tmp_path) -> None:
    store = make_store(tmp_path / "runtime.db")
    create_queued_run(store, "run-head", thread_id="seed-thread", order=1)
    seeded = RunRecord(
        run_id="run-seed-behind-head",
        tenant_id=TENANT_ID,
        thread_id="seed-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.QUEUED,
        input={"user_message": "use stale client state"},
        state=AgentState(thread_id="seed-thread", destination="Seoul"),
        created_at="2026-08-22T00:00:02+00:00",
        updated_at="2026-08-22T00:00:02+00:00",
    )

    with pytest.raises(
        ThreadStateConflictError,
        match="can only initialize an empty thread",
    ):
        store.create_run_with_event(seeded, event_type="run.queued")

    assert store.get_run_internal(seeded.run_id) is None
    assert [
        event.event_type for event in store.list_events("run-head")
    ] == ["run.queued"]
