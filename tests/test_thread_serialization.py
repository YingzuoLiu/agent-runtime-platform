from __future__ import annotations

import sqlite3
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
    assert (
        store_b.claim_next_run(
            owner_id="manager-b",
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )
        is None
    )

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


def test_expired_predecessor_is_recovered_before_same_thread_successor(
    tmp_path,
    manual_store_clock,
) -> None:
    clock = manual_store_clock
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
    assert (
        store_a.claim_next_run(
            owner_id="manager-c",
            lease_duration_seconds=LEASE_DURATION_SECONDS,
        )
        is None
    )

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
    manual_store_clock,
) -> None:
    clock = manual_store_clock
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
    assert [event.event_type for event in store.list_events("run-head")] == ["run.queued"]
