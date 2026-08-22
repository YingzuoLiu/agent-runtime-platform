from __future__ import annotations

import pytest

from domains.travel.state import AgentState
from runtime_service.models import RunCommitOutcome, RunLeaseRecoveryReason, RunStatus
from runtime_service.store import (
    RunLeaseLostError,
    ThreadCheckpointRevisionConflictError,
)

from .backends import InjectedConformanceFailure, SQLiteConformanceBackend
from .scenarios import (
    LEASE_SECONDS,
    TENANT_ID,
    claim_concurrently,
    claim_run,
    complete_with_budget,
    create_queued_run,
)


def test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced(
    store_backend: SQLiteConformanceBackend,
) -> None:
    first_store = store_backend.open_run_store()
    second_store = store_backend.open_run_store()
    create_queued_run(
        first_store,
        "run-owner-fence",
        thread_id="owner-fence-thread",
        order=1,
    )

    raced = claim_concurrently(first_store, second_store)
    live_claims = [claim for claim in raced if claim is not None]
    assert len(live_claims) == 1
    first_claim = live_claims[0]
    assert first_claim.run.attempt == 1
    assert first_claim.run.lease_expires_at is not None

    unexpired_store = second_store if raced[0] is not None else first_store
    assert claim_run(unexpired_store, "must-not-double-own") is None

    store_backend.clock.advance(first_claim.run.lease_expires_at - store_backend.clock())
    reopened = store_backend.open_run_store()
    replacement = claim_run(reopened, "replacement-owner")
    assert replacement is not None
    assert replacement.run.run_id == first_claim.run.run_id
    assert replacement.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
    assert replacement.run.attempt == 2
    assert replacement.lease_token != first_claim.lease_token

    with pytest.raises(RunLeaseLostError):
        first_store.append_attempt_event(
            first_claim.run.run_id,
            lease_token=first_claim.lease_token,
            event_type="stale.attempt.write",
        )
    first_claim.run.state = AgentState(
        thread_id=first_claim.run.thread_id,
        destination="Stale",
        budget=1,
    )
    first_claim.run.output_message = "stale output"
    assert (
        first_store.commit_completed_run(
            first_claim.run,
            lease_token=first_claim.lease_token,
        )
        == RunCommitOutcome.LEASE_LOST
    )

    assert complete_with_budget(reopened, replacement, budget=8_000) == RunCommitOutcome.COMMITTED
    persisted = first_store.get_run_internal(first_claim.run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 2
    assert persisted.output_message == "budget=8000"
    assert not any(
        event.event_type == "stale.attempt.write"
        for event in first_store.list_events(first_claim.run.run_id)
    )


def test_i3_one_running_run_per_tenant_qualified_thread(
    store_backend: SQLiteConformanceBackend,
) -> None:
    first_store = store_backend.open_run_store()
    second_store = store_backend.open_run_store()
    create_queued_run(
        first_store,
        "run-thread-first",
        thread_id="serialized-thread",
        order=1,
    )
    create_queued_run(
        first_store,
        "run-thread-second",
        thread_id="serialized-thread",
        order=2,
    )

    claims = claim_concurrently(first_store, second_store)

    live_claims = [claim for claim in claims if claim is not None]
    assert len(live_claims) == 1
    assert live_claims[0].run.run_id == "run-thread-first"
    assert live_claims[0].run.checkpoint_base_revision == 0
    queued = first_store.get_run_internal("run-thread-second")
    assert queued is not None
    assert queued.status == RunStatus.QUEUED
    assert queued.attempt == 0


@pytest.mark.parametrize(
    ("first_tenant", "first_thread", "second_tenant", "second_thread"),
    [
        (TENANT_ID, "thread-a", TENANT_ID, "thread-b"),
        ("tenant-a", "shared-name", "tenant-b", "shared-name"),
    ],
    ids=["different-threads", "same-thread-name-different-tenants"],
)
def test_i4_thread_scope_allows_independent_claims(
    store_backend: SQLiteConformanceBackend,
    first_tenant: str,
    first_thread: str,
    second_tenant: str,
    second_thread: str,
) -> None:
    first_store = store_backend.open_run_store()
    second_store = store_backend.open_run_store()
    create_queued_run(
        first_store,
        "run-thread-a",
        tenant_id=first_tenant,
        thread_id=first_thread,
        order=1,
    )
    create_queued_run(
        first_store,
        "run-thread-b",
        tenant_id=second_tenant,
        thread_id=second_thread,
        order=2,
    )

    claims = claim_concurrently(first_store, second_store)

    assert {claim.run.run_id for claim in claims if claim is not None} == {
        "run-thread-a",
        "run-thread-b",
    }


def test_i5_checkpoint_load_and_completion_require_expected_revision(
    store_backend: SQLiteConformanceBackend,
) -> None:
    store = store_backend.open_run_store()
    create_queued_run(store, "run-checkpoint-seed", thread_id="cas-thread", order=1)
    seed = claim_run(store, "seed-owner")
    assert seed is not None
    assert complete_with_budget(store, seed, budget=6_000) == RunCommitOutcome.COMMITTED

    create_queued_run(store, "run-checkpoint-stale", thread_id="cas-thread", order=2)
    stale = claim_run(store, "stale-owner")
    assert stale is not None
    assert stale.run.checkpoint_base_revision == 1
    store_backend.replace_checkpoint_out_of_band(
        tenant_id=TENANT_ID,
        thread_id="cas-thread",
        state=AgentState(
            thread_id="cas-thread",
            destination="Tokyo",
            budget=7_777,
        ),
        expected_revision=1,
    )

    with pytest.raises(ThreadCheckpointRevisionConflictError) as raised:
        store.load_thread_state_snapshot(
            "cas-thread",
            tenant_id=TENANT_ID,
            domain_id="travel",
            schema_version="1",
            expected_revision=stale.run.checkpoint_base_revision,
            require_revision_match=True,
        )
    assert raised.value.expected_revision == 1
    assert raised.value.observed_revision == 2

    stale.run.state = AgentState(
        thread_id="cas-thread",
        destination="Tokyo",
        budget=9_999,
    )
    stale.run.output_message = "stale checkpoint must not win"
    assert (
        store.commit_completed_run(stale.run, lease_token=stale.lease_token)
        == RunCommitOutcome.CHECKPOINT_CONFLICT
    )

    reopened = store_backend.open_run_store()
    persisted = reopened.get_run_internal(stale.run.run_id)
    snapshot = reopened.load_thread_state_snapshot(
        "cas-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert persisted is not None
    assert persisted.status == RunStatus.FAILED
    assert persisted.error_code == "thread_checkpoint_conflict"
    assert persisted.lease_token is None
    assert "expected 1, observed 2" in (persisted.error or "")
    assert snapshot.revision == 2
    assert isinstance(snapshot.state, AgentState)
    assert snapshot.state.budget == 7_777
    events = reopened.list_events(stale.run.run_id)
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


def test_i6_completion_checkpoint_and_required_events_commit_atomically(
    store_backend: SQLiteConformanceBackend,
) -> None:
    store = store_backend.open_run_store()
    create_queued_run(store, "run-atomic-completion", thread_id="atomic-thread", order=1)
    claim = claim_run(store, "atomic-owner")
    assert claim is not None
    claim.run.state = AgentState(
        thread_id=claim.run.thread_id,
        destination="Tokyo",
        budget=8_000,
    )
    claim.run.output_message = "atomic result"

    with store_backend.fail_run_event(store, "run.completed"):
        with pytest.raises(InjectedConformanceFailure):
            store.commit_completed_run(claim.run, lease_token=claim.lease_token)

    reopened = store_backend.open_run_store()
    persisted = reopened.get_run_internal(claim.run.run_id)
    snapshot = reopened.load_thread_state_snapshot(
        claim.run.thread_id,
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert persisted.lease_token == claim.lease_token
    assert persisted.output_message is None
    assert snapshot.state is None
    assert snapshot.revision == 0
    assert [event.event_type for event in reopened.list_events(claim.run.run_id)] == [
        "run.queued",
        "run.started",
    ]

    assert (
        reopened.commit_completed_run(claim.run, lease_token=claim.lease_token)
        == RunCommitOutcome.COMMITTED
    )
    durable = store_backend.open_run_store()
    durable_snapshot = durable.load_thread_state_snapshot(
        claim.run.thread_id,
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    durable_run = durable.get_run_internal(claim.run.run_id)
    assert durable_run is not None and durable_run.status == RunStatus.COMPLETED
    assert durable_snapshot.revision == 1
    assert [event.event_type for event in durable.list_events(claim.run.run_id)][-2:] == [
        "checkpoint.saved",
        "run.completed",
    ]


def test_failure_and_cancellation_preserve_checkpoint_revision(
    store_backend: SQLiteConformanceBackend,
) -> None:
    store = store_backend.open_run_store()
    create_queued_run(store, "run-terminal-seed", thread_id="terminal-thread", order=1)
    seed = claim_run(store, "terminal-seed-owner")
    assert seed is not None
    assert complete_with_budget(store, seed, budget=6_000) == RunCommitOutcome.COMMITTED

    create_queued_run(store, "run-terminal-failed", thread_id="terminal-thread", order=2)
    failed = claim_run(store, "terminal-failed-owner")
    assert failed is not None
    assert (
        store.commit_failed_run(
            failed.run,
            lease_token=failed.lease_token,
            error_code="conformance_expected_failure",
            error="sanitized conformance failure",
        )
        == RunCommitOutcome.COMMITTED
    )

    create_queued_run(
        store,
        "run-terminal-cancelled",
        thread_id="terminal-thread",
        order=3,
    )
    cancelled = claim_run(store, "terminal-cancel-owner")
    assert cancelled is not None
    store.request_cancel_atomically(cancelled.run.run_id, tenant_id=TENANT_ID)
    assert (
        store.commit_cancelled_run(
            cancelled.run,
            reason="conformance cancellation",
            lease_token=cancelled.lease_token,
        )
        == RunCommitOutcome.COMMITTED
    )

    reopened = store_backend.open_run_store()
    snapshot = reopened.load_thread_state_snapshot(
        "terminal-thread",
        tenant_id=TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )
    assert snapshot.revision == 1
    assert isinstance(snapshot.state, AgentState)
    assert snapshot.state.budget == 6_000
    failed_run = reopened.get_run_internal("run-terminal-failed")
    cancelled_run = reopened.get_run_internal("run-terminal-cancelled")
    assert failed_run is not None and failed_run.error_code == "conformance_expected_failure"
    assert cancelled_run is not None and cancelled_run.status == RunStatus.CANCELLED
    assert not any(
        event.event_type == "checkpoint.saved"
        for run_id in (failed_run.run_id, cancelled_run.run_id)
        for event in reopened.list_events(run_id)
    )


def test_lease_expiry_uses_the_injected_store_clock_exactly(
    store_backend: SQLiteConformanceBackend,
) -> None:
    store = store_backend.open_run_store()
    create_queued_run(store, "run-exact-expiry", thread_id="exact-expiry", order=1)
    first = claim_run(store, "exact-expiry-owner")
    assert first is not None
    assert first.run.attempt == 1
    assert first.run.lease_heartbeat_at == store_backend.clock()
    assert first.run.lease_expires_at == store_backend.clock() + LEASE_SECONDS * 1_000

    store_backend.clock.advance(LEASE_SECONDS * 1_000 - 1)
    assert claim_run(store_backend.open_run_store(), "too-early-owner") is None
    store_backend.clock.advance(1)
    assert not store.renew_run_lease(
        first.run.run_id,
        lease_token=first.lease_token,
        lease_duration_seconds=LEASE_SECONDS,
    )
    assert (
        store.commit_completed_run(first.run, lease_token=first.lease_token)
        == RunCommitOutcome.LEASE_LOST
    )
    assert (
        store.commit_failed_run(
            first.run,
            lease_token=first.lease_token,
            error_code="stale_attempt_failed",
            error="the expired attempt must not persist this failure",
        )
        == RunCommitOutcome.LEASE_LOST
    )
    recovered = claim_run(store_backend.open_run_store(), "boundary-owner")
    assert recovered is not None
    assert recovered.recovery_reason == RunLeaseRecoveryReason.LEASE_EXPIRED
    assert recovered.run.attempt == 2
    assert recovered.lease_token != first.lease_token

    assert complete_with_budget(store, recovered, budget=8_000) == RunCommitOutcome.COMMITTED
    persisted = store_backend.open_run_store().get_run_internal(first.run.run_id)
    assert persisted is not None
    assert persisted.status == RunStatus.COMPLETED
    assert persisted.attempt == 2
    assert persisted.output_message == "budget=8000"
    event_types = [
        event.event_type for event in store_backend.open_run_store().list_events(first.run.run_id)
    ]
    assert event_types.count("run.started") == 2
    assert event_types.count("run.recovered") == 1
    assert event_types.count("checkpoint.saved") == 1
    assert event_types.count("run.completed") == 1
    assert "run.failed" not in event_types
