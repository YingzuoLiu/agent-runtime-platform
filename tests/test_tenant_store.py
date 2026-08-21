import pytest

from domains.travel.state import AgentState
from runtime_service import RunLeaseLostError, RunRecord, RunStatus, SQLiteRunStore


def queued_run(run_id: str, tenant_id: str, *, client_request_id: str) -> RunRecord:
    return RunRecord(
        run_id=run_id,
        tenant_id=tenant_id,
        thread_id="shared-thread",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.QUEUED,
        input={"user_message": "Plan a five-day trip."},
        client_request_id=client_request_id,
    )


def test_run_and_idempotency_lookup_are_tenant_scoped(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    first = queued_run("run-a", "tenant-a", client_request_id="same-request")
    second = queued_run("run-b", "tenant-b", client_request_id="same-request")
    store.create_run(first)
    store.create_run(second)

    assert store.get_run_by_client_request_id("tenant-a", "same-request").run_id == "run-a"
    assert store.get_run_by_client_request_id("tenant-b", "same-request").run_id == "run-b"
    assert store.get_run_for_tenant("run-a", "tenant-a").run_id == "run-a"
    assert store.get_run_for_tenant("run-a", "tenant-b") is None


def test_cancel_and_event_reads_cannot_cross_tenant_boundary(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")
    store.create_run_with_event(run, event_type="run.queued")

    with pytest.raises(KeyError, match="Run not found"):
        store.request_cancel_atomically(run.run_id, tenant_id="tenant-b")

    assert store.list_events_for_tenant(run.run_id, "tenant-b") == []
    assert [
        event.event_type
        for event in store.list_events_for_tenant(run.run_id, "tenant-a")
    ] == ["run.queued"]
    assert store.get_run_internal(run.run_id).cancel_requested is False


def test_unfenced_run_event_append_is_blocked_and_control_plane_is_narrow(
    tmp_path,
):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")
    store.create_run_with_event(run, event_type="run.queued")

    with pytest.raises(RunLeaseLostError, match="Unfenced Run event append"):
        store.append_event(run.run_id, "run.failed", {"from": "stale-attempt"})
    with pytest.raises(ValueError, match="Unsupported control-plane Run event"):
        store.append_control_plane_event(
            run.run_id,
            tenant_id="tenant-a",
            event_type="run.failed",
            payload={"from": "control-plane"},
        )
    with pytest.raises(KeyError, match="Run not found"):
        store.append_control_plane_event(
            run.run_id,
            tenant_id="tenant-b",
            event_type="sandbox.execution_started",
        )

    event = store.append_control_plane_event(
        run.run_id,
        tenant_id="tenant-a",
        event_type="sandbox.execution_started",
        payload={"tool_name": "route_cost_summary"},
    )

    assert event.sequence == 2
    assert [item.event_type for item in store.list_events(run.run_id)] == [
        "run.queued",
        "sandbox.execution_started",
    ]


def test_unmanaged_and_legacy_mutation_surfaces_are_explicitly_separated(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")

    assert not hasattr(store, "save_thread_state")
    assert not hasattr(store, "claim_run_start")
    assert not hasattr(store, "recover_run_for_restart")
    assert not hasattr(store, "update_run")
    assert not hasattr(store, "mark_reconciliation_pending")
    assert not hasattr(store, "finalize_completed_run")
    assert not hasattr(store, "finalize_cancelled_run")


@pytest.mark.parametrize(
    "updates",
    [
        {"status": RunStatus.RUNNING},
        {"status": RunStatus.COMPLETED},
        {"attempt": 1},
        {"cancel_requested": True},
        {"started_at": "2026-01-01T00:00:00Z"},
        {"completed_at": "2026-01-01T00:00:00Z"},
        {"output_message": "already executed"},
        {"validation_errors": ["already validated"]},
        {"error_code": "already_failed"},
        {"error": "already failed"},
        {"lease_owner_id": "manager-a"},
        {"lease_token": "lease-a"},
        {"lease_heartbeat_at": 1_000},
        {"lease_expires_at": 31_000},
    ],
)
def test_public_create_only_accepts_pristine_unleased_queued_runs(
    tmp_path,
    updates,
):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")

    with pytest.raises(
        ValueError,
        match="pristine queued records without lease authority",
    ):
        store.create_run(run.model_copy(update=updates))

    assert store.get_run_internal(run.run_id) is None


def test_public_create_with_event_only_accepts_run_queued(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")

    with pytest.raises(ValueError, match="initial event must be run.queued"):
        store.create_run_with_event(run, event_type="run.failed")

    assert store.get_run_internal(run.run_id) is None


def test_thread_checkpoints_are_independent_per_tenant(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    store.save_unmanaged_thread_state(
        AgentState(thread_id="shared-thread", destination="Tokyo", budget=7000),
        tenant_id="tenant-a",
    )
    store.save_unmanaged_thread_state(
        AgentState(thread_id="shared-thread", destination="Seoul", budget=9000),
        tenant_id="tenant-b",
    )

    tenant_a = store.load_thread_state("shared-thread", tenant_id="tenant-a")
    tenant_b = store.load_thread_state("shared-thread", tenant_id="tenant-b")

    assert tenant_a.destination == "Tokyo"
    assert tenant_a.budget == 7000
    assert tenant_b.destination == "Seoul"
    assert tenant_b.budget == 9000


def test_fenced_terminal_write_cannot_reassign_tenant(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")
    store.create_run(run)
    claim = store.claim_next_run(
        owner_id="manager-a",
        lease_duration_seconds=30,
    )
    assert claim is not None
    claimed = claim.run
    claimed.tenant_id = "tenant-b"
    claimed.state = AgentState(thread_id="shared-thread", destination="Tokyo")

    with pytest.raises(KeyError, match="Run not found"):
        store.commit_completed_run(claimed, lease_token=claim.lease_token)

    persisted = store.get_run_for_tenant("run-a", "tenant-a")
    assert persisted is not None
    assert persisted.status == RunStatus.RUNNING
    assert store.get_run_for_tenant("run-a", "tenant-b") is None
    assert store.load_thread_state("shared-thread", tenant_id="tenant-b") is None
