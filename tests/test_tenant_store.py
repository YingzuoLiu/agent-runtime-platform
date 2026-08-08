import pytest

from domains.travel.state import AgentState
from runtime_service import RunRecord, RunStatus, SQLiteRunStore


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
    store.create_run(run)
    store.append_event(run.run_id, "run.queued", {})

    with pytest.raises(KeyError, match="Run not found"):
        store.request_cancel_atomically(run.run_id, tenant_id="tenant-b")

    assert store.list_events_for_tenant(run.run_id, "tenant-b") == []
    assert [
        event.event_type
        for event in store.list_events_for_tenant(run.run_id, "tenant-a")
    ] == ["run.queued"]
    assert store.get_run_internal(run.run_id).cancel_requested is False


def test_thread_checkpoints_are_independent_per_tenant(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    store.save_thread_state(
        AgentState(thread_id="shared-thread", destination="Tokyo", budget=7000),
        tenant_id="tenant-a",
    )
    store.save_thread_state(
        AgentState(thread_id="shared-thread", destination="Seoul", budget=9000),
        tenant_id="tenant-b",
    )

    tenant_a = store.load_thread_state("shared-thread", tenant_id="tenant-a")
    tenant_b = store.load_thread_state("shared-thread", tenant_id="tenant-b")

    assert tenant_a.destination == "Tokyo"
    assert tenant_a.budget == 7000
    assert tenant_b.destination == "Seoul"
    assert tenant_b.budget == 9000


def test_persisted_run_tenant_identity_cannot_be_reassigned(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    store.create_run(queued_run("run-a", "tenant-a", client_request_id="request-a"))
    loaded = store.get_run_internal("run-a")
    loaded.tenant_id = "tenant-b"

    with pytest.raises(KeyError, match="Run not found"):
        store.update_run(loaded)

    assert store.get_run_for_tenant("run-a", "tenant-a") is not None
    assert store.get_run_for_tenant("run-a", "tenant-b") is None


def test_terminal_write_cannot_finalize_under_a_different_tenant(tmp_path):
    store = SQLiteRunStore(tmp_path / "runtime.db")
    run = queued_run("run-a", "tenant-a", client_request_id="request-a")
    run.status = RunStatus.RUNNING
    store.create_run(run)
    run.tenant_id = "tenant-b"
    run.state = AgentState(thread_id="shared-thread", destination="Tokyo")

    assert store.finalize_completed_run(run) is False
    assert store.get_run_for_tenant("run-a", "tenant-a").status == RunStatus.RUNNING
    assert store.load_thread_state("shared-thread", tenant_id="tenant-b") is None
