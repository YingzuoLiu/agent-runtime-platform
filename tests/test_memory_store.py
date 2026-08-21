from __future__ import annotations

import json

import pytest

from agent.contracts import RuntimeExecutionAuthority, RuntimeExecutionContext
from runtime_service import RunRecord, RunStatus, SQLiteRunStore
from runtime_service.memory import (
    MemoryKind,
    MemoryMutationAction,
    MemoryStatus,
    MemoryWrite,
    GovernedMemory,
    SQLiteMemoryStore,
)


def create_run(
    store: SQLiteRunStore,
    run_id: str,
    *,
    tenant_id: str = "tenant-a",
    subject_id: str = "subject-a",
    thread_id: str | None = None,
) -> None:
    store.create_run(
        RunRecord(
            run_id=run_id,
            tenant_id=tenant_id,
            thread_id=thread_id or f"thread-{run_id}",
            agent_id="travel-agent",
            agent_version="1.1.0",
            status=RunStatus.QUEUED,
            input={"user_message": "Plan a trip."},
            execution_authority=RuntimeExecutionAuthority(
                tenant_id=tenant_id,
                subject_id=subject_id,
                permissions=("memory:read", "memory:write"),
            ),
        )
    )


def claim_run(store: SQLiteRunStore, run_id: str) -> str:
    claim = store.claim_next_run(
        owner_id=f"manager-{run_id}",
        lease_duration_seconds=30,
    )
    assert claim is not None
    assert claim.run.run_id == run_id
    return claim.lease_token


def write_preference(value: bool) -> MemoryWrite:
    return MemoryWrite(
        kind=MemoryKind.PREFERENCE,
        key="flight.avoid_red_eye",
        value=value,
    )


def test_memory_versions_are_subject_scoped_and_audited_without_values(tmp_path):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    create_run(run_store, "run-create")
    create_run(run_store, "run-update")

    created = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-create",
        source_thread_id="thread-a",
        actor_subject_id="subject-a",
    )
    repeated = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-create",
        source_thread_id="thread-a",
        actor_subject_id="subject-a",
    )
    updated = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(False),
        source_run_id="run-update",
        source_thread_id="thread-b",
        actor_subject_id="subject-a",
    )

    assert created.action == MemoryMutationAction.CREATED
    assert created.record.version == 1
    assert repeated.action == MemoryMutationAction.UNCHANGED
    assert repeated.record.memory_id == created.record.memory_id
    assert updated.action == MemoryMutationAction.SUPERSEDED
    assert updated.record.version == 2
    assert updated.record.value is False
    assert updated.superseded_record is not None
    assert updated.superseded_record.status == MemoryStatus.SUPERSEDED

    active = memory_store.list_memories(
        tenant_id="tenant-a",
        subject_id="subject-a",
    )
    assert [(record.version, record.value) for record in active] == [(2, False)]
    assert memory_store.list_memories(
        tenant_id="tenant-a",
        subject_id="subject-b",
    ) == []
    assert memory_store.list_memories(
        tenant_id="tenant-b",
        subject_id="subject-a",
    ) == []

    events = memory_store.list_events_for_subject(
        tenant_id="tenant-a",
        subject_id="subject-a",
    )
    assert [event.event_type for event in events] == [
        "memory.created",
        "memory.superseded",
        "memory.created",
    ]
    assert "true" not in json.dumps([event.model_dump(mode="json") for event in events])
    assert "false" not in json.dumps([event.model_dump(mode="json") for event in events])


def test_forget_tombstones_every_version_and_hides_cross_subject_ids(tmp_path):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    create_run(run_store, "run-create")
    create_run(run_store, "run-update")

    first = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-create",
        source_thread_id="thread-a",
        actor_subject_id="subject-a",
    ).record
    second = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(False),
        source_run_id="run-update",
        source_thread_id="thread-b",
        actor_subject_id="subject-a",
    ).record

    assert memory_store.get_memory_for_subject(
        second.memory_id,
        tenant_id="tenant-a",
        subject_id="subject-b",
    ) is None
    with pytest.raises(KeyError, match="Memory not found"):
        memory_store.forget_memory(
            second.memory_id,
            tenant_id="tenant-a",
            subject_id="subject-b",
            actor_subject_id="subject-b",
        )

    forgotten = memory_store.forget_memory(
        second.memory_id,
        tenant_id="tenant-a",
        subject_id="subject-a",
        actor_subject_id="subject-a",
    )
    assert forgotten.status == MemoryStatus.DELETED
    assert forgotten.value is None
    assert memory_store.list_memories(
        tenant_id="tenant-a",
        subject_id="subject-a",
    ) == []
    history = memory_store.list_memories(
        tenant_id="tenant-a",
        subject_id="subject-a",
        include_inactive=True,
    )
    assert {record.memory_id for record in history} == {first.memory_id, second.memory_id}
    assert all(record.status == MemoryStatus.DELETED for record in history)
    assert all(record.value is None for record in history)
    assert memory_store.list_events_for_subject(
        tenant_id="tenant-a",
        subject_id="subject-a",
    )[-1].event_type == "memory.deleted"


def test_run_snapshot_is_sealed_across_updates_and_empty_retrieval(tmp_path):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    for run_id in ("run-create", "run-snapshot", "run-update", "run-new"):
        create_run(run_store, run_id)
    create_run(
        run_store,
        "run-empty",
        subject_id="subject-with-no-memory",
    )

    first = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-create",
        source_thread_id="thread-a",
        actor_subject_id="subject-a",
    ).record
    sealed = memory_store.get_or_create_run_snapshot(
        run_id="run-snapshot",
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert [(memory.memory_id, memory.value) for memory in sealed.memories] == [
        (first.memory_id, True)
    ]

    second = memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(False),
        source_run_id="run-update",
        source_thread_id="thread-b",
        actor_subject_id="subject-a",
    ).record
    recovered = memory_store.get_or_create_run_snapshot(
        run_id="run-snapshot",
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    new_run = memory_store.get_or_create_run_snapshot(
        run_id="run-new",
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert recovered == sealed
    assert [(memory.memory_id, memory.value) for memory in new_run.memories] == [
        (second.memory_id, False)
    ]

    empty = memory_store.get_or_create_run_snapshot(
        run_id="run-empty",
        tenant_id="tenant-a",
        subject_id="subject-with-no-memory",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert empty.memories == ()
    memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-with-no-memory",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-create",
        source_thread_id="thread-c",
        actor_subject_id="subject-with-no-memory",
    )
    assert memory_store.get_or_create_run_snapshot(
        run_id="run-empty",
        tenant_id="tenant-a",
        subject_id="subject-with-no-memory",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    ).memories == ()


def test_snapshot_identity_cannot_be_rebound_to_another_subject(tmp_path):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    create_run(run_store, "run-snapshot")
    memory_store.get_or_create_run_snapshot(
        run_id="run-snapshot",
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        allowed_keys=(),
    )

    with pytest.raises(ValueError, match="identity"):
        memory_store.get_or_create_run_snapshot(
            run_id="run-snapshot",
            tenant_id="tenant-a",
            subject_id="subject-b",
            domain_id="travel",
            allowed_keys=(),
        )


def test_retry_mirrors_committed_memory_audit_without_duplicate_run_events(tmp_path):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    create_run(run_store, "run-retry", thread_id="thread-retry")
    lease_token = claim_run(run_store, "run-retry")
    context = RuntimeExecutionContext(
        run_id="run-retry",
        thread_id="thread-retry",
        recovered_after_restart=True,
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-a",
            subject_id="subject-a",
            permissions=("memory:read", "memory:write"),
        ),
        lease_token=lease_token,
    )
    governed = GovernedMemory(memory_store, run_store)

    memory_store.upsert(
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        write=write_preference(True),
        source_run_id="run-retry",
        source_thread_id="thread-retry",
        actor_subject_id="subject-a",
    )
    memory_store.get_or_create_run_snapshot(
        run_id="run-retry",
        tenant_id="tenant-a",
        subject_id="subject-a",
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert not any(
        event.event_type.startswith("memory.")
        for event in run_store.list_events("run-retry")
    )

    governed.retrieve(
        context,
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    governed.retrieve(
        context,
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    governed.remember(
        context,
        domain_id="travel",
        source_thread_id="thread-retry",
        writes=(write_preference(True),),
    )
    governed.remember(
        context,
        domain_id="travel",
        source_thread_id="thread-retry",
        writes=(write_preference(True),),
    )

    events = [
        event
        for event in run_store.list_events("run-retry")
        if event.event_type.startswith("memory.")
    ]
    assert [event.event_type for event in events] == [
        "memory.retrieved",
        "memory.created",
    ]


def test_mutation_mirroring_reads_run_events_once_per_batch(tmp_path, monkeypatch):
    database_path = tmp_path / "runtime.db"
    run_store = SQLiteRunStore(database_path)
    memory_store = SQLiteMemoryStore(database_path)
    create_run(run_store, "run-batch", thread_id="thread-batch")
    lease_token = claim_run(run_store, "run-batch")
    context = RuntimeExecutionContext(
        run_id="run-batch",
        thread_id="thread-batch",
        authority=RuntimeExecutionAuthority(
            tenant_id="tenant-a",
            subject_id="subject-a",
            permissions=("memory:write",),
        ),
        lease_token=lease_token,
    )
    governed = GovernedMemory(memory_store, run_store)
    writes = (
        write_preference(True),
        MemoryWrite(
            kind=MemoryKind.PREFERENCE,
            key="hotel.near_subway",
            value=True,
        ),
        MemoryWrite(
            kind=MemoryKind.PREFERENCE,
            key="travel.style",
            value="relaxed",
        ),
    )

    original_list_events = run_store.list_events
    list_events_calls = 0

    def counting_list_events(run_id):
        nonlocal list_events_calls
        list_events_calls += 1
        return original_list_events(run_id)

    monkeypatch.setattr(run_store, "list_events", counting_list_events)

    governed.remember(
        context,
        domain_id="travel",
        source_thread_id="thread-batch",
        writes=writes,
    )
    governed.remember(
        context,
        domain_id="travel",
        source_thread_id="thread-batch",
        writes=writes,
    )

    assert list_events_calls == 2
    assert [
        event.event_type
        for event in original_list_events("run-batch")
        if event.event_type.startswith("memory.")
    ] == [
        "memory.created",
        "memory.created",
        "memory.created",
    ]
