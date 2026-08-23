from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from agent.contracts import RuntimeExecutionAuthority, RuntimeExecutionContext
from runtime_service.memory import (
    GovernedMemory,
    MemoryKind,
    MemoryMutationAction,
    MemoryStatus,
    MemoryWrite,
)
from runtime_service.models import RunRecord, RunStatus
from runtime_service.run_store import RunStore
from runtime_service.store import RunLeaseLostError

from .backends import (
    InjectedConformanceFailure,
    MemoryConformanceStore,
    StoreConformanceBackend,
)


TENANT_ID = "memory-conformance-tenant"
SUBJECT_ID = "memory-conformance-subject"
DOMAIN_ID = "travel"
LEASE_SECONDS = 10
AUTHORITY = RuntimeExecutionAuthority(
    tenant_id=TENANT_ID,
    subject_id=SUBJECT_ID,
    permissions=("memory:read", "memory:write"),
)


def create_run(
    store: RunStore,
    run_id: str,
    *,
    tenant_id: str = TENANT_ID,
    subject_id: str = SUBJECT_ID,
    domain_id: str = DOMAIN_ID,
    order: int = 1,
) -> None:
    timestamp = f"2026-08-23T00:00:{order:02d}+00:00"
    store.create_run(
        RunRecord(
            run_id=run_id,
            tenant_id=tenant_id,
            thread_id=f"thread-{run_id}",
            agent_id="travel-agent",
            agent_version="1.1.0",
            domain_id=domain_id,
            schema_version="1",
            status=RunStatus.QUEUED,
            input={"user_message": "Plan a trip."},
            execution_authority=RuntimeExecutionAuthority(
                tenant_id=tenant_id,
                subject_id=subject_id,
                permissions=("memory:read", "memory:write"),
            ),
            created_at=timestamp,
            updated_at=timestamp,
        )
    )


def claim_run(store: RunStore, run_id: str, *, owner_id: str) -> str:
    claim = store.claim_next_run(
        owner_id=owner_id,
        lease_duration_seconds=LEASE_SECONDS,
    )
    assert claim is not None
    assert claim.run.run_id == run_id
    return claim.lease_token


def context_for(run_id: str, lease_token: str | None) -> RuntimeExecutionContext:
    return RuntimeExecutionContext(
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        authority=AUTHORITY,
        lease_token=lease_token,
    )


def preference(value: object, *, key: str = "flight.avoid_red_eye") -> MemoryWrite:
    return MemoryWrite(
        kind=MemoryKind.PREFERENCE,
        key=key,
        value=value,
    )


def memory_run_events(store: RunStore, run_id: str):
    return [
        event
        for event in store.list_events(run_id)
        if event.event_type.startswith("memory.")
    ]


def test_memory_versions_are_scoped_monotonic_and_audited_without_values(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_memory_store()
    created = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-create",
        source_thread_id="thread-create",
        actor_subject_id=SUBJECT_ID,
    )
    repeated = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-create",
        source_thread_id="thread-create",
        actor_subject_id=SUBJECT_ID,
    )
    updated = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(False),
        source_run_id="run-update",
        source_thread_id="thread-update",
        actor_subject_id=SUBJECT_ID,
    )

    assert created.action == MemoryMutationAction.CREATED
    assert created.record.version == 1
    assert repeated.action == MemoryMutationAction.UNCHANGED
    assert repeated.record.memory_id == created.record.memory_id
    assert updated.action == MemoryMutationAction.SUPERSEDED
    assert updated.record.version == 2
    assert updated.superseded_record is not None
    assert updated.superseded_record.status == MemoryStatus.SUPERSEDED
    assert [(record.version, record.value) for record in store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )] == [(2, False)]
    assert store.list_memories(
        tenant_id=TENANT_ID,
        subject_id="other-subject",
    ) == []
    assert store.list_memories(
        tenant_id="other-tenant",
        subject_id=SUBJECT_ID,
    ) == []

    events = store.list_events_for_subject(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )
    assert [event.event_type for event in events] == [
        "memory.created",
        "memory.superseded",
        "memory.created",
    ]
    encoded_events = json.dumps([event.model_dump(mode="json") for event in events])
    assert "true" not in encoded_events
    assert "false" not in encoded_events


def test_forget_tombstones_all_versions_and_is_idempotent(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_memory_store()
    first = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-create",
        source_thread_id="thread-create",
        actor_subject_id=SUBJECT_ID,
    ).record
    second = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(False),
        source_run_id="run-update",
        source_thread_id="thread-update",
        actor_subject_id=SUBJECT_ID,
    ).record

    assert store.get_memory_for_subject(
        second.memory_id,
        tenant_id=TENANT_ID,
        subject_id="other-subject",
    ) is None
    with pytest.raises(KeyError, match="Memory not found"):
        store.forget_memory(
            second.memory_id,
            tenant_id=TENANT_ID,
            subject_id="other-subject",
            actor_subject_id="other-subject",
        )

    forgotten = store.forget_memory(
        second.memory_id,
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        actor_subject_id=SUBJECT_ID,
    )
    repeated = store.forget_memory(
        second.memory_id,
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        actor_subject_id=SUBJECT_ID,
    )
    assert forgotten.status == repeated.status == MemoryStatus.DELETED
    assert forgotten.value is repeated.value is None
    history = store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        include_inactive=True,
    )
    assert {record.memory_id for record in history} == {first.memory_id, second.memory_id}
    assert all(record.status == MemoryStatus.DELETED for record in history)
    assert all(record.value is None for record in history)
    assert [
        event.event_type
        for event in store.list_events_for_subject(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
        )
    ] == ["memory.created", "memory.superseded", "memory.created", "memory.deleted"]


def test_nonempty_and_empty_run_snapshots_remain_sealed(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    for order, run_id in enumerate(
        ("run-create", "run-snapshot", "run-update", "run-new"),
        start=1,
    ):
        create_run(run_store, run_id, order=order)
    create_run(
        run_store,
        "run-empty",
        subject_id="subject-with-no-memory",
        order=5,
    )

    first = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-create",
        source_thread_id="thread-run-create",
        actor_subject_id=SUBJECT_ID,
    ).record
    sealed = store.get_or_create_run_snapshot(
        run_id="run-snapshot",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert [(memory.memory_id, memory.value) for memory in sealed.memories] == [
        (first.memory_id, True)
    ]

    second = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(False),
        source_run_id="run-update",
        source_thread_id="thread-run-update",
        actor_subject_id=SUBJECT_ID,
    ).record
    assert store.get_or_create_run_snapshot(
        run_id="run-snapshot",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    ) == sealed
    assert [memory.memory_id for memory in store.get_or_create_run_snapshot(
        run_id="run-new",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    ).memories] == [second.memory_id]

    empty = store.get_or_create_run_snapshot(
        run_id="run-empty",
        tenant_id=TENANT_ID,
        subject_id="subject-with-no-memory",
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert empty.memories == ()
    store.upsert(
        tenant_id=TENANT_ID,
        subject_id="subject-with-no-memory",
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-create",
        source_thread_id="thread-create",
        actor_subject_id="subject-with-no-memory",
    )
    assert store.get_or_create_run_snapshot(
        run_id="run-empty",
        tenant_id=TENANT_ID,
        subject_id="subject-with-no-memory",
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    ).memories == ()


@pytest.mark.parametrize(
    ("tenant_id", "subject_id", "domain_id"),
    [
        ("wrong-tenant", SUBJECT_ID, DOMAIN_ID),
        (TENANT_ID, "wrong-subject", DOMAIN_ID),
        (TENANT_ID, SUBJECT_ID, "wrong-domain"),
    ],
    ids=["wrong-tenant", "wrong-subject", "wrong-domain"],
)
def test_run_identity_mismatch_cannot_snapshot_or_mutate(
    store_backend: StoreConformanceBackend,
    tenant_id: str,
    subject_id: str,
    domain_id: str,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    create_run(run_store, "run-identity")
    lease_token = claim_run(run_store, "run-identity", owner_id="owner-identity")

    with pytest.raises(ValueError, match="identity"):
        store.get_or_create_run_snapshot_for_run(
            lease_token=lease_token,
            run_id="run-identity",
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            allowed_keys=(),
        )
    with pytest.raises(ValueError, match="identity"):
        store.upsert_from_run(
            lease_token=lease_token,
            tenant_id=tenant_id,
            subject_id=subject_id,
            domain_id=domain_id,
            write=preference(True),
            source_run_id="run-identity",
            source_thread_id="thread-run-identity",
            actor_subject_id=subject_id,
        )

    assert store.get_run_snapshot("run-identity") is None
    assert store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    ) == []


def test_current_run_lease_gates_snapshot_mutation_and_public_mirrors(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="admin-seed",
        source_thread_id="admin-seed",
        actor_subject_id=SUBJECT_ID,
    )
    create_run(run_store, "run-current")
    lease_token = claim_run(run_store, "run-current", owner_id="owner-current")
    context = context_for("run-current", lease_token)
    governed = GovernedMemory(store, run_store)

    snapshot = governed.retrieve(
        context,
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    )
    governed.remember(
        context,
        domain_id=DOMAIN_ID,
        source_thread_id=context.thread_id,
        writes=(preference(True, key="hotel.near_subway"),),
    )

    assert [memory.key for memory in snapshot.memories] == ["flight.avoid_red_eye"]
    assert [event.event_type for event in memory_run_events(run_store, context.run_id)] == [
        "memory.retrieved",
        "memory.created",
    ]


def test_stale_and_exactly_expired_leases_cannot_snapshot_or_mutate(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    create_run(run_store, "run-stale", order=1)
    stale_token = claim_run(run_store, "run-stale", owner_id="owner-stale")
    stale_context = context_for("run-stale", stale_token)
    governed = GovernedMemory(store, run_store)
    store_backend.clock.advance(LEASE_SECONDS * 1_000)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.retrieve(
            stale_context,
            domain_id=DOMAIN_ID,
            allowed_keys=("flight.avoid_red_eye",),
        )
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            stale_context,
            domain_id=DOMAIN_ID,
            source_thread_id=stale_context.thread_id,
            writes=(preference(True),),
        )

    replacement_token = claim_run(run_store, "run-stale", owner_id="owner-replacement")
    assert replacement_token != stale_token
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        store.upsert_from_run(
            lease_token=stale_token,
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            write=preference(True),
            source_run_id="run-stale",
            source_thread_id="thread-run-stale",
            actor_subject_id=SUBJECT_ID,
        )
    assert store.get_run_snapshot("run-stale") is None
    assert store.list_memories(tenant_id=TENANT_ID, subject_id=SUBJECT_ID) == []
    assert store.list_events_for_run(
        "run-stale",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    ) == []
    assert memory_run_events(run_store, "run-stale") == []


def test_missing_or_empty_lease_token_fails_before_memory_work(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    create_run(run_store, "run-missing-token")
    claim_run(run_store, "run-missing-token", owner_id="owner-missing-token")

    for lease_token in (None, ""):
        context = context_for("run-missing-token", lease_token)
        governed = GovernedMemory(store, run_store)
        with pytest.raises(RunLeaseLostError, match="has no lease token"):
            governed.retrieve(
                context,
                domain_id=DOMAIN_ID,
                allowed_keys=("flight.avoid_red_eye",),
            )
        with pytest.raises(RunLeaseLostError, match="has no lease token"):
            governed.remember(
                context,
                domain_id=DOMAIN_ID,
                source_thread_id=context.thread_id,
                writes=(preference(True),),
            )

    with pytest.raises(ValueError, match="lease_token"):
        store.get_or_create_run_snapshot_for_run(
            lease_token="",
            run_id="run-missing-token",
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            allowed_keys=(),
        )
    with pytest.raises(ValueError, match="lease_token"):
        store.upsert_from_run(
            lease_token="",
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            write=preference(True),
            source_run_id="run-missing-token",
            source_thread_id="thread-run-missing-token",
            actor_subject_id=SUBJECT_ID,
        )
    assert store.get_run_snapshot("run-missing-token") is None
    assert store.list_memories(tenant_id=TENANT_ID, subject_id=SUBJECT_ID) == []


def test_successor_repairs_committed_audit_without_duplicate_run_events(
    store_backend: StoreConformanceBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    create_run(run_store, "run-repair")
    stale_token = claim_run(run_store, "run-repair", owner_id="owner-stale")
    stale_context = context_for("run-repair", stale_token)
    governed = GovernedMemory(store, run_store)
    write = preference(True)
    original_upsert = store.upsert_from_run
    replacement_tokens: list[str] = []

    def rotate_lease_after_memory_commit(**kwargs):
        result = original_upsert(**kwargs)
        if not replacement_tokens:
            store_backend.clock.advance((LEASE_SECONDS + 1) * 1_000)
            replacement_tokens.append(
                claim_run(run_store, "run-repair", owner_id="owner-replacement")
            )
        return result

    monkeypatch.setattr(store, "upsert_from_run", rotate_lease_after_memory_commit)
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            stale_context,
            domain_id=DOMAIN_ID,
            source_thread_id=stale_context.thread_id,
            writes=(write,),
        )

    audit_events = store.list_events_for_run(
        "run-repair",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )
    assert [event.event_type for event in audit_events] == ["memory.created"]
    assert memory_run_events(run_store, "run-repair") == []

    replacement_context = context_for("run-repair", replacement_tokens[0])
    governed.remember(
        replacement_context,
        domain_id=DOMAIN_ID,
        source_thread_id=replacement_context.thread_id,
        writes=(write,),
    )
    governed.remember(
        replacement_context,
        domain_id=DOMAIN_ID,
        source_thread_id=replacement_context.thread_id,
        writes=(write,),
    )
    mirrored = memory_run_events(run_store, "run-repair")
    assert [event.event_type for event in mirrored] == ["memory.created"]
    assert mirrored[0].payload["audit_event_id"] == audit_events[0].event_id


def test_concurrent_writes_preserve_one_active_version_and_ordered_audit(
    store_backend: StoreConformanceBackend,
) -> None:
    first_store = store_backend.open_memory_store()
    second_store = store_backend.open_memory_store()
    barrier = threading.Barrier(3)

    def write_concurrently(
        store: MemoryConformanceStore,
        *,
        value: str,
        source_run_id: str,
    ):
        barrier.wait(timeout=10)
        return store.upsert(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            write=preference(value),
            source_run_id=source_run_id,
            source_thread_id=f"thread-{source_run_id}",
            actor_subject_id=SUBJECT_ID,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            write_concurrently,
            first_store,
            value="first",
            source_run_id="run-concurrent-a",
        )
        second = executor.submit(
            write_concurrently,
            second_store,
            value="second",
            source_run_id="run-concurrent-b",
        )
        barrier.wait(timeout=10)
        results = [first.result(timeout=10), second.result(timeout=10)]

    assert sorted(result.record.version for result in results) == [1, 2]
    assert {result.action for result in results} == {
        MemoryMutationAction.CREATED,
        MemoryMutationAction.SUPERSEDED,
    }
    active = first_store.list_memories(tenant_id=TENANT_ID, subject_id=SUBJECT_ID)
    assert len(active) == 1
    assert active[0].version == 2
    history = first_store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        include_inactive=True,
    )
    assert sorted(record.version for record in history) == [1, 2]
    assert sum(record.status == MemoryStatus.ACTIVE for record in history) == 1
    events = first_store.list_events_for_subject(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )
    assert [event.event_type for event in events] == [
        "memory.created",
        "memory.superseded",
        "memory.created",
    ]
    assert [event.payload["version"] for event in events] == [1, 1, 2]


def test_record_supersession_and_events_roll_back_as_one_transaction(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_memory_store()
    first = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True),
        source_run_id="run-first",
        source_thread_id="thread-first",
        actor_subject_id=SUBJECT_ID,
    ).record

    with store_backend.fail_memory_event(store, "memory.created"):
        with pytest.raises(InjectedConformanceFailure, match="memory.created"):
            store.upsert(
                tenant_id=TENANT_ID,
                subject_id=SUBJECT_ID,
                domain_id=DOMAIN_ID,
                write=preference(False),
                source_run_id="run-failed",
                source_thread_id="thread-failed",
                actor_subject_id=SUBJECT_ID,
            )

    history = store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        include_inactive=True,
    )
    assert [(record.memory_id, record.status, record.version, record.value) for record in history] == [
        (first.memory_id, MemoryStatus.ACTIVE, 1, True)
    ]
    assert [
        event.event_type
        for event in store.list_events_for_subject(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
        )
    ] == ["memory.created"]


def test_expired_records_and_disallowed_keys_are_excluded_from_snapshot(
    store_backend: StoreConformanceBackend,
) -> None:
    run_store = store_backend.open_run_store()
    store = store_backend.open_memory_store()
    create_run(run_store, "run-expiry")
    store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=MemoryWrite(
            kind=MemoryKind.PREFERENCE,
            key="flight.avoid_red_eye",
            value=True,
            expires_at="2000-01-01T00:00:00+00:00",
        ),
        source_run_id="run-expired-seed",
        source_thread_id="thread-expired-seed",
        actor_subject_id=SUBJECT_ID,
    )
    store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=preference(True, key="hotel.near_subway"),
        source_run_id="run-active-seed",
        source_thread_id="thread-active-seed",
        actor_subject_id=SUBJECT_ID,
    )

    assert [record.key for record in store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )] == ["hotel.near_subway"]
    snapshot = store.get_or_create_run_snapshot(
        run_id="run-expiry",
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        allowed_keys=("flight.avoid_red_eye",),
    )
    assert snapshot.memories == ()


def test_invalid_json_fails_before_any_record_or_audit_write(
    store_backend: StoreConformanceBackend,
) -> None:
    store = store_backend.open_memory_store()
    with pytest.raises(ValueError, match="JSON serializable"):
        store.upsert(
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            write=preference(float("nan")),
            source_run_id="run-invalid-json",
            source_thread_id="thread-invalid-json",
            actor_subject_id=SUBJECT_ID,
        )
    assert store.list_memories(tenant_id=TENANT_ID, subject_id=SUBJECT_ID) == []
    assert store.list_events_for_subject(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    ) == []
