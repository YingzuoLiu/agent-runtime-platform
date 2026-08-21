from __future__ import annotations

import pytest

from agent.contracts import RuntimeExecutionAuthority, RuntimeExecutionContext
from runtime_service import RunLeaseLostError, RunRecord, RunStatus, SQLiteRunStore
from runtime_service.memory import (
    GovernedMemory,
    MemoryKind,
    MemoryWrite,
    SQLiteMemoryStore,
)


class ManualLeaseClock:
    def __init__(self) -> None:
        self.now_ms = 1_000_000

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, *, seconds: int) -> None:
        self.now_ms += seconds * 1000


AUTHORITY = RuntimeExecutionAuthority(
    tenant_id="tenant-a",
    subject_id="subject-a",
    permissions=("memory:read", "memory:write"),
)


def create_run(store: SQLiteRunStore, run_id: str) -> None:
    store.create_run(
        RunRecord(
            run_id=run_id,
            tenant_id=AUTHORITY.tenant_id,
            thread_id=f"thread-{run_id}",
            agent_id="travel-agent",
            agent_version="1.1.0",
            domain_id="travel",
            schema_version="1",
            status=RunStatus.QUEUED,
            input={"user_message": "Plan a trip."},
            execution_authority=AUTHORITY,
        )
    )


def claim_run(store: SQLiteRunStore, run_id: str, *, owner_id: str) -> str:
    claim = store.claim_next_run(
        owner_id=owner_id,
        lease_duration_seconds=10,
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


def preference(key: str, value: object = True) -> MemoryWrite:
    return MemoryWrite(
        kind=MemoryKind.PREFERENCE,
        key=key,
        value=value,
    )


def memory_run_events(store: SQLiteRunStore, run_id: str):
    return [
        event
        for event in store.list_events(run_id)
        if event.event_type.startswith("memory.")
    ]


def test_current_run_lease_fences_snapshot_mutation_and_public_mirrors(tmp_path):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)

    # The raw upsert remains the compatible control-plane seed API.
    memory_store.upsert(
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
        domain_id="travel",
        write=preference("flight.avoid_red_eye"),
        source_run_id="admin-seed",
        source_thread_id="admin-seed",
        actor_subject_id=AUTHORITY.subject_id,
    )
    create_run(run_store, "run-current")
    lease_token = claim_run(run_store, "run-current", owner_id="manager-a")
    context = context_for("run-current", lease_token)
    governed = GovernedMemory(memory_store, run_store)

    snapshot = governed.retrieve(
        context,
        domain_id="travel",
        allowed_keys=("flight.avoid_red_eye",),
    )
    governed.remember(
        context,
        domain_id="travel",
        source_thread_id=context.thread_id,
        writes=(preference("hotel.near_subway"),),
    )

    assert [memory.key for memory in snapshot.memories] == ["flight.avoid_red_eye"]
    assert [event.event_type for event in memory_run_events(run_store, context.run_id)] == [
        "memory.retrieved",
        "memory.created",
    ]


def test_stale_run_lease_cannot_create_snapshot_or_mutate_memory(tmp_path):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)
    create_run(run_store, "run-stale")
    stale_token = claim_run(run_store, "run-stale", owner_id="manager-a")
    stale_context = context_for("run-stale", stale_token)
    governed = GovernedMemory(memory_store, run_store)

    clock.advance(seconds=11)
    replacement_token = claim_run(run_store, "run-stale", owner_id="manager-b")
    assert replacement_token != stale_token

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.retrieve(
            stale_context,
            domain_id="travel",
            allowed_keys=("flight.avoid_red_eye",),
        )
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            stale_context,
            domain_id="travel",
            source_thread_id=stale_context.thread_id,
            writes=(preference("flight.avoid_red_eye"),),
        )

    assert memory_store.get_run_snapshot(stale_context.run_id) is None
    assert memory_store.list_memories(
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    ) == []
    assert memory_store.list_events_for_run(
        stale_context.run_id,
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    ) == []
    assert memory_run_events(run_store, stale_context.run_id) == []


def test_exact_expiry_without_takeover_fences_snapshot_and_memory_mutation(tmp_path):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)
    create_run(run_store, "run-exact-expiry")
    lease_token = claim_run(
        run_store,
        "run-exact-expiry",
        owner_id="manager-a",
    )
    context = context_for("run-exact-expiry", lease_token)
    governed = GovernedMemory(memory_store, run_store)

    clock.advance(seconds=10)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.retrieve(
            context,
            domain_id="travel",
            allowed_keys=("flight.avoid_red_eye",),
        )
    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            context,
            domain_id="travel",
            source_thread_id=context.thread_id,
            writes=(preference("flight.avoid_red_eye"),),
        )

    assert memory_store.get_run_snapshot(context.run_id) is None
    assert memory_store.list_memories(
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    ) == []
    assert memory_store.list_events_for_run(
        context.run_id,
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    ) == []
    assert memory_run_events(run_store, context.run_id) == []


@pytest.mark.parametrize("lease_token", [None, ""], ids=["missing", "empty"])
def test_missing_or_empty_lease_token_fails_closed_before_memory_work(
    tmp_path,
    lease_token: str | None,
):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)
    create_run(run_store, "run-missing-token")
    claim_run(run_store, "run-missing-token", owner_id="manager-a")
    context = context_for("run-missing-token", lease_token)
    governed = GovernedMemory(memory_store, run_store)

    with pytest.raises(RunLeaseLostError, match="has no lease token"):
        governed.retrieve(
            context,
            domain_id="travel",
            allowed_keys=("flight.avoid_red_eye",),
        )
    with pytest.raises(RunLeaseLostError, match="has no lease token"):
        governed.remember(
            context,
            domain_id="travel",
            source_thread_id=context.thread_id,
            writes=(preference("flight.avoid_red_eye"),),
        )

    assert memory_store.get_run_snapshot(context.run_id) is None
    assert memory_store.list_memories(
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    ) == []
    assert memory_run_events(run_store, context.run_id) == []


def test_exact_expiry_after_memory_commit_fences_public_mirror(tmp_path, monkeypatch):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)
    create_run(run_store, "run-exact-mirror-expiry")
    lease_token = claim_run(
        run_store,
        "run-exact-mirror-expiry",
        owner_id="manager-a",
    )
    context = context_for("run-exact-mirror-expiry", lease_token)
    governed = GovernedMemory(memory_store, run_store)
    original_upsert = memory_store.upsert_from_run

    def expire_after_memory_commit(**kwargs):
        result = original_upsert(**kwargs)
        clock.advance(seconds=10)
        return result

    monkeypatch.setattr(memory_store, "upsert_from_run", expire_after_memory_commit)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            context,
            domain_id="travel",
            source_thread_id=context.thread_id,
            writes=(preference("flight.avoid_red_eye"),),
        )

    audit_events = memory_store.list_events_for_run(
        context.run_id,
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    )
    assert [event.event_type for event in audit_events] == ["memory.created"]
    assert memory_run_events(run_store, context.run_id) == []


def test_replacement_attempt_repairs_audit_committed_before_stale_mirror(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "runtime.db"
    clock = ManualLeaseClock()
    run_store = SQLiteRunStore(database_path, lease_clock_ms=clock)
    memory_store = SQLiteMemoryStore(database_path, lease_clock_ms=clock)
    create_run(run_store, "run-repair")
    stale_token = claim_run(run_store, "run-repair", owner_id="manager-a")
    stale_context = context_for("run-repair", stale_token)
    governed = GovernedMemory(memory_store, run_store)
    write = preference("flight.avoid_red_eye")

    original_upsert = memory_store.upsert_from_run
    replacement_tokens: list[str] = []

    def rotate_lease_after_memory_commit(**kwargs):
        result = original_upsert(**kwargs)
        if not replacement_tokens:
            clock.advance(seconds=11)
            replacement_tokens.append(
                claim_run(run_store, "run-repair", owner_id="manager-b")
            )
        return result

    monkeypatch.setattr(memory_store, "upsert_from_run", rotate_lease_after_memory_commit)

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        governed.remember(
            stale_context,
            domain_id="travel",
            source_thread_id=stale_context.thread_id,
            writes=(write,),
        )

    audit_events = memory_store.list_events_for_run(
        stale_context.run_id,
        tenant_id=AUTHORITY.tenant_id,
        subject_id=AUTHORITY.subject_id,
    )
    assert [event.event_type for event in audit_events] == ["memory.created"]
    assert memory_run_events(run_store, stale_context.run_id) == []

    replacement_context = context_for("run-repair", replacement_tokens[0])
    governed.remember(
        replacement_context,
        domain_id="travel",
        source_thread_id=replacement_context.thread_id,
        writes=(write,),
    )
    governed.remember(
        replacement_context,
        domain_id="travel",
        source_thread_id=replacement_context.thread_id,
        writes=(write,),
    )

    mirrored_events = memory_run_events(run_store, replacement_context.run_id)
    assert [event.event_type for event in mirrored_events] == ["memory.created"]
    assert mirrored_events[0].payload["audit_event_id"] == audit_events[0].event_id
