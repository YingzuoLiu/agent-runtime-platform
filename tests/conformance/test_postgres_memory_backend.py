from __future__ import annotations

import psycopg
import pytest

from agent.contracts import RuntimeExecutionAuthority
from runtime_service.memory import MemoryKind, MemoryWrite
from runtime_service.models import RunRecord, RunStatus
from runtime_service.postgres_memory_store import PostgresMemoryStore
from runtime_service.postgres_schema import (
    POSTGRES_MEMORY_SCHEMA_VERSION,
    POSTGRES_SCHEMA_VERSION,
    PostgresSchemaError,
    initialize_postgres_memory_schema,
    initialize_postgres_schema,
    open_postgres_connection,
)
from runtime_service.postgres_store import PostgresRunStore
from runtime_service.store import RunLeaseLostError

from .backends import PostgresConformanceBackend, StoreConformanceBackend


TENANT_ID = "postgres-memory-tenant"
SUBJECT_ID = "postgres-memory-subject"
DOMAIN_ID = "travel"


def _postgres_backend(
    store_backend: StoreConformanceBackend,
) -> PostgresConformanceBackend:
    if not isinstance(store_backend, PostgresConformanceBackend):
        pytest.skip("PostgreSQL-specific Memory implementation proof")
    return store_backend


def _preference(value: object) -> MemoryWrite:
    return MemoryWrite(
        kind=MemoryKind.PREFERENCE,
        key="flight.avoid_red_eye",
        value=value,
    )


def _create_run(store: PostgresRunStore, run_id: str) -> None:
    store.create_run(
        RunRecord(
            run_id=run_id,
            tenant_id=TENANT_ID,
            thread_id=f"thread-{run_id}",
            agent_id="travel-agent",
            agent_version="1.1.0",
            domain_id=DOMAIN_ID,
            schema_version="1",
            status=RunStatus.QUEUED,
            input={"user_message": "Plan a trip."},
            execution_authority=RuntimeExecutionAuthority(
                tenant_id=TENANT_ID,
                subject_id=SUBJECT_ID,
                permissions=("memory:read", "memory:write"),
            ),
        )
    )


def test_postgres_memory_clean_bootstrap_and_repeated_initialization(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)
    initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)
    store = backend.open_postgres_memory_store()
    store.ping()

    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        rows = connection.execute(
            "SELECT component, version FROM runtime_store_schema ORDER BY component"
        ).fetchall()
    finally:
        connection.close()
    assert [(row["component"], int(row["version"])) for row in rows] == [
        ("execution-plane", POSTGRES_SCHEMA_VERSION),
        ("memory", POSTGRES_MEMORY_SCHEMA_VERSION),
    ]


def test_postgres_memory_component_installs_on_accepted_execution_v1(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_schema(backend.dsn, schema=backend.schema)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        before = connection.execute(
            "SELECT version FROM runtime_store_schema WHERE component = %s",
            ("memory",),
        ).fetchone()
        table_before = connection.execute(
            "SELECT to_regclass('memory_records') AS table_name"
        ).fetchone()
    finally:
        connection.close()
    assert before is None
    assert table_before is not None and table_before["table_name"] is None

    initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        execution = connection.execute(
            "SELECT version FROM runtime_store_schema WHERE component = %s",
            ("execution-plane",),
        ).fetchone()
        memory = connection.execute(
            "SELECT version FROM runtime_store_schema WHERE component = %s",
            ("memory",),
        ).fetchone()
    finally:
        connection.close()
    assert execution is not None and int(execution["version"]) == POSTGRES_SCHEMA_VERSION
    assert memory is not None and int(memory["version"]) == POSTGRES_MEMORY_SCHEMA_VERSION


def test_postgres_memory_incompatible_component_version_fails_closed(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with connection.transaction():
            connection.execute(
                "UPDATE runtime_store_schema SET version = 999 WHERE component = %s",
                ("memory",),
            )
    finally:
        connection.close()
    with pytest.raises(PostgresSchemaError, match="Memory schema version is incompatible"):
        initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)


def test_postgres_unversioned_memory_schema_fails_closed(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_schema(backend.dsn, schema=backend.schema)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with connection.transaction():
            connection.execute(
                "CREATE TABLE memory_records (memory_id TEXT PRIMARY KEY)"
            )
    finally:
        connection.close()
    with pytest.raises(PostgresSchemaError, match="unversioned"):
        initialize_postgres_memory_schema(backend.dsn, schema=backend.schema)


def test_postgres_memory_committed_state_survives_reopen(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    first = backend.open_postgres_memory_store()
    created = first.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=_preference(True),
        source_run_id="run-reopen",
        source_thread_id="thread-reopen",
        actor_subject_id=SUBJECT_ID,
    ).record

    second = backend.open_postgres_memory_store()
    restored = second.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )
    assert len(restored) == 1
    assert restored[0] == created


def test_postgres_active_key_unique_index_is_an_independent_backstop(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    store = backend.open_postgres_memory_store()
    first = store.upsert(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
        domain_id=DOMAIN_ID,
        write=_preference(True),
        source_run_id="run-unique",
        source_thread_id="thread-unique",
        actor_subject_id=SUBJECT_ID,
    ).record
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with pytest.raises(psycopg.errors.UniqueViolation):
            with connection.transaction():
                connection.execute(
                    """
                    INSERT INTO memory_records (
                        memory_id, tenant_id, subject_id, domain_id, kind, memory_key,
                        value_json, status, source_run_id, source_thread_id, confidence,
                        version, created_at, updated_at, expires_at
                    )
                    SELECT
                        %s, tenant_id, subject_id, domain_id, kind, memory_key,
                        value_json, status, source_run_id, source_thread_id, confidence,
                        2, created_at, updated_at, expires_at
                    FROM memory_records WHERE memory_id = %s
                    """,
                    ("mem_duplicate_active", first.memory_id),
                )
    finally:
        connection.close()
    assert [record.memory_id for record in store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    )] == [first.memory_id]


def test_postgres_memory_exact_expiry_uses_server_transaction_time(
    store_backend: StoreConformanceBackend,
) -> None:
    backend = _postgres_backend(store_backend)
    run_store = PostgresRunStore(
        backend.dsn,
        schema=backend.schema,
        lease_clock_ms=None,
    )
    memory_store = PostgresMemoryStore(
        backend.dsn,
        schema=backend.schema,
        lease_clock_ms=None,
    )
    _create_run(run_store, "run-server-expiry")
    claim = run_store.claim_next_run(
        owner_id="owner-server-expiry",
        lease_duration_seconds=30,
    )
    assert claim is not None

    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with connection.transaction():
            connection.execute(
                """
                UPDATE runs
                SET lease_expires_at = floor(
                    extract(epoch FROM transaction_timestamp()) * 1000
                )::bigint
                WHERE run_id = %s
                """,
                (claim.run.run_id,),
            )
    finally:
        connection.close()

    with pytest.raises(RunLeaseLostError, match="no longer current"):
        memory_store.upsert_from_run(
            lease_token=claim.lease_token,
            tenant_id=TENANT_ID,
            subject_id=SUBJECT_ID,
            domain_id=DOMAIN_ID,
            write=_preference(True),
            source_run_id=claim.run.run_id,
            source_thread_id=claim.run.thread_id,
            actor_subject_id=SUBJECT_ID,
        )
    assert memory_store.list_memories(
        tenant_id=TENANT_ID,
        subject_id=SUBJECT_ID,
    ) == []
