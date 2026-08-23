from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from runtime_service.postgres_schema import (
    PostgresSchemaError,
    initialize_postgres_schema,
    open_postgres_connection,
    postgres_connection,
    validate_postgres_schema_name,
)
from runtime_service.postgres_store import PostgresRunStore

from .backends import PostgresConformanceBackend, StoreConformanceBackend
from .scenarios import TENANT_ID, claim_run, create_queued_run


def _postgres_backend(
    store_backend: StoreConformanceBackend,
) -> PostgresConformanceBackend:
    if not isinstance(store_backend, PostgresConformanceBackend):
        pytest.skip("PostgreSQL-specific implementation proof")
    return store_backend


def test_postgres_schema_initialization_is_idempotent(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_schema(backend.dsn, schema=backend.schema)
    initialize_postgres_schema(backend.dsn, schema=backend.schema)
    store = backend.open_postgres_run_store()
    store.ping()


def test_postgres_incompatible_schema_version_fails_closed(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    initialize_postgres_schema(backend.dsn, schema=backend.schema)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with connection.transaction():
            connection.execute(
                "UPDATE runtime_store_schema SET version = 999 WHERE component = %s",
                ("execution-plane",),
            )
    finally:
        connection.close()
    with pytest.raises(PostgresSchemaError, match="incompatible"):
        initialize_postgres_schema(backend.dsn, schema=backend.schema)


def test_postgres_unversioned_execution_schema_fails_closed(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    connection = psycopg.connect(backend.dsn, autocommit=True)
    try:
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(backend.schema)
            )
        )
        connection.execute(
            sql.SQL("CREATE TABLE {}.runs (run_id TEXT PRIMARY KEY)").format(
                sql.Identifier(backend.schema)
            )
        )
    finally:
        connection.close()
    with pytest.raises(PostgresSchemaError, match="unversioned"):
        initialize_postgres_schema(backend.dsn, schema=backend.schema)


def test_postgres_rejects_unvalidated_schema_names(store_backend) -> None:
    _postgres_backend(store_backend)
    for invalid in ("", "BadSchema", "bad-name", "bad;drop schema public", "a" * 64):
        with pytest.raises(ValueError):
            validate_postgres_schema_name(invalid)


def test_postgres_stores_use_independent_server_connections(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    first = open_postgres_connection(backend.dsn, schema=backend.schema)
    second = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        first_pid = first.execute("SELECT pg_backend_pid() AS pid").fetchone()
        second_pid = second.execute("SELECT pg_backend_pid() AS pid").fetchone()
        assert first_pid is not None and second_pid is not None
        assert int(first_pid["pid"]) != int(second_pid["pid"])
    finally:
        first.close()
        second.close()

    run_store_a = backend.open_postgres_run_store()
    run_store_b = backend.open_postgres_run_store()
    assert not hasattr(run_store_a, "_lock")
    assert not hasattr(run_store_b, "_lock")
    create_queued_run(run_store_a, "pg-independent-a", thread_id="pg-thread-a", order=1)
    create_queued_run(run_store_a, "pg-independent-b", thread_id="pg-thread-b", order=2)
    first_claim = claim_run(run_store_a, "pg-owner-a")
    second_claim = claim_run(run_store_b, "pg-owner-b")
    assert first_claim is not None
    assert second_claim is not None
    assert {first_claim.run.run_id, second_claim.run.run_id} == {
        "pg-independent-a",
        "pg-independent-b",
    }


def test_postgres_production_lease_clock_comes_from_server(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    store = PostgresRunStore(
        backend.dsn,
        schema=backend.schema,
        lease_clock_ms=None,
    )
    create_queued_run(store, "pg-server-clock", thread_id="pg-server-clock-thread", order=1)
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        before = connection.execute(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms"
        ).fetchone()
    finally:
        connection.close()
    claim = store.claim_next_run(owner_id="pg-clock-owner", lease_duration_seconds=10)
    assert claim is not None
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        after = connection.execute(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms"
        ).fetchone()
    finally:
        connection.close()
    assert before is not None and after is not None
    assert claim.run.lease_heartbeat_at is not None
    assert int(before["now_ms"]) <= claim.run.lease_heartbeat_at <= int(after["now_ms"])
    assert claim.run.lease_expires_at == claim.run.lease_heartbeat_at + 10_000


def test_postgres_manual_clock_exact_expiry_boundary(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    store = backend.open_postgres_run_store()
    create_queued_run(store, "pg-clock-boundary", thread_id="pg-clock-boundary-thread", order=1)
    first = store.claim_next_run(owner_id="owner-a", lease_duration_seconds=10)
    assert first is not None
    backend.clock.advance(9_999)
    assert store.claim_next_run(owner_id="owner-b", lease_duration_seconds=10) is None
    backend.clock.advance(1)
    takeover = store.claim_next_run(owner_id="owner-b", lease_duration_seconds=10)
    assert takeover is not None
    assert takeover.run.run_id == first.run.run_id
    assert takeover.run.attempt == first.run.attempt + 1
    assert takeover.lease_token != first.lease_token


def test_postgres_failed_transaction_rolls_back_and_connection_closes(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    connection = None
    with pytest.raises(RuntimeError, match="rollback probe"):
        with postgres_connection(backend.dsn, schema=backend.schema) as active:
            connection = active
            with active.transaction():
                active.execute(
                    """
                    INSERT INTO runs (
                        run_id, tenant_id, thread_id, agent_id, agent_version,
                        domain_id, schema_version, status, input_message,
                        validation_errors_json, attempt, cancel_requested,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, FALSE, %s, %s)
                    """,
                    (
                        "pg-rollback",
                        TENANT_ID,
                        "pg-rollback-thread",
                        "travel-agent",
                        "0.3.0",
                        "travel",
                        "1",
                        "queued",
                        "probe",
                        "[]",
                        "2026-08-23T00:00:00+00:00",
                        "2026-08-23T00:00:00+00:00",
                    ),
                )
                raise RuntimeError("rollback probe")
    assert connection is not None and connection.closed
    reopened = backend.open_postgres_run_store()
    assert reopened.get_run_internal("pg-rollback") is None


def test_postgres_committed_state_survives_reopen(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    first = backend.open_postgres_run_store()
    create_queued_run(first, "pg-reopen", thread_id="pg-reopen-thread", order=1)
    second = backend.open_postgres_run_store()
    restored = second.get_run_internal("pg-reopen")
    assert restored is not None
    assert restored.run_id == "pg-reopen"


def test_postgres_expected_unique_and_fk_constraints_are_enforced(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    store = backend.open_postgres_run_store()
    create_queued_run(store, "pg-unique-a", thread_id="pg-shared-thread", order=1)
    create_queued_run(store, "pg-unique-b", thread_id="pg-shared-thread", order=2)
    claim = claim_run(store, "pg-owner")
    assert claim is not None
    connection = open_postgres_connection(backend.dsn, schema=backend.schema)
    try:
        with pytest.raises(psycopg.errors.UniqueViolation):
            with connection.transaction():
                connection.execute(
                    "UPDATE runs SET status = 'running' WHERE run_id = %s",
                    ("pg-unique-b",),
                )
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.transaction():
                connection.execute(
                    """
                    INSERT INTO workflow_events(run_id, sequence, event_type, payload_json, created_at)
                    VALUES (%s, 1, 'probe', '{}', %s)
                    """,
                    ("missing-workflow", "2026-08-23T00:00:00+00:00"),
                )
    finally:
        connection.close()


def test_postgres_test_schemas_do_not_leak_data(store_backend) -> None:
    backend = _postgres_backend(store_backend)
    first = backend.open_postgres_run_store()
    create_queued_run(first, "pg-isolation", thread_id="pg-isolation-thread", order=1)

    sibling = PostgresConformanceBackend(
        dsn=backend.dsn,
        schema=f"arp_test_{uuid4().hex}",
        clock=backend.clock,
    )
    try:
        second = sibling.open_postgres_run_store()
        assert second.get_run_internal("pg-isolation") is None
    finally:
        sibling.close()
