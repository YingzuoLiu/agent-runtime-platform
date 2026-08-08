"""Characterization of the current SQLite schema produced by SQLiteRunStore.

Phase 4A adds tenant routing metadata and tenant-qualified idempotency/checkpoint
keys on top of Phase 3A's domain/schema routing and canonical structured input.
The snapshot proves that migration preserves all earlier run, event, and
checkpoint data.
"""

from __future__ import annotations

import sqlite3

from domains.travel.state import AgentState
from runtime_service import RunRecord, RunStatus
from runtime_service.registry import build_default_registry
from runtime_service.models import LEGACY_TENANT_ID
from runtime_service.store import SQLiteRunStore

EXPECTED_COLUMNS = {
    "runs": [
        ("run_id", "TEXT", 0, 1),
        ("tenant_id", "TEXT", 1, 0),
        ("thread_id", "TEXT", 1, 0),
        ("agent_id", "TEXT", 1, 0),
        ("agent_version", "TEXT", 1, 0),
        ("domain_id", "TEXT", 1, 0),
        ("schema_version", "TEXT", 1, 0),
        ("status", "TEXT", 1, 0),
        ("input_message", "TEXT", 1, 0),
        ("input_json", "TEXT", 0, 0),
        ("state_json", "TEXT", 0, 0),
        ("output_message", "TEXT", 0, 0),
        ("validation_errors_json", "TEXT", 1, 0),
        ("error", "TEXT", 0, 0),
        ("attempt", "INTEGER", 1, 0),
        ("cancel_requested", "INTEGER", 1, 0),
        ("client_request_id", "TEXT", 0, 0),
        ("created_at", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
        ("started_at", "TEXT", 0, 0),
        ("completed_at", "TEXT", 0, 0),
    ],
    "run_events": [
        ("event_id", "INTEGER", 0, 1),
        ("run_id", "TEXT", 1, 0),
        ("sequence", "INTEGER", 1, 0),
        ("event_type", "TEXT", 1, 0),
        ("payload_json", "TEXT", 1, 0),
        ("created_at", "TEXT", 1, 0),
    ],
    "thread_states": [
        ("tenant_id", "TEXT", 1, 1),
        ("thread_id", "TEXT", 1, 2),
        ("domain_id", "TEXT", 1, 0),
        ("schema_version", "TEXT", 1, 0),
        ("state_json", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
    ],
}


def _columns(database_path) -> dict[str, list[tuple]]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        result = {}
        for table in EXPECTED_COLUMNS:
            rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
            result[table] = [
                (row["name"], row["type"], row["notnull"], row["pk"])
                for row in rows
            ]
        return result
    finally:
        connection.close()


def test_schema_matches_current_column_snapshot(tmp_path):
    SQLiteRunStore(tmp_path / "runtime.db")

    columns = _columns(tmp_path / "runtime.db")
    assert columns == EXPECTED_COLUMNS


def test_expected_tables_are_exactly_runs_run_events_thread_states(tmp_path):
    SQLiteRunStore(tmp_path / "runtime.db")

    connection = sqlite3.connect(tmp_path / "runtime.db")
    try:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    finally:
        connection.close()

    assert {row[0] for row in rows} == set(EXPECTED_COLUMNS)


def test_pre_phase_3a_rows_migrate_to_travel_schema_without_data_loss(tmp_path):
    database_path = tmp_path / "legacy.db"
    state = AgentState(thread_id="legacy-thread", destination="Tokyo")
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                agent_version TEXT NOT NULL,
                status TEXT NOT NULL,
                input_message TEXT NOT NULL,
                state_json TEXT,
                output_message TEXT,
                validation_errors_json TEXT NOT NULL,
                error TEXT,
                attempt INTEGER NOT NULL,
                cancel_requested INTEGER NOT NULL,
                client_request_id TEXT UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT
            );
            CREATE TABLE thread_states (
                thread_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE run_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(run_id, sequence),
                FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            """
        )
        connection.execute(
            """
            INSERT INTO runs VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                "run_legacy",
                "legacy-thread",
                "travel-agent",
                "0.3.0",
                "completed",
                "Plan Tokyo",
                state.model_dump_json(),
                "Planned.",
                "[]",
                "legacy-request",
                1,
                0,
                None,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO thread_states VALUES (?, ?, ?)",
            (
                "legacy-thread",
                state.model_dump_json(),
                "2026-01-01T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO run_events (run_id, sequence, event_type, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "run_legacy",
                1,
                "run.completed",
                "{}",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    store = SQLiteRunStore(database_path, state_registry=build_default_registry())
    migrated_run = store.get_run_internal("run_legacy")
    migrated_state = store.load_thread_state(
        "legacy-thread",
        tenant_id=LEGACY_TENANT_ID,
        domain_id="travel",
        schema_version="1",
    )

    assert migrated_run is not None
    assert migrated_run.tenant_id == LEGACY_TENANT_ID
    assert migrated_run.domain_id == "travel"
    assert migrated_run.schema_version == "1"
    assert migrated_run.input == {"user_message": "Plan Tokyo"}
    assert migrated_run.state is not None
    assert migrated_run.state.destination == "Tokyo"
    assert migrated_state is not None
    assert migrated_state.destination == "Tokyo"
    assert [event.event_type for event in store.list_events("run_legacy")] == [
        "run.completed"
    ]
    store.create_run(
        RunRecord(
            run_id="run_new_tenant",
            tenant_id="tenant-new",
            thread_id="new-tenant-thread",
            agent_id="travel-agent",
            agent_version="0.3.0",
            status=RunStatus.QUEUED,
            input={"user_message": "Plan Seoul"},
            client_request_id="legacy-request",
        )
    )
    assert store.get_run_by_client_request_id(
        "tenant-new",
        "legacy-request",
    ).run_id == "run_new_tenant"
