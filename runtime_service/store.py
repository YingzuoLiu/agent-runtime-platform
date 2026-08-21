from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Protocol
from uuid import uuid4

from agent.contracts import BaseRuntimeState, utc_now
from .models import (
    LEGACY_TENANT_ID,
    RunCommitOutcome,
    RunEvent,
    RunLeaseClaim,
    RunLeaseRecoveryReason,
    RunRecord,
    RunStatus,
)


class RunLeaseLostError(RuntimeError):
    """Raised when an attempt-owned mutation no longer holds its Run lease."""


class StateRegistry(Protocol):
    def parse_state(
        self,
        domain_id: str,
        schema_version: str,
        payload: dict[str, Any],
    ) -> BaseRuntimeState:
        ...


class SQLiteRunStore:
    """Durable run, event and thread-state storage backed by SQLite."""

    _CONTROL_PLANE_EVENT_TYPES = frozenset(
        {
            "sandbox.execution_started",
            "sandbox.execution_finished",
        }
    )

    def __init__(
        self,
        database_path: str | Path,
        *,
        state_registry: StateRegistry | None = None,
        lease_operation_timeout_seconds: float = 1.0,
        lease_clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if lease_operation_timeout_seconds <= 0:
            raise ValueError("lease_operation_timeout_seconds must be positive")
        self.database_path = str(database_path)
        self._lock = threading.RLock()
        self._state_registry = state_registry
        self._state_models: dict[tuple[str, str], type[BaseRuntimeState]] = {}
        self._lease_operation_timeout_seconds = lease_operation_timeout_seconds
        self._lease_clock_ms = lease_clock_ms
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def bind_state_registry(self, state_registry: StateRegistry) -> None:
        if self._state_registry is not None and self._state_registry is not state_registry:
            raise ValueError("SQLiteRunStore is already bound to a different state registry")
        self._state_registry = state_registry

    @property
    def lease_operation_timeout_seconds(self) -> float:
        """Maximum SQLite wait used by claim, heartbeat, and fenced commits."""

        return self._lease_operation_timeout_seconds

    def _connect(self, *, timeout_seconds: float = 30) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=timeout_seconds)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _lease_connect(self) -> sqlite3.Connection:
        return self._connect(timeout_seconds=self._lease_operation_timeout_seconds)

    def _lease_now_ms(self, connection: sqlite3.Connection) -> int:
        """Read the lease clock from SQLite, not from a worker's wall clock."""

        if self._lease_clock_ms is not None:
            return self._lease_clock_ms()
        row = connection.execute(
            "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
        ).fetchone()
        assert row is not None
        return int(row[0])

    def initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    execution_authority_json TEXT,
                    thread_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    agent_version TEXT NOT NULL,
                    domain_id TEXT NOT NULL DEFAULT 'travel',
                    schema_version TEXT NOT NULL DEFAULT '1',
                    status TEXT NOT NULL,
                    input_message TEXT NOT NULL,
                    input_json TEXT,
                    state_json TEXT,
                    output_message TEXT,
                    validation_errors_json TEXT NOT NULL,
                    error_code TEXT,
                    error TEXT,
                    attempt INTEGER NOT NULL,
                    cancel_requested INTEGER NOT NULL,
                    client_request_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    lease_owner_id TEXT,
                    lease_token TEXT,
                    lease_heartbeat_at INTEGER,
                    lease_expires_at INTEGER
                );

                CREATE TABLE IF NOT EXISTS run_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_id, sequence),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS thread_states (
                    tenant_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    domain_id TEXT NOT NULL DEFAULT 'travel',
                    schema_version TEXT NOT NULL DEFAULT '1',
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(tenant_id, thread_id)
                );
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "client_request_id" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN client_request_id TEXT")
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_client_request_id "
                    "ON runs(client_request_id) WHERE client_request_id IS NOT NULL"
                )
            run_migrations = {
                "tenant_id": (
                    "ALTER TABLE runs ADD COLUMN tenant_id "
                    f"TEXT NOT NULL DEFAULT '{LEGACY_TENANT_ID}'"
                ),
                "domain_id": "ALTER TABLE runs ADD COLUMN domain_id TEXT NOT NULL DEFAULT 'travel'",
                "schema_version": "ALTER TABLE runs ADD COLUMN schema_version TEXT NOT NULL DEFAULT '1'",
                "input_json": "ALTER TABLE runs ADD COLUMN input_json TEXT",
                "execution_authority_json": (
                    "ALTER TABLE runs ADD COLUMN execution_authority_json TEXT"
                ),
                "error_code": "ALTER TABLE runs ADD COLUMN error_code TEXT",
                "lease_owner_id": "ALTER TABLE runs ADD COLUMN lease_owner_id TEXT",
                "lease_token": "ALTER TABLE runs ADD COLUMN lease_token TEXT",
                "lease_heartbeat_at": (
                    "ALTER TABLE runs ADD COLUMN lease_heartbeat_at INTEGER"
                ),
                "lease_expires_at": "ALTER TABLE runs ADD COLUMN lease_expires_at INTEGER",
            }
            for column, statement in run_migrations.items():
                if column not in columns:
                    connection.execute(statement)

            thread_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(thread_states)").fetchall()
            }
            if "domain_id" not in thread_columns:
                connection.execute(
                    "ALTER TABLE thread_states ADD COLUMN domain_id TEXT NOT NULL DEFAULT 'travel'"
                )
            if "schema_version" not in thread_columns:
                connection.execute(
                    "ALTER TABLE thread_states ADD COLUMN schema_version TEXT NOT NULL DEFAULT '1'"
                )
            if "tenant_id" not in thread_columns:
                connection.execute(
                    "ALTER TABLE thread_states ADD COLUMN tenant_id "
                    f"TEXT NOT NULL DEFAULT '{LEGACY_TENANT_ID}'"
                )

            if self._has_global_client_request_uniqueness(connection):
                self._rebuild_runs_table(connection)
            if not self._thread_states_has_tenant_primary_key(connection):
                self._rebuild_thread_states_table(connection)

            connection.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_runs_tenant_thread_id
                    ON runs(tenant_id, thread_id);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE INDEX IF NOT EXISTS idx_runs_claimable
                    ON runs(status, lease_expires_at, created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_tenant_client_request_id
                    ON runs(tenant_id, client_request_id)
                    WHERE client_request_id IS NOT NULL;
                """
            )
            connection.commit()
            connection.execute("PRAGMA foreign_keys=ON")
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError("Tenant schema migration left invalid foreign keys")

    @staticmethod
    def _has_global_client_request_uniqueness(connection: sqlite3.Connection) -> bool:
        for index in connection.execute("PRAGMA index_list(runs)").fetchall():
            if not index["unique"]:
                continue
            quoted_name = index["name"].replace('"', '""')
            columns = [
                row["name"]
                for row in connection.execute(
                    f'PRAGMA index_info("{quoted_name}")'
                ).fetchall()
            ]
            if columns == ["client_request_id"]:
                return True
        return False

    @staticmethod
    def _thread_states_has_tenant_primary_key(connection: sqlite3.Connection) -> bool:
        columns = connection.execute("PRAGMA table_info(thread_states)").fetchall()
        primary_key = {
            row["name"]: row["pk"]
            for row in columns
            if row["pk"]
        }
        return primary_key == {"tenant_id": 1, "thread_id": 2}

    @staticmethod
    def _rebuild_runs_table(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE runs_tenant_migration (
                run_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                execution_authority_json TEXT,
                thread_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                agent_version TEXT NOT NULL,
                domain_id TEXT NOT NULL DEFAULT 'travel',
                schema_version TEXT NOT NULL DEFAULT '1',
                status TEXT NOT NULL,
                input_message TEXT NOT NULL,
                input_json TEXT,
                state_json TEXT,
                output_message TEXT,
                validation_errors_json TEXT NOT NULL,
                error_code TEXT,
                error TEXT,
                attempt INTEGER NOT NULL,
                cancel_requested INTEGER NOT NULL,
                client_request_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                lease_owner_id TEXT,
                lease_token TEXT,
                lease_heartbeat_at INTEGER,
                lease_expires_at INTEGER
            );

            INSERT INTO runs_tenant_migration (
                run_id, tenant_id, execution_authority_json, thread_id, agent_id, agent_version,
                domain_id, schema_version, status, input_message, input_json,
                state_json, output_message, validation_errors_json, error_code, error,
                attempt, cancel_requested, client_request_id, created_at,
                updated_at, started_at, completed_at, lease_owner_id,
                lease_token, lease_heartbeat_at, lease_expires_at
            )
            SELECT
                run_id, tenant_id, execution_authority_json, thread_id, agent_id, agent_version,
                domain_id, schema_version, status, input_message, input_json,
                state_json, output_message, validation_errors_json, error_code, error,
                attempt, cancel_requested, client_request_id, created_at,
                updated_at, started_at, completed_at, lease_owner_id,
                lease_token, lease_heartbeat_at, lease_expires_at
            FROM runs;

            DROP TABLE runs;
            ALTER TABLE runs_tenant_migration RENAME TO runs;
            COMMIT;
            """
        )

    @staticmethod
    def _rebuild_thread_states_table(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE thread_states_tenant_migration (
                tenant_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                domain_id TEXT NOT NULL DEFAULT 'travel',
                schema_version TEXT NOT NULL DEFAULT '1',
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(tenant_id, thread_id)
            );

            INSERT INTO thread_states_tenant_migration (
                tenant_id, thread_id, domain_id, schema_version, state_json, updated_at
            )
            SELECT tenant_id, thread_id, domain_id, schema_version, state_json, updated_at
            FROM thread_states;

            DROP TABLE thread_states;
            ALTER TABLE thread_states_tenant_migration RENAME TO thread_states;
            COMMIT;
            """
        )

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    def create_run(self, run: RunRecord) -> RunRecord:
        self._assert_new_queued_run(run)
        with self._lock, self._connect() as connection:
            self._insert_run_with_connection(connection, run)
        return run

    def create_run_with_event(
        self,
        run: RunRecord,
        *,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunRecord:
        """Create a Run and its first event in one transaction."""

        self._assert_new_queued_run(run)
        if event_type != "run.queued":
            raise ValueError("A new Run's initial event must be run.queued")
        with self._lock, self._connect() as connection:
            self._insert_run_with_connection(connection, run)
            self._append_event_with_connection(
                connection,
                run.run_id,
                event_type,
                payload,
            )
        return run

    @staticmethod
    def _assert_new_queued_run(run: RunRecord) -> None:
        lease_fields = (
            run.lease_owner_id,
            run.lease_token,
            run.lease_heartbeat_at,
            run.lease_expires_at,
        )
        if (
            run.status != RunStatus.QUEUED
            or run.attempt != 0
            or run.cancel_requested
            or run.started_at is not None
            or run.completed_at is not None
            or run.output_message is not None
            or bool(run.validation_errors)
            or run.error_code is not None
            or run.error is not None
            or any(value is not None for value in lease_fields)
        ):
            raise ValueError(
                "New Runs must be pristine queued records without lease authority"
            )

    def _seed_historical_run_for_migration(
        self,
        run: RunRecord,
        *,
        event_type: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RunRecord:
        """Seed pre-leasing history for migrations and compatibility tests only."""

        with self._lock, self._connect() as connection:
            self._insert_run_with_connection(connection, run)
            if event_type is not None:
                self._append_event_with_connection(
                    connection,
                    run.run_id,
                    event_type,
                    payload,
                )
        return run

    def _insert_run_with_connection(
        self,
        connection: sqlite3.Connection,
        run: RunRecord,
    ) -> None:
        connection.execute(
            """
            INSERT INTO runs (
                run_id, tenant_id, execution_authority_json, thread_id, agent_id,
                agent_version, domain_id,
                schema_version, status, input_message, input_json,
                state_json, output_message,
                validation_errors_json, error_code, error, attempt, cancel_requested,
                client_request_id, created_at, updated_at, started_at, completed_at,
                lease_owner_id, lease_token, lease_heartbeat_at, lease_expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._run_values(run),
        )

    def get_run_internal(self, run_id: str) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._row_to_run(row) if row else None

    def get_run_for_tenant(self, run_id: str, tenant_id: str) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ? AND tenant_id = ?",
                (run_id, tenant_id),
            ).fetchone()
        return self._row_to_run(row) if row else None

    def get_run_by_client_request_id(
        self,
        tenant_id: str,
        client_request_id: str,
    ) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE tenant_id = ? AND client_request_id = ?",
                (tenant_id, client_request_id),
            ).fetchone()
        return self._row_to_run(row) if row else None

    def commit_reconciliation_pending(
        self,
        run_id: str,
        *,
        tenant_id: str,
        lease_token: str,
        error_code: str,
        error: str,
    ) -> RunCommitOutcome:
        """Fence and persist a non-terminal external-action recovery marker."""

        updated_at = utc_now()
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET
                    error_code = ?, error = ?, completed_at = NULL, updated_at = ?
                WHERE run_id = ? AND tenant_id = ? AND status = ?
                    AND lease_token = ? AND lease_expires_at > ?
                """,
                (
                    error_code,
                    error,
                    updated_at,
                    run_id,
                    tenant_id,
                    RunStatus.RUNNING.value,
                    lease_token,
                    now_ms,
                ),
            )
            if cursor.rowcount == 1:
                return RunCommitOutcome.COMMITTED
            return self._classify_commit_outcome(
                connection,
                run_id,
                tenant_id=tenant_id,
                lease_token=lease_token,
                now_ms=now_ms,
            )

    def claim_next_run(
        self,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        reconciliation_pending_code: str | None = None,
    ) -> RunLeaseClaim | None:
        """Atomically claim one queued or expired Run attempt.

        SQLite's write transaction is the arbitration point across Manager
        processes.  The in-memory wake signal used by a Manager is never part
        of the ownership decision.
        """

        if not owner_id:
            raise ValueError("owner_id must not be empty")
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        lease_token = f"lease_{uuid4().hex}"
        timestamp = utc_now()
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            expires_at = now_ms + lease_duration_seconds * 1000
            candidate = connection.execute(
                """
                SELECT * FROM runs
                WHERE
                    (status = ? AND cancel_requested = 0)
                    OR (
                        status = ?
                        AND (
                            lease_token IS NULL
                            OR lease_expires_at IS NULL
                            OR lease_expires_at <= ?
                        )
                    )
                ORDER BY created_at, run_id
                LIMIT 1
                """,
                (RunStatus.QUEUED.value, RunStatus.RUNNING.value, now_ms),
            ).fetchone()
            if candidate is None:
                return None

            was_running = candidate["status"] == RunStatus.RUNNING.value
            recovery_reason: RunLeaseRecoveryReason | None = None
            if was_running:
                recovery_reason = (
                    RunLeaseRecoveryReason.LEGACY_UNLEASED
                    if candidate["lease_token"] is None
                    or candidate["lease_expires_at"] is None
                    else RunLeaseRecoveryReason.LEASE_EXPIRED
                )

            cursor = connection.execute(
                """
                UPDATE runs SET
                    status = ?, started_at = ?, attempt = attempt + 1,
                    error = CASE WHEN error_code = ? THEN error ELSE NULL END,
                    error_code = CASE WHEN error_code = ? THEN error_code ELSE NULL END,
                    updated_at = ?, lease_owner_id = ?, lease_token = ?,
                    lease_heartbeat_at = ?, lease_expires_at = ?
                WHERE run_id = ?
                    AND (
                        (status = ? AND cancel_requested = 0)
                        OR (
                            status = ?
                            AND (
                                lease_token IS NULL
                                OR lease_expires_at IS NULL
                                OR lease_expires_at <= ?
                            )
                        )
                    )
                """,
                (
                    RunStatus.RUNNING.value,
                    timestamp,
                    reconciliation_pending_code,
                    reconciliation_pending_code,
                    timestamp,
                    owner_id,
                    lease_token,
                    now_ms,
                    expires_at,
                    candidate["run_id"],
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                    now_ms,
                ),
            )
            if cursor.rowcount != 1:
                return None

            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (candidate["run_id"],),
            ).fetchone()
            assert row is not None
            run = self._row_to_run(row)
            if recovery_reason is not None:
                self._append_event_with_connection(
                    connection,
                    run.run_id,
                    "run.recovered",
                    {
                        "reason": (
                            reconciliation_pending_code
                            if reconciliation_pending_code is not None
                            and candidate["error_code"] == reconciliation_pending_code
                            else recovery_reason.value
                        ),
                    },
                )
            self._append_event_with_connection(
                connection,
                run.run_id,
                "run.started",
                {
                    "attempt": run.attempt,
                    "recovered": recovery_reason is not None,
                },
            )

        return RunLeaseClaim(
            run=run,
            owner_id=owner_id,
            lease_token=lease_token,
            recovery_reason=recovery_reason,
        )

    def renew_run_lease(
        self,
        run_id: str,
        *,
        lease_token: str,
        lease_duration_seconds: int,
    ) -> bool:
        """Renew only a still-current, still-unexpired lease.

        The new deadline is based on the store's current time, never the old
        deadline, so delayed heartbeats cannot accumulate a far-future lease.
        """

        if not lease_token:
            raise ValueError("lease_token must not be empty")
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET
                    lease_heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
                WHERE run_id = ? AND status = ? AND lease_token = ?
                    AND lease_expires_at > ?
                """,
                (
                    now_ms,
                    now_ms + lease_duration_seconds * 1000,
                    utc_now(),
                    run_id,
                    RunStatus.RUNNING.value,
                    lease_token,
                    now_ms,
                ),
            )
        return cursor.rowcount == 1

    def expire_run_lease(self, run_id: str, *, lease_token: str) -> bool:
        """Relinquish one drained attempt using its token as authority."""

        if not lease_token:
            raise ValueError("lease_token must not be empty")
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET
                    lease_heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
                WHERE run_id = ? AND status = ? AND lease_token = ?
                """,
                (
                    now_ms,
                    now_ms,
                    utc_now(),
                    run_id,
                    RunStatus.RUNNING.value,
                    lease_token,
                ),
            )
        return cursor.rowcount == 1

    def assert_current_run_lease(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        lease_token: str | None,
    ) -> None:
        """Fence an attempt-owned mutation inside its existing transaction."""

        if lease_token is None:
            raise RunLeaseLostError(f"Run lease token is required: {run_id}")
        now_ms = self._lease_now_ms(connection)
        row = connection.execute(
            """
            SELECT 1 FROM runs
            WHERE run_id = ? AND status = ? AND lease_token = ?
                AND lease_expires_at > ?
            """,
            (run_id, RunStatus.RUNNING.value, lease_token, now_ms),
        ).fetchone()
        if row is None:
            raise RunLeaseLostError(f"Run lease is no longer current: {run_id}")

    def append_attempt_event(
        self,
        run_id: str,
        *,
        lease_token: str | None,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
            return self._append_event_with_connection(
                connection,
                run_id,
                event_type,
                payload,
            )

    def finalize_next_queued_cancellation(self) -> RunRecord | None:
        """Terminalize one cancelled-before-claim Run without acquiring a lease."""

        completed_at = utc_now()
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM runs
                WHERE status = ? AND cancel_requested = 1
                ORDER BY created_at, run_id
                LIMIT 1
                """,
                (RunStatus.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None
            cursor = connection.execute(
                """
                UPDATE runs SET
                    status = ?, error_code = NULL, error = NULL,
                    completed_at = ?, updated_at = ?,
                    lease_owner_id = NULL, lease_token = NULL,
                    lease_heartbeat_at = NULL, lease_expires_at = NULL
                WHERE run_id = ? AND status = ? AND cancel_requested = 1
                """,
                (
                    RunStatus.CANCELLED.value,
                    completed_at,
                    completed_at,
                    row["run_id"],
                    RunStatus.QUEUED.value,
                ),
            )
            if cursor.rowcount != 1:
                return None
            self._append_event_with_connection(
                connection,
                row["run_id"],
                "run.cancelled",
                {"reason": "cancelled_before_start"},
            )
            updated = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (row["run_id"],),
            ).fetchone()
            assert updated is not None
            return self._row_to_run(updated)

    def request_cancel_atomically(self, run_id: str, *, tenant_id: str) -> RunRecord:
        """Atomically flip `cancel_requested` 0 -> 1 for a QUEUED/RUNNING run.

        The eligibility check and the flag flip are the same statement: a
        single conditional UPDATE using an explicit allowlist --
        `status IN ('queued', 'running')` -- acts as the compare-and-set.
        This is deliberately not "status NOT IN (completed, failed,
        cancelled)": a future non-terminal status (e.g. AWAITING_APPROVAL)
        must be added to this allowlist explicitly before it becomes
        cancellable here, instead of silently inheriting cancellability
        just because it happens not to be one of today's three terminal
        values.

        Only when the UPDATE actually flips the flag (rowcount == 1) does
        this append a `run.cancel_requested` event, in the same
        connection/transaction as the UPDATE. Two distinct situations both
        leave `rowcount == 0` and are deliberately *not* told apart here,
        because neither should write a new event or change anything:
        - the run is already terminal (COMPLETED/FAILED/CANCELLED) -- the
          caller can see this from the returned run's `status`;
        - the run is still QUEUED/RUNNING but `cancel_requested` is already
          1 (a duplicate cancel request) -- same returned run either way.
        """
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs
                SET cancel_requested = 1, updated_at = ?
                WHERE run_id = ? AND tenant_id = ?
                    AND status IN (?, ?) AND cancel_requested = 0
                """,
                (
                    utc_now(),
                    run_id,
                    tenant_id,
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ? AND tenant_id = ?",
                (run_id, tenant_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Run not found: {run_id}")
            if cursor.rowcount == 1:
                self._append_event_with_connection(
                    connection,
                    run_id,
                    "run.cancel_requested",
                    {"status": row["status"]},
                )
        return self._row_to_run(row)

    def commit_completed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        """Atomically fence and commit a completed Run plus checkpoint/events."""

        return self._commit_completed_run(run, lease_token=lease_token)

    def _commit_completed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        completed_at = utc_now()
        if run.state is not None:
            self._remember_state_model(run.state)
        state_json = run.state.model_dump_json() if run.state is not None else None
        validation_errors_json = json.dumps(run.validation_errors)

        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            self._assert_thread_schema_available(
                connection,
                run.tenant_id,
                run.thread_id,
                run.domain_id,
                run.schema_version,
            )
            cursor = connection.execute(
                """
                UPDATE runs SET
                    status = ?, state_json = ?, output_message = ?,
                    validation_errors_json = ?, error_code = NULL, error = NULL,
                    completed_at = ?, updated_at = ?,
                    lease_owner_id = NULL, lease_token = NULL,
                    lease_heartbeat_at = NULL, lease_expires_at = NULL
                WHERE run_id = ? AND tenant_id = ?
                    AND status = ? AND cancel_requested = 0
                    AND lease_token = ? AND lease_expires_at > ?
                """,
                (
                    RunStatus.COMPLETED.value,
                    state_json,
                    run.output_message,
                    validation_errors_json,
                    completed_at,
                    completed_at,
                    run.run_id,
                    run.tenant_id,
                    RunStatus.RUNNING.value,
                    lease_token,
                    now_ms,
                ),
            )
            if cursor.rowcount != 1:
                return self._classify_commit_outcome(
                    connection,
                    run.run_id,
                    tenant_id=run.tenant_id,
                    lease_token=lease_token,
                    now_ms=now_ms,
                )

            if run.state is not None:
                connection.execute(
                    """
                    INSERT INTO thread_states (
                        tenant_id, thread_id, domain_id, schema_version, state_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, thread_id) DO UPDATE SET
                        domain_id = excluded.domain_id,
                        schema_version = excluded.schema_version,
                        state_json = excluded.state_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        run.tenant_id,
                        run.thread_id,
                        run.domain_id,
                        run.schema_version,
                        state_json,
                        completed_at,
                    ),
                )

            trace_events = len(run.state.execution_trace) if run.state is not None else 0
            self._append_event_with_connection(
                connection,
                run.run_id,
                "checkpoint.saved",
                {"thread_id": run.thread_id, "trace_events": trace_events},
            )
            self._append_event_with_connection(
                connection,
                run.run_id,
                "run.completed",
                {"validation_errors": run.validation_errors},
            )

        run.status = RunStatus.COMPLETED
        run.error_code = None
        run.error = None
        run.completed_at = completed_at
        run.updated_at = completed_at
        run.lease_owner_id = None
        run.lease_token = None
        run.lease_heartbeat_at = None
        run.lease_expires_at = None
        return RunCommitOutcome.COMMITTED

    def commit_cancelled_run(
        self,
        run: RunRecord,
        *,
        reason: str,
        lease_token: str,
    ) -> RunCommitOutcome:
        return self._commit_cancelled_run(
            run,
            reason=reason,
            lease_token=lease_token,
        )

    def _commit_cancelled_run(
        self,
        run: RunRecord,
        *,
        reason: str,
        lease_token: str,
    ) -> RunCommitOutcome:
        completed_at = utc_now()
        if run.state is not None:
            self._remember_state_model(run.state)
        state_json = run.state.model_dump_json() if run.state is not None else None
        validation_errors_json = json.dumps(run.validation_errors)

        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET
                    execution_authority_json = ?, thread_id = ?, agent_id = ?,
                    agent_version = ?, domain_id = ?,
                    schema_version = ?, status = ?, input_message = ?, input_json = ?,
                    state_json = ?, output_message = ?,
                    validation_errors_json = ?, error_code = NULL, error = NULL,
                    attempt = ?,
                    cancel_requested = ?, client_request_id = ?, created_at = ?,
                    updated_at = ?, started_at = ?, completed_at = ?,
                    lease_owner_id = NULL, lease_token = NULL,
                    lease_heartbeat_at = NULL, lease_expires_at = NULL
                WHERE run_id = ? AND tenant_id = ?
                    AND status IN (?, ?) AND cancel_requested = 1
                    AND lease_token = ? AND lease_expires_at > ?
                """,
                (
                    (
                        run.execution_authority.model_dump_json()
                        if run.execution_authority is not None
                        else None
                    ),
                    run.thread_id,
                    run.agent_id,
                    run.agent_version,
                    run.domain_id,
                    run.schema_version,
                    RunStatus.CANCELLED.value,
                    self._legacy_input_message(run.input),
                    json.dumps(run.input),
                    state_json,
                    run.output_message,
                    validation_errors_json,
                    run.attempt,
                    1,
                    run.client_request_id,
                    run.created_at,
                    completed_at,
                    run.started_at,
                    completed_at,
                    run.run_id,
                    run.tenant_id,
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                    lease_token,
                    now_ms,
                ),
            )
            if cursor.rowcount != 1:
                return self._classify_commit_outcome(
                    connection,
                    run.run_id,
                    tenant_id=run.tenant_id,
                    lease_token=lease_token,
                    now_ms=now_ms,
                )

            self._append_event_with_connection(
                connection,
                run.run_id,
                "run.cancelled",
                {"reason": reason},
            )

        run.status = RunStatus.CANCELLED
        run.cancel_requested = True
        run.error_code = None
        run.error = None
        run.completed_at = completed_at
        run.updated_at = completed_at
        run.lease_owner_id = None
        run.lease_token = None
        run.lease_heartbeat_at = None
        run.lease_expires_at = None
        return RunCommitOutcome.COMMITTED

    def commit_failed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
        error_code: str,
        error: str,
        traceback_text: str | None = None,
        allow_cancel_requested: bool = False,
    ) -> RunCommitOutcome:
        """Atomically fence and commit a failed Run plus its failure event."""

        completed_at = utc_now()
        with self._lease_connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now_ms = self._lease_now_ms(connection)
            cursor = connection.execute(
                """
                UPDATE runs SET
                    status = ?, error_code = ?, error = ?, completed_at = ?,
                    updated_at = ?, lease_owner_id = NULL, lease_token = NULL,
                    lease_heartbeat_at = NULL, lease_expires_at = NULL
                WHERE run_id = ? AND tenant_id = ? AND status = ?
                    AND (cancel_requested = 0 OR ? = 1) AND lease_token = ?
                    AND lease_expires_at > ?
                """,
                (
                    RunStatus.FAILED.value,
                    error_code,
                    error,
                    completed_at,
                    completed_at,
                    run.run_id,
                    run.tenant_id,
                    RunStatus.RUNNING.value,
                    int(allow_cancel_requested),
                    lease_token,
                    now_ms,
                ),
            )
            if cursor.rowcount != 1:
                return self._classify_commit_outcome(
                    connection,
                    run.run_id,
                    tenant_id=run.tenant_id,
                    lease_token=lease_token,
                    now_ms=now_ms,
                )
            payload: dict[str, Any] = {"error_code": error_code, "error": error}
            if traceback_text is not None:
                payload["traceback"] = traceback_text
            self._append_event_with_connection(
                connection,
                run.run_id,
                "run.failed",
                payload,
            )

        run.status = RunStatus.FAILED
        run.error_code = error_code
        run.error = error
        run.completed_at = completed_at
        run.updated_at = completed_at
        run.lease_owner_id = None
        run.lease_token = None
        run.lease_heartbeat_at = None
        run.lease_expires_at = None
        return RunCommitOutcome.COMMITTED

    def _classify_commit_outcome(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        tenant_id: str,
        lease_token: str,
        now_ms: int,
    ) -> RunCommitOutcome:
        row = connection.execute(
            """
            SELECT status, cancel_requested, lease_token, lease_expires_at
            FROM runs WHERE run_id = ? AND tenant_id = ?
            """,
            (run_id, tenant_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"Run not found: {run_id}")
        status = RunStatus(row["status"])
        if status.is_terminal:
            return RunCommitOutcome.ALREADY_TERMINAL
        current_lease = (
            row["lease_token"] == lease_token
            and row["lease_expires_at"] is not None
            and int(row["lease_expires_at"]) > now_ms
        )
        if not current_lease:
            return RunCommitOutcome.LEASE_LOST
        if bool(row["cancel_requested"]):
            return RunCommitOutcome.CANCEL_REQUESTED
        return RunCommitOutcome.NOT_ELIGIBLE

    def list_recoverable_runs(self) -> list[RunRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runs WHERE status IN (?, ?) ORDER BY created_at",
                (RunStatus.QUEUED.value, RunStatus.RUNNING.value),
            ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        """Reject unfenced writes to an existing managed Run event stream."""

        del event_type, payload
        raise RunLeaseLostError(
            f"Unfenced Run event append is prohibited: {run_id}"
        )

    def append_control_plane_event(
        self,
        run_id: str,
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        """Append one allowlisted API control-plane observation.

        These events describe a synchronous sandbox call made outside a
        managed attempt. They cannot encode lifecycle, checkpoint, workflow,
        or other attempt-owned evidence.
        """

        if event_type not in self._CONTROL_PLANE_EVENT_TYPES:
            raise ValueError(f"Unsupported control-plane Run event: {event_type}")
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT 1 FROM runs WHERE run_id = ? AND tenant_id = ?",
                (run_id, tenant_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Run not found: {run_id}")
            return self._append_event_with_connection(
                connection,
                run_id,
                event_type,
                payload,
            )

    @staticmethod
    def _append_event_with_connection(
        connection: sqlite3.Connection,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        """Append an event using a connection the caller already owns.

        Callers that need the event insert to participate in a larger
        transaction pass their own open connection. The sequence computation
        and insert always share that connection.
        """
        sequence = int(
            connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
        )
        event = RunEvent(
            run_id=run_id,
            sequence=sequence,
            event_type=event_type,
            payload=payload or {},
        )
        cursor = connection.execute(
            """
            INSERT INTO run_events (
                run_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event.run_id,
                event.sequence,
                event.event_type,
                json.dumps(event.payload),
                event.created_at,
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("SQLite did not return an event id")
        event.event_id = int(cursor.lastrowid)
        return event

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[RunEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM run_events
                WHERE run_id = ? AND sequence > ?
                ORDER BY sequence
                """,
                (run_id, after_sequence),
            ).fetchall()
        return self._rows_to_events(rows)

    def list_events_for_tenant(
        self,
        run_id: str,
        tenant_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RunEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event.* FROM run_events AS event
                INNER JOIN runs AS run ON run.run_id = event.run_id
                WHERE event.run_id = ? AND run.tenant_id = ? AND event.sequence > ?
                ORDER BY event.sequence
                """,
                (run_id, tenant_id, after_sequence),
            ).fetchall()
        return self._rows_to_events(rows)

    @staticmethod
    def _rows_to_events(rows: list[sqlite3.Row]) -> list[RunEvent]:
        return [
            RunEvent(
                event_id=row["event_id"],
                run_id=row["run_id"],
                sequence=row["sequence"],
                event_type=row["event_type"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def save_unmanaged_thread_state(
        self,
        state: BaseRuntimeState,
        *,
        tenant_id: str,
    ) -> None:
        """Persist state for the legacy synchronous, non-Run API only.

        Managed attempts must commit their checkpoint through a fenced Run
        terminalization method instead of this explicitly unmanaged surface.
        """

        self._remember_state_model(state)
        with self._lock, self._connect() as connection:
            self._assert_thread_schema_available(
                connection,
                tenant_id,
                state.thread_id,
                state.domain_id,
                state.schema_version,
            )
            connection.execute(
                """
                INSERT INTO thread_states (
                    tenant_id, thread_id, domain_id, schema_version, state_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, thread_id) DO UPDATE SET
                    domain_id = excluded.domain_id,
                    schema_version = excluded.schema_version,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    tenant_id,
                    state.thread_id,
                    state.domain_id,
                    state.schema_version,
                    state.model_dump_json(),
                    utc_now(),
                ),
            )

    def load_thread_state(
        self,
        thread_id: str,
        *,
        tenant_id: str,
        domain_id: str | None = None,
        schema_version: str | None = None,
    ) -> BaseRuntimeState | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT domain_id, schema_version, state_json FROM thread_states "
                "WHERE tenant_id = ? AND thread_id = ?",
                (tenant_id, thread_id),
            ).fetchone()
        if row is None:
            return None
        if domain_id is not None and row["domain_id"] != domain_id:
            raise ValueError(
                f"Thread {thread_id!r} belongs to domain {row['domain_id']!r}, not {domain_id!r}"
            )
        if schema_version is not None and row["schema_version"] != schema_version:
            raise ValueError(
                f"Thread {thread_id!r} uses schema {row['schema_version']!r}, "
                f"not {schema_version!r}"
            )
        return self._deserialize_state(
            row["domain_id"],
            row["schema_version"],
            row["state_json"],
        )

    def _run_values(self, run: RunRecord) -> tuple[Any, ...]:
        if run.state is not None:
            self._remember_state_model(run.state)
        return (
            run.run_id,
            run.tenant_id,
            (
                run.execution_authority.model_dump_json()
                if run.execution_authority is not None
                else None
            ),
            run.thread_id,
            run.agent_id,
            run.agent_version,
            run.domain_id,
            run.schema_version,
            run.status.value,
            self._legacy_input_message(run.input),
            json.dumps(run.input),
            run.state.model_dump_json() if run.state else None,
            run.output_message,
            json.dumps(run.validation_errors),
            run.error_code,
            run.error,
            run.attempt,
            int(run.cancel_requested),
            run.client_request_id,
            run.created_at,
            run.updated_at,
            run.started_at,
            run.completed_at,
            run.lease_owner_id,
            run.lease_token,
            run.lease_heartbeat_at,
            run.lease_expires_at,
        )

    def _row_to_run(self, row: sqlite3.Row) -> RunRecord:
        keys = set(row.keys())
        domain_id = row["domain_id"] if "domain_id" in keys else "travel"
        schema_version = row["schema_version"] if "schema_version" in keys else "1"
        input_payload = (
            json.loads(row["input_json"])
            if "input_json" in keys and row["input_json"]
            else {"user_message": row["input_message"]}
        )
        return RunRecord(
            run_id=row["run_id"],
            tenant_id=(row["tenant_id"] if "tenant_id" in keys else LEGACY_TENANT_ID),
            thread_id=row["thread_id"],
            agent_id=row["agent_id"],
            agent_version=row["agent_version"],
            domain_id=domain_id,
            schema_version=schema_version,
            status=RunStatus(row["status"]),
            input=input_payload,
            state=(
                self._deserialize_state(domain_id, schema_version, row["state_json"])
                if row["state_json"]
                else None
            ),
            output_message=row["output_message"],
            validation_errors=json.loads(row["validation_errors_json"]),
            error_code=(row["error_code"] if "error_code" in keys else None),
            error=row["error"],
            execution_authority=(
                json.loads(row["execution_authority_json"])
                if "execution_authority_json" in keys
                and row["execution_authority_json"]
                else None
            ),
            attempt=row["attempt"],
            cancel_requested=bool(row["cancel_requested"]),
            client_request_id=(
                row["client_request_id"] if "client_request_id" in keys else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            lease_owner_id=(row["lease_owner_id"] if "lease_owner_id" in keys else None),
            lease_token=(row["lease_token"] if "lease_token" in keys else None),
            lease_heartbeat_at=(
                row["lease_heartbeat_at"] if "lease_heartbeat_at" in keys else None
            ),
            lease_expires_at=(
                row["lease_expires_at"] if "lease_expires_at" in keys else None
            ),
        )

    @staticmethod
    def _legacy_input_message(payload: dict[str, Any] | None) -> str:
        if payload is None:
            return ""
        value = payload.get("user_message")
        return value if isinstance(value, str) else ""

    def _remember_state_model(self, state: BaseRuntimeState) -> None:
        self._state_models[(state.domain_id, state.schema_version)] = type(state)

    def _deserialize_state(
        self,
        domain_id: str,
        schema_version: str,
        state_json: str,
    ) -> BaseRuntimeState:
        payload = json.loads(state_json)
        if self._state_registry is not None:
            return self._state_registry.parse_state(domain_id, schema_version, payload)
        state_model = self._state_models.get((domain_id, schema_version))
        if state_model is None:
            raise RuntimeError(
                "A state registry is required to deserialize persisted state "
                f"for {domain_id}:{schema_version}"
            )
        return state_model.model_validate(payload)

    @staticmethod
    def _assert_thread_schema_available(
        connection: sqlite3.Connection,
        tenant_id: str,
        thread_id: str,
        domain_id: str,
        schema_version: str,
    ) -> None:
        existing = connection.execute(
            "SELECT domain_id, schema_version FROM thread_states "
            "WHERE tenant_id = ? AND thread_id = ?",
            (tenant_id, thread_id),
        ).fetchone()
        if existing is None:
            return
        if (
            existing["domain_id"] != domain_id
            or existing["schema_version"] != schema_version
        ):
            raise ValueError(
                f"Thread {thread_id!r} is already bound to "
                f"{existing['domain_id']}:{existing['schema_version']}"
            )
