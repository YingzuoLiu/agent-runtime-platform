from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from agent.contracts import utc_now
from domains.travel.state import AgentState
from .models import RunEvent, RunRecord, RunStatus


class SQLiteRunStore:
    """Durable run, event and thread-state storage backed by SQLite."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = str(database_path)
        self._lock = threading.RLock()
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
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

                CREATE INDEX IF NOT EXISTS idx_runs_thread_id ON runs(thread_id);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

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
                    thread_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    def create_run(self, run: RunRecord) -> RunRecord:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                    run_id, thread_id, agent_id, agent_version, status,
                    input_message, state_json, output_message,
                    validation_errors_json, error, attempt, cancel_requested,
                    client_request_id, created_at, updated_at, started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._run_values(run),
            )
        return run

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._row_to_run(row) if row else None

    def get_run_by_client_request_id(self, client_request_id: str) -> RunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE client_request_id = ?",
                (client_request_id,),
            ).fetchone()
        return self._row_to_run(row) if row else None

    def update_run(self, run: RunRecord) -> RunRecord:
        run.updated_at = utc_now()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs SET
                    thread_id = ?, agent_id = ?, agent_version = ?, status = ?,
                    input_message = ?, state_json = ?, output_message = ?,
                    validation_errors_json = ?, error = ?, attempt = ?,
                    cancel_requested = ?, client_request_id = ?, created_at = ?,
                    updated_at = ?, started_at = ?, completed_at = ?
                WHERE run_id = ?
                """,
                (*self._run_values(run)[1:], run.run_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Run not found: {run.run_id}")
        return run

    def request_cancel_atomically(self, run_id: str) -> RunRecord:
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
                WHERE run_id = ? AND status IN (?, ?) AND cancel_requested = 0
                """,
                (
                    utc_now(),
                    run_id,
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
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

    def finalize_completed_run(self, run: RunRecord) -> bool:
        """Atomically transition a RUNNING run to COMPLETED with its checkpoint.

        The eligibility check and the COMPLETED transition are the same
        statement: a single conditional `UPDATE ... WHERE run_id = ? AND
        status = 'running' AND cancel_requested = 0` acts as a compare-and-set.
        There is no separate read-then-decide step, so a concurrent cancel
        (which only ever sets `cancel_requested = 1` on non-terminal rows,
        see `request_cancel_atomically`) and a concurrent/duplicate finalize
        attempt cannot both believe they won.

        If the UPDATE does not affect exactly one row -- the run was already
        finalized, or a cancel committed first, or it is not currently
        RUNNING for any other reason -- nothing else in this method runs: no
        thread checkpoint, no `checkpoint.saved` event, no `run.completed`
        event. The caller must re-read the run and follow whatever terminal
        branch actually applies (typically cancellation).

        If the UPDATE succeeds, the thread-state checkpoint UPSERT and both
        describing events are written using the *same* connection/transaction
        as the UPDATE, so a reader can never observe `status == "completed"`
        without the checkpoint and `run.completed` event already committed
        alongside it, and calling this a second time for the same run is a
        no-op (the second UPDATE affects zero rows because status is no
        longer RUNNING).
        """
        completed_at = utc_now()
        state_json = run.state.model_dump_json() if run.state is not None else None
        validation_errors_json = json.dumps(run.validation_errors)

        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs SET
                    status = ?, state_json = ?, output_message = ?,
                    validation_errors_json = ?, completed_at = ?, updated_at = ?
                WHERE run_id = ? AND status = ? AND cancel_requested = 0
                """,
                (
                    RunStatus.COMPLETED.value,
                    state_json,
                    run.output_message,
                    validation_errors_json,
                    completed_at,
                    completed_at,
                    run.run_id,
                    RunStatus.RUNNING.value,
                ),
            )
            if cursor.rowcount != 1:
                return False

            if run.state is not None:
                connection.execute(
                    """
                    INSERT INTO thread_states (thread_id, state_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(thread_id) DO UPDATE SET
                        state_json = excluded.state_json,
                        updated_at = excluded.updated_at
                    """,
                    (run.thread_id, state_json, completed_at),
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
        run.completed_at = completed_at
        run.updated_at = completed_at
        return True

    def finalize_cancelled_run(self, run: RunRecord, *, reason: str) -> bool:
        """Atomically transition a cancel-requested QUEUED/RUNNING run to CANCELLED.

        Replaces the two separate commits `_mark_cancelled` used to issue
        (`update_run` then `append_event`) with a single compare-and-set,
        the same shape as `finalize_completed_run`. The eligibility check --
        `status IN ('queued', 'running') AND cancel_requested = 1` -- and
        the CANCELLED transition happen in the same UPDATE:

        - `status IN ('queued', 'running')` covers both call shapes this
          replaces: a run cancelled before it ever started running (still
          QUEUED) and a run cancelled at or after the execution boundary
          (still RUNNING, since only a successful `finalize_completed_run`
          ever moves a RUNNING row away from RUNNING).
        - `cancel_requested = 1` asserts the precondition every call site
          already relies on (each only calls this once it has confirmed
          `cancel_requested` is set); it also means a bare duplicate call
          before this method has flipped `status` away from QUEUED/RUNNING
          would still be caught if `cancel_requested` were ever 0 here,
          though that combination should not occur given the call sites.

        Every column other than `status`/`cancel_requested`/`completed_at`/
        `updated_at` is written exactly as it currently stands on `run`, so
        each existing call site's semantics survive unchanged: a
        before-start cancellation leaves the original request/thread-store
        state alone (the caller never overwrote `run.state`), while an
        after-execution-boundary cancellation carries over the
        just-computed result state (the caller set `run.state` to it before
        calling this) -- `output_message`/`validation_errors`/`error`/
        `attempt` follow whatever the caller set on `run`, matching
        `_mark_cancelled`'s previous full-row `update_run` semantics.

        If the UPDATE does not affect exactly one row -- already cancelled,
        already completed/failed by a concurrent finalize, or (defensively)
        not actually cancel-requested -- nothing else in this method runs:
        no `run.cancelled` event, no duplicate write, and `run` itself is
        left unmodified. Calling this a second time for the same run is a
        no-op for the same reason `finalize_completed_run` is.
        """
        completed_at = utc_now()
        state_json = run.state.model_dump_json() if run.state is not None else None
        validation_errors_json = json.dumps(run.validation_errors)

        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs SET
                    thread_id = ?, agent_id = ?, agent_version = ?, status = ?,
                    input_message = ?, state_json = ?, output_message = ?,
                    validation_errors_json = ?, error = ?, attempt = ?,
                    cancel_requested = ?, client_request_id = ?, created_at = ?,
                    updated_at = ?, started_at = ?, completed_at = ?
                WHERE run_id = ? AND status IN (?, ?) AND cancel_requested = 1
                """,
                (
                    run.thread_id,
                    run.agent_id,
                    run.agent_version,
                    RunStatus.CANCELLED.value,
                    run.input_message,
                    state_json,
                    run.output_message,
                    validation_errors_json,
                    run.error,
                    run.attempt,
                    1,
                    run.client_request_id,
                    run.created_at,
                    completed_at,
                    run.started_at,
                    completed_at,
                    run.run_id,
                    RunStatus.QUEUED.value,
                    RunStatus.RUNNING.value,
                ),
            )
            if cursor.rowcount != 1:
                return False

            self._append_event_with_connection(
                connection,
                run.run_id,
                "run.cancelled",
                {"reason": reason},
            )

        run.status = RunStatus.CANCELLED
        run.cancel_requested = True
        run.completed_at = completed_at
        run.updated_at = completed_at
        return True

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
        with self._lock, self._connect() as connection:
            return self._append_event_with_connection(connection, run_id, event_type, payload)

    @staticmethod
    def _append_event_with_connection(
        connection: sqlite3.Connection,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        """Append an event using a connection the caller already owns.

        Callers that need the event insert to participate in a larger
        transaction (see `finalize_completed_run`) pass their own open
        connection instead of going through `append_event`, which opens and
        commits its own connection. The sequence computation and the insert
        always share one connection, whether that connection is scoped here
        or by the caller.
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

    def save_thread_state(self, state: AgentState) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO thread_states (thread_id, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (state.thread_id, state.model_dump_json(), utc_now()),
            )

    def load_thread_state(self, thread_id: str) -> AgentState | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM thread_states WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        return AgentState.model_validate_json(row["state_json"]) if row else None

    @staticmethod
    def _run_values(run: RunRecord) -> tuple[Any, ...]:
        return (
            run.run_id,
            run.thread_id,
            run.agent_id,
            run.agent_version,
            run.status.value,
            run.input_message,
            run.state.model_dump_json() if run.state else None,
            run.output_message,
            json.dumps(run.validation_errors),
            run.error,
            run.attempt,
            int(run.cancel_requested),
            run.client_request_id,
            run.created_at,
            run.updated_at,
            run.started_at,
            run.completed_at,
        )

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> RunRecord:
        keys = set(row.keys())
        return RunRecord(
            run_id=row["run_id"],
            thread_id=row["thread_id"],
            agent_id=row["agent_id"],
            agent_version=row["agent_version"],
            status=RunStatus(row["status"]),
            input_message=row["input_message"],
            state=(
                AgentState.model_validate_json(row["state_json"])
                if row["state_json"]
                else None
            ),
            output_message=row["output_message"],
            validation_errors=json.loads(row["validation_errors_json"]),
            error=row["error"],
            attempt=row["attempt"],
            cancel_requested=bool(row["cancel_requested"]),
            client_request_id=(
                row["client_request_id"] if "client_request_id" in keys else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
        )
