from __future__ import annotations

import json
import sqlite3
import threading
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from agent.contracts import utc_now


class WorkflowStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    READY = "ready"
    BLOCKED = "blocked"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in {self.READY, self.BLOCKED, self.FAILED}


class ToolCallStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ClaimOutcome(str, Enum):
    CLAIMED = "claimed"
    CACHED = "cached"
    ALREADY_RUNNING = "already_running"
    INPUT_MISMATCH = "input_mismatch"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"


class ExecutionOutcome(str, Enum):
    CREATED = "created"
    EXISTING = "existing"
    INPUT_MISMATCH = "input_mismatch"
    WORKFLOW_TYPE_MISMATCH = "workflow_type_mismatch"


class StepReuseOutcome(str, Enum):
    COPIED = "copied"
    EXISTING = "existing"
    NOT_REUSABLE = "not_reusable"


class StaleAttemptError(Exception):
    """Raised when complete_step/fail_step target an attempt_token that no
    longer holds the row -- a newer attempt (a retry or an interrupted-step
    recovery) has already superseded it. The caller must discard its result
    rather than retry the write: silently succeeding here would let a dead
    attempt's output overwrite a live one's.
    """


class WorkflowExecutionRecord(BaseModel):
    run_id: str
    workflow_type: str
    input_hash: str
    status: WorkflowStatus
    result_json: str | None = None
    error_code: str | None = None
    created_at: str
    updated_at: str
    completed_at: str | None = None


class ToolCallRecord(BaseModel):
    call_id: str
    run_id: str
    step_id: str
    tool_name: str
    input_hash: str
    status: ToolCallStatus
    attempt_count: int
    attempt_token: str | None = None
    result_json: str | None = None
    error_code: str | None = None
    created_at: str
    updated_at: str


class WorkflowEvent(BaseModel):
    event_id: int | None = None
    run_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)


class ExecutionClaimResult(BaseModel):
    outcome: ExecutionOutcome
    execution: WorkflowExecutionRecord


class ClaimResult(BaseModel):
    outcome: ClaimOutcome
    step: ToolCallRecord | None = None
    attempt_token: str | None = None


class StepReuseResult(BaseModel):
    outcome: StepReuseOutcome
    step: ToolCallRecord | None = None


class SQLiteWorkflowStore:
    """Durable persistence for a multi-step registered-tool
    workflow: one `workflow_executions` row per run, one `tool_calls` row
    per step, and an append-only `workflow_events` log.

    This lives in the same physical SQLite file as `SQLiteRunStore` but
    owns a completely separate schema and does not import a domain state,
    `RunRecord`, or `RuntimeManager`. A domain workflow may use this store
    directly and expose an explicit adapter through the generic manager.

    Graph topology and replay policy remain caller-owned. The store provides
    atomic step claims plus a narrow primitive for copying compatible,
    completed evidence from a terminal source execution into a new run.
    """

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
                CREATE TABLE IF NOT EXISTS workflow_executions (
                    run_id TEXT PRIMARY KEY,
                    workflow_type TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS tool_calls (
                    call_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES workflow_executions(run_id) ON DELETE CASCADE,
                    step_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    attempt_token TEXT,
                    result_json TEXT,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, step_id)
                );

                CREATE INDEX IF NOT EXISTS idx_tool_calls_run_id ON tool_calls(run_id);

                CREATE TABLE IF NOT EXISTS workflow_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES workflow_executions(run_id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_id, sequence)
                );
                """
            )

    def ping(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    # ---- execution-level ----

    def create_or_get_execution(
        self, run_id: str, workflow_type: str, input_hash: str
    ) -> ExecutionClaimResult:
        """Create-or-get with explicit identity-mismatch reporting.

        A bare `INSERT ... ON CONFLICT DO NOTHING` would silently discard
        the caller's `workflow_type`/`input_hash` on a resubmitted
        `run_id` -- the exact class of bug this store exists to prevent at
        the step level. So a conflict always re-reads the existing row and
        compares explicitly instead of trusting the insert outcome alone.
        """
        now = utc_now()
        with self._lock, self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                return self._resolve_execution_conflict(existing, workflow_type, input_hash)
            try:
                connection.execute(
                    """
                    INSERT INTO workflow_executions (
                        run_id, workflow_type, input_hash, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, workflow_type, input_hash, WorkflowStatus.PENDING.value, now, now),
                )
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
                ).fetchone()
                return self._resolve_execution_conflict(row, workflow_type, input_hash)
            row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
            ).fetchone()
            return ExecutionClaimResult(
                outcome=ExecutionOutcome.CREATED,
                execution=self._row_to_execution(row),
            )

    @staticmethod
    def _resolve_execution_conflict(
        row: sqlite3.Row, workflow_type: str, input_hash: str
    ) -> ExecutionClaimResult:
        record = SQLiteWorkflowStore._row_to_execution(row)
        if record.workflow_type != workflow_type:
            return ExecutionClaimResult(
                outcome=ExecutionOutcome.WORKFLOW_TYPE_MISMATCH, execution=record
            )
        if record.input_hash != input_hash:
            return ExecutionClaimResult(
                outcome=ExecutionOutcome.INPUT_MISMATCH, execution=record
            )
        return ExecutionClaimResult(outcome=ExecutionOutcome.EXISTING, execution=record)

    def get_execution(self, run_id: str) -> WorkflowExecutionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._row_to_execution(row) if row else None

    def mark_running(self, run_id: str) -> WorkflowExecutionRecord:
        now = utc_now()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_executions SET status = ?, updated_at = ?
                WHERE run_id = ? AND status = ?
                """,
                (WorkflowStatus.RUNNING.value, now, run_id, WorkflowStatus.PENDING.value),
            )
            row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Execution not found: {run_id}")
            if cursor.rowcount == 1:
                self._append_event_with_connection(connection, run_id, "workflow.started", {})
        return self._row_to_execution(row)

    def finalize_ready(self, run_id: str, result_json: str) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.READY,
            result_json=result_json,
            error_code=None,
            event_type="workflow.ready",
        )

    def finalize_blocked(
        self, run_id: str, result_json: str, error_code: str
    ) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.BLOCKED,
            result_json=result_json,
            error_code=error_code,
            event_type="workflow.blocked",
        )

    def finalize_failed(self, run_id: str, error_code: str) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.FAILED,
            result_json=None,
            error_code=error_code,
            event_type="workflow.failed",
        )

    def _finalize_execution(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        result_json: str | None,
        error_code: str | None,
        event_type: str,
    ) -> WorkflowExecutionRecord:
        """Atomically transition a RUNNING execution to a terminal status.

        Same compare-and-set discipline as `SQLiteRunStore.finalize_completed_run`:
        the eligibility check (`status = 'running'`) and the terminal
        transition are the same UPDATE, and the describing event commits in
        the same transaction, so a reader can never observe the new status
        without the event already present. A second call for the same run
        is a no-op (rowcount 0, no duplicate event).
        """
        now = utc_now()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_executions SET
                    status = ?, result_json = ?, error_code = ?, updated_at = ?, completed_at = ?
                WHERE run_id = ? AND status = ?
                """,
                (
                    status.value,
                    result_json,
                    error_code,
                    now,
                    now,
                    run_id,
                    WorkflowStatus.RUNNING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Execution not found: {run_id}")
            if cursor.rowcount == 1:
                self._append_event_with_connection(
                    connection, run_id, event_type, {"error_code": error_code}
                )
        return self._row_to_execution(row)

    # ---- step-level ----

    def reuse_completed_step(
        self,
        source_run_id: str,
        target_run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
    ) -> StepReuseResult:
        """Copy compatible completed evidence into a different execution.

        The source execution must be READY or BLOCKED and use the same
        workflow type as the target. The copied target row records zero
        attempts because no tool ran in the target execution. A mismatch is
        reported to the caller, which owns DAG invalidation policy.
        """
        if source_run_id == target_run_id:
            raise ValueError("selective replay requires a different target run_id")

        connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")

            source_execution_row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (source_run_id,)
            ).fetchone()
            target_execution_row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?", (target_run_id,)
            ).fetchone()
            if source_execution_row is None:
                raise KeyError(f"Source workflow execution not found: {source_run_id}")
            if target_execution_row is None:
                raise KeyError(f"Target workflow execution not found: {target_run_id}")

            source_execution = self._row_to_execution(source_execution_row)
            target_execution = self._row_to_execution(target_execution_row)
            if source_execution.workflow_type != target_execution.workflow_type:
                raise ValueError(
                    "source and target workflow types do not match: "
                    f"{source_execution.workflow_type!r} != {target_execution.workflow_type!r}"
                )
            if source_execution.status not in {WorkflowStatus.READY, WorkflowStatus.BLOCKED}:
                raise ValueError(
                    f"source execution {source_run_id!r} is not replayable from status "
                    f"{source_execution.status.value!r}"
                )
            if target_execution.status != WorkflowStatus.RUNNING:
                raise ValueError(
                    f"target execution {target_run_id!r} must be RUNNING to accept replay "
                    f"evidence, not {target_execution.status.value!r}"
                )

            source_row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (source_run_id, step_id),
            ).fetchone()
            if source_row is None:
                connection.rollback()
                return StepReuseResult(outcome=StepReuseOutcome.NOT_REUSABLE)
            source_step = self._row_to_step(source_row)
            if (
                source_step.status != ToolCallStatus.COMPLETED
                or source_step.tool_name != tool_name
                or source_step.input_hash != input_hash
                or source_step.result_json is None
            ):
                connection.rollback()
                return StepReuseResult(
                    outcome=StepReuseOutcome.NOT_REUSABLE,
                    step=source_step,
                )

            target_row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (target_run_id, step_id),
            ).fetchone()
            if target_row is not None:
                target_step = self._row_to_step(target_row)
                connection.rollback()
                if (
                    target_step.status == ToolCallStatus.COMPLETED
                    and target_step.tool_name == tool_name
                    and target_step.input_hash == input_hash
                    and target_step.result_json is not None
                ):
                    return StepReuseResult(
                        outcome=StepReuseOutcome.EXISTING,
                        step=target_step,
                    )
                return StepReuseResult(
                    outcome=StepReuseOutcome.NOT_REUSABLE,
                    step=target_step,
                )

            now = utc_now()
            connection.execute(
                """
                INSERT INTO tool_calls (
                    call_id, run_id, step_id, tool_name, input_hash,
                    status, attempt_count, attempt_token, result_json,
                    error_code, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?)
                """,
                (
                    f"call_{uuid4().hex}",
                    target_run_id,
                    step_id,
                    tool_name,
                    input_hash,
                    ToolCallStatus.COMPLETED.value,
                    0,
                    source_step.result_json,
                    now,
                    now,
                ),
            )
            self._append_event_with_connection(
                connection,
                target_run_id,
                "step.replay_reused",
                {
                    "step_id": step_id,
                    "tool_name": tool_name,
                    "source_run_id": source_run_id,
                    "attempt_count": 0,
                    "outcome": "reused",
                },
            )
            copied_row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (target_run_id, step_id),
            ).fetchone()
            connection.commit()
            return StepReuseResult(
                outcome=StepReuseOutcome.COPIED,
                step=self._row_to_step(copied_row),
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def claim_step(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
        *,
        max_attempts: int,
    ) -> ClaimResult:
        """Exclusively claim a step for execution.

        Deliberately does not take `self._lock`. The point of this method
        is that two callers racing to claim the same brand-new
        `(run_id, step_id)` are serialized by SQLite itself -- `BEGIN
        IMMEDIATE` acquires the write lock before the read, so a second
        connection's `BEGIN IMMEDIATE` blocks until the first commits and
        then sees the row the first one created. An in-process mutex would
        make a two-thread test pass trivially without proving anything
        about a second OS process sharing the same database file.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")

            execution_row = connection.execute(
                "SELECT run_id FROM workflow_executions WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if execution_row is None:
                raise KeyError(f"Workflow execution not found: {run_id}")

            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()

            if row is None:
                result = self._claim_fresh(connection, run_id, step_id, tool_name, input_hash)
                connection.commit()
                return result

            record = self._row_to_step(row)

            if record.status == ToolCallStatus.COMPLETED:
                connection.rollback()
                if record.input_hash != input_hash:
                    return ClaimResult(outcome=ClaimOutcome.INPUT_MISMATCH, step=record)
                return ClaimResult(outcome=ClaimOutcome.CACHED, step=record)

            if record.status == ToolCallStatus.RUNNING:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.ALREADY_RUNNING, step=record)

            # Only FAILED remains: eligible for a fresh attempt unless the
            # input changed underneath it or attempts are exhausted.
            if record.input_hash != input_hash:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.INPUT_MISMATCH, step=record)
            if record.attempt_count >= max_attempts:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.ATTEMPTS_EXHAUSTED, step=record)

            result = self._claim_retry(connection, run_id, step_id, tool_name, record)
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _claim_fresh(
        connection: sqlite3.Connection,
        run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ClaimResult:
        now = utc_now()
        token = f"attempt_{uuid4().hex}"
        try:
            connection.execute(
                """
                INSERT INTO tool_calls (
                    call_id, run_id, step_id, tool_name, input_hash,
                    status, attempt_count, attempt_token, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"call_{uuid4().hex}",
                    run_id,
                    step_id,
                    tool_name,
                    input_hash,
                    ToolCallStatus.RUNNING.value,
                    1,
                    token,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            # Defense in depth only: the caller's `claim_step` has already
            # confirmed the parent execution row exists in this same
            # transaction, so the only remaining cause here is a genuine
            # concurrent UNIQUE(run_id, step_id) claim -- BEGIN IMMEDIATE
            # should make even that unreachable in practice (a competing
            # claimant would have blocked on its own BEGIN IMMEDIATE until
            # this transaction finished). One bounded re-read, no retry
            # loop. If no competing row is found, this is not a claim
            # race at all -- it is an internal inconsistency, and it must
            # not be papered over as ALREADY_RUNNING.
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise RuntimeError(
                    f"Unexpected IntegrityError claiming {run_id}/{step_id} with no "
                    "competing row found; expected a concurrent UNIQUE(run_id, step_id) claim"
                ) from None
            return ClaimResult(
                outcome=ClaimOutcome.ALREADY_RUNNING,
                step=SQLiteWorkflowStore._row_to_step(row),
            )

        SQLiteWorkflowStore._append_event_with_connection(
            connection,
            run_id,
            "step.claimed",
            {"step_id": step_id, "tool_name": tool_name, "attempt_count": 1},
        )
        row = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
            (run_id, step_id),
        ).fetchone()
        return ClaimResult(
            outcome=ClaimOutcome.CLAIMED,
            step=SQLiteWorkflowStore._row_to_step(row),
            attempt_token=token,
        )

    @staticmethod
    def _claim_retry(
        connection: sqlite3.Connection,
        run_id: str,
        step_id: str,
        tool_name: str,
        previous: ToolCallRecord,
    ) -> ClaimResult:
        now = utc_now()
        token = f"attempt_{uuid4().hex}"
        cursor = connection.execute(
            """
            UPDATE tool_calls SET
                status = ?, attempt_count = attempt_count + 1, attempt_token = ?,
                tool_name = ?, error_code = NULL, updated_at = ?
            WHERE run_id = ? AND step_id = ? AND status = ?
            """,
            (
                ToolCallStatus.RUNNING.value,
                token,
                tool_name,
                now,
                run_id,
                step_id,
                ToolCallStatus.FAILED.value,
            ),
        )
        if cursor.rowcount != 1:
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            return ClaimResult(
                outcome=ClaimOutcome.ALREADY_RUNNING,
                step=SQLiteWorkflowStore._row_to_step(row),
            )
        SQLiteWorkflowStore._append_event_with_connection(
            connection,
            run_id,
            "step.claimed",
            {
                "step_id": step_id,
                "tool_name": tool_name,
                "attempt_count": previous.attempt_count + 1,
            },
        )
        row = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
            (run_id, step_id),
        ).fetchone()
        return ClaimResult(
            outcome=ClaimOutcome.CLAIMED,
            step=SQLiteWorkflowStore._row_to_step(row),
            attempt_token=token,
        )

    def complete_step(
        self, run_id: str, step_id: str, attempt_token: str, result_json: str
    ) -> ToolCallRecord:
        return self._finalize_step(
            run_id,
            step_id,
            attempt_token,
            status=ToolCallStatus.COMPLETED,
            result_json=result_json,
            error_code=None,
            event_type="step.completed",
            outcome="completed",
        )

    def fail_step(
        self, run_id: str, step_id: str, attempt_token: str, error_code: str
    ) -> ToolCallRecord:
        return self._finalize_step(
            run_id,
            step_id,
            attempt_token,
            status=ToolCallStatus.FAILED,
            result_json=None,
            error_code=error_code,
            event_type="step.failed",
            outcome="failed",
        )

    def _finalize_step(
        self,
        run_id: str,
        step_id: str,
        attempt_token: str,
        *,
        status: ToolCallStatus,
        result_json: str | None,
        error_code: str | None,
        event_type: str,
        outcome: str,
    ) -> ToolCallRecord:
        now = utc_now()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tool_calls SET
                    status = ?, result_json = ?, error_code = ?, updated_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ? AND attempt_token = ?
                """,
                (
                    status.value,
                    result_json,
                    error_code,
                    now,
                    run_id,
                    step_id,
                    ToolCallStatus.RUNNING.value,
                    attempt_token,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleAttemptError(
                    f"attempt_token does not currently hold {run_id}/{step_id}; "
                    "a newer attempt has already superseded it"
                )
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            record = self._row_to_step(row)
            # tool_name/attempt_count come from the row just persisted by
            # the UPDATE above, in the same transaction, not from the
            # caller -- the event must describe the attempt that actually
            # got recorded, and must reflect it before any later claim can
            # move attempt_count further.
            self._append_event_with_connection(
                connection,
                run_id,
                event_type,
                {
                    "step_id": step_id,
                    "tool_name": record.tool_name,
                    "attempt_count": record.attempt_count,
                    "error_code": record.error_code,
                    "outcome": outcome,
                },
            )
        return record

    def recover_interrupted_step(self, run_id: str, step_id: str) -> ToolCallRecord:
        """Explicitly declare a RUNNING step abandoned.

        Never invoked automatically by `claim_step`: a caller must decide,
        out of band, that no live process still holds the current
        `attempt_token` before calling this (Phase 2A has no lease or
        heartbeat to detect that on its own). Flips RUNNING -> FAILED with
        `error_code="interrupted"`, which makes the step eligible for
        `claim_step` to hand out a fresh attempt on a subsequent call.
        """
        now = utc_now()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tool_calls SET status = ?, error_code = ?, updated_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ?
                """,
                (
                    ToolCallStatus.FAILED.value,
                    "interrupted",
                    now,
                    run_id,
                    step_id,
                    ToolCallStatus.RUNNING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Step not found: {run_id}/{step_id}")
            if cursor.rowcount == 1:
                self._append_event_with_connection(
                    connection, run_id, "step.interrupted_recovery", {"step_id": step_id}
                )
        return self._row_to_step(row)

    def get_step(self, run_id: str, step_id: str) -> ToolCallRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
        return self._row_to_step(row) if row else None

    def list_steps(self, run_id: str) -> list[ToolCallRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? ORDER BY created_at",
                (run_id,),
            ).fetchall()
        return [self._row_to_step(row) for row in rows]

    # ---- events ----

    def append_event(
        self, run_id: str, event_type: str, payload: dict[str, Any] | None = None
    ) -> WorkflowEvent:
        """Append a domain-level annotation event, e.g. a cache-reuse note.

        Every state-transition method above (`mark_running`,
        `finalize_*`, `claim_step`, `complete_step`, `fail_step`,
        `recover_interrupted_step`) already appends its own event
        atomically with its own state change; this is for a caller that
        needs to record something *without* an accompanying state
        transition (a read-only outcome like "this step's cached result
        was reused"). `run_id` must already have a `workflow_executions`
        row -- the same `ON DELETE CASCADE` foreign key as every other
        event insert applies here too.
        """
        with self._lock, self._connect() as connection:
            return self._append_event_with_connection(connection, run_id, event_type, payload)

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[WorkflowEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = ? AND sequence > ?
                ORDER BY sequence
                """,
                (run_id, after_sequence),
            ).fetchall()
        return [
            WorkflowEvent(
                event_id=row["event_id"],
                run_id=row["run_id"],
                sequence=row["sequence"],
                event_type=row["event_type"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    @staticmethod
    def _append_event_with_connection(
        connection: sqlite3.Connection,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> WorkflowEvent:
        sequence = int(
            connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM workflow_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
        )
        event = WorkflowEvent(
            run_id=run_id,
            sequence=sequence,
            event_type=event_type,
            payload=payload or {},
        )
        cursor = connection.execute(
            """
            INSERT INTO workflow_events (
                run_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (event.run_id, event.sequence, event.event_type, json.dumps(event.payload), event.created_at),
        )
        assert cursor.lastrowid is not None
        event.event_id = cursor.lastrowid
        return event

    # ---- row conversion ----

    @staticmethod
    def _row_to_execution(row: sqlite3.Row) -> WorkflowExecutionRecord:
        return WorkflowExecutionRecord(
            run_id=row["run_id"],
            workflow_type=row["workflow_type"],
            input_hash=row["input_hash"],
            status=WorkflowStatus(row["status"]),
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _row_to_step(row: sqlite3.Row) -> ToolCallRecord:
        return ToolCallRecord(
            call_id=row["call_id"],
            run_id=row["run_id"],
            step_id=row["step_id"],
            tool_name=row["tool_name"],
            input_hash=row["input_hash"],
            status=ToolCallStatus(row["status"]),
            attempt_count=row["attempt_count"],
            attempt_token=row["attempt_token"],
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
