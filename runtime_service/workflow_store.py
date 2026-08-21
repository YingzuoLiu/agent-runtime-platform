from __future__ import annotations

import json
import sqlite3
import threading
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, Field

from agent.contracts import utc_now
from .sandbox import ToolRetryMode
from .store import RunLeaseLostError


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


class ExternalActionStatus(str, Enum):
    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    OUTCOME_UNKNOWN = "outcome_unknown"

    @property
    def is_terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.OUTCOME_UNKNOWN}


# Backward-compatible store-facing name while preserving one canonical retry
# contract across ToolSpec declarations, provider dispatch and persistence.
ExternalActionRetryMode: TypeAlias = ToolRetryMode


class ClaimOutcome(str, Enum):
    CLAIMED = "claimed"
    CACHED = "cached"
    ALREADY_RUNNING = "already_running"
    INPUT_MISMATCH = "input_mismatch"
    DEFINITION_MISMATCH = "definition_mismatch"
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


class ExternalActionPrepareOutcome(str, Enum):
    CREATED = "created"
    EXISTING = "existing"
    IDENTITY_MISMATCH = "identity_mismatch"
    TOOL_ATTEMPT_MISMATCH = "tool_attempt_mismatch"


class ExternalActionDispatchOutcome(str, Enum):
    CLAIMED = "claimed"
    RETRY_CLAIMED = "retry_claimed"
    ALREADY_DISPATCHING = "already_dispatching"
    RETRY_UNSAFE = "retry_unsafe"
    NOT_DISPATCHING = "not_dispatching"
    RUN_CANCELLED = "run_cancelled"
    TERMINAL = "terminal"


class StaleAttemptError(Exception):
    """Raised when complete_step/fail_step target an attempt_token that no
    longer holds the row -- a newer attempt (a retry or an interrupted-step
    recovery) has already superseded it. The caller must discard its result
    rather than retry the write: silently succeeding here would let a dead
    attempt's output overwrite a live one's.
    """


class StaleDispatchError(Exception):
    """Raised when a provider result belongs to an obsolete dispatch token.

    A provider-idempotent action may be redispatched with the same stable
    idempotency key after a restart or ambiguous transport result. Each
    dispatch receives a fresh token so a late response from an older dispatch
    cannot overwrite evidence from the current one.
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


class ExternalActionRecord(BaseModel):
    action_id: str
    run_id: str
    step_id: str
    tenant_id: str
    subject_id: str
    workflow_type: str
    tool_name: str
    provider_name: str
    provider_identity: str
    input_hash: str
    arguments_json: str
    retry_mode: ExternalActionRetryMode
    idempotency_key: str
    status: ExternalActionStatus
    dispatch_count: int
    dispatch_token: str | None = None
    provider_reference: str | None = None
    result_json: str | None = None
    error_code: str | None = None
    created_at: str
    updated_at: str
    dispatched_at: str | None = None
    finalized_at: str | None = None


class WorkflowEvent(BaseModel):
    event_id: int | None = None
    run_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)


class WorkflowRunSnapshot(BaseModel):
    """One read-transaction view used by compound public projections."""

    execution: WorkflowExecutionRecord | None = None
    steps: list[ToolCallRecord] = Field(default_factory=list)
    external_actions: list[ExternalActionRecord] = Field(default_factory=list)
    events: list[WorkflowEvent] = Field(default_factory=list)


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


class ExternalActionPrepareResult(BaseModel):
    outcome: ExternalActionPrepareOutcome
    action: ExternalActionRecord | None = None


class ExternalActionDispatchResult(BaseModel):
    outcome: ExternalActionDispatchOutcome
    action: ExternalActionRecord
    dispatch_token: str | None = None


class WorkflowStore(Protocol):
    """Structural persistence contract shared by durable workflow consumers.

    Implementations must preserve more than these Python signatures. The
    contract includes stable execution and step identity outcomes, attempt- and
    dispatch-token fencing, prepare-before-dispatch ordering, atomic
    state-transition/event commits, external-action/parent-tool finalization,
    cancellation arbitration before first dispatch, append-only per-run event
    ordering, and restart-visible committed writes.

    ``SQLiteWorkflowStore`` is the only implementation composed by this
    repository today. This protocol isolates runtime consumers from that
    concrete class; it does not by itself make another backend semantically
    interchangeable.
    """

    def ping(self) -> None:
        ...

    def create_or_get_execution(
        self,
        run_id: str,
        workflow_type: str,
        input_hash: str,
        *,
        lease_token: str | None = None,
    ) -> ExecutionClaimResult:
        ...

    def get_execution(self, run_id: str) -> WorkflowExecutionRecord | None:
        ...

    def mark_running(
        self,
        run_id: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        ...

    def finalize_ready(
        self,
        run_id: str,
        result_json: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        ...

    def finalize_blocked(
        self,
        run_id: str,
        result_json: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        ...

    def finalize_failed(
        self,
        run_id: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        ...

    def reuse_completed_step(
        self,
        source_run_id: str,
        target_run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
        *,
        lease_token: str | None = None,
    ) -> StepReuseResult:
        ...

    def claim_step(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
        *,
        max_attempts: int,
        lease_token: str | None = None,
    ) -> ClaimResult:
        ...

    def complete_step(
        self,
        run_id: str,
        step_id: str,
        attempt_token: str,
        result_json: str,
        *,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        ...

    def fail_step(
        self,
        run_id: str,
        step_id: str,
        attempt_token: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        ...

    def recover_interrupted_step(
        self,
        run_id: str,
        step_id: str,
        *,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        ...

    def get_step(
        self,
        run_id: str,
        step_id: str,
    ) -> ToolCallRecord | None:
        ...

    def list_steps(self, run_id: str) -> list[ToolCallRecord]:
        ...

    def prepare_external_action(
        self,
        *,
        run_id: str,
        step_id: str,
        tool_attempt_token: str,
        tenant_id: str,
        subject_id: str,
        workflow_type: str,
        tool_name: str,
        provider_name: str,
        provider_identity: str,
        input_hash: str,
        arguments_json: str,
        retry_mode: ExternalActionRetryMode | str,
        idempotency_key: str,
        lease_token: str | None = None,
    ) -> ExternalActionPrepareResult:
        ...

    def begin_external_action_dispatch(
        self,
        run_id: str,
        step_id: str,
        *,
        tool_attempt_token: str,
        lease_token: str | None = None,
    ) -> ExternalActionDispatchResult:
        ...

    def retry_external_action_dispatch(
        self,
        run_id: str,
        step_id: str,
        *,
        previous_dispatch_token: str,
        tool_attempt_token: str,
        lease_token: str | None = None,
    ) -> ExternalActionDispatchResult:
        ...

    def finalize_external_action_succeeded(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        result_json: str,
        provider_reference: str,
        error_code: str | None = None,
    ) -> ExternalActionRecord:
        ...

    def finalize_external_action_failed(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
    ) -> ExternalActionRecord:
        ...

    def finalize_external_action_outcome_unknown(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
    ) -> ExternalActionRecord:
        ...

    def finalize_external_action_reconciliation_unknown(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
        lease_token: str | None = None,
    ) -> ExternalActionRecord:
        ...

    def finalize_unsafe_interrupted_action(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str = "external_action_outcome_unknown",
        lease_token: str | None = None,
    ) -> ExternalActionRecord:
        ...

    def get_external_action(
        self,
        run_id: str,
        step_id: str,
    ) -> ExternalActionRecord | None:
        ...

    def list_external_actions(self, run_id: str) -> list[ExternalActionRecord]:
        ...

    def has_external_action_requiring_reconciliation(self, run_id: str) -> bool:
        ...

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        lease_token: str | None = None,
    ) -> WorkflowEvent:
        ...

    def list_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[WorkflowEvent]:
        ...

    def read_run_snapshot(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> WorkflowRunSnapshot:
        ...


class SQLiteWorkflowStore:
    """Durable persistence for a multi-step registered-tool
    workflow: one `workflow_executions` row per run, one `tool_calls` row
    per step, at most one `external_actions` row per side-effecting step,
    and an append-only `workflow_events` log.

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

                CREATE TABLE IF NOT EXISTS external_actions (
                    action_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    workflow_type TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    provider_identity TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    arguments_json TEXT NOT NULL,
                    retry_mode TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    dispatch_count INTEGER NOT NULL DEFAULT 0,
                    dispatch_token TEXT,
                    provider_reference TEXT,
                    result_json TEXT,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    dispatched_at TEXT,
                    finalized_at TEXT,
                    UNIQUE(run_id, step_id),
                    FOREIGN KEY(run_id, step_id)
                        REFERENCES tool_calls(run_id, step_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_external_actions_run_id
                    ON external_actions(run_id);

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
        self,
        run_id: str,
        workflow_type: str,
        input_hash: str,
        *,
        lease_token: str | None = None,
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
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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

    def mark_running(
        self,
        run_id: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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

    def finalize_ready(
        self,
        run_id: str,
        result_json: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.READY,
            result_json=result_json,
            error_code=None,
            event_type="workflow.ready",
            lease_token=lease_token,
        )

    def finalize_blocked(
        self,
        run_id: str,
        result_json: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.BLOCKED,
            result_json=result_json,
            error_code=error_code,
            event_type="workflow.blocked",
            lease_token=lease_token,
        )

    def finalize_failed(
        self,
        run_id: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._finalize_execution(
            run_id,
            WorkflowStatus.FAILED,
            result_json=None,
            error_code=error_code,
            event_type="workflow.failed",
            lease_token=lease_token,
        )

    def _finalize_execution(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        result_json: str | None,
        error_code: str | None,
        event_type: str,
        lease_token: str | None,
    ) -> WorkflowExecutionRecord:
        """Atomically transition a RUNNING execution to a terminal status.

        Same compare-and-set discipline as `SQLiteRunStore.commit_completed_run`:
        the eligibility check (`status = 'running'`) and the terminal
        transition are the same UPDATE, and the describing event commits in
        the same transaction, so a reader can never observe the new status
        without the event already present. A second call for the same run
        is a no-op (rowcount 0, no duplicate event).
        """
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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
        *,
        lease_token: str | None = None,
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
            self._assert_current_run_lease(
                connection,
                target_run_id,
                lease_token=lease_token,
            )

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

            # A completed external action is evidence that a side effect
            # occurred for the source run. Copying only its tool result into a
            # different run would falsely make the target look as if it owned
            # that side effect, while copying the action itself would make two
            # runs share one provider idempotency identity. External actions
            # are therefore never eligible for selective replay.
            source_action = connection.execute(
                "SELECT action_id FROM external_actions WHERE run_id = ? AND step_id = ?",
                (source_run_id, step_id),
            ).fetchone()
            if source_action is not None:
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
        lease_token: str | None = None,
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
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )

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

            if record.tool_name != tool_name:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.DEFINITION_MISMATCH, step=record)
            if record.input_hash != input_hash:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.INPUT_MISMATCH, step=record)

            if record.status == ToolCallStatus.COMPLETED:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.CACHED, step=record)

            if record.status == ToolCallStatus.RUNNING:
                connection.rollback()
                return ClaimResult(outcome=ClaimOutcome.ALREADY_RUNNING, step=record)

            # Only FAILED remains: eligible for a fresh attempt unless the
            # attempt budget is exhausted. Tool and input identity were
            # already checked above for every persisted status.
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
            competing = SQLiteWorkflowStore._row_to_step(row)
            if competing.tool_name != tool_name:
                return ClaimResult(
                    outcome=ClaimOutcome.DEFINITION_MISMATCH,
                    step=competing,
                )
            if competing.input_hash != input_hash:
                return ClaimResult(
                    outcome=ClaimOutcome.INPUT_MISMATCH,
                    step=competing,
                )
            return ClaimResult(
                outcome=ClaimOutcome.ALREADY_RUNNING,
                step=competing,
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
        self,
        run_id: str,
        step_id: str,
        attempt_token: str,
        result_json: str,
        *,
        lease_token: str | None = None,
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
            lease_token=lease_token,
        )

    def fail_step(
        self,
        run_id: str,
        step_id: str,
        attempt_token: str,
        error_code: str,
        *,
        lease_token: str | None = None,
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
            lease_token=lease_token,
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
        lease_token: str | None,
    ) -> ToolCallRecord:
        now = utc_now()
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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

    def recover_interrupted_step(
        self,
        run_id: str,
        step_id: str,
        *,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
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
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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

    # ---- external actions ----

    def prepare_external_action(
        self,
        *,
        run_id: str,
        step_id: str,
        tool_attempt_token: str,
        tenant_id: str,
        subject_id: str,
        workflow_type: str,
        tool_name: str,
        provider_name: str,
        provider_identity: str,
        input_hash: str,
        arguments_json: str,
        retry_mode: ExternalActionRetryMode | str,
        idempotency_key: str,
        lease_token: str | None = None,
    ) -> ExternalActionPrepareResult:
        """Persist an exact external-write intent before provider dispatch.

        Preparation is create-or-get, but an existing `(run_id, step_id)` is
        reusable only when every authority, workflow, tool, provider, input,
        retry and idempotency field matches. The current tool attempt token is
        checked in the same `BEGIN IMMEDIATE` transaction so an obsolete
        claimant cannot prepare an action after a newer step attempt has taken
        ownership.
        """

        retry = ExternalActionRetryMode(retry_mode)
        canonical_arguments = self._canonical_json_object(arguments_json)
        identity_values = (
            tenant_id,
            subject_id,
            workflow_type,
            tool_name,
            provider_name,
            provider_identity,
            input_hash,
            canonical_arguments,
            retry,
            idempotency_key,
        )
        if not all(
            isinstance(value, str) and value
            for value in (
                run_id,
                step_id,
                tool_attempt_token,
                tenant_id,
                subject_id,
                workflow_type,
                tool_name,
                provider_name,
                provider_identity,
                input_hash,
                idempotency_key,
            )
        ):
            raise ValueError("External action identity fields must be non-empty strings")

        connection = self._immediate_connection()
        try:
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
            run_row = connection.execute(
                """
                SELECT tenant_id, execution_authority_json
                FROM runs WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if run_row is None:
                raise KeyError(f"Run not found: {run_id}")
            try:
                execution_authority = json.loads(
                    run_row["execution_authority_json"]
                )
            except (TypeError, json.JSONDecodeError):
                execution_authority = None
            if (
                run_row["tenant_id"] != tenant_id
                or not isinstance(execution_authority, dict)
                or execution_authority.get("tenant_id") != tenant_id
                or execution_authority.get("subject_id") != subject_id
            ):
                connection.rollback()
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                )
            execution_row = connection.execute(
                "SELECT workflow_type FROM workflow_executions WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if execution_row is None:
                raise KeyError(f"Workflow execution not found: {run_id}")
            step_row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if step_row is None:
                raise KeyError(f"Step not found: {run_id}/{step_id}")
            step = self._row_to_step(step_row)

            if (
                execution_row["workflow_type"] != workflow_type
                or step.tool_name != tool_name
                or step.input_hash != input_hash
            ):
                connection.rollback()
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                )
            if step.status != ToolCallStatus.RUNNING or step.attempt_token != tool_attempt_token:
                connection.rollback()
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.TOOL_ATTEMPT_MISMATCH
                )

            existing_row = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if existing_row is not None:
                existing = self._row_to_external_action(existing_row)
                connection.rollback()
                if self._external_action_identity(existing) == identity_values:
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.EXISTING,
                        action=existing,
                    )
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH,
                    action=existing,
                )

            key_owner = connection.execute(
                "SELECT action_id FROM external_actions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if key_owner is not None:
                connection.rollback()
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                )

            now = utc_now()
            action = ExternalActionRecord(
                action_id=f"action_{uuid4().hex}",
                run_id=run_id,
                step_id=step_id,
                tenant_id=tenant_id,
                subject_id=subject_id,
                workflow_type=workflow_type,
                tool_name=tool_name,
                provider_name=provider_name,
                provider_identity=provider_identity,
                input_hash=input_hash,
                arguments_json=canonical_arguments,
                retry_mode=retry,
                idempotency_key=idempotency_key,
                status=ExternalActionStatus.PREPARED,
                dispatch_count=0,
                created_at=now,
                updated_at=now,
            )
            connection.execute(
                """
                INSERT INTO external_actions (
                    action_id, run_id, step_id, tenant_id, subject_id,
                    workflow_type, tool_name, provider_name, provider_identity,
                    input_hash, arguments_json, retry_mode, idempotency_key, status,
                    dispatch_count, dispatch_token, provider_reference,
                    result_json, error_code, created_at, updated_at,
                    dispatched_at, finalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                          NULL, NULL, NULL, ?, ?, NULL, NULL)
                """,
                (
                    action.action_id,
                    action.run_id,
                    action.step_id,
                    action.tenant_id,
                    action.subject_id,
                    action.workflow_type,
                    action.tool_name,
                    action.provider_name,
                    action.provider_identity,
                    action.input_hash,
                    action.arguments_json,
                    action.retry_mode.value,
                    action.idempotency_key,
                    action.status.value,
                    action.dispatch_count,
                    action.created_at,
                    action.updated_at,
                ),
            )
            self._append_event_with_connection(
                connection,
                run_id,
                "external_action.prepared",
                {
                    "evidence_id": f"action:{action.action_id}:prepared",
                    "action_id": action.action_id,
                    "step_id": step_id,
                    "tool_name": tool_name,
                    "provider_name": provider_name,
                    "retry_mode": retry.value,
                    "status": action.status.value,
                },
            )
            connection.commit()
            return ExternalActionPrepareResult(
                outcome=ExternalActionPrepareOutcome.CREATED,
                action=action,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def begin_external_action_dispatch(
        self,
        run_id: str,
        step_id: str,
        *,
        tool_attempt_token: str,
        lease_token: str | None = None,
    ) -> ExternalActionDispatchResult:
        """Claim a PREPARED action for its first provider dispatch."""

        connection = self._immediate_connection()
        try:
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
            row = self._require_external_action_row(connection, run_id, step_id)
            action = self._row_to_external_action(row)
            if action.status.is_terminal:
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.TERMINAL,
                    action=action,
                )
            if action.status == ExternalActionStatus.DISPATCHING:
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.ALREADY_DISPATCHING,
                    action=action,
                    dispatch_token=action.dispatch_token,
                )
            run_row = connection.execute(
                "SELECT status, cancel_requested FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row is None:
                raise KeyError(f"Run not found: {run_id}")
            if run_row["status"] != "running" or bool(run_row["cancel_requested"]):
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.RUN_CANCELLED,
                    action=action,
                )
            self._assert_current_tool_attempt(
                connection,
                run_id,
                step_id,
                tool_attempt_token,
            )
            token = f"dispatch_{uuid4().hex}"
            now = utc_now()
            cursor = connection.execute(
                """
                UPDATE external_actions SET
                    status = ?, dispatch_count = dispatch_count + 1,
                    dispatch_token = ?, updated_at = ?, dispatched_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ?
                """,
                (
                    ExternalActionStatus.DISPATCHING.value,
                    token,
                    now,
                    now,
                    run_id,
                    step_id,
                    ExternalActionStatus.PREPARED.value,
                ),
            )
            if cursor.rowcount != 1:  # pragma: no cover - BEGIN IMMEDIATE serializes writers
                raise StaleDispatchError(
                    f"External action changed before dispatch: {run_id}/{step_id}"
                )
            refreshed = self._row_to_external_action(
                self._require_external_action_row(connection, run_id, step_id)
            )
            self._append_dispatch_event(connection, refreshed, retry=False)
            connection.commit()
            return ExternalActionDispatchResult(
                outcome=ExternalActionDispatchOutcome.CLAIMED,
                action=refreshed,
                dispatch_token=token,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def retry_external_action_dispatch(
        self,
        run_id: str,
        step_id: str,
        *,
        previous_dispatch_token: str,
        tool_attempt_token: str,
        lease_token: str | None = None,
    ) -> ExternalActionDispatchResult:
        """Rotate the dispatch token for a safely retryable in-flight action."""

        connection = self._immediate_connection()
        try:
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
            row = self._require_external_action_row(connection, run_id, step_id)
            action = self._row_to_external_action(row)
            if action.status.is_terminal:
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.TERMINAL,
                    action=action,
                )
            if action.status != ExternalActionStatus.DISPATCHING:
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.NOT_DISPATCHING,
                    action=action,
                )
            if action.retry_mode == ExternalActionRetryMode.UNSAFE:
                connection.rollback()
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.RETRY_UNSAFE,
                    action=action,
                    dispatch_token=action.dispatch_token,
                )
            if action.dispatch_token != previous_dispatch_token:
                raise StaleDispatchError(
                    f"dispatch_token does not currently hold {run_id}/{step_id}"
                )
            self._assert_current_tool_attempt(
                connection,
                run_id,
                step_id,
                tool_attempt_token,
            )
            token = f"dispatch_{uuid4().hex}"
            now = utc_now()
            cursor = connection.execute(
                """
                UPDATE external_actions SET
                    dispatch_count = dispatch_count + 1,
                    dispatch_token = ?, updated_at = ?, dispatched_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ?
                    AND dispatch_token = ?
                """,
                (
                    token,
                    now,
                    now,
                    run_id,
                    step_id,
                    ExternalActionStatus.DISPATCHING.value,
                    previous_dispatch_token,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleDispatchError(
                    f"dispatch_token does not currently hold {run_id}/{step_id}"
                )
            refreshed = self._row_to_external_action(
                self._require_external_action_row(connection, run_id, step_id)
            )
            self._append_dispatch_event(connection, refreshed, retry=True)
            connection.commit()
            return ExternalActionDispatchResult(
                outcome=ExternalActionDispatchOutcome.RETRY_CLAIMED,
                action=refreshed,
                dispatch_token=token,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def finalize_external_action_succeeded(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        result_json: str,
        provider_reference: str,
        error_code: str | None = None,
    ) -> ExternalActionRecord:
        return self._finalize_external_action(
            run_id,
            step_id,
            dispatch_token=dispatch_token,
            tool_attempt_token=tool_attempt_token,
            action_status=ExternalActionStatus.SUCCEEDED,
            tool_status=ToolCallStatus.COMPLETED,
            result_json=result_json,
            provider_reference=provider_reference,
            error_code=error_code,
        )

    def finalize_external_action_failed(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
    ) -> ExternalActionRecord:
        return self._finalize_external_action(
            run_id,
            step_id,
            dispatch_token=dispatch_token,
            tool_attempt_token=tool_attempt_token,
            action_status=ExternalActionStatus.FAILED,
            tool_status=ToolCallStatus.FAILED,
            result_json=None,
            provider_reference=provider_reference,
            error_code=error_code,
        )

    def finalize_external_action_outcome_unknown(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
    ) -> ExternalActionRecord:
        return self._finalize_external_action(
            run_id,
            step_id,
            dispatch_token=dispatch_token,
            tool_attempt_token=tool_attempt_token,
            action_status=ExternalActionStatus.OUTCOME_UNKNOWN,
            tool_status=ToolCallStatus.FAILED,
            result_json=None,
            provider_reference=provider_reference,
            error_code=error_code,
        )

    def finalize_external_action_reconciliation_unknown(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str,
        provider_reference: str | None = None,
        lease_token: str | None = None,
    ) -> ExternalActionRecord:
        """Close a dispatch from current-attempt reconciliation policy.

        Unlike a provider response, this is a local Runtime decision.  The
        dispatch and tool-attempt tokens still bind the affected rows, while
        the Run lease proves that this attempt may make the decision now.
        """

        return self._finalize_external_action(
            run_id,
            step_id,
            dispatch_token=dispatch_token,
            tool_attempt_token=tool_attempt_token,
            action_status=ExternalActionStatus.OUTCOME_UNKNOWN,
            tool_status=ToolCallStatus.FAILED,
            result_json=None,
            provider_reference=provider_reference,
            error_code=error_code,
            lease_token=lease_token,
            require_run_lease=True,
        )

    def finalize_unsafe_interrupted_action(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        error_code: str = "external_action_outcome_unknown",
        lease_token: str | None = None,
    ) -> ExternalActionRecord:
        """Fail closed after restart when an unsafe dispatch may have escaped."""

        return self._finalize_external_action(
            run_id,
            step_id,
            dispatch_token=dispatch_token,
            tool_attempt_token=tool_attempt_token,
            action_status=ExternalActionStatus.OUTCOME_UNKNOWN,
            tool_status=ToolCallStatus.FAILED,
            result_json=None,
            provider_reference=None,
            error_code=error_code,
            required_retry_mode=ExternalActionRetryMode.UNSAFE,
            lease_token=lease_token,
            require_run_lease=True,
        )

    def get_external_action(
        self,
        run_id: str,
        step_id: str,
    ) -> ExternalActionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
        return self._row_to_external_action(row) if row is not None else None

    def list_external_actions(self, run_id: str) -> list[ExternalActionRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = ? ORDER BY created_at",
                (run_id,),
            ).fetchall()
        return [self._row_to_external_action(row) for row in rows]

    def has_external_action_requiring_reconciliation(self, run_id: str) -> bool:
        """Return whether restart recovery must reconcile a dispatched write.

        A persisted cancellation may otherwise cause RuntimeManager to cancel a
        recovered run before the external-action loop resolves an in-flight
        request or mirrors an already-terminal provider outcome into run
        evidence. PREPARED rows are excluded because no dispatch can have
        occurred.
        """

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM external_actions
                WHERE run_id = ? AND dispatch_count > 0
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return row is not None

    def _finalize_external_action(
        self,
        run_id: str,
        step_id: str,
        *,
        dispatch_token: str,
        tool_attempt_token: str,
        action_status: ExternalActionStatus,
        tool_status: ToolCallStatus,
        result_json: str | None,
        provider_reference: str | None,
        error_code: str | None,
        required_retry_mode: ExternalActionRetryMode | None = None,
        lease_token: str | None = None,
        require_run_lease: bool = False,
    ) -> ExternalActionRecord:
        if action_status not in {
            ExternalActionStatus.SUCCEEDED,
            ExternalActionStatus.FAILED,
            ExternalActionStatus.OUTCOME_UNKNOWN,
        }:
            raise ValueError("External action final status must be terminal")
        if action_status == ExternalActionStatus.SUCCEEDED:
            if tool_status != ToolCallStatus.COMPLETED or result_json is None:
                raise ValueError("A succeeded external action requires a completed tool result")
            if not provider_reference:
                raise ValueError("A succeeded external action requires a provider reference")
            if error_code is not None:
                raise ValueError("A succeeded external action cannot carry an error code")
            canonical_result = self._canonical_json_object(result_json)
        else:
            if tool_status != ToolCallStatus.FAILED or not error_code:
                raise ValueError("A failed or unknown external action requires an error code")
            self._validate_error_code(error_code)
            canonical_result = None

        connection = self._immediate_connection()
        try:
            if require_run_lease:
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
            action_row = self._require_external_action_row(connection, run_id, step_id)
            action = self._row_to_external_action(action_row)
            if required_retry_mode is not None and action.retry_mode != required_retry_mode:
                raise ValueError(
                    f"External action {run_id}/{step_id} has retry mode "
                    f"{action.retry_mode.value!r}, not {required_retry_mode.value!r}"
                )
            if action.status.is_terminal:
                step_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                    (run_id, step_id),
                ).fetchone()
                if step_row is None:
                    raise RuntimeError("Terminal external action lost its parent tool call")
                step = self._row_to_step(step_row)
                exact_duplicate = (
                    action.status == action_status
                    and action.dispatch_token == dispatch_token
                    and action.provider_reference == provider_reference
                    and action.result_json == canonical_result
                    and action.error_code == error_code
                    and step.status == tool_status
                    and step.attempt_token == tool_attempt_token
                    and step.result_json == canonical_result
                    and step.error_code == error_code
                )
                connection.rollback()
                if exact_duplicate:
                    return action
                raise StaleDispatchError(
                    f"External action {run_id}/{step_id} already has a different terminal outcome"
                )
            if (
                action.status != ExternalActionStatus.DISPATCHING
                or action.dispatch_token != dispatch_token
            ):
                raise StaleDispatchError(
                    f"dispatch_token does not currently hold {run_id}/{step_id}"
                )
            self._assert_current_tool_attempt(
                connection,
                run_id,
                step_id,
                tool_attempt_token,
            )

            now = utc_now()
            action_cursor = connection.execute(
                """
                UPDATE external_actions SET
                    status = ?, provider_reference = ?, result_json = ?,
                    error_code = ?, updated_at = ?, finalized_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ?
                    AND dispatch_token = ?
                """,
                (
                    action_status.value,
                    provider_reference,
                    canonical_result,
                    error_code,
                    now,
                    now,
                    run_id,
                    step_id,
                    ExternalActionStatus.DISPATCHING.value,
                    dispatch_token,
                ),
            )
            if action_cursor.rowcount != 1:
                raise StaleDispatchError(
                    f"dispatch_token does not currently hold {run_id}/{step_id}"
                )
            tool_cursor = connection.execute(
                """
                UPDATE tool_calls SET
                    status = ?, result_json = ?, error_code = ?, updated_at = ?
                WHERE run_id = ? AND step_id = ? AND status = ?
                    AND attempt_token = ?
                """,
                (
                    tool_status.value,
                    canonical_result,
                    error_code,
                    now,
                    run_id,
                    step_id,
                    ToolCallStatus.RUNNING.value,
                    tool_attempt_token,
                ),
            )
            if tool_cursor.rowcount != 1:
                raise StaleAttemptError(
                    f"attempt_token does not currently hold {run_id}/{step_id}; "
                    "a newer attempt has already superseded it"
                )

            refreshed_action = self._row_to_external_action(
                self._require_external_action_row(connection, run_id, step_id)
            )
            step = self._row_to_step(
                connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
                    (run_id, step_id),
                ).fetchone()
            )
            self._append_event_with_connection(
                connection,
                run_id,
                f"external_action.{action_status.value}",
                {
                    "evidence_id": f"action:{action.action_id}:outcome",
                    "action_id": action.action_id,
                    "step_id": step_id,
                    "tool_name": action.tool_name,
                    "provider_name": action.provider_name,
                    "status": action_status.value,
                    "dispatch_count": action.dispatch_count,
                    "provider_reference": provider_reference,
                    "error_code": error_code,
                },
            )
            self._append_event_with_connection(
                connection,
                run_id,
                "step.completed" if tool_status == ToolCallStatus.COMPLETED else "step.failed",
                {
                    "step_id": step_id,
                    "tool_name": step.tool_name,
                    "attempt_count": step.attempt_count,
                    "error_code": step.error_code,
                    "outcome": (
                        "completed" if tool_status == ToolCallStatus.COMPLETED else "failed"
                    ),
                },
            )
            connection.commit()
            return refreshed_action
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _immediate_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        return connection

    @staticmethod
    def _assert_current_run_lease(
        connection: sqlite3.Connection,
        run_id: str,
        *,
        lease_token: str | None,
    ) -> None:
        """Fence a managed workflow mutation inside its write transaction.

        ``SQLiteWorkflowStore`` also supports standalone workflow ledgers used
        without ``SQLiteRunStore``.  Such a ledger has no matching ``runs`` row
        and therefore no Run authority to validate.  Once a matching Run row
        exists, however, every attempt-owned mutation must prove the current,
        unexpired token; a legacy or partially migrated row fails closed.
        """

        runs_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'runs'
            """
        ).fetchone()
        if runs_table is None:
            if lease_token is None:
                return
            raise RunLeaseLostError(f"Run lease is not enforceable: {run_id}")

        matching_run = connection.execute(
            "SELECT 1 FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if matching_run is None:
            if lease_token is None:
                return
            raise RunLeaseLostError(f"Run lease is not enforceable: {run_id}")

        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(runs)").fetchall()
        }
        if not {"status", "lease_token", "lease_expires_at"}.issubset(columns):
            raise RunLeaseLostError(f"Run lease is not enforceable: {run_id}")

        store_now = connection.execute(
            "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
        ).fetchone()
        assert store_now is not None
        current = connection.execute(
            """
            SELECT 1 FROM runs
            WHERE run_id = ? AND status = 'running' AND lease_token = ?
                AND lease_expires_at > ?
            """,
            (run_id, lease_token, int(store_now[0])),
        ).fetchone()
        if lease_token is None or current is None:
            raise RunLeaseLostError(f"Run lease is no longer current: {run_id}")

    @staticmethod
    def _canonical_json_object(encoded: str) -> str:
        try:
            value = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise ValueError("External action JSON must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("External action JSON must encode an object")
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _validate_error_code(error_code: str) -> None:
        if len(error_code) > 200 or not error_code[0].isalpha() or any(
            not (character.isalnum() or character in "_.-")
            for character in error_code
        ):
            raise ValueError("External action error_code must be a stable machine code")

    @staticmethod
    def _external_action_identity(
        action: ExternalActionRecord,
    ) -> tuple[str, str, str, str, str, str, str, str, ExternalActionRetryMode, str]:
        return (
            action.tenant_id,
            action.subject_id,
            action.workflow_type,
            action.tool_name,
            action.provider_name,
            action.provider_identity,
            action.input_hash,
            action.arguments_json,
            action.retry_mode,
            action.idempotency_key,
        )

    @staticmethod
    def _require_external_action_row(
        connection: sqlite3.Connection,
        run_id: str,
        step_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM external_actions WHERE run_id = ? AND step_id = ?",
            (run_id, step_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"External action not found: {run_id}/{step_id}")
        return row

    @staticmethod
    def _assert_current_tool_attempt(
        connection: sqlite3.Connection,
        run_id: str,
        step_id: str,
        attempt_token: str,
    ) -> ToolCallRecord:
        row = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = ? AND step_id = ?",
            (run_id, step_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"Step not found: {run_id}/{step_id}")
        step = SQLiteWorkflowStore._row_to_step(row)
        if step.status != ToolCallStatus.RUNNING or step.attempt_token != attempt_token:
            raise StaleAttemptError(
                f"attempt_token does not currently hold {run_id}/{step_id}; "
                "a newer attempt has already superseded it"
            )
        return step

    @staticmethod
    def _append_dispatch_event(
        connection: sqlite3.Connection,
        action: ExternalActionRecord,
        *,
        retry: bool,
    ) -> None:
        SQLiteWorkflowStore._append_event_with_connection(
            connection,
            action.run_id,
            "external_action.dispatch_started",
            {
                "evidence_id": (f"action:{action.action_id}:dispatch:{action.dispatch_count}"),
                "action_id": action.action_id,
                "step_id": action.step_id,
                "tool_name": action.tool_name,
                "provider_name": action.provider_name,
                "status": action.status.value,
                "dispatch_count": action.dispatch_count,
                "retry": retry,
            },
        )

    # ---- events ----

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        lease_token: str | None = None,
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
            connection.execute("BEGIN IMMEDIATE")
            self._assert_current_run_lease(
                connection,
                run_id,
                lease_token=lease_token,
            )
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

    def read_run_snapshot(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> WorkflowRunSnapshot:
        """Read all Action projection facts from one SQLite snapshot.

        External-action and parent-step finalization is one write transaction.
        A projector that reads those tables through separate connections can
        nevertheless combine a pre-commit step with a post-commit ledger row.
        An explicit read transaction keeps the compound view coherent.
        """

        with self._connect() as connection:
            connection.execute("BEGIN")
            execution_row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            step_rows = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = ? ORDER BY created_at",
                (run_id,),
            ).fetchall()
            action_rows = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = ? ORDER BY created_at",
                (run_id,),
            ).fetchall()
            event_rows = connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = ? AND sequence > ?
                ORDER BY sequence
                """,
                (run_id, after_sequence),
            ).fetchall()
            connection.commit()

        return WorkflowRunSnapshot(
            execution=(
                self._row_to_execution(execution_row)
                if execution_row is not None
                else None
            ),
            steps=[self._row_to_step(row) for row in step_rows],
            external_actions=[
                self._row_to_external_action(row) for row in action_rows
            ],
            events=[
                WorkflowEvent(
                    event_id=row["event_id"],
                    run_id=row["run_id"],
                    sequence=row["sequence"],
                    event_type=row["event_type"],
                    payload=json.loads(row["payload_json"]),
                    created_at=row["created_at"],
                )
                for row in event_rows
            ],
        )

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

    @staticmethod
    def _row_to_external_action(row: sqlite3.Row) -> ExternalActionRecord:
        return ExternalActionRecord(
            action_id=row["action_id"],
            run_id=row["run_id"],
            step_id=row["step_id"],
            tenant_id=row["tenant_id"],
            subject_id=row["subject_id"],
            workflow_type=row["workflow_type"],
            tool_name=row["tool_name"],
            provider_name=row["provider_name"],
            provider_identity=row["provider_identity"],
            input_hash=row["input_hash"],
            arguments_json=row["arguments_json"],
            retry_mode=ExternalActionRetryMode(row["retry_mode"]),
            idempotency_key=row["idempotency_key"],
            status=ExternalActionStatus(row["status"]),
            dispatch_count=row["dispatch_count"],
            dispatch_token=row["dispatch_token"],
            provider_reference=row["provider_reference"],
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            dispatched_at=row["dispatched_at"],
            finalized_at=row["finalized_at"],
        )
