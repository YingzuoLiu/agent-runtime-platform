from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Callable
from uuid import uuid4

from agent.contracts import utc_now

from .postgres_schema import (
    initialize_postgres_schema,
    open_postgres_connection,
    validate_postgres_schema_name,
)
from .sandbox import ToolRetryMode
from .store import RunLeaseLostError
from .workflow_store import (
    ClaimOutcome,
    ClaimResult,
    ExecutionClaimResult,
    ExecutionOutcome,
    ExternalActionDispatchOutcome,
    ExternalActionDispatchResult,
    ExternalActionPrepareOutcome,
    ExternalActionPrepareResult,
    ExternalActionRecord,
    ExternalActionRetryMode,
    ExternalActionStatus,
    StaleAttemptError,
    StaleDispatchError,
    StepReuseOutcome,
    StepReuseResult,
    ToolCallRecord,
    ToolCallStatus,
    WorkflowEvent,
    WorkflowExecutionRecord,
    WorkflowRunSnapshot,
    WorkflowStatus,
)


Row = Mapping[str, Any]


class PostgresWorkflowStore:
    """PostgreSQL implementation of the durable ``WorkflowStore`` contract.

    The store shares one PostgreSQL database/schema with ``PostgresRunStore``.
    Short database transactions, not process locks, arbitrate ownership. Text
    JSON and text timestamps are intentionally retained so plan/evidence hashes
    observe the same representation as the SQLite reference backend.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "agent_runtime",
        lease_clock_ms: Callable[[], int] | None = None,
        connect_timeout_seconds: float = 30,
        statement_timeout_seconds: float | None = None,
        lock_timeout_seconds: float | None = None,
        initialize: bool = True,
    ) -> None:
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self._dsn = dsn
        self.schema = validate_postgres_schema_name(schema)
        self._lease_clock_ms = lease_clock_ms
        self._connect_timeout_seconds = connect_timeout_seconds
        self._statement_timeout_seconds = statement_timeout_seconds
        self._lock_timeout_seconds = lock_timeout_seconds
        if initialize:
            initialize_postgres_schema(
                dsn,
                schema=self.schema,
                connect_timeout_seconds=connect_timeout_seconds,
                statement_timeout_seconds=statement_timeout_seconds,
                lock_timeout_seconds=lock_timeout_seconds,
            )

    def _connect(self):
        return open_postgres_connection(
            self._dsn,
            schema=self.schema,
            connect_timeout_seconds=self._connect_timeout_seconds,
            statement_timeout_seconds=self._statement_timeout_seconds,
            lock_timeout_seconds=self._lock_timeout_seconds,
        )

    def ping(self) -> None:
        connection = self._connect()
        try:
            connection.execute("SELECT 1").fetchone()
        finally:
            connection.close()

    def _lease_now_ms(self, connection) -> int:
        if self._lease_clock_ms is not None:
            return self._lease_clock_ms()
        row = connection.execute(
            """
            SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS now_ms
            """
        ).fetchone()
        assert row is not None
        return int(row["now_ms"])

    def _assert_current_run_lease(
        self,
        connection,
        run_id: str,
        *,
        lease_token: str | None,
    ) -> None:
        """Fence one managed Workflow mutation in the mutation transaction.

        The PostgreSQL schema always contains ``runs``. A workflow execution
        whose ``run_id`` has no Run row remains a supported standalone ledger,
        matching the SQLite protocol behavior. Once a Run row exists, it is
        locked and the same current unexpired lease predicate is required.
        """

        run = connection.execute(
            "SELECT status, lease_token, lease_expires_at FROM runs WHERE run_id = %s FOR UPDATE",
            (run_id,),
        ).fetchone()
        if run is None:
            if lease_token is None:
                return
            raise RunLeaseLostError(f"Run lease is not enforceable: {run_id}")
        if lease_token is None:
            raise RunLeaseLostError(f"Run lease is no longer current: {run_id}")
        now_ms = self._lease_now_ms(connection)
        if not (
            run["status"] == "running"
            and run["lease_token"] == lease_token
            and run["lease_expires_at"] is not None
            and int(run["lease_expires_at"]) > now_ms
        ):
            raise RunLeaseLostError(f"Run lease is no longer current: {run_id}")

    # ---- execution-level ----

    def create_or_get_execution(
        self,
        run_id: str,
        workflow_type: str,
        input_hash: str,
        *,
        lease_token: str | None = None,
    ) -> ExecutionClaimResult:
        now = utc_now()
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                existing = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                ).fetchone()
                if existing is not None:
                    return self._resolve_execution_conflict(
                        existing,
                        workflow_type,
                        input_hash,
                    )
                created = connection.execute(
                    """
                    INSERT INTO workflow_executions (
                        run_id, workflow_type, input_hash, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        run_id,
                        workflow_type,
                        input_hash,
                        WorkflowStatus.PENDING.value,
                        now,
                        now,
                    ),
                ).fetchone()
                if created is not None:
                    return ExecutionClaimResult(
                        outcome=ExecutionOutcome.CREATED,
                        execution=self._row_to_execution(created),
                    )
                existing = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                ).fetchone()
                if existing is None:
                    raise RuntimeError("Workflow execution conflict lost its durable row")
                return self._resolve_execution_conflict(
                    existing,
                    workflow_type,
                    input_hash,
                )
        finally:
            connection.close()

    @staticmethod
    def _resolve_execution_conflict(
        row: Row,
        workflow_type: str,
        input_hash: str,
    ) -> ExecutionClaimResult:
        record = PostgresWorkflowStore._row_to_execution(row)
        if record.workflow_type != workflow_type:
            return ExecutionClaimResult(
                outcome=ExecutionOutcome.WORKFLOW_TYPE_MISMATCH,
                execution=record,
            )
        if record.input_hash != input_hash:
            return ExecutionClaimResult(
                outcome=ExecutionOutcome.INPUT_MISMATCH,
                execution=record,
            )
        return ExecutionClaimResult(
            outcome=ExecutionOutcome.EXISTING,
            execution=record,
        )

    def get_execution(self, run_id: str) -> WorkflowExecutionRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM workflow_executions WHERE run_id = %s",
                (run_id,),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_execution(row) if row is not None else None

    def mark_running(
        self,
        run_id: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._transition_execution(
            run_id,
            from_status=WorkflowStatus.PENDING,
            to_status=WorkflowStatus.RUNNING,
            event_type="workflow.started",
            result_json=None,
            error_code=None,
            lease_token=lease_token,
        )

    def finalize_ready(
        self,
        run_id: str,
        result_json: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._transition_execution(
            run_id,
            from_status=WorkflowStatus.RUNNING,
            to_status=WorkflowStatus.READY,
            event_type="workflow.ready",
            result_json=result_json,
            error_code=None,
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
        return self._transition_execution(
            run_id,
            from_status=WorkflowStatus.RUNNING,
            to_status=WorkflowStatus.BLOCKED,
            event_type="workflow.blocked",
            result_json=result_json,
            error_code=error_code,
            lease_token=lease_token,
        )

    def finalize_failed(
        self,
        run_id: str,
        error_code: str,
        *,
        lease_token: str | None = None,
    ) -> WorkflowExecutionRecord:
        return self._transition_execution(
            run_id,
            from_status=WorkflowStatus.RUNNING,
            to_status=WorkflowStatus.FAILED,
            event_type="workflow.failed",
            result_json=None,
            error_code=error_code,
            lease_token=lease_token,
        )

    def _transition_execution(
        self,
        run_id: str,
        *,
        from_status: WorkflowStatus,
        to_status: WorkflowStatus,
        event_type: str,
        result_json: str | None,
        error_code: str | None,
        lease_token: str | None,
    ) -> WorkflowExecutionRecord:
        now = utc_now()
        completed_at = now if to_status.is_terminal else None
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                row = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"Execution not found: {run_id}")
                updated = connection.execute(
                    """
                    UPDATE workflow_executions SET
                        status = %s, result_json = %s, error_code = %s,
                        updated_at = %s, completed_at = %s
                    WHERE run_id = %s AND status = %s
                    RETURNING *
                    """,
                    (
                        to_status.value,
                        result_json,
                        error_code,
                        now,
                        completed_at,
                        run_id,
                        from_status.value,
                    ),
                ).fetchone()
                if updated is None:
                    return self._row_to_execution(row)
                self._append_event_with_connection(
                    connection,
                    run_id,
                    event_type,
                    {"error_code": error_code} if to_status.is_terminal else {},
                )
                return self._row_to_execution(updated)
        finally:
            connection.close()

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
        if source_run_id == target_run_id:
            raise ValueError("selective replay requires a different target run_id")
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    target_run_id,
                    lease_token=lease_token,
                )
                source_execution_row = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s FOR SHARE",
                    (source_run_id,),
                ).fetchone()
                target_execution_row = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (target_run_id,),
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
                if source_execution.status not in {
                    WorkflowStatus.READY,
                    WorkflowStatus.BLOCKED,
                }:
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
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR SHARE",
                    (source_run_id, step_id),
                ).fetchone()
                if source_row is None:
                    return StepReuseResult(outcome=StepReuseOutcome.NOT_REUSABLE)
                source_step = self._row_to_step(source_row)
                if (
                    source_step.status != ToolCallStatus.COMPLETED
                    or source_step.tool_name != tool_name
                    or source_step.input_hash != input_hash
                    or source_step.result_json is None
                ):
                    return StepReuseResult(
                        outcome=StepReuseOutcome.NOT_REUSABLE,
                        step=source_step,
                    )
                source_action = connection.execute(
                    "SELECT action_id FROM external_actions WHERE run_id = %s AND step_id = %s",
                    (source_run_id, step_id),
                ).fetchone()
                if source_action is not None:
                    return StepReuseResult(
                        outcome=StepReuseOutcome.NOT_REUSABLE,
                        step=source_step,
                    )
                target_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (target_run_id, step_id),
                ).fetchone()
                if target_row is not None:
                    target_step = self._row_to_step(target_row)
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
                copied = connection.execute(
                    """
                    INSERT INTO tool_calls (
                        call_id, run_id, step_id, tool_name, input_hash,
                        status, attempt_count, attempt_token, result_json,
                        error_code, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, 0, NULL, %s, NULL, %s, %s)
                    ON CONFLICT (run_id, step_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        f"call_{uuid4().hex}",
                        target_run_id,
                        step_id,
                        tool_name,
                        input_hash,
                        ToolCallStatus.COMPLETED.value,
                        source_step.result_json,
                        now,
                        now,
                    ),
                ).fetchone()
                if copied is None:
                    target_row = connection.execute(
                        "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                        (target_run_id, step_id),
                    ).fetchone()
                    if target_row is None:
                        raise RuntimeError("Replay conflict lost target tool row")
                    target_step = self._row_to_step(target_row)
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
                return StepReuseResult(
                    outcome=StepReuseOutcome.COPIED,
                    step=self._row_to_step(copied),
                )
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
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                execution = connection.execute(
                    "SELECT run_id FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                ).fetchone()
                if execution is None:
                    raise KeyError(f"Workflow execution not found: {run_id}")
                row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                ).fetchone()
                if row is None:
                    return self._claim_fresh(
                        connection,
                        run_id,
                        step_id,
                        tool_name,
                        input_hash,
                    )
                record = self._row_to_step(row)
                if record.tool_name != tool_name:
                    return ClaimResult(
                        outcome=ClaimOutcome.DEFINITION_MISMATCH,
                        step=record,
                    )
                if record.input_hash != input_hash:
                    return ClaimResult(
                        outcome=ClaimOutcome.INPUT_MISMATCH,
                        step=record,
                    )
                if record.status == ToolCallStatus.COMPLETED:
                    return ClaimResult(outcome=ClaimOutcome.CACHED, step=record)
                if record.status == ToolCallStatus.RUNNING:
                    return ClaimResult(
                        outcome=ClaimOutcome.ALREADY_RUNNING,
                        step=record,
                    )
                if record.attempt_count >= max_attempts:
                    return ClaimResult(
                        outcome=ClaimOutcome.ATTEMPTS_EXHAUSTED,
                        step=record,
                    )
                return self._claim_retry(
                    connection,
                    run_id,
                    step_id,
                    tool_name,
                    record,
                )
        finally:
            connection.close()

    def _claim_fresh(
        self,
        connection,
        run_id: str,
        step_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ClaimResult:
        now = utc_now()
        token = f"attempt_{uuid4().hex}"
        row = connection.execute(
            """
            INSERT INTO tool_calls (
                call_id, run_id, step_id, tool_name, input_hash,
                status, attempt_count, attempt_token, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (run_id, step_id) DO NOTHING
            RETURNING *
            """,
            (
                f"call_{uuid4().hex}",
                run_id,
                step_id,
                tool_name,
                input_hash,
                ToolCallStatus.RUNNING.value,
                token,
                now,
                now,
            ),
        ).fetchone()
        if row is None:
            competing = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                (run_id, step_id),
            ).fetchone()
            if competing is None:
                raise RuntimeError("Concurrent tool claim lost its durable row")
            record = self._row_to_step(competing)
            if record.tool_name != tool_name:
                return ClaimResult(
                    outcome=ClaimOutcome.DEFINITION_MISMATCH,
                    step=record,
                )
            if record.input_hash != input_hash:
                return ClaimResult(
                    outcome=ClaimOutcome.INPUT_MISMATCH,
                    step=record,
                )
            return ClaimResult(
                outcome=ClaimOutcome.ALREADY_RUNNING,
                step=record,
            )
        self._append_event_with_connection(
            connection,
            run_id,
            "step.claimed",
            {"step_id": step_id, "tool_name": tool_name, "attempt_count": 1},
        )
        return ClaimResult(
            outcome=ClaimOutcome.CLAIMED,
            step=self._row_to_step(row),
            attempt_token=token,
        )

    def _claim_retry(
        self,
        connection,
        run_id: str,
        step_id: str,
        tool_name: str,
        previous: ToolCallRecord,
    ) -> ClaimResult:
        now = utc_now()
        token = f"attempt_{uuid4().hex}"
        row = connection.execute(
            """
            UPDATE tool_calls SET
                status = %s, attempt_count = attempt_count + 1,
                attempt_token = %s, tool_name = %s,
                error_code = NULL, updated_at = %s
            WHERE run_id = %s AND step_id = %s AND status = %s
            RETURNING *
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
        ).fetchone()
        if row is None:
            current = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                (run_id, step_id),
            ).fetchone()
            if current is None:
                raise RuntimeError("Retry claim lost its durable tool row")
            return ClaimResult(
                outcome=ClaimOutcome.ALREADY_RUNNING,
                step=self._row_to_step(current),
            )
        record = self._row_to_step(row)
        self._append_event_with_connection(
            connection,
            run_id,
            "step.claimed",
            {
                "step_id": step_id,
                "tool_name": tool_name,
                "attempt_count": previous.attempt_count + 1,
            },
        )
        return ClaimResult(
            outcome=ClaimOutcome.CLAIMED,
            step=record,
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
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                self._lock_execution_for_events(connection, run_id)
                row = connection.execute(
                    """
                    UPDATE tool_calls SET
                        status = %s, result_json = %s, error_code = %s, updated_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                        AND attempt_token = %s
                    RETURNING *
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
                ).fetchone()
                if row is None:
                    raise StaleAttemptError(
                        f"attempt_token does not currently hold {run_id}/{step_id}; "
                        "a newer attempt has already superseded it"
                    )
                record = self._row_to_step(row)
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
        finally:
            connection.close()

    def recover_interrupted_step(
        self,
        run_id: str,
        step_id: str,
        *,
        lease_token: str | None = None,
    ) -> ToolCallRecord:
        now = utc_now()
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                self._lock_execution_for_events(connection, run_id)
                updated = connection.execute(
                    """
                    UPDATE tool_calls SET status = %s, error_code = %s, updated_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                    RETURNING *
                    """,
                    (
                        ToolCallStatus.FAILED.value,
                        "interrupted",
                        now,
                        run_id,
                        step_id,
                        ToolCallStatus.RUNNING.value,
                    ),
                ).fetchone()
                if updated is not None:
                    self._append_event_with_connection(
                        connection,
                        run_id,
                        "step.interrupted_recovery",
                        {"step_id": step_id},
                    )
                    return self._row_to_step(updated)
                row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                ).fetchone()
                if row is None:
                    raise KeyError(f"Step not found: {run_id}/{step_id}")
                return self._row_to_step(row)
        finally:
            connection.close()

    def get_step(self, run_id: str, step_id: str) -> ToolCallRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s",
                (run_id, step_id),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_step(row) if row is not None else None

    def list_steps(self, run_id: str) -> list[ToolCallRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM tool_calls WHERE run_id = %s ORDER BY created_at, call_id",
                (run_id,),
            ).fetchall()
        finally:
            connection.close()
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
        required = (
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
        if not all(isinstance(value, str) and value for value in required):
            raise ValueError("External action identity fields must be non-empty strings")

        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                run_row = connection.execute(
                    "SELECT tenant_id, execution_authority_json FROM runs WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
                if run_row is None:
                    raise KeyError(f"Run not found: {run_id}")
                try:
                    authority = json.loads(run_row["execution_authority_json"])
                except (TypeError, json.JSONDecodeError):
                    authority = None
                if (
                    run_row["tenant_id"] != tenant_id
                    or not isinstance(authority, dict)
                    or authority.get("tenant_id") != tenant_id
                    or authority.get("subject_id") != subject_id
                ):
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                    )
                execution = connection.execute(
                    "SELECT workflow_type FROM workflow_executions WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                ).fetchone()
                if execution is None:
                    raise KeyError(f"Workflow execution not found: {run_id}")
                step_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                ).fetchone()
                if step_row is None:
                    raise KeyError(f"Step not found: {run_id}/{step_id}")
                step = self._row_to_step(step_row)
                if (
                    execution["workflow_type"] != workflow_type
                    or step.tool_name != tool_name
                    or step.input_hash != input_hash
                ):
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                    )
                if (
                    step.status != ToolCallStatus.RUNNING
                    or step.attempt_token != tool_attempt_token
                ):
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.TOOL_ATTEMPT_MISMATCH
                    )
                existing_row = connection.execute(
                    "SELECT * FROM external_actions WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                ).fetchone()
                if existing_row is not None:
                    existing = self._row_to_external_action(existing_row)
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
                    "SELECT action_id FROM external_actions WHERE idempotency_key = %s FOR SHARE",
                    (idempotency_key,),
                ).fetchone()
                if key_owner is not None:
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                    )
                now = utc_now()
                action_id = f"action_{uuid4().hex}"
                inserted = connection.execute(
                    """
                    INSERT INTO external_actions (
                        action_id, run_id, step_id, tenant_id, subject_id,
                        workflow_type, tool_name, provider_name, provider_identity,
                        input_hash, arguments_json, retry_mode, idempotency_key, status,
                        dispatch_count, dispatch_token, provider_reference,
                        result_json, error_code, created_at, updated_at,
                        dispatched_at, finalized_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, 0, NULL, NULL, NULL, NULL, %s, %s, NULL, NULL
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        action_id,
                        run_id,
                        step_id,
                        tenant_id,
                        subject_id,
                        workflow_type,
                        tool_name,
                        provider_name,
                        provider_identity,
                        input_hash,
                        canonical_arguments,
                        retry.value,
                        idempotency_key,
                        ExternalActionStatus.PREPARED.value,
                        now,
                        now,
                    ),
                ).fetchone()
                if inserted is None:
                    existing_row = connection.execute(
                        "SELECT * FROM external_actions WHERE run_id = %s AND step_id = %s FOR UPDATE",
                        (run_id, step_id),
                    ).fetchone()
                    if existing_row is None:
                        return ExternalActionPrepareResult(
                            outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH
                        )
                    existing = self._row_to_external_action(existing_row)
                    if self._external_action_identity(existing) == identity_values:
                        return ExternalActionPrepareResult(
                            outcome=ExternalActionPrepareOutcome.EXISTING,
                            action=existing,
                        )
                    return ExternalActionPrepareResult(
                        outcome=ExternalActionPrepareOutcome.IDENTITY_MISMATCH,
                        action=existing,
                    )
                action = self._row_to_external_action(inserted)
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
                return ExternalActionPrepareResult(
                    outcome=ExternalActionPrepareOutcome.CREATED,
                    action=action,
                )
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
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                self._lock_execution_for_events(connection, run_id)
                row = self._require_external_action_row(
                    connection,
                    run_id,
                    step_id,
                    for_update=True,
                )
                action = self._row_to_external_action(row)
                if action.status.is_terminal:
                    return ExternalActionDispatchResult(
                        outcome=ExternalActionDispatchOutcome.TERMINAL,
                        action=action,
                    )
                if action.status == ExternalActionStatus.DISPATCHING:
                    return ExternalActionDispatchResult(
                        outcome=ExternalActionDispatchOutcome.ALREADY_DISPATCHING,
                        action=action,
                        dispatch_token=action.dispatch_token,
                    )
                run_row = connection.execute(
                    "SELECT status, cancel_requested FROM runs WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
                if run_row is None:
                    raise KeyError(f"Run not found: {run_id}")
                if run_row["status"] != "running" or bool(run_row["cancel_requested"]):
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
                updated = connection.execute(
                    """
                    UPDATE external_actions SET
                        status = %s, dispatch_count = dispatch_count + 1,
                        dispatch_token = %s, updated_at = %s, dispatched_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                    RETURNING *
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
                ).fetchone()
                if updated is None:
                    raise StaleDispatchError(
                        f"External action changed before dispatch: {run_id}/{step_id}"
                    )
                refreshed = self._row_to_external_action(updated)
                self._append_dispatch_event(connection, refreshed, retry=False)
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.CLAIMED,
                    action=refreshed,
                    dispatch_token=token,
                )
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
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
                    connection,
                    run_id,
                    lease_token=lease_token,
                )
                self._lock_execution_for_events(connection, run_id)
                row = self._require_external_action_row(
                    connection,
                    run_id,
                    step_id,
                    for_update=True,
                )
                action = self._row_to_external_action(row)
                if action.status.is_terminal:
                    return ExternalActionDispatchResult(
                        outcome=ExternalActionDispatchOutcome.TERMINAL,
                        action=action,
                    )
                if action.status != ExternalActionStatus.DISPATCHING:
                    return ExternalActionDispatchResult(
                        outcome=ExternalActionDispatchOutcome.NOT_DISPATCHING,
                        action=action,
                    )
                if action.retry_mode == ToolRetryMode.UNSAFE:
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
                updated = connection.execute(
                    """
                    UPDATE external_actions SET
                        dispatch_count = dispatch_count + 1,
                        dispatch_token = %s, updated_at = %s, dispatched_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                        AND dispatch_token = %s
                    RETURNING *
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
                ).fetchone()
                if updated is None:
                    raise StaleDispatchError(
                        f"dispatch_token does not currently hold {run_id}/{step_id}"
                    )
                refreshed = self._row_to_external_action(updated)
                self._append_dispatch_event(connection, refreshed, retry=True)
                return ExternalActionDispatchResult(
                    outcome=ExternalActionDispatchOutcome.RETRY_CLAIMED,
                    action=refreshed,
                    dispatch_token=token,
                )
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
            required_retry_mode=ToolRetryMode.UNSAFE,
            lease_token=lease_token,
            require_run_lease=True,
        )

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
        required_retry_mode: ToolRetryMode | None = None,
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

        connection = self._connect()
        try:
            with connection.transaction():
                if require_run_lease:
                    self._assert_current_run_lease(
                        connection,
                        run_id,
                        lease_token=lease_token,
                    )
                # All terminal action writers lock the execution first. I9
                # operator repair uses the same order before locking action rows,
                # avoiding an action<->execution lock inversion.
                self._lock_execution_for_events(connection, run_id)
                action_row = self._require_external_action_row(
                    connection,
                    run_id,
                    step_id,
                    for_update=True,
                )
                action = self._row_to_external_action(action_row)
                if (
                    required_retry_mode is not None
                    and action.retry_mode != required_retry_mode
                ):
                    raise ValueError(
                        f"External action {run_id}/{step_id} has retry mode "
                        f"{action.retry_mode.value!r}, not {required_retry_mode.value!r}"
                    )
                step_row = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
                    (run_id, step_id),
                ).fetchone()
                if step_row is None:
                    raise RuntimeError("External action lost its parent tool call")
                step = self._row_to_step(step_row)
                if action.status.is_terminal:
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
                if (
                    step.status != ToolCallStatus.RUNNING
                    or step.attempt_token != tool_attempt_token
                ):
                    raise StaleAttemptError(
                        f"attempt_token does not currently hold {run_id}/{step_id}; "
                        "a newer attempt has already superseded it"
                    )
                now = utc_now()
                updated_action = connection.execute(
                    """
                    UPDATE external_actions SET
                        status = %s, provider_reference = %s, result_json = %s,
                        error_code = %s, updated_at = %s, finalized_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                        AND dispatch_token = %s
                    RETURNING *
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
                ).fetchone()
                if updated_action is None:
                    raise StaleDispatchError(
                        f"dispatch_token does not currently hold {run_id}/{step_id}"
                    )
                updated_step = connection.execute(
                    """
                    UPDATE tool_calls SET
                        status = %s, result_json = %s, error_code = %s, updated_at = %s
                    WHERE run_id = %s AND step_id = %s AND status = %s
                        AND attempt_token = %s
                    RETURNING *
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
                ).fetchone()
                if updated_step is None:
                    raise StaleAttemptError(
                        f"attempt_token does not currently hold {run_id}/{step_id}; "
                        "a newer attempt has already superseded it"
                    )
                refreshed_action = self._row_to_external_action(updated_action)
                refreshed_step = self._row_to_step(updated_step)
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
                    (
                        "step.completed"
                        if tool_status == ToolCallStatus.COMPLETED
                        else "step.failed"
                    ),
                    {
                        "step_id": step_id,
                        "tool_name": refreshed_step.tool_name,
                        "attempt_count": refreshed_step.attempt_count,
                        "error_code": refreshed_step.error_code,
                        "outcome": (
                            "completed"
                            if tool_status == ToolCallStatus.COMPLETED
                            else "failed"
                        ),
                    },
                )
                return refreshed_action
        finally:
            connection.close()

    def get_external_action(
        self,
        run_id: str,
        step_id: str,
    ) -> ExternalActionRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = %s AND step_id = %s",
                (run_id, step_id),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_external_action(row) if row is not None else None

    def list_external_actions(self, run_id: str) -> list[ExternalActionRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM external_actions WHERE run_id = %s ORDER BY created_at, action_id",
                (run_id,),
            ).fetchall()
        finally:
            connection.close()
        return [self._row_to_external_action(row) for row in rows]

    def has_external_action_requiring_reconciliation(self, run_id: str) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM external_actions WHERE run_id = %s AND dispatch_count > 0 LIMIT 1",
                (run_id,),
            ).fetchone()
        finally:
            connection.close()
        return row is not None

    # ---- events ----

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        lease_token: str | None = None,
    ) -> WorkflowEvent:
        connection = self._connect()
        try:
            with connection.transaction():
                self._assert_current_run_lease(
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
        finally:
            connection.close()

    def list_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[WorkflowEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = %s AND sequence > %s
                ORDER BY sequence
                """,
                (run_id, after_sequence),
            ).fetchall()
        finally:
            connection.close()
        return [self._row_to_event(row) for row in rows]

    def read_run_snapshot(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> WorkflowRunSnapshot:
        connection = self._connect()
        try:
            with connection.transaction():
                connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                execution_row = connection.execute(
                    "SELECT * FROM workflow_executions WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
                step_rows = connection.execute(
                    "SELECT * FROM tool_calls WHERE run_id = %s ORDER BY created_at, call_id",
                    (run_id,),
                ).fetchall()
                action_rows = connection.execute(
                    "SELECT * FROM external_actions WHERE run_id = %s ORDER BY created_at, action_id",
                    (run_id,),
                ).fetchall()
                event_rows = connection.execute(
                    """
                    SELECT * FROM workflow_events
                    WHERE run_id = %s AND sequence > %s
                    ORDER BY sequence
                    """,
                    (run_id, after_sequence),
                ).fetchall()
        finally:
            connection.close()
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
            events=[self._row_to_event(row) for row in event_rows],
        )

    def _lock_execution_for_events(self, connection, run_id: str) -> None:
        row = connection.execute(
            "SELECT run_id FROM workflow_executions WHERE run_id = %s FOR UPDATE",
            (run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Workflow execution not found: {run_id}")

    def _append_event_with_connection(
        self,
        connection,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> WorkflowEvent:
        # The parent execution row is the per-run event-stream mutex. PostgreSQL
        # identity values may have rollback gaps; logical sequence may not.
        self._lock_execution_for_events(connection, run_id)
        sequence_row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM workflow_events WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert sequence_row is not None
        event = WorkflowEvent(
            run_id=run_id,
            sequence=int(sequence_row["sequence"]),
            event_type=event_type,
            payload=payload or {},
        )
        row = connection.execute(
            """
            INSERT INTO workflow_events (
                run_id, sequence, event_type, payload_json, created_at
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING event_id
            """,
            (
                event.run_id,
                event.sequence,
                event.event_type,
                json.dumps(event.payload),
                event.created_at,
            ),
        ).fetchone()
        assert row is not None
        event.event_id = int(row["event_id"])
        return event

    def _append_dispatch_event(
        self,
        connection,
        action: ExternalActionRecord,
        *,
        retry: bool,
    ) -> None:
        self._append_event_with_connection(
            connection,
            action.run_id,
            "external_action.dispatch_started",
            {
                "evidence_id": f"action:{action.action_id}:dispatch:{action.dispatch_count}",
                "action_id": action.action_id,
                "step_id": action.step_id,
                "tool_name": action.tool_name,
                "provider_name": action.provider_name,
                "status": action.status.value,
                "dispatch_count": action.dispatch_count,
                "retry": retry,
            },
        )

    @staticmethod
    def _canonical_json_object(encoded: str) -> str:
        try:
            value = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise ValueError("External action JSON must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("External action JSON must encode an object")
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @staticmethod
    def _validate_error_code(error_code: str) -> None:
        if (
            not error_code
            or len(error_code) > 200
            or not error_code[0].isalpha()
            or any(
                not (character.isalnum() or character in "_.-")
                for character in error_code
            )
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

    def _require_external_action_row(
        self,
        connection,
        run_id: str,
        step_id: str,
        *,
        for_update: bool = False,
    ) -> Row:
        suffix = " FOR UPDATE" if for_update else ""
        row = connection.execute(
            "SELECT * FROM external_actions WHERE run_id = %s AND step_id = %s" + suffix,
            (run_id, step_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"External action not found: {run_id}/{step_id}")
        return row

    def _assert_current_tool_attempt(
        self,
        connection,
        run_id: str,
        step_id: str,
        attempt_token: str,
    ) -> ToolCallRecord:
        row = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = %s AND step_id = %s FOR UPDATE",
            (run_id, step_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"Step not found: {run_id}/{step_id}")
        step = self._row_to_step(row)
        if step.status != ToolCallStatus.RUNNING or step.attempt_token != attempt_token:
            raise StaleAttemptError(
                f"attempt_token does not currently hold {run_id}/{step_id}; "
                "a newer attempt has already superseded it"
            )
        return step

    # ---- row conversion ----

    @staticmethod
    def _row_to_execution(row: Row) -> WorkflowExecutionRecord:
        return WorkflowExecutionRecord(
            run_id=str(row["run_id"]),
            workflow_type=str(row["workflow_type"]),
            input_hash=str(row["input_hash"]),
            status=WorkflowStatus(row["status"]),
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _row_to_step(row: Row) -> ToolCallRecord:
        return ToolCallRecord(
            call_id=str(row["call_id"]),
            run_id=str(row["run_id"]),
            step_id=str(row["step_id"]),
            tool_name=str(row["tool_name"]),
            input_hash=str(row["input_hash"]),
            status=ToolCallStatus(row["status"]),
            attempt_count=int(row["attempt_count"]),
            attempt_token=row["attempt_token"],
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _row_to_external_action(row: Row) -> ExternalActionRecord:
        return ExternalActionRecord(
            action_id=str(row["action_id"]),
            run_id=str(row["run_id"]),
            step_id=str(row["step_id"]),
            tenant_id=str(row["tenant_id"]),
            subject_id=str(row["subject_id"]),
            workflow_type=str(row["workflow_type"]),
            tool_name=str(row["tool_name"]),
            provider_name=str(row["provider_name"]),
            provider_identity=str(row["provider_identity"]),
            input_hash=str(row["input_hash"]),
            arguments_json=str(row["arguments_json"]),
            retry_mode=ExternalActionRetryMode(row["retry_mode"]),
            idempotency_key=str(row["idempotency_key"]),
            status=ExternalActionStatus(row["status"]),
            dispatch_count=int(row["dispatch_count"]),
            dispatch_token=row["dispatch_token"],
            provider_reference=row["provider_reference"],
            result_json=row["result_json"],
            error_code=row["error_code"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            dispatched_at=row["dispatched_at"],
            finalized_at=row["finalized_at"],
        )

    @staticmethod
    def _row_to_event(row: Row) -> WorkflowEvent:
        return WorkflowEvent(
            event_id=int(row["event_id"]),
            run_id=str(row["run_id"]),
            sequence=int(row["sequence"]),
            event_type=str(row["event_type"]),
            payload=json.loads(row["payload_json"]),
            created_at=str(row["created_at"]),
        )
