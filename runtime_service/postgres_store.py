from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import uuid4

import psycopg

from agent.contracts import BaseRuntimeState, project_thread_checkpoint_state, utc_now

from .models import (
    RunCommitOutcome,
    RunEvent,
    RunLeaseClaim,
    RunLeaseRecoveryReason,
    RunRecord,
    RunStatus,
)
from .postgres_schema import (
    initialize_postgres_schema,
    open_postgres_connection,
    validate_postgres_schema_name,
)
from .quarantine import (
    ExternalActionStatusSummary,
    QuarantineResolutionCommit,
    QuarantineResolutionEvidenceIncompleteError,
    QuarantineResolutionKind,
    QuarantineResolutionPlan,
    QuarantineResolutionStalePlanError,
    QuarantineResolutionTarget,
    QuarantineTargetKind,
    QuarantineThreadReference,
)
from .store import (
    RunLeaseLostError,
    StateRegistry,
    THREAD_CHECKPOINT_CONFLICT_CODE,
    THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
    ThreadCheckpointRevisionConflictError,
    ThreadStateConflictError,
    ThreadStateSnapshot,
)


Row = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _QuarantinePlanSnapshot:
    plan: QuarantineResolutionPlan
    workflow_evidence_fingerprint: str
    checkpoint_evidence_fingerprint: str


class _CheckpointCASChanged(RuntimeError):
    pass


class PostgresRunStore:
    """Durable Run/event/checkpoint storage backed by PostgreSQL.

    The implementation preserves the SQLite reference representation and
    externally observable I1-I9 semantics while using PostgreSQL-native short
    row-locking transactions. Runtime execution never holds a database lock.
    """

    _CONTROL_PLANE_EVENT_TYPES = frozenset(
        {"sandbox.execution_started", "sandbox.execution_finished"}
    )
    _RUNNING_THREAD_INDEX = "idx_runs_one_running_per_thread"

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "agent_runtime",
        state_registry: StateRegistry | None = None,
        lease_operation_timeout_seconds: float = 1.0,
        lease_clock_ms: Callable[[], int] | None = None,
        connect_timeout_seconds: float = 30,
        statement_timeout_seconds: float | None = None,
        lock_timeout_seconds: float | None = None,
        idle_in_transaction_session_timeout_seconds: float = 5.0,
        initialize: bool = True,
    ) -> None:
        if lease_operation_timeout_seconds <= 0:
            raise ValueError("lease_operation_timeout_seconds must be positive")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if idle_in_transaction_session_timeout_seconds <= 0:
            raise ValueError(
                "idle_in_transaction_session_timeout_seconds must be positive"
            )
        if (
            idle_in_transaction_session_timeout_seconds
            <= lease_operation_timeout_seconds
        ):
            raise ValueError(
                "idle_in_transaction_session_timeout_seconds must be greater than "
                "lease_operation_timeout_seconds"
            )
        self._dsn = dsn
        self.schema = validate_postgres_schema_name(schema)
        self._state_registry = state_registry
        self._state_models: dict[tuple[str, str], type[BaseRuntimeState]] = {}
        self._lease_operation_timeout_seconds = lease_operation_timeout_seconds
        self._lease_clock_ms = lease_clock_ms
        self._connect_timeout_seconds = connect_timeout_seconds
        self._statement_timeout_seconds = statement_timeout_seconds
        self._lock_timeout_seconds = lock_timeout_seconds
        self._idle_in_transaction_session_timeout_seconds = (
            idle_in_transaction_session_timeout_seconds
        )
        if initialize:
            initialize_postgres_schema(
                dsn,
                schema=self.schema,
                connect_timeout_seconds=connect_timeout_seconds,
                statement_timeout_seconds=statement_timeout_seconds,
                lock_timeout_seconds=lock_timeout_seconds,
                idle_in_transaction_session_timeout_seconds=(
                    idle_in_transaction_session_timeout_seconds
                ),
            )

    def bind_state_registry(self, state_registry: StateRegistry) -> None:
        if self._state_registry is not None and self._state_registry is not state_registry:
            raise ValueError("PostgresRunStore is already bound to a different state registry")
        self._state_registry = state_registry

    @property
    def lease_operation_timeout_seconds(self) -> float:
        return self._lease_operation_timeout_seconds

    def _connect(self, *, timeout_seconds: float | None = None):
        connect_timeout = self._connect_timeout_seconds
        if timeout_seconds is not None:
            connect_timeout = min(connect_timeout, timeout_seconds)
        return open_postgres_connection(
            self._dsn,
            schema=self.schema,
            connect_timeout_seconds=connect_timeout,
            statement_timeout_seconds=self._statement_timeout_seconds,
            lock_timeout_seconds=self._lock_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                self._idle_in_transaction_session_timeout_seconds
            ),
        )

    def _lease_connect(self):
        return open_postgres_connection(
            self._dsn,
            schema=self.schema,
            connect_timeout_seconds=min(
                self._connect_timeout_seconds,
                self._lease_operation_timeout_seconds,
            ),
            statement_timeout_seconds=self._lease_operation_timeout_seconds,
            lock_timeout_seconds=self._lease_operation_timeout_seconds,
            idle_in_transaction_session_timeout_seconds=(
                self._idle_in_transaction_session_timeout_seconds
            ),
        )

    def _lease_now_ms(self, connection) -> int:
        """Read one transaction-stable PostgreSQL server timestamp in milliseconds."""

        if self._lease_clock_ms is not None:
            return self._lease_clock_ms()
        row = connection.execute(
            """
            SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS now_ms
            """
        ).fetchone()
        assert row is not None
        return int(row["now_ms"])

    def ping(self) -> None:
        connection = self._connect()
        try:
            connection.execute("SELECT 1").fetchone()
        finally:
            connection.close()

    # ---- Run creation and reads ----

    def create_run(self, run: RunRecord) -> RunRecord:
        self._assert_new_queued_run(run)
        connection = self._connect()
        try:
            with connection.transaction():
                self._insert_run_with_connection(connection, run)
                self._prepare_client_state_seed(connection, run)
            return run
        finally:
            connection.close()

    def create_run_with_event(
        self,
        run: RunRecord,
        *,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunRecord:
        self._assert_new_queued_run(run)
        if event_type != "run.queued":
            raise ValueError("A new Run's initial event must be run.queued")
        connection = self._connect()
        try:
            with connection.transaction():
                self._insert_run_with_connection(connection, run)
                self._prepare_client_state_seed(connection, run)
                self._append_event_with_connection(
                    connection,
                    run.run_id,
                    event_type,
                    payload,
                )
            return run
        finally:
            connection.close()

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
            or run.checkpoint_base_revision is not None
            or any(value is not None for value in lease_fields)
        ):
            raise ValueError(
                "New Runs must be pristine queued records without lease authority"
            )

    def _insert_run_with_connection(self, connection, run: RunRecord) -> None:
        connection.execute(
            """
            INSERT INTO runs (
                run_id, tenant_id, execution_authority_json, thread_id, agent_id,
                agent_version, domain_id, schema_version, status, input_message,
                input_json, state_json, output_message, validation_errors_json,
                error_code, error, attempt, cancel_requested, client_request_id,
                created_at, updated_at, started_at, completed_at, lease_owner_id,
                lease_token, lease_heartbeat_at, lease_expires_at,
                checkpoint_base_revision
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            self._run_values(run),
        )

    def _prepare_client_state_seed(self, connection, run: RunRecord) -> None:
        if run.state is None:
            return
        # There may be no checkpoint row to lock for a brand-new Thread. This
        # short transaction-scoped advisory lock is only the initial-seed mutex;
        # live execution ownership remains the Run lease + partial unique index.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"agent-runtime-thread-seed:{run.tenant_id}\0{run.thread_id}",),
        )
        checkpoint = connection.execute(
            "SELECT 1 FROM thread_states WHERE tenant_id = %s AND thread_id = %s",
            (run.tenant_id, run.thread_id),
        ).fetchone()
        nonterminal = connection.execute(
            """
            SELECT 1 FROM runs
            WHERE tenant_id = %s AND thread_id = %s AND run_id != %s
                AND status IN (%s, %s)
            LIMIT 1
            """,
            (
                run.tenant_id,
                run.thread_id,
                run.run_id,
                RunStatus.QUEUED.value,
                RunStatus.RUNNING.value,
            ),
        ).fetchone()
        if checkpoint is not None or nonterminal is not None:
            raise ThreadStateConflictError(
                "Client-provided state can only initialize an empty thread; "
                f"omit state to continue thread {run.thread_id!r}"
            )
        updated = connection.execute(
            "UPDATE runs SET checkpoint_base_revision = 0 WHERE run_id = %s RETURNING run_id",
            (run.run_id,),
        ).fetchone()
        if updated is None:
            raise RuntimeError("Client-state seed Run disappeared during creation")
        run.checkpoint_base_revision = 0

    def get_run_internal(self, run_id: str) -> RunRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_run(row) if row is not None else None

    def get_run_for_tenant(
        self,
        run_id: str,
        tenant_id: str,
        *,
        timeout_seconds: float = 30,
    ) -> RunRecord | None:
        connection = self._connect(timeout_seconds=timeout_seconds)
        try:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = %s AND tenant_id = %s",
                (run_id, tenant_id),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_run(row) if row is not None else None

    def get_run_by_client_request_id(
        self,
        tenant_id: str,
        client_request_id: str,
    ) -> RunRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM runs WHERE tenant_id = %s AND client_request_id = %s",
                (tenant_id, client_request_id),
            ).fetchone()
        finally:
            connection.close()
        return self._row_to_run(row) if row is not None else None

    def list_recoverable_runs(self) -> list[RunRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM runs WHERE status IN (%s, %s) ORDER BY created_at, run_id",
                (RunStatus.QUEUED.value, RunStatus.RUNNING.value),
            ).fetchall()
        finally:
            connection.close()
        return [self._row_to_run(row) for row in rows]

    # ---- durable leasing / fencing ----

    def claim_next_run(
        self,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        reconciliation_pending_code: str | None = None,
    ) -> RunLeaseClaim | None:
        if not owner_id:
            raise ValueError("owner_id must not be empty")
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")

        lease_token = f"lease_{uuid4().hex}"
        timestamp = utc_now()
        connection = self._lease_connect()
        try:
            try:
                with connection.transaction():
                    now_ms = self._lease_now_ms(connection)
                    expires_at = now_ms + lease_duration_seconds * 1000
                    candidate = connection.execute(
                        """
                        SELECT candidate.* FROM runs AS candidate
                        WHERE
                            (
                                candidate.status = %s
                                AND candidate.cancel_requested = FALSE
                                AND NOT EXISTS (
                                    SELECT 1 FROM runs AS thread_run
                                    WHERE thread_run.tenant_id = candidate.tenant_id
                                        AND thread_run.thread_id = candidate.thread_id
                                        AND thread_run.status = %s
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM runs AS earlier
                                    WHERE earlier.tenant_id = candidate.tenant_id
                                        AND earlier.thread_id = candidate.thread_id
                                        AND earlier.status = %s
                                        AND earlier.cancel_requested = FALSE
                                        AND (earlier.created_at, earlier.run_id)
                                            < (candidate.created_at, candidate.run_id)
                                )
                            )
                            OR (
                                candidate.status = %s
                                AND candidate.checkpoint_base_revision IS NOT NULL
                                AND candidate.error_code IS DISTINCT FROM %s
                                AND (
                                    candidate.lease_token IS NULL
                                    OR candidate.lease_expires_at IS NULL
                                    OR candidate.lease_expires_at <= %s
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM runs AS live_thread_run
                                    WHERE live_thread_run.tenant_id = candidate.tenant_id
                                        AND live_thread_run.thread_id = candidate.thread_id
                                        AND live_thread_run.run_id != candidate.run_id
                                        AND live_thread_run.status = %s
                                        AND live_thread_run.lease_token IS NOT NULL
                                        AND live_thread_run.lease_expires_at IS NOT NULL
                                        AND live_thread_run.lease_expires_at > %s
                                )
                            )
                        ORDER BY candidate.created_at, candidate.run_id
                        FOR UPDATE OF candidate SKIP LOCKED
                        LIMIT 1
                        """,
                        (
                            RunStatus.QUEUED.value,
                            RunStatus.RUNNING.value,
                            RunStatus.QUEUED.value,
                            RunStatus.RUNNING.value,
                            THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
                            now_ms,
                            RunStatus.RUNNING.value,
                            now_ms,
                        ),
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

                    updated = connection.execute(
                        """
                        UPDATE runs SET
                            status = %s, started_at = %s, attempt = attempt + 1,
                            error = CASE WHEN error_code = %s THEN error ELSE NULL END,
                            error_code = CASE WHEN error_code = %s THEN error_code ELSE NULL END,
                            updated_at = %s, lease_owner_id = %s, lease_token = %s,
                            lease_heartbeat_at = %s, lease_expires_at = %s,
                            checkpoint_base_revision = COALESCE(
                                checkpoint_base_revision,
                                (
                                    SELECT revision FROM thread_states
                                    WHERE tenant_id = runs.tenant_id
                                        AND thread_id = runs.thread_id
                                ),
                                0
                            )
                        WHERE run_id = %s
                            AND (
                                (
                                    status = %s AND cancel_requested = FALSE
                                    AND NOT EXISTS (
                                        SELECT 1 FROM runs AS thread_run
                                        WHERE thread_run.tenant_id = runs.tenant_id
                                            AND thread_run.thread_id = runs.thread_id
                                            AND thread_run.run_id != runs.run_id
                                            AND thread_run.status = %s
                                    )
                                )
                                OR (
                                    status = %s
                                    AND checkpoint_base_revision IS NOT NULL
                                    AND error_code IS DISTINCT FROM %s
                                    AND (
                                        lease_token IS NULL
                                        OR lease_expires_at IS NULL
                                        OR lease_expires_at <= %s
                                    )
                                    AND NOT EXISTS (
                                        SELECT 1 FROM runs AS live_thread_run
                                        WHERE live_thread_run.tenant_id = runs.tenant_id
                                            AND live_thread_run.thread_id = runs.thread_id
                                            AND live_thread_run.run_id != runs.run_id
                                            AND live_thread_run.status = %s
                                            AND live_thread_run.lease_token IS NOT NULL
                                            AND live_thread_run.lease_expires_at IS NOT NULL
                                            AND live_thread_run.lease_expires_at > %s
                                    )
                                )
                            )
                        RETURNING *
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
                            RunStatus.RUNNING.value,
                            THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
                            now_ms,
                            RunStatus.RUNNING.value,
                            now_ms,
                        ),
                    ).fetchone()
                    if updated is None:
                        return None
                    run = self._row_to_run(updated)
                    if recovery_reason is not None:
                        self._append_event_with_connection(
                            connection,
                            run.run_id,
                            "run.recovered",
                            {
                                "reason": (
                                    reconciliation_pending_code
                                    if reconciliation_pending_code is not None
                                    and candidate["error_code"]
                                    == reconciliation_pending_code
                                    else recovery_reason.value
                                )
                            },
                        )
                    self._append_event_with_connection(
                        connection,
                        run.run_id,
                        "run.started",
                        {
                            "attempt": run.attempt,
                            "recovered": recovery_reason is not None,
                            "checkpoint_base_revision": run.checkpoint_base_revision,
                        },
                    )
                    return RunLeaseClaim(
                        run=run,
                        owner_id=owner_id,
                        lease_token=lease_token,
                        recovery_reason=recovery_reason,
                    )
            except psycopg.errors.UniqueViolation as exc:
                # Only the partial running-Thread index is an expected scheduling
                # race. Client-request or any future integrity failure must not be
                # silently interpreted as an empty queue.
                if exc.diag.constraint_name == self._RUNNING_THREAD_INDEX:
                    return None
                raise
        finally:
            connection.close()

    def renew_run_lease(
        self,
        run_id: str,
        *,
        lease_token: str,
        lease_duration_seconds: int,
    ) -> bool:
        if not lease_token:
            raise ValueError("lease_token must not be empty")
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                row = connection.execute(
                    """
                    UPDATE runs SET
                        lease_heartbeat_at = %s, lease_expires_at = %s, updated_at = %s
                    WHERE run_id = %s AND status = %s AND lease_token = %s
                        AND lease_expires_at > %s
                    RETURNING run_id
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
                ).fetchone()
                return row is not None
        finally:
            connection.close()

    def expire_run_lease(self, run_id: str, *, lease_token: str) -> bool:
        if not lease_token:
            raise ValueError("lease_token must not be empty")
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                row = connection.execute(
                    """
                    UPDATE runs SET
                        lease_heartbeat_at = %s, lease_expires_at = %s, updated_at = %s
                    WHERE run_id = %s AND status = %s AND lease_token = %s
                    RETURNING run_id
                    """,
                    (
                        now_ms,
                        now_ms,
                        utc_now(),
                        run_id,
                        RunStatus.RUNNING.value,
                        lease_token,
                    ),
                ).fetchone()
                return row is not None
        finally:
            connection.close()

    def assert_current_run_lease(
        self,
        connection,
        run_id: str,
        *,
        lease_token: str | None,
    ) -> None:
        if lease_token is None:
            raise RunLeaseLostError(f"Run lease token is required: {run_id}")
        now_ms = self._lease_now_ms(connection)
        row = connection.execute(
            """
            SELECT 1 FROM runs
            WHERE run_id = %s AND status = %s AND lease_token = %s
                AND lease_expires_at > %s
            FOR UPDATE
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
        connection = self._lease_connect()
        try:
            with connection.transaction():
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
        finally:
            connection.close()

    # ---- cancellation and terminal commits ----

    def finalize_next_queued_cancellation(self) -> RunRecord | None:
        completed_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                row = connection.execute(
                    """
                    SELECT * FROM runs
                    WHERE status = %s AND cancel_requested = TRUE
                    ORDER BY created_at, run_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (RunStatus.QUEUED.value,),
                ).fetchone()
                if row is None:
                    return None
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        status = %s, error_code = NULL, error = NULL,
                        completed_at = %s, updated_at = %s,
                        lease_owner_id = NULL, lease_token = NULL,
                        lease_heartbeat_at = NULL, lease_expires_at = NULL
                    WHERE run_id = %s AND status = %s AND cancel_requested = TRUE
                    RETURNING *
                    """,
                    (
                        RunStatus.CANCELLED.value,
                        completed_at,
                        completed_at,
                        row["run_id"],
                        RunStatus.QUEUED.value,
                    ),
                ).fetchone()
                if updated is None:
                    return None
                self._append_event_with_connection(
                    connection,
                    str(row["run_id"]),
                    "run.cancelled",
                    {"reason": "cancelled_before_start"},
                )
                return self._row_to_run(updated)
        finally:
            connection.close()

    def request_cancel_atomically(self, run_id: str, *, tenant_id: str) -> RunRecord:
        connection = self._connect()
        try:
            with connection.transaction():
                row = connection.execute(
                    "SELECT * FROM runs WHERE run_id = %s AND tenant_id = %s FOR UPDATE",
                    (run_id, tenant_id),
                ).fetchone()
                if row is None:
                    raise KeyError(f"Run not found: {run_id}")
                updated = connection.execute(
                    """
                    UPDATE runs SET cancel_requested = TRUE, updated_at = %s
                    WHERE run_id = %s AND tenant_id = %s
                        AND status IN (%s, %s) AND cancel_requested = FALSE
                    RETURNING *
                    """,
                    (
                        utc_now(),
                        run_id,
                        tenant_id,
                        RunStatus.QUEUED.value,
                        RunStatus.RUNNING.value,
                    ),
                ).fetchone()
                if updated is not None:
                    self._append_event_with_connection(
                        connection,
                        run_id,
                        "run.cancel_requested",
                        {"status": row["status"]},
                    )
                    row = updated
                return self._row_to_run(row)
        finally:
            connection.close()

    def commit_reconciliation_pending(
        self,
        run_id: str,
        *,
        tenant_id: str,
        lease_token: str,
        error_code: str,
        error: str,
    ) -> RunCommitOutcome:
        updated_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        error_code = %s, error = %s, completed_at = NULL, updated_at = %s
                    WHERE run_id = %s AND tenant_id = %s AND status = %s
                        AND lease_token = %s AND lease_expires_at > %s
                    RETURNING run_id
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
                ).fetchone()
                if updated is not None:
                    return RunCommitOutcome.COMMITTED
                return self._classify_commit_outcome(
                    connection,
                    run_id,
                    tenant_id=tenant_id,
                    lease_token=lease_token,
                    now_ms=now_ms,
                )
        finally:
            connection.close()

    def commit_completed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        completed_at = utc_now()
        validation_errors_json = json.dumps(run.validation_errors)
        cas_changed = False
        connection = self._lease_connect()
        try:
            try:
                with connection.transaction():
                    now_ms = self._lease_now_ms(connection)
                    persisted = connection.execute(
                        """
                        SELECT status, cancel_requested, lease_token, lease_expires_at,
                            checkpoint_base_revision, thread_id, domain_id, schema_version
                        FROM runs WHERE run_id = %s AND tenant_id = %s
                        FOR UPDATE
                        """,
                        (run.run_id, run.tenant_id),
                    ).fetchone()
                    if persisted is None:
                        raise KeyError(f"Run not found: {run.run_id}")
                    persisted_status = RunStatus(persisted["status"])
                    current_lease = (
                        persisted["lease_token"] == lease_token
                        and persisted["lease_expires_at"] is not None
                        and int(persisted["lease_expires_at"]) > now_ms
                    )
                    if (
                        persisted_status != RunStatus.RUNNING
                        or bool(persisted["cancel_requested"])
                        or not current_lease
                    ):
                        return self._classify_commit_outcome(
                            connection,
                            run.run_id,
                            tenant_id=run.tenant_id,
                            lease_token=lease_token,
                            now_ms=now_ms,
                        )
                    if run.state is None:
                        raise ValueError("A completed managed Run must include checkpoint state")
                    persisted_identity = (
                        persisted["thread_id"],
                        persisted["domain_id"],
                        persisted["schema_version"],
                    )
                    expected_revision = persisted["checkpoint_base_revision"]
                    observed_revision = self._thread_revision(
                        connection,
                        run.tenant_id,
                        str(persisted["thread_id"]),
                        for_update=True,
                    )
                    if (
                        expected_revision is None
                        or int(expected_revision) != observed_revision
                    ):
                        error = self._fail_checkpoint_conflict_with_connection(
                            connection,
                            run_id=run.run_id,
                            tenant_id=run.tenant_id,
                            lease_token=lease_token,
                            expected_revision=(
                                int(expected_revision)
                                if expected_revision is not None
                                else None
                            ),
                            observed_revision=observed_revision,
                            phase="completion",
                            completed_at=completed_at,
                            now_ms=now_ms,
                        )
                        self._apply_checkpoint_conflict_to_run(
                            run,
                            error=error,
                            completed_at=completed_at,
                        )
                        return RunCommitOutcome.CHECKPOINT_CONFLICT
                    if (
                        (run.thread_id, run.domain_id, run.schema_version)
                        != persisted_identity
                        or (
                            run.state.thread_id,
                            run.state.domain_id,
                            run.state.schema_version,
                        )
                        != persisted_identity
                    ):
                        raise ValueError(
                            "Completed Run and checkpoint state identity must match the persisted Run"
                        )
                    self._remember_state_model(run.state)
                    run_state_json = run.state.model_dump_json()
                    checkpoint_state = project_thread_checkpoint_state(run.state)
                    checkpoint_state_json = checkpoint_state.model_dump_json()
                    self._assert_thread_schema_available(
                        connection,
                        run.tenant_id,
                        str(persisted["thread_id"]),
                        str(persisted["domain_id"]),
                        str(persisted["schema_version"]),
                    )
                    updated = connection.execute(
                        """
                        UPDATE runs SET
                            status = %s, state_json = %s, output_message = %s,
                            validation_errors_json = %s, error_code = NULL, error = NULL,
                            completed_at = %s, updated_at = %s,
                            lease_owner_id = NULL, lease_token = NULL,
                            lease_heartbeat_at = NULL, lease_expires_at = NULL
                        WHERE run_id = %s AND tenant_id = %s
                            AND status = %s AND cancel_requested = FALSE
                            AND lease_token = %s AND lease_expires_at > %s
                        RETURNING run_id
                        """,
                        (
                            RunStatus.COMPLETED.value,
                            run_state_json,
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
                    ).fetchone()
                    if updated is None:
                        return self._classify_commit_outcome(
                            connection,
                            run.run_id,
                            tenant_id=run.tenant_id,
                            lease_token=lease_token,
                            now_ms=now_ms,
                        )

                    checkpoint_revision = observed_revision + 1
                    checkpoint_row = self._write_checkpoint_with_connection(
                        connection,
                        run=run,
                        state_json=checkpoint_state_json,
                        updated_at=completed_at,
                        observed_revision=observed_revision,
                    )
                    if checkpoint_row is None:
                        raise _CheckpointCASChanged()
                    if int(checkpoint_row["revision"]) != checkpoint_revision:
                        raise RuntimeError("Thread checkpoint CAS advanced by an unexpected amount")

                    self._append_event_with_connection(
                        connection,
                        run.run_id,
                        "checkpoint.saved",
                        {
                            "thread_id": persisted["thread_id"],
                            "trace_events": len(checkpoint_state.execution_trace),
                            "run_trace_events": len(run.state.execution_trace),
                            "projection": "execution_trace_reset",
                            "base_revision": observed_revision,
                            "revision": checkpoint_revision,
                        },
                    )
                    self._append_event_with_connection(
                        connection,
                        run.run_id,
                        "run.completed",
                        {"validation_errors": run.validation_errors},
                    )
            except _CheckpointCASChanged:
                cas_changed = True
        finally:
            connection.close()

        if cas_changed:
            # The completion transaction rolled back in full. Resolve the newly
            # observed revision drift through the normal fenced conflict path;
            # this is not a retry of Runtime/provider work.
            return self.commit_checkpoint_conflict(
                run,
                lease_token=lease_token,
                phase="completion",
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

    def _write_checkpoint_with_connection(
        self,
        connection,
        *,
        run: RunRecord,
        state_json: str,
        updated_at: str,
        observed_revision: int,
    ) -> Row | None:
        return connection.execute(
            """
            INSERT INTO thread_states (
                tenant_id, thread_id, domain_id, schema_version,
                state_json, updated_at, revision
            ) VALUES (%s, %s, %s, %s, %s, %s, 1)
            ON CONFLICT (tenant_id, thread_id) DO UPDATE SET
                domain_id = EXCLUDED.domain_id,
                schema_version = EXCLUDED.schema_version,
                state_json = EXCLUDED.state_json,
                updated_at = EXCLUDED.updated_at,
                revision = thread_states.revision + 1
            WHERE thread_states.revision = %s
            RETURNING revision
            """,
            (
                run.tenant_id,
                run.thread_id,
                run.domain_id,
                run.schema_version,
                state_json,
                updated_at,
                observed_revision,
            ),
        ).fetchone()

    def commit_checkpoint_conflict(
        self,
        run: RunRecord,
        *,
        lease_token: str,
        phase: str,
    ) -> RunCommitOutcome:
        completed_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                persisted = connection.execute(
                    """
                    SELECT status, cancel_requested, lease_token, lease_expires_at,
                        checkpoint_base_revision, thread_id
                    FROM runs WHERE run_id = %s AND tenant_id = %s
                    FOR UPDATE
                    """,
                    (run.run_id, run.tenant_id),
                ).fetchone()
                if persisted is None:
                    raise KeyError(f"Run not found: {run.run_id}")
                status = RunStatus(persisted["status"])
                current_lease = (
                    persisted["lease_token"] == lease_token
                    and persisted["lease_expires_at"] is not None
                    and int(persisted["lease_expires_at"]) > now_ms
                )
                if (
                    status != RunStatus.RUNNING
                    or bool(persisted["cancel_requested"])
                    or not current_lease
                ):
                    return self._classify_commit_outcome(
                        connection,
                        run.run_id,
                        tenant_id=run.tenant_id,
                        lease_token=lease_token,
                        now_ms=now_ms,
                    )
                expected_revision = persisted["checkpoint_base_revision"]
                observed_revision = self._thread_revision(
                    connection,
                    run.tenant_id,
                    str(persisted["thread_id"]),
                    for_update=True,
                )
                error = self._fail_checkpoint_conflict_with_connection(
                    connection,
                    run_id=run.run_id,
                    tenant_id=run.tenant_id,
                    lease_token=lease_token,
                    expected_revision=(
                        int(expected_revision) if expected_revision is not None else None
                    ),
                    observed_revision=observed_revision,
                    phase=phase,
                    completed_at=completed_at,
                    now_ms=now_ms,
                )
            self._apply_checkpoint_conflict_to_run(
                run,
                error=error,
                completed_at=completed_at,
            )
            return RunCommitOutcome.CHECKPOINT_CONFLICT
        finally:
            connection.close()

    def quarantine_checkpoint_conflict_for_reconciliation(
        self,
        run: RunRecord,
        *,
        lease_token: str,
        phase: str,
    ) -> RunCommitOutcome:
        updated_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                persisted = connection.execute(
                    """
                    SELECT status, lease_token, lease_expires_at,
                        checkpoint_base_revision, thread_id
                    FROM runs WHERE run_id = %s AND tenant_id = %s
                    FOR UPDATE
                    """,
                    (run.run_id, run.tenant_id),
                ).fetchone()
                if persisted is None:
                    raise KeyError(f"Run not found: {run.run_id}")
                status = RunStatus(persisted["status"])
                current_lease = (
                    persisted["lease_token"] == lease_token
                    and persisted["lease_expires_at"] is not None
                    and int(persisted["lease_expires_at"]) > now_ms
                )
                if status != RunStatus.RUNNING or not current_lease:
                    return self._classify_commit_outcome(
                        connection,
                        run.run_id,
                        tenant_id=run.tenant_id,
                        lease_token=lease_token,
                        now_ms=now_ms,
                    )
                expected_revision = persisted["checkpoint_base_revision"]
                observed_revision = self._thread_revision(
                    connection,
                    run.tenant_id,
                    str(persisted["thread_id"]),
                    for_update=True,
                )
                normalized_expected_revision = (
                    int(expected_revision) if expected_revision is not None else None
                )
                if normalized_expected_revision == observed_revision:
                    raise RuntimeError(
                        "Checkpoint reconciliation quarantine requires a current revision mismatch"
                    )
                error = (
                    "Thread checkpoint revision changed while external-action "
                    "reconciliation remains unresolved; the Run is quarantined: "
                    f"expected {expected_revision}, observed {observed_revision}"
                )
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        error_code = %s, error = %s, completed_at = NULL,
                        updated_at = %s, lease_owner_id = NULL, lease_token = NULL,
                        lease_heartbeat_at = NULL, lease_expires_at = NULL
                    WHERE run_id = %s AND tenant_id = %s AND status = %s
                        AND lease_token = %s AND lease_expires_at > %s
                    RETURNING run_id
                    """,
                    (
                        THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
                        error,
                        updated_at,
                        run.run_id,
                        run.tenant_id,
                        RunStatus.RUNNING.value,
                        lease_token,
                        now_ms,
                    ),
                ).fetchone()
                if updated is None:
                    raise RuntimeError(
                        "Checkpoint reconciliation quarantine lost its Run lease"
                    )
                self._append_event_with_connection(
                    connection,
                    run.run_id,
                    "checkpoint.conflict",
                    {
                        "phase": phase,
                        "expected_revision": normalized_expected_revision,
                        "observed_revision": observed_revision,
                        "disposition": "external_action_reconciliation_quarantined",
                    },
                )
            run.status = RunStatus.RUNNING
            run.error_code = THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
            run.error = error
            run.completed_at = None
            run.updated_at = updated_at
            run.lease_owner_id = None
            run.lease_token = None
            run.lease_heartbeat_at = None
            run.lease_expires_at = None
            return RunCommitOutcome.COMMITTED
        finally:
            connection.close()

    def commit_cancelled_run(
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
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        execution_authority_json = %s, thread_id = %s, agent_id = %s,
                        agent_version = %s, domain_id = %s, schema_version = %s,
                        status = %s, input_message = %s, input_json = %s,
                        state_json = %s, output_message = %s,
                        validation_errors_json = %s, error_code = NULL, error = NULL,
                        attempt = %s, cancel_requested = TRUE, client_request_id = %s,
                        created_at = %s, updated_at = %s, started_at = %s, completed_at = %s,
                        lease_owner_id = NULL, lease_token = NULL,
                        lease_heartbeat_at = NULL, lease_expires_at = NULL
                    WHERE run_id = %s AND tenant_id = %s
                        AND status IN (%s, %s) AND cancel_requested = TRUE
                        AND lease_token = %s AND lease_expires_at > %s
                    RETURNING run_id
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
                ).fetchone()
                if updated is None:
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
        finally:
            connection.close()

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
        completed_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                now_ms = self._lease_now_ms(connection)
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        status = %s, error_code = %s, error = %s, completed_at = %s,
                        updated_at = %s, lease_owner_id = NULL, lease_token = NULL,
                        lease_heartbeat_at = NULL, lease_expires_at = NULL
                    WHERE run_id = %s AND tenant_id = %s AND status = %s
                        AND (cancel_requested = FALSE OR %s = TRUE)
                        AND lease_token = %s AND lease_expires_at > %s
                    RETURNING run_id
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
                        allow_cancel_requested,
                        lease_token,
                        now_ms,
                    ),
                ).fetchone()
                if updated is None:
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
        finally:
            connection.close()

    def _classify_commit_outcome(
        self,
        connection,
        run_id: str,
        *,
        tenant_id: str,
        lease_token: str,
        now_ms: int,
    ) -> RunCommitOutcome:
        row = connection.execute(
            """
            SELECT status, cancel_requested, lease_token, lease_expires_at
            FROM runs WHERE run_id = %s AND tenant_id = %s
            FOR UPDATE
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

    # ---- Run events ----

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        del event_type, payload
        raise RunLeaseLostError(f"Unfenced Run event append is prohibited: {run_id}")

    def append_control_plane_event(
        self,
        run_id: str,
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        if event_type not in self._CONTROL_PLANE_EVENT_TYPES:
            raise ValueError(f"Unsupported control-plane Run event: {event_type}")
        connection = self._connect()
        try:
            with connection.transaction():
                row = connection.execute(
                    "SELECT run_id FROM runs WHERE run_id = %s AND tenant_id = %s FOR UPDATE",
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
        finally:
            connection.close()

    def _append_event_with_connection(
        self,
        connection,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        # Locking the Run row makes MAX(sequence)+1 a per-Run logical sequence
        # allocator. PostgreSQL identity values are surrogate IDs and may gap on
        # rollback; the shared contract intentionally does not require otherwise.
        run_row = connection.execute(
            "SELECT run_id FROM runs WHERE run_id = %s FOR UPDATE",
            (run_id,),
        ).fetchone()
        if run_row is None:
            raise KeyError(f"Run not found: {run_id}")
        sequence_row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM run_events WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert sequence_row is not None
        event = RunEvent(
            run_id=run_id,
            sequence=int(sequence_row["sequence"]),
            event_type=event_type,
            payload=payload or {},
        )
        row = connection.execute(
            """
            INSERT INTO run_events (
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

    def list_events(self, run_id: str, *, after_sequence: int = 0) -> list[RunEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM run_events
                WHERE run_id = %s AND sequence > %s
                ORDER BY sequence
                """,
                (run_id, after_sequence),
            ).fetchall()
        finally:
            connection.close()
        return self._rows_to_events(rows)

    def list_events_for_tenant(
        self,
        run_id: str,
        tenant_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RunEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT event.* FROM run_events AS event
                INNER JOIN runs AS run ON run.run_id = event.run_id
                WHERE event.run_id = %s AND run.tenant_id = %s AND event.sequence > %s
                ORDER BY event.sequence
                """,
                (run_id, tenant_id, after_sequence),
            ).fetchall()
        finally:
            connection.close()
        return self._rows_to_events(rows)

    @staticmethod
    def _rows_to_events(rows: list[Row]) -> list[RunEvent]:
        return [
            RunEvent(
                event_id=int(row["event_id"]),
                run_id=str(row["run_id"]),
                sequence=int(row["sequence"]),
                event_type=str(row["event_type"]),
                payload=json.loads(row["payload_json"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    # ---- Thread checkpoint reads ----

    def load_thread_state(
        self,
        thread_id: str,
        *,
        tenant_id: str,
        domain_id: str | None = None,
        schema_version: str | None = None,
    ) -> BaseRuntimeState | None:
        return self.load_thread_state_snapshot(
            thread_id,
            tenant_id=tenant_id,
            domain_id=domain_id,
            schema_version=schema_version,
        ).state

    def load_thread_state_snapshot(
        self,
        thread_id: str,
        *,
        tenant_id: str,
        domain_id: str | None = None,
        schema_version: str | None = None,
        expected_revision: int | None = None,
        require_revision_match: bool = False,
    ) -> ThreadStateSnapshot:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT domain_id, schema_version, state_json, revision
                FROM thread_states WHERE tenant_id = %s AND thread_id = %s
                """,
                (tenant_id, thread_id),
            ).fetchone()
        finally:
            connection.close()
        observed_revision = int(row["revision"]) if row is not None else 0
        if require_revision_match and expected_revision != observed_revision:
            raise ThreadCheckpointRevisionConflictError(
                expected_revision=expected_revision,
                observed_revision=observed_revision,
            )
        if row is None:
            return ThreadStateSnapshot(state=None, revision=0)
        if domain_id is not None and row["domain_id"] != domain_id:
            raise ValueError(
                f"Thread {thread_id!r} belongs to domain {row['domain_id']!r}, not {domain_id!r}"
            )
        if schema_version is not None and row["schema_version"] != schema_version:
            raise ValueError(
                f"Thread {thread_id!r} uses schema {row['schema_version']!r}, "
                f"not {schema_version!r}"
            )
        return ThreadStateSnapshot(
            state=self._deserialize_state(
                str(row["domain_id"]),
                str(row["schema_version"]),
                str(row["state_json"]),
            ),
            revision=observed_revision,
        )

    # ---- quarantine planning / operator repair ----

    def plan_quarantine_resolution(
        self,
        run_id: str,
        *,
        tenant_id: str,
        target: QuarantineResolutionTarget,
        resolution: QuarantineResolutionKind,
    ) -> QuarantineResolutionPlan:
        connection = self._connect()
        try:
            with connection.transaction():
                connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                snapshot = self._derive_quarantine_plan_with_connection(
                    connection,
                    run_id,
                    tenant_id=tenant_id,
                    target=target,
                    resolution=resolution,
                )
            return snapshot.plan
        finally:
            connection.close()

    def apply_quarantine_resolution(
        self,
        run_id: str,
        *,
        tenant_id: str,
        target: QuarantineResolutionTarget,
        resolution: QuarantineResolutionKind,
        expected_plan_id: str,
        operator_subject_id: str,
        operator_credential_id: str,
    ) -> QuarantineResolutionCommit:
        completed_at = utc_now()
        connection = self._lease_connect()
        try:
            with connection.transaction():
                self._lock_quarantine_evidence(connection, run_id, tenant_id=tenant_id)
                existing_resolution = self._find_resolution_event_with_connection(
                    connection,
                    run_id,
                    expected_plan_id=expected_plan_id,
                )
                current = self._derive_quarantine_plan_with_connection(
                    connection,
                    run_id,
                    tenant_id=tenant_id,
                    target=target,
                    resolution=resolution,
                )
                if existing_resolution is not None:
                    stored_plan, stored_workflow_fingerprint, stored_checkpoint_fingerprint = (
                        existing_resolution
                    )
                    checkpoint_progress_is_valid = self._checkpoint_progress_is_valid(
                        baseline_revision=stored_plan.observed_checkpoint_revision,
                        baseline_fingerprint=stored_checkpoint_fingerprint,
                        observed_revision=current.plan.observed_checkpoint_revision,
                        observed_fingerprint=current.checkpoint_evidence_fingerprint,
                    )
                    if (
                        stored_plan.target != target
                        or stored_plan.resolution != resolution
                        or stored_workflow_fingerprint
                        != current.workflow_evidence_fingerprint
                        or not checkpoint_progress_is_valid
                    ):
                        raise QuarantineResolutionEvidenceIncompleteError(
                            "Committed quarantine resolution evidence is inconsistent"
                        )
                    return QuarantineResolutionCommit(
                        plan=stored_plan,
                        reused=True,
                        workflow_evidence_fingerprint=stored_workflow_fingerprint,
                        checkpoint_evidence_fingerprint=stored_checkpoint_fingerprint,
                    )

                plan = current.plan
                if not plan.eligible or plan.plan_id != expected_plan_id:
                    raise QuarantineResolutionStalePlanError(plan)

                error = (
                    "An operator terminalized an eligible quarantined Run while "
                    "preserving the authoritative checkpoint and external-action evidence."
                )
                updated = connection.execute(
                    """
                    UPDATE runs SET
                        status = %s, error_code = %s, error = %s, completed_at = %s,
                        updated_at = %s, lease_owner_id = NULL, lease_token = NULL,
                        lease_heartbeat_at = NULL, lease_expires_at = NULL
                    WHERE run_id = %s AND tenant_id = %s AND status = %s
                        AND error_code = %s AND lease_owner_id IS NULL
                        AND lease_token IS NULL AND lease_heartbeat_at IS NULL
                        AND lease_expires_at IS NULL
                    RETURNING run_id
                    """,
                    (
                        RunStatus.FAILED.value,
                        THREAD_CHECKPOINT_CONFLICT_CODE,
                        error,
                        completed_at,
                        completed_at,
                        run_id,
                        tenant_id,
                        RunStatus.RUNNING.value,
                        THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE,
                    ),
                ).fetchone()
                if updated is None:
                    raise QuarantineResolutionStalePlanError(plan)

                audit_payload = {
                    "resolution": resolution.value,
                    "plan_id": expected_plan_id,
                    "target_kind": target.kind.value,
                    "source_quarantine_code": plan.current_quarantine_code,
                    "checkpoint_base_revision": plan.checkpoint_base_revision,
                    "observed_checkpoint_revision": plan.observed_checkpoint_revision,
                    "checkpoint_disposition": plan.checkpoint_disposition,
                    "external_evidence_disposition": plan.external_evidence_disposition,
                    "provider_calls": plan.provider_calls,
                    "operator_subject_id": operator_subject_id,
                    "operator_credential_id": operator_credential_id,
                    "workflow_evidence_fingerprint": current.workflow_evidence_fingerprint,
                    "checkpoint_evidence_fingerprint": current.checkpoint_evidence_fingerprint,
                    "plan": plan.model_dump(mode="json"),
                }
                self._append_event_with_connection(
                    connection,
                    run_id,
                    "quarantine.resolution_applied",
                    audit_payload,
                )
                self._append_event_with_connection(
                    connection,
                    run_id,
                    "run.failed",
                    {
                        "error_code": THREAD_CHECKPOINT_CONFLICT_CODE,
                        "error": error,
                        "resolution": resolution.value,
                        "plan_id": expected_plan_id,
                    },
                )
                return QuarantineResolutionCommit(
                    plan=plan,
                    reused=False,
                    workflow_evidence_fingerprint=current.workflow_evidence_fingerprint,
                    checkpoint_evidence_fingerprint=current.checkpoint_evidence_fingerprint,
                )
        finally:
            connection.close()

    def verify_quarantine_resolution(self, commit: QuarantineResolutionCommit) -> None:
        plan = commit.plan
        plan_id = plan.plan_id
        if plan_id is None:
            raise QuarantineResolutionEvidenceIncompleteError(
                "Committed quarantine resolution lost its plan ID"
            )
        run_id = plan.target.identifier
        connection = self._connect()
        try:
            with connection.transaction():
                connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                run = connection.execute(
                    "SELECT * FROM runs WHERE run_id = %s AND tenant_id = %s",
                    (run_id, plan.thread.tenant_id),
                ).fetchone()
                if run is None:
                    raise QuarantineResolutionEvidenceIncompleteError(
                        "Resolved Run is not durably visible"
                    )
                checkpoint = connection.execute(
                    """
                    SELECT revision, state_json FROM thread_states
                    WHERE tenant_id = %s AND thread_id = %s
                    """,
                    (run["tenant_id"], run["thread_id"]),
                ).fetchone()
                workflow_fingerprint = self._workflow_evidence_fingerprint(
                    connection,
                    run_id,
                )
                resolution_events = self._matching_resolution_events(
                    connection,
                    run_id,
                    plan_id=plan_id,
                )
                failed_events = self._matching_resolution_failed_events(
                    connection,
                    run_id,
                    plan_id=plan_id,
                )
        finally:
            connection.close()

        observed_revision = int(checkpoint["revision"]) if checkpoint else 0
        checkpoint_state_json = str(checkpoint["state_json"]) if checkpoint else None
        checkpoint_fingerprint = self._checkpoint_evidence_fingerprint(
            observed_revision,
            checkpoint_state_json,
        )
        checkpoint_progress_is_valid = self._checkpoint_progress_is_valid(
            baseline_revision=plan.observed_checkpoint_revision,
            baseline_fingerprint=commit.checkpoint_evidence_fingerprint,
            observed_revision=observed_revision,
            observed_fingerprint=checkpoint_fingerprint,
        )
        lease_cleared = all(
            run[field] is None
            for field in (
                "lease_owner_id",
                "lease_token",
                "lease_heartbeat_at",
                "lease_expires_at",
            )
        )
        verified = bool(
            run["status"] == RunStatus.FAILED.value
            and run["error_code"] == THREAD_CHECKPOINT_CONFLICT_CODE
            and lease_cleared
            and checkpoint_progress_is_valid
            and workflow_fingerprint == commit.workflow_evidence_fingerprint
            and len(resolution_events) == 1
            and len(failed_events) == 1
        )
        if not verified:
            raise QuarantineResolutionEvidenceIncompleteError(
                "Quarantine resolution durable evidence is incomplete"
            )

    def _lock_quarantine_evidence(self, connection, run_id: str, *, tenant_id: str) -> None:
        run = connection.execute(
            "SELECT thread_id FROM runs WHERE run_id = %s AND tenant_id = %s FOR UPDATE",
            (run_id, tenant_id),
        ).fetchone()
        if run is None:
            raise KeyError(f"Run not found: {run_id}")
        connection.execute(
            "SELECT revision FROM thread_states WHERE tenant_id = %s AND thread_id = %s FOR UPDATE",
            (tenant_id, run["thread_id"]),
        ).fetchone()
        # Workflow writers lock the execution row before terminal action/event
        # changes. Taking it here freezes the legitimate evidence stream while
        # the repair plan is re-derived and applied.
        connection.execute(
            "SELECT run_id FROM workflow_executions WHERE run_id = %s FOR UPDATE",
            (run_id,),
        ).fetchone()
        connection.execute(
            "SELECT call_id FROM tool_calls WHERE run_id = %s ORDER BY call_id FOR UPDATE",
            (run_id,),
        ).fetchall()
        connection.execute(
            "SELECT action_id FROM external_actions WHERE run_id = %s ORDER BY action_id FOR UPDATE",
            (run_id,),
        ).fetchall()

    def _derive_quarantine_plan_with_connection(
        self,
        connection,
        run_id: str,
        *,
        tenant_id: str,
        target: QuarantineResolutionTarget,
        resolution: QuarantineResolutionKind,
    ) -> _QuarantinePlanSnapshot:
        if target.identifier != run_id:
            raise ValueError("Quarantine target identity does not match run_id")
        run = connection.execute(
            "SELECT * FROM runs WHERE run_id = %s AND tenant_id = %s",
            (run_id, tenant_id),
        ).fetchone()
        if run is None:
            raise KeyError(f"Run not found: {run_id}")
        checkpoint = connection.execute(
            """
            SELECT revision, state_json FROM thread_states
            WHERE tenant_id = %s AND thread_id = %s
            """,
            (tenant_id, run["thread_id"]),
        ).fetchone()
        observed_revision = int(checkpoint["revision"]) if checkpoint else 0
        checkpoint_state_json = str(checkpoint["state_json"]) if checkpoint else None
        checkpoint_fingerprint = self._checkpoint_evidence_fingerprint(
            observed_revision,
            checkpoint_state_json,
        )
        run_events = connection.execute(
            """
            SELECT sequence, event_type, payload_json, created_at
            FROM run_events WHERE run_id = %s ORDER BY sequence
            """,
            (run_id,),
        ).fetchall()
        conflict_event: Row | None = None
        conflict_payload: dict[str, Any] | None = None
        for event in run_events:
            if event["event_type"] != "checkpoint.conflict":
                continue
            payload = self._decode_json_object(event["payload_json"])
            if payload is None:
                continue
            if payload.get("disposition") == "external_action_reconciliation_quarantined":
                conflict_event = event
                conflict_payload = payload

        action_rows = connection.execute(
            "SELECT * FROM external_actions WHERE run_id = %s ORDER BY created_at, action_id",
            (run_id,),
        ).fetchall()
        step_rows = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = %s ORDER BY created_at, call_id",
            (run_id,),
        ).fetchall()
        workflow_events = connection.execute(
            "SELECT * FROM workflow_events WHERE run_id = %s ORDER BY sequence",
            (run_id,),
        ).fetchall()
        execution = connection.execute(
            "SELECT * FROM workflow_executions WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        workflow_fingerprint = self._workflow_evidence_fingerprint_from_rows(
            execution,
            step_rows,
            action_rows,
            workflow_events,
        )
        counts = {
            "prepared": 0,
            "dispatching": 0,
            "succeeded": 0,
            "failed": 0,
            "outcome_unknown": 0,
            "unrecognized": 0,
        }
        for action in action_rows:
            action_status = str(action["status"])
            if action_status in {
                "prepared",
                "dispatching",
                "succeeded",
                "failed",
                "outcome_unknown",
            }:
                counts[action_status] += 1
            else:
                counts["unrecognized"] += 1
        unknown_status = counts["unrecognized"] > 0
        action_summary = ExternalActionStatusSummary(total=len(action_rows), **counts)
        reasons: list[str] = []

        def add_reason(reason: str) -> None:
            if reason not in reasons:
                reasons.append(reason)

        if run["status"] != RunStatus.RUNNING.value or (
            run["error_code"] != THREAD_CHECKPOINT_RECONCILIATION_BLOCKED_CODE
        ):
            add_reason("run_not_quarantined")
        if any(
            run[field] is not None
            for field in (
                "lease_owner_id",
                "lease_token",
                "lease_heartbeat_at",
                "lease_expires_at",
            )
        ):
            add_reason("execution_authority_present")
        base_revision = run["checkpoint_base_revision"]
        normalized_base_revision = int(base_revision) if base_revision is not None else None
        if conflict_event is None or conflict_payload is None:
            add_reason("checkpoint_conflict_event_missing")
        else:
            if (
                conflict_payload.get("expected_revision") != normalized_base_revision
                or conflict_payload.get("observed_revision") != observed_revision
            ):
                add_reason("checkpoint_conflict_event_mismatch")
        if normalized_base_revision == observed_revision:
            add_reason("checkpoint_revision_not_drifted")
        if execution is None:
            add_reason("workflow_execution_missing")
        if not action_rows:
            add_reason("external_action_evidence_missing")
        if counts["prepared"] or counts["dispatching"] or unknown_status:
            add_reason("external_action_nonterminal")
        if action_rows and not self._terminal_external_action_evidence_is_consistent(
            action_rows,
            step_rows,
            workflow_events,
            execution=execution,
            tenant_id=tenant_id,
        ):
            add_reason("external_action_evidence_inconsistent")

        eligible = not reasons
        thread = QuarantineThreadReference(
            tenant_id=tenant_id,
            reference_kind=(
                "thread_id" if target.kind == QuarantineTargetKind.RUN else "action_id"
            ),
            reference=(
                str(run["thread_id"])
                if target.kind == QuarantineTargetKind.RUN
                else target.identifier
            ),
        )
        safe_conflict = (
            {
                "sequence": int(conflict_event["sequence"]),
                "created_at": conflict_event["created_at"],
                "phase": conflict_payload.get("phase"),
                "expected_revision": conflict_payload.get("expected_revision"),
                "observed_revision": conflict_payload.get("observed_revision"),
                "disposition": conflict_payload.get("disposition"),
            }
            if conflict_event is not None and conflict_payload is not None
            else None
        )
        plan_material = {
            "contract": "quarantine-resolution-plan-v1",
            "tenant_id": tenant_id,
            "target": target.model_dump(mode="json"),
            "resolution": resolution.value,
            "run": {
                "status": run["status"],
                "error_code": run["error_code"],
                "attempt": int(run["attempt"]),
                "cancel_requested": bool(run["cancel_requested"]),
                "checkpoint_base_revision": normalized_base_revision,
                "updated_at": run["updated_at"],
                "lease_cleared": not any(
                    run[field] is not None
                    for field in (
                        "lease_owner_id",
                        "lease_token",
                        "lease_heartbeat_at",
                        "lease_expires_at",
                    )
                ),
            },
            "checkpoint": {
                "revision": observed_revision,
                "evidence_fingerprint": checkpoint_fingerprint,
            },
            "checkpoint_conflict": safe_conflict,
            "workflow_evidence_fingerprint": workflow_fingerprint,
            "external_action_status_summary": action_summary.model_dump(mode="json"),
        }
        plan_id = (
            "qrp_" + self._canonical_resolution_hash(plan_material)
            if eligible
            else None
        )
        plan = QuarantineResolutionPlan(
            target=target,
            resolution=resolution,
            eligible=eligible,
            plan_id=plan_id,
            thread=thread,
            current_run_status=RunStatus(run["status"]),
            current_quarantine_code=run["error_code"],
            cancel_requested=bool(run["cancel_requested"]),
            checkpoint_base_revision=normalized_base_revision,
            observed_checkpoint_revision=observed_revision,
            external_actions=action_summary,
            workflow_reconciliation_required=bool(
                counts["prepared"] or counts["dispatching"] or unknown_status
            ),
            planned_run_transition=("running -> failed" if eligible else "no change"),
            new_audit_events=(
                ("quarantine.resolution_applied", "run.failed") if eligible else ()
            ),
            thread_disposition=(
                "released_after_atomic_commit" if eligible else "remains_blocked"
            ),
            ineligibility_reasons=tuple(reasons),
        )
        return _QuarantinePlanSnapshot(
            plan=plan,
            workflow_evidence_fingerprint=workflow_fingerprint,
            checkpoint_evidence_fingerprint=checkpoint_fingerprint,
        )

    @staticmethod
    def _terminal_external_action_evidence_is_consistent(
        action_rows: list[Row],
        step_rows: list[Row],
        workflow_events: list[Row],
        *,
        execution: Row | None,
        tenant_id: str,
    ) -> bool:
        if execution is None:
            return False
        steps = {(row["run_id"], row["step_id"]): row for row in step_rows}
        decoded_events: list[tuple[str, dict[str, Any]]] = []
        for event in workflow_events:
            payload = PostgresRunStore._decode_json_object(event["payload_json"])
            if payload is None:
                return False
            decoded_events.append((str(event["event_type"]), payload))
        for action in action_rows:
            status = action["status"]
            if status not in {"succeeded", "failed", "outcome_unknown"}:
                return False
            if (
                action["tenant_id"] != tenant_id
                or action["workflow_type"] != execution["workflow_type"]
                or not action["provider_name"]
                or not action["provider_identity"]
                or not action["idempotency_key"]
                or PostgresRunStore._decode_json_object(action["arguments_json"])
                is None
            ):
                return False
            if int(action["dispatch_count"]) < 1 or action["dispatch_token"] is None:
                return False
            step = steps.get((action["run_id"], action["step_id"]))
            if step is None or (
                step["tool_name"] != action["tool_name"]
                or step["input_hash"] != action["input_hash"]
            ):
                return False
            if status == "succeeded":
                row_consistent = bool(
                    action["provider_reference"]
                    and action["result_json"] is not None
                    and PostgresRunStore._decode_json_object(action["result_json"])
                    is not None
                    and action["error_code"] is None
                    and step["status"] == "completed"
                    and step["result_json"] == action["result_json"]
                    and step["error_code"] is None
                )
            else:
                row_consistent = bool(
                    action["result_json"] is None
                    and action["error_code"]
                    and step["status"] == "failed"
                    and step["result_json"] is None
                    and step["error_code"] == action["error_code"]
                )
            if not row_consistent:
                return False
            expected_event_type = f"external_action.{status}"
            terminal_events = [
                (event_type, payload)
                for event_type, payload in decoded_events
                if event_type
                in {
                    "external_action.succeeded",
                    "external_action.failed",
                    "external_action.outcome_unknown",
                }
                and payload.get("action_id") == action["action_id"]
            ]
            if len(terminal_events) != 1:
                return False
            event_type, event_payload = terminal_events[0]
            if not (
                event_type == expected_event_type
                and event_payload.get("step_id") == action["step_id"]
                and event_payload.get("tool_name") == action["tool_name"]
                and event_payload.get("provider_name") == action["provider_name"]
                and event_payload.get("status") == status
                and event_payload.get("dispatch_count") == int(action["dispatch_count"])
                and event_payload.get("provider_reference") == action["provider_reference"]
                and event_payload.get("error_code") == action["error_code"]
            ):
                return False
            expected_step_event_type = (
                "step.completed" if status == "succeeded" else "step.failed"
            )
            step_events = [
                (event_type, payload)
                for event_type, payload in decoded_events
                if event_type in {"step.completed", "step.failed"}
                and payload.get("step_id") == action["step_id"]
            ]
            if len(step_events) != 1:
                return False
            step_event_type, step_payload = step_events[0]
            if not (
                step_event_type == expected_step_event_type
                and step_payload.get("tool_name") == step["tool_name"]
                and step_payload.get("attempt_count") == int(step["attempt_count"])
                and step_payload.get("error_code") == step["error_code"]
                and step_payload.get("outcome")
                == ("completed" if status == "succeeded" else "failed")
            ):
                return False
        return True

    def _workflow_evidence_fingerprint(self, connection, run_id: str) -> str:
        execution = connection.execute(
            "SELECT * FROM workflow_executions WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        steps = connection.execute(
            "SELECT * FROM tool_calls WHERE run_id = %s ORDER BY created_at, call_id",
            (run_id,),
        ).fetchall()
        actions = connection.execute(
            "SELECT * FROM external_actions WHERE run_id = %s ORDER BY created_at, action_id",
            (run_id,),
        ).fetchall()
        events = connection.execute(
            "SELECT * FROM workflow_events WHERE run_id = %s ORDER BY sequence",
            (run_id,),
        ).fetchall()
        return self._workflow_evidence_fingerprint_from_rows(
            execution,
            steps,
            actions,
            events,
        )

    @classmethod
    def _workflow_evidence_fingerprint_from_rows(
        cls,
        execution: Row | None,
        steps: list[Row],
        actions: list[Row],
        events: list[Row],
    ) -> str:
        material = {
            "execution": (
                cls._resolution_row_material(execution) if execution is not None else None
            ),
            "steps": [cls._resolution_row_material(row) for row in steps],
            "actions": [cls._resolution_row_material(row) for row in actions],
            "events": [cls._resolution_row_material(row) for row in events],
        }
        return cls._canonical_resolution_hash(material)

    @staticmethod
    def _resolution_row_material(row: Row) -> dict[str, Any]:
        return {str(column): row[column] for column in row.keys()}

    @classmethod
    def _checkpoint_evidence_fingerprint(
        cls,
        revision: int,
        state_json: str | None,
    ) -> str:
        state_fingerprint = (
            sha256(state_json.encode("utf-8")).hexdigest()
            if state_json is not None
            else None
        )
        return cls._canonical_resolution_hash(
            {"revision": revision, "state_fingerprint": state_fingerprint}
        )

    @staticmethod
    def _checkpoint_progress_is_valid(
        *,
        baseline_revision: int,
        baseline_fingerprint: str,
        observed_revision: int,
        observed_fingerprint: str,
    ) -> bool:
        if observed_revision < baseline_revision:
            return False
        if observed_revision == baseline_revision:
            return observed_fingerprint == baseline_fingerprint
        return True

    @staticmethod
    def _canonical_resolution_hash(payload: dict[str, Any]) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _decode_json_object(encoded: Any) -> dict[str, Any] | None:
        if not isinstance(encoded, str):
            return None
        try:
            decoded = json.loads(encoded)
        except (TypeError, json.JSONDecodeError):
            return None
        return decoded if isinstance(decoded, dict) else None

    def _find_resolution_event_with_connection(
        self,
        connection,
        run_id: str,
        *,
        expected_plan_id: str,
    ) -> tuple[QuarantineResolutionPlan, str, str] | None:
        matching = self._matching_resolution_events(
            connection,
            run_id,
            plan_id=expected_plan_id,
        )
        if not matching:
            return None
        if len(matching) != 1:
            raise QuarantineResolutionEvidenceIncompleteError(
                "Duplicate quarantine resolution audit evidence"
            )
        payload = self._decode_json_object(matching[0]["payload_json"])
        if payload is None:
            raise QuarantineResolutionEvidenceIncompleteError(
                "Quarantine resolution audit evidence is invalid"
            )
        try:
            plan = QuarantineResolutionPlan.model_validate(payload["plan"])
            workflow_fingerprint = str(payload["workflow_evidence_fingerprint"])
            checkpoint_fingerprint = str(payload["checkpoint_evidence_fingerprint"])
        except (KeyError, TypeError, ValueError):
            raise QuarantineResolutionEvidenceIncompleteError(
                "Quarantine resolution audit evidence is invalid"
            ) from None
        if (
            payload.get("resolution") != plan.resolution.value
            or payload.get("target_kind") != plan.target.kind.value
            or plan.plan_id != expected_plan_id
        ):
            raise QuarantineResolutionEvidenceIncompleteError(
                "Quarantine resolution audit identity is inconsistent"
            )
        return plan, workflow_fingerprint, checkpoint_fingerprint

    @staticmethod
    def _matching_resolution_events(
        connection,
        run_id: str,
        *,
        plan_id: str,
    ) -> list[Row]:
        rows = connection.execute(
            """
            SELECT * FROM run_events
            WHERE run_id = %s AND event_type = 'quarantine.resolution_applied'
            ORDER BY sequence
            """,
            (run_id,),
        ).fetchall()
        matching: list[Row] = []
        for row in rows:
            payload = PostgresRunStore._decode_json_object(row["payload_json"])
            if payload is not None and payload.get("plan_id") == plan_id:
                matching.append(row)
        return matching

    @staticmethod
    def _matching_resolution_failed_events(
        connection,
        run_id: str,
        *,
        plan_id: str,
    ) -> list[Row]:
        rows = connection.execute(
            """
            SELECT * FROM run_events
            WHERE run_id = %s AND event_type = 'run.failed'
            ORDER BY sequence
            """,
            (run_id,),
        ).fetchall()
        matching: list[Row] = []
        for row in rows:
            payload = PostgresRunStore._decode_json_object(row["payload_json"])
            if payload is not None and payload.get("plan_id") == plan_id:
                matching.append(row)
        return matching

    def _fail_checkpoint_conflict_with_connection(
        self,
        connection,
        *,
        run_id: str,
        tenant_id: str,
        lease_token: str,
        expected_revision: int | None,
        observed_revision: int,
        phase: str,
        completed_at: str,
        now_ms: int,
    ) -> str:
        error = (
            "Thread checkpoint revision changed while the Run held execution "
            f"ownership: expected {expected_revision}, observed {observed_revision}"
        )
        updated = connection.execute(
            """
            UPDATE runs SET
                status = %s, error_code = %s, error = %s, completed_at = %s,
                updated_at = %s, lease_owner_id = NULL, lease_token = NULL,
                lease_heartbeat_at = NULL, lease_expires_at = NULL
            WHERE run_id = %s AND tenant_id = %s AND status = %s
                AND cancel_requested = FALSE AND lease_token = %s
                AND lease_expires_at > %s
            RETURNING run_id
            """,
            (
                RunStatus.FAILED.value,
                THREAD_CHECKPOINT_CONFLICT_CODE,
                error,
                completed_at,
                completed_at,
                run_id,
                tenant_id,
                RunStatus.RUNNING.value,
                lease_token,
                now_ms,
            ),
        ).fetchone()
        if updated is None:
            raise RuntimeError("Checkpoint conflict terminalization lost its Run lease")
        self._append_event_with_connection(
            connection,
            run_id,
            "checkpoint.conflict",
            {
                "phase": phase,
                "expected_revision": expected_revision,
                "observed_revision": observed_revision,
            },
        )
        self._append_event_with_connection(
            connection,
            run_id,
            "run.failed",
            {"error_code": THREAD_CHECKPOINT_CONFLICT_CODE, "error": error},
        )
        return error

    @staticmethod
    def _apply_checkpoint_conflict_to_run(
        run: RunRecord,
        *,
        error: str,
        completed_at: str,
    ) -> None:
        run.status = RunStatus.FAILED
        run.error_code = THREAD_CHECKPOINT_CONFLICT_CODE
        run.error = error
        run.completed_at = completed_at
        run.updated_at = completed_at
        run.lease_owner_id = None
        run.lease_token = None
        run.lease_heartbeat_at = None
        run.lease_expires_at = None

    # ---- state and row helpers ----

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
            run.cancel_requested,
            run.client_request_id,
            run.created_at,
            run.updated_at,
            run.started_at,
            run.completed_at,
            run.lease_owner_id,
            run.lease_token,
            run.lease_heartbeat_at,
            run.lease_expires_at,
            run.checkpoint_base_revision,
        )

    def _row_to_run(self, row: Row) -> RunRecord:
        domain_id = str(row["domain_id"])
        schema_version = str(row["schema_version"])
        input_payload = (
            json.loads(row["input_json"])
            if row.get("input_json")
            else {"user_message": row["input_message"]}
        )
        return RunRecord(
            run_id=str(row["run_id"]),
            tenant_id=str(row["tenant_id"]),
            thread_id=str(row["thread_id"]),
            agent_id=str(row["agent_id"]),
            agent_version=str(row["agent_version"]),
            domain_id=domain_id,
            schema_version=schema_version,
            status=RunStatus(row["status"]),
            input=input_payload,
            state=(
                self._deserialize_state(domain_id, schema_version, str(row["state_json"]))
                if row["state_json"]
                else None
            ),
            output_message=row["output_message"],
            validation_errors=json.loads(row["validation_errors_json"]),
            error_code=row["error_code"],
            error=row["error"],
            execution_authority=(
                json.loads(row["execution_authority_json"])
                if row["execution_authority_json"]
                else None
            ),
            attempt=int(row["attempt"]),
            cancel_requested=bool(row["cancel_requested"]),
            client_request_id=row["client_request_id"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            lease_owner_id=row["lease_owner_id"],
            lease_token=row["lease_token"],
            lease_heartbeat_at=(
                int(row["lease_heartbeat_at"])
                if row["lease_heartbeat_at"] is not None
                else None
            ),
            lease_expires_at=(
                int(row["lease_expires_at"])
                if row["lease_expires_at"] is not None
                else None
            ),
            checkpoint_base_revision=(
                int(row["checkpoint_base_revision"])
                if row["checkpoint_base_revision"] is not None
                else None
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
    def _thread_revision(
        connection,
        tenant_id: str,
        thread_id: str,
        *,
        for_update: bool = False,
    ) -> int:
        suffix = " FOR UPDATE" if for_update else ""
        row = connection.execute(
            "SELECT revision FROM thread_states WHERE tenant_id = %s AND thread_id = %s"
            + suffix,
            (tenant_id, thread_id),
        ).fetchone()
        return int(row["revision"]) if row is not None else 0

    @staticmethod
    def _assert_thread_schema_available(
        connection,
        tenant_id: str,
        thread_id: str,
        domain_id: str,
        schema_version: str,
    ) -> None:
        existing = connection.execute(
            """
            SELECT domain_id, schema_version FROM thread_states
            WHERE tenant_id = %s AND thread_id = %s
            """,
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
