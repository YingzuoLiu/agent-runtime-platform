from __future__ import annotations

import sqlite3
from typing import Any, Protocol

from agent.contracts import BaseRuntimeState

from .models import RunCommitOutcome, RunEvent, RunLeaseClaim, RunRecord
from .quarantine import (
    QuarantineResolutionCommit,
    QuarantineResolutionKind,
    QuarantineResolutionPlan,
    QuarantineResolutionTarget,
)
from .store import StateRegistry, ThreadStateSnapshot


class RunStore(Protocol):
    """Structural Run/checkpoint persistence contract used by production consumers.

    The surface is intentionally derived from RuntimeManager, the HTTP/Action
    façades, and quarantine resolution. It is not a generic repository and it
    exposes no database path, connection, SQL dialect, or test fault hooks.

    Implementations must additionally preserve the executable I1-I9 semantics:
    store-authoritative leases, token fencing, tenant-qualified Thread
    serialization, checkpoint revision CAS, atomic Run/checkpoint/event commits,
    recovery precedence, inspectable quarantine, and evidence-bound repair.
    """

    @property
    def lease_operation_timeout_seconds(self) -> float:
        ...

    def bind_state_registry(self, state_registry: StateRegistry) -> None:
        ...

    def ping(self) -> None:
        ...

    def create_run_with_event(
        self,
        run: RunRecord,
        *,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunRecord:
        ...

    def get_run_internal(self, run_id: str) -> RunRecord | None:
        ...

    def get_run_for_tenant(
        self,
        run_id: str,
        tenant_id: str,
        *,
        timeout_seconds: float = 30,
    ) -> RunRecord | None:
        ...

    def get_run_by_client_request_id(
        self,
        tenant_id: str,
        client_request_id: str,
    ) -> RunRecord | None:
        ...

    def list_recoverable_runs(self) -> list[RunRecord]:
        ...

    def claim_next_run(
        self,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        reconciliation_pending_code: str | None = None,
    ) -> RunLeaseClaim | None:
        ...

    def renew_run_lease(
        self,
        run_id: str,
        *,
        lease_token: str,
        lease_duration_seconds: int,
    ) -> bool:
        ...

    def expire_run_lease(self, run_id: str, *, lease_token: str) -> bool:
        ...

    def append_attempt_event(
        self,
        run_id: str,
        *,
        lease_token: str | None,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        ...

    def append_control_plane_event(
        self,
        run_id: str,
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> RunEvent:
        ...

    def list_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RunEvent]:
        ...

    def list_events_for_tenant(
        self,
        run_id: str,
        tenant_id: str,
        *,
        after_sequence: int = 0,
    ) -> list[RunEvent]:
        ...

    def load_thread_state(
        self,
        thread_id: str,
        *,
        tenant_id: str,
        domain_id: str | None = None,
        schema_version: str | None = None,
    ) -> BaseRuntimeState | None:
        ...

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
        ...

    def finalize_next_queued_cancellation(self) -> RunRecord | None:
        ...

    def request_cancel_atomically(self, run_id: str, *, tenant_id: str) -> RunRecord:
        ...

    def commit_completed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        ...

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
        ...

    def commit_cancelled_run(
        self,
        run: RunRecord,
        *,
        reason: str,
        lease_token: str,
    ) -> RunCommitOutcome:
        ...

    def commit_reconciliation_pending(
        self,
        run_id: str,
        *,
        tenant_id: str,
        lease_token: str,
        error_code: str,
        error: str,
    ) -> RunCommitOutcome:
        ...

    def commit_checkpoint_conflict(
        self,
        run: RunRecord,
        *,
        lease_token: str,
        phase: str,
    ) -> RunCommitOutcome:
        ...

    def quarantine_checkpoint_conflict_for_reconciliation(
        self,
        run: RunRecord,
        *,
        lease_token: str,
        phase: str,
    ) -> RunCommitOutcome:
        ...

    def plan_quarantine_resolution(
        self,
        run_id: str,
        *,
        tenant_id: str,
        target: QuarantineResolutionTarget,
        resolution: QuarantineResolutionKind,
    ) -> QuarantineResolutionPlan:
        ...

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
        ...

    def verify_quarantine_resolution(
        self,
        commit: QuarantineResolutionCommit,
    ) -> None:
        ...


def _exception_chain(exc: BaseException):
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        next_exc = current.__cause__ or current.__context__
        current = next_exc if isinstance(next_exc, BaseException) else None


def _sqlstate(exc: BaseException) -> str | None:
    value = getattr(exc, "sqlstate", None)
    return value if isinstance(value, str) else None


def is_run_store_integrity_error(exc: BaseException) -> bool:
    """Recognize only database integrity conflicts needed for idempotent submit.

    This deliberately avoids importing a concrete PostgreSQL driver into the
    manager. SQLSTATE class 23 is the portable integrity-constraint family.
    """

    for current in _exception_chain(exc):
        if isinstance(current, sqlite3.IntegrityError):
            return True
        state = _sqlstate(current)
        if state is not None and state.startswith("23"):
            return True
    return False


def is_run_store_contention_error(exc: BaseException) -> bool:
    """Classify a bounded observation failure as transient DB contention."""

    for current in _exception_chain(exc):
        if isinstance(current, sqlite3.OperationalError):
            code = getattr(current, "sqlite_errorcode", None)
            if code is not None and code & 0xFF in {
                sqlite3.SQLITE_BUSY,
                sqlite3.SQLITE_LOCKED,
            }:
                return True
        state = _sqlstate(current)
        if state in {"40001", "40P01", "55P03", "57014"}:
            return True
    return False


def is_run_store_retryable_error(exc: BaseException) -> bool:
    """Recognize bounded, side-effect-free store-loop failures only.

    RuntimeManager uses this solely around queue/cancellation polling before
    Runtime or provider code is invoked. It is intentionally not a generic
    transaction/provider retry policy.
    """

    for current in _exception_chain(exc):
        if isinstance(current, sqlite3.Error):
            return True
        state = _sqlstate(current)
        if state is not None and (
            state.startswith("08") or state in {"40001", "40P01", "55P03", "57014"}
        ):
            return True
        if current.__class__.__module__.startswith("psycopg") and current.__class__.__name__ in {
            "OperationalError",
            "InterfaceError",
        }:
            return True
    return False
