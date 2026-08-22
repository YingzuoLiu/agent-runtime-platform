from __future__ import annotations

import sqlite3
import threading
import time
import traceback
from collections.abc import Callable
from uuid import uuid4

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionAuthority,
    RuntimeExecutionContext,
    RuntimeExecutionError,
)
from .auth import TenantContext
from .external_actions import ExternalActionReconciliationPendingError
from .models import RunCommitOutcome, RunCreateRequest, RunLeaseClaim, RunRecord, RunStatus
from .registry import AgentRegistry, RuntimeRegistration
from .store import SQLiteRunStore, ThreadCheckpointRevisionConflictError


class ReferencedRunNotFoundError(KeyError):
    """A tenant-scoped run reference is not visible to the authenticated caller."""


class RuntimeManager:
    """Durable run lifecycle manager with an in-process worker pool."""

    def __init__(
        self,
        store: SQLiteRunStore,
        registry: AgentRegistry,
        *,
        worker_count: int = 1,
        recovery_reconciliation_required: Callable[[str], bool] | None = None,
        owner_id: str | None = None,
        lease_duration_seconds: int = 30,
        heartbeat_interval_seconds: float = 10,
        poll_interval_seconds: float = 0.25,
        shutdown_grace_seconds: float = 5,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be positive")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if heartbeat_interval_seconds >= lease_duration_seconds:
            raise ValueError("heartbeat_interval_seconds must be shorter than the lease")
        if (
            heartbeat_interval_seconds + store.lease_operation_timeout_seconds
            >= lease_duration_seconds
        ):
            raise ValueError(
                "heartbeat interval plus lease-operation timeout must leave "
                "time before lease expiry"
            )
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if shutdown_grace_seconds < 0:
            raise ValueError("shutdown_grace_seconds must not be negative")
        self.store = store
        self.registry = registry
        self.store.bind_state_registry(registry)
        self.worker_count = worker_count
        self.owner_id = owner_id or f"manager_{uuid4().hex}"
        self.lease_duration_seconds = lease_duration_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.shutdown_grace_seconds = shutdown_grace_seconds
        self._recovery_reconciliation_required = recovery_reconciliation_required
        self._wake = threading.Event()
        self._stop_claiming = threading.Event()
        self._claim_admission_lock = threading.Lock()
        self._active_heartbeats: dict[
            str,
            tuple[threading.Event, threading.Event],
        ] = {}
        self._owned_claims: dict[str, str] = {}
        self._heartbeat_lock = threading.Lock()
        self._renewals_disabled = threading.Event()
        self._workers: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()
        self._stop_in_progress: threading.Event | None = None

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if any(worker.is_alive() for worker in self._workers):
                raise RuntimeError(
                    "RuntimeManager cannot restart while a previous worker is draining"
                )
            if self._workers:
                self._expire_drained_claims()
            self._workers.clear()
            recoverable_runs = self.store.list_recoverable_runs()
            self._assert_recoverable_runs_registered(recoverable_runs)
            self._stop_claiming.clear()
            self._wake.clear()
            with self._heartbeat_lock:
                self._renewals_disabled.clear()
            self._started = True
            for index in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"agent-runtime-worker-{index}",
                    daemon=True,
                )
                worker.start()
                self._workers.append(worker)
            self._wake.set()

    def stop(self) -> None:
        with self._lock:
            if self._stop_in_progress is not None:
                stop_complete = self._stop_in_progress
                stop_leader = False
            else:
                stop_complete = threading.Event()
                stop_leader = True
            if not self._started:
                return
            if stop_leader:
                self._stop_in_progress = stop_complete
                workers = list(self._workers)
                # Serialize the stop gate with the final check immediately
                # before claim_next_run. A worker may finish an already-admitted
                # claim, but it cannot acquire a new one after this gate closes.
                with self._claim_admission_lock:
                    self._stop_claiming.set()
                self._wake.set()

        if not stop_leader:
            # Coalesce concurrent calls into the stop generation they observed.
            # A waiter must never wake later and stop a newly started generation.
            stop_complete.wait()
            return

        try:
            deadline = time.monotonic() + self.shutdown_grace_seconds
            for worker in workers:
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
        finally:
            workers_still_alive = any(worker.is_alive() for worker in workers)
            with self._heartbeat_lock:
                # Latch the renewal cutoff before observing or modifying active
                # heartbeat registrations. A worker paused after claim but
                # before registration must not renew after stop() returns.
                self._renewals_disabled.set()
                if workers_still_alive:
                    for stop_heartbeat, lease_lost in self._active_heartbeats.values():
                        lease_lost.set()
                        stop_heartbeat.set()
            if not workers_still_alive:
                # All execution threads have stopped, so relinquishing their
                # non-terminal leases cannot expose a still-running stale writer.
                self._expire_drained_claims()
            with self._lock:
                self._workers = [worker for worker in workers if worker.is_alive()]
                self._started = False
                self._stop_in_progress = None
                stop_complete.set()

    def submit(
        self,
        request: RunCreateRequest,
        *,
        tenant_context: TenantContext,
    ) -> RunRecord:
        registration = self.registry.registration(request.agent_id, request.agent_version)
        assert request.input is not None
        runtime_input = registration.parse_input(request.input)
        self._assert_referenced_runs_visible(
            registration.referenced_run_ids(runtime_input),
            tenant_id=tenant_context.tenant_id,
        )
        state = registration.parse_state(request.state) if request.state is not None else None
        if state is not None and state.thread_id != request.thread_id:
            raise ValueError("state.thread_id must match request.thread_id")
        with self._lock:
            if request.client_request_id:
                existing = self.store.get_run_by_client_request_id(
                    tenant_context.tenant_id,
                    request.client_request_id,
                )
                if existing is not None:
                    return existing
            run = RunRecord(
                run_id=f"run_{uuid4().hex}",
                tenant_id=tenant_context.tenant_id,
                thread_id=request.thread_id,
                agent_id=request.agent_id,
                agent_version=request.agent_version,
                domain_id=registration.domain_id,
                schema_version=registration.schema_version,
                status=RunStatus.QUEUED,
                input=runtime_input.model_dump(mode="json"),
                state=state,
                execution_authority=tenant_context.execution_authority,
                client_request_id=request.client_request_id,
            )
            try:
                self.store.create_run_with_event(
                    run,
                    event_type="run.queued",
                    payload={
                        "agent_id": run.agent_id,
                        "agent_version": run.agent_version,
                        "domain_id": run.domain_id,
                        "schema_version": run.schema_version,
                        "tenant_id": run.tenant_id,
                        "subject_id": tenant_context.subject_id,
                        "thread_id": run.thread_id,
                        "client_request_id": run.client_request_id,
                    },
                )
            except sqlite3.IntegrityError:
                if not request.client_request_id:
                    raise
                existing = self.store.get_run_by_client_request_id(
                    tenant_context.tenant_id,
                    request.client_request_id,
                )
                if existing is None:
                    raise
                return existing
            if self._started:
                self._wake.set()
            return run

    def get_run(
        self,
        run_id: str,
        *,
        tenant_context: TenantContext,
    ) -> RunRecord | None:
        return self.store.get_run_for_tenant(run_id, tenant_context.tenant_id)

    def request_cancel(
        self,
        run_id: str,
        *,
        tenant_context: TenantContext,
    ) -> RunRecord:
        """Request cancellation of a run.

        Delegates entirely to the store's atomic compare-and-set: an
        already-terminal run is returned unchanged (no exception, no new
        event -- the store's CAS simply does not match it), a run not
        found raises KeyError (-> 404 at the API layer), and a genuine
        QUEUED/RUNNING -> cancel-requested transition is what actually
        appends a `run.cancel_requested` event.
        """
        return self.store.request_cancel_atomically(
            run_id,
            tenant_id=tenant_context.tenant_id,
        )

    def _worker_loop(self) -> None:
        while not self._stop_claiming.is_set():
            try:
                cancelled = self.store.finalize_next_queued_cancellation()
                if cancelled is not None:
                    continue
                with self._claim_admission_lock:
                    if self._stop_claiming.is_set():
                        return
                    claim = self.store.claim_next_run(
                        owner_id=self.owner_id,
                        lease_duration_seconds=self.lease_duration_seconds,
                        reconciliation_pending_code=(
                            ExternalActionReconciliationPendingError.CODE
                        ),
                    )
            except sqlite3.Error:
                self._wake.wait(self.poll_interval_seconds)
                self._wake.clear()
                continue
            if claim is not None:
                self._execute_claim(claim)
                continue
            self._wake.wait(self.poll_interval_seconds)
            self._wake.clear()

    def _execute_claim(self, claim: RunLeaseClaim) -> None:
        stop_heartbeat = threading.Event()
        lease_lost = threading.Event()
        with self._heartbeat_lock:
            if self._renewals_disabled.is_set():
                admitted = False
            else:
                admitted = True
                self._active_heartbeats[claim.lease_token] = (
                    stop_heartbeat,
                    lease_lost,
                )
                # At most one retained generation exists per Run. Replacing an
                # expired generation prevents recovery attempts from growing
                # this bookkeeping without bound.
                self._owned_claims[claim.run.run_id] = claim.lease_token
        if not admitted:
            # The claim transaction completed before shutdown closed renewal
            # admission, but Runtime code has not started. Relinquish it now;
            # a storage failure still falls back to natural lease expiry.
            try:
                self.store.expire_run_lease(
                    claim.run.run_id,
                    lease_token=claim.lease_token,
                )
            except Exception:
                pass
            return
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(claim, stop_heartbeat, lease_lost),
            name=f"agent-runtime-heartbeat-{claim.run.run_id}",
            daemon=True,
        )
        heartbeat.start()
        try:
            self._execute_owned_run(claim, lease_lost=lease_lost)
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=self.heartbeat_interval_seconds + 1)
            try:
                persisted = self.store.get_run_internal(claim.run.run_id)
            except Exception:
                # Cleanup is best effort. A transient read failure must not
                # kill the worker; the durable lease still expires normally.
                persisted = None
            with self._heartbeat_lock:
                self._active_heartbeats.pop(claim.lease_token, None)
                if persisted is None or persisted.lease_token != claim.lease_token:
                    if self._owned_claims.get(claim.run.run_id) == claim.lease_token:
                        self._owned_claims.pop(claim.run.run_id, None)

    def _expire_drained_claims(self) -> None:
        with self._heartbeat_lock:
            claims = list(self._owned_claims.items())
            self._owned_claims.clear()
        for run_id, lease_token in claims:
            try:
                self.store.expire_run_lease(run_id, lease_token=lease_token)
            except Exception:
                # Shutdown cleanup cannot make a lease less safe: failure to
                # relinquish leaves the store deadline as the authority.
                continue

    def _heartbeat_loop(
        self,
        claim: RunLeaseClaim,
        stop_heartbeat: threading.Event,
        lease_lost: threading.Event,
    ) -> None:
        while not stop_heartbeat.wait(self.heartbeat_interval_seconds):
            try:
                renewed = self.store.renew_run_lease(
                    claim.run.run_id,
                    lease_token=claim.lease_token,
                    lease_duration_seconds=self.lease_duration_seconds,
                )
            except Exception:
                renewed = False
            if not renewed:
                # A definite failed renewal permanently latches this attempt
                # stale. Never retry the same token after clock regression.
                lease_lost.set()
                return

    def _execute_owned_run(
        self,
        claim: RunLeaseClaim,
        *,
        lease_lost: threading.Event,
    ) -> None:
        run = claim.run
        recovered_after_restart = claim.recovery_reason is not None
        if lease_lost.is_set():
            return
        reconcile_before_cancelling = (
            recovered_after_restart
            and (
                run.error_code == ExternalActionReconciliationPendingError.CODE
                or (
                    self._recovery_reconciliation_required is not None
                    and self._recovery_reconciliation_required(run.run_id)
                )
            )
        )
        if run.cancel_requested and not reconcile_before_cancelling:
            self._mark_cancelled(
                run,
                reason="cancelled_after_start_claim",
                lease_token=claim.lease_token,
            )
            return
        try:
            registration = self.registry.registration(run.agent_id, run.agent_version)
            if (
                run.domain_id != registration.domain_id
                or run.schema_version != registration.schema_version
            ):
                raise ValueError(
                    "Persisted run schema does not match its registered runtime: "
                    f"{run.domain_id}:{run.schema_version}"
                )
            try:
                checkpoint = self.store.load_thread_state_snapshot(
                    run.thread_id,
                    tenant_id=run.tenant_id,
                    domain_id=run.domain_id,
                    schema_version=run.schema_version,
                    expected_revision=run.checkpoint_base_revision,
                    require_revision_match=True,
                )
            except ThreadCheckpointRevisionConflictError:
                if reconcile_before_cancelling:
                    # CAS can prevent a stale checkpoint write, but it cannot
                    # undo effects produced by executing an arbitrary Runtime
                    # against known-stale state. Quarantine this unsupported
                    # mixed-writer/corruption state without releasing the
                    # thread or hiding the unresolved external action.
                    self.store.quarantine_checkpoint_conflict_for_reconciliation(
                        run,
                        lease_token=claim.lease_token,
                        phase="load",
                    )
                    return
                outcome = self.store.commit_checkpoint_conflict(
                    run,
                    lease_token=claim.lease_token,
                    phase="load",
                )
                if outcome == RunCommitOutcome.CANCEL_REQUESTED:
                    latest = self._require_run(run.run_id)
                    self._mark_cancelled(
                        latest,
                        reason="cancelled_before_execution_boundary",
                        lease_token=claim.lease_token,
                    )
                return
            runtime = self.registry.resolve(run.agent_id, run.agent_version)
            persisted_state = checkpoint.state
            state_source = (
                "request"
                if run.state is not None
                else "thread_store"
                if persisted_state is not None
                else "new_state"
            )
            candidate_state = (
                run.state
                or persisted_state
                or runtime.initial_state(run.thread_id)
            )
            state = self._validate_runtime_state(
                registration,
                candidate_state,
                thread_id=run.thread_id,
                boundary="initial_state",
            )
            self.store.append_attempt_event(
                run.run_id,
                lease_token=claim.lease_token,
                event_type="checkpoint.loaded",
                payload={
                    "source": state_source,
                    "revision": checkpoint.revision,
                },
            )
            assert run.input is not None
            runtime_input = registration.parse_input(run.input)
            self._assert_referenced_runs_visible(
                registration.referenced_run_ids(runtime_input),
                tenant_id=run.tenant_id,
            )
            result = runtime.execute(
                state,
                runtime_input,
                RuntimeExecutionContext(
                    run_id=run.run_id,
                    thread_id=run.thread_id,
                    recovered_after_restart=recovered_after_restart,
                    authority=self._execution_authority(run),
                    lease_token=claim.lease_token,
                ),
            )
            result_state = self._validate_runtime_state(
                registration,
                result.state,
                thread_id=run.thread_id,
                boundary="execute",
            )
            if lease_lost.is_set():
                return
            run.state = result_state
            run.output_message = result.message
            run.validation_errors = result.validation_errors
            run.error_code = None
            run.error = None
            outcome = self.store.commit_completed_run(
                run,
                lease_token=claim.lease_token,
            )
            if outcome == RunCommitOutcome.CANCEL_REQUESTED:
                latest = self._require_run(run.run_id)
                latest.state = result_state
                latest.output_message = result.message
                latest.validation_errors = result.validation_errors
                latest.error_code = None
                latest.error = None
                self._mark_cancelled(
                    latest,
                    reason="cancelled_after_execution_boundary",
                    lease_token=claim.lease_token,
                )
            return
        except ExternalActionReconciliationPendingError as exc:
            if lease_lost.is_set():
                return
            # Persist a non-terminal recovery marker.  It is sufficient on its
            # own to arbitrate restart-time cancellation even when a custom
            # RuntimeManager composition omitted the optional ledger callback.
            self.store.commit_reconciliation_pending(
                run.run_id,
                tenant_id=run.tenant_id,
                lease_token=claim.lease_token,
                error_code=exc.code,
                error=f"{type(exc).__name__}: {exc}",
            )
            return
        except Exception as exc:  # pragma: no cover
            if lease_lost.is_set():
                return
            reconciliation_pending = (
                run.error_code == ExternalActionReconciliationPendingError.CODE
                or reconcile_before_cancelling
            )
            reconciliation_resolution_codes = {
                "external_action_outcome_unknown",
                "external_action_evidence_incomplete",
                "external_action_failed",
                "run_cancel_requested",
            }
            if reconciliation_pending and not (
                isinstance(exc, RuntimeExecutionError)
                and exc.code in reconciliation_resolution_codes
            ):
                # Setup, registry, input/state loading, or other local failures
                # cannot supersede a still-pending external-action outcome.
                pending = ExternalActionReconciliationPendingError()
                self.store.commit_reconciliation_pending(
                    run.run_id,
                    tenant_id=run.tenant_id,
                    lease_token=claim.lease_token,
                    error_code=pending.code,
                    error=f"{type(pending).__name__}: {pending}",
                )
                return
            external_action_safety_failure = (
                isinstance(exc, RuntimeExecutionError)
                and exc.code
                in {
                    "external_action_outcome_unknown",
                    "external_action_evidence_incomplete",
                    "external_action_failed",
                }
            )
            latest = self._require_run(run.run_id)
            if latest.cancel_requested and not external_action_safety_failure:
                self._mark_cancelled(
                    latest,
                    reason="cancelled_during_failure_boundary",
                    lease_token=claim.lease_token,
                )
                return
            error_code = (
                exc.code
                if isinstance(exc, RuntimeExecutionError)
                else "runtime_execution_failed"
            )
            error = f"{type(exc).__name__}: {exc}"
            outcome = self.store.commit_failed_run(
                run,
                lease_token=claim.lease_token,
                error_code=error_code,
                error=error,
                traceback_text=(
                    None
                    if isinstance(exc, RuntimeExecutionError)
                    else traceback.format_exc(limit=5)
                ),
                allow_cancel_requested=external_action_safety_failure,
            )
            if (
                outcome == RunCommitOutcome.CANCEL_REQUESTED
                and not external_action_safety_failure
            ):
                latest = self._require_run(run.run_id)
                self._mark_cancelled(
                    latest,
                    reason="cancelled_during_failure_boundary",
                    lease_token=claim.lease_token,
                )

    def _mark_cancelled(
        self,
        run: RunRecord,
        *,
        reason: str,
        lease_token: str,
    ) -> RunCommitOutcome:
        return self.store.commit_cancelled_run(
            run,
            reason=reason,
            lease_token=lease_token,
        )

    def _require_run(self, run_id: str) -> RunRecord:
        run = self.store.get_run_internal(run_id)
        if run is None:
            raise KeyError(f"Run not found: {run_id}")
        return run

    def _assert_recoverable_runs_registered(
        self,
        recoverable_runs: list[RunRecord],
    ) -> None:
        """Keep durable work recoverable when a deployment omits its extension."""

        for run in recoverable_runs:
            if (
                run.status == RunStatus.RUNNING
                and run.checkpoint_base_revision is None
            ):
                raise RuntimeError(
                    "RuntimeManager cannot start: running Run "
                    f"{run.run_id} has no checkpoint base revision; drain legacy "
                    "nonterminal Runs before enabling thread serialization"
                )
            try:
                registration = self.registry.registration(
                    run.agent_id,
                    run.agent_version,
                )
            except KeyError as exc:
                raise RuntimeError(
                    "RuntimeManager cannot start: recoverable run "
                    f"{run.run_id} requires unregistered Agent version "
                    f"{run.agent_id}:{run.agent_version}"
                ) from exc
            if (
                registration.domain_id != run.domain_id
                or registration.schema_version != run.schema_version
            ):
                raise RuntimeError(
                    "RuntimeManager cannot start: recoverable run "
                    f"{run.run_id} schema {run.domain_id}:{run.schema_version} "
                    "does not match its registered Agent version"
                )

    @staticmethod
    def _validate_runtime_state(
        registration: RuntimeRegistration,
        state: BaseRuntimeState,
        *,
        thread_id: str,
        boundary: str,
    ) -> BaseRuntimeState:
        """Fail closed before a third-party Runtime state reaches persistence."""

        try:
            validated = registration.parse_state(state)
        except Exception as exc:
            raise ValueError(
                f"Runtime {boundary} state does not match registered schema "
                f"{registration.domain_id}:{registration.schema_version}"
            ) from exc
        if validated.thread_id != thread_id:
            raise ValueError(
                f"Runtime {boundary} state.thread_id must match run.thread_id"
            )
        return validated

    @staticmethod
    def _execution_authority(run: RunRecord) -> RuntimeExecutionAuthority:
        authority = run.execution_authority
        if authority is None:
            return RuntimeExecutionAuthority(
                tenant_id=run.tenant_id,
                subject_id="legacy-unknown",
                permissions=(),
            )
        if authority.tenant_id != run.tenant_id:
            raise ValueError("Persisted execution authority tenant does not match run tenant")
        return authority

    def _assert_referenced_runs_visible(
        self,
        run_ids: tuple[str, ...],
        *,
        tenant_id: str,
    ) -> None:
        for run_id in run_ids:
            if self.store.get_run_for_tenant(run_id, tenant_id) is None:
                raise ReferencedRunNotFoundError("Referenced run not found")
