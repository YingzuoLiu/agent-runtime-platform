from __future__ import annotations

import queue
import sqlite3
import threading
import traceback
from collections.abc import Callable
from uuid import uuid4

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionAuthority,
    RuntimeExecutionContext,
    RuntimeExecutionError,
    utc_now,
)
from .auth import TenantContext
from .external_actions import ExternalActionReconciliationPendingError
from .models import RunCreateRequest, RunRecord, RunStatus
from .registry import AgentRegistry, RuntimeRegistration
from .store import SQLiteRunStore


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
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        self.store = store
        self.registry = registry
        self.store.bind_state_registry(registry)
        self.worker_count = worker_count
        self._recovery_reconciliation_required = recovery_reconciliation_required
        self._queue: queue.Queue[tuple[str, bool] | None] = queue.Queue()
        self._workers: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            recoverable_runs = self.store.list_recoverable_runs()
            self._assert_recoverable_runs_registered(recoverable_runs)
            self._started = True
            for index in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"agent-runtime-worker-{index}",
                    daemon=True,
                )
                worker.start()
                self._workers.append(worker)
            for run in recoverable_runs:
                if run.status == RunStatus.RUNNING:
                    recovered = self.store.recover_run_for_restart(
                        run.run_id,
                        tenant_id=run.tenant_id,
                        reconciliation_pending_code=(
                            ExternalActionReconciliationPendingError.CODE
                        ),
                    )
                    if recovered is None:
                        continue
                    run = recovered
                self._queue.put((run.run_id, True))

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            for _ in self._workers:
                self._queue.put(None)
            for worker in self._workers:
                worker.join(timeout=5)
            self._workers.clear()
            self._started = False

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
                self.store.create_run(run)
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
            self.store.append_event(
                run.run_id,
                "run.queued",
                {
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
            if self._started:
                self._queue.put((run.run_id, False))
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
        while True:
            queue_item = self._queue.get()
            try:
                if queue_item is None:
                    return
                run_id, recovered_after_restart = queue_item
                self._execute_run(
                    run_id,
                    recovered_after_restart=recovered_after_restart,
                )
            finally:
                self._queue.task_done()

    def _execute_run(
        self,
        run_id: str,
        *,
        recovered_after_restart: bool = False,
    ) -> None:
        run = self._require_run(run_id)
        if run.status.is_terminal:
            return
        reconcile_before_cancelling = (
            recovered_after_restart
            and (
                run.error_code == ExternalActionReconciliationPendingError.CODE
                or (
                    self._recovery_reconciliation_required is not None
                    and self._recovery_reconciliation_required(run_id)
                )
            )
        )
        if run.cancel_requested and not reconcile_before_cancelling:
            self._mark_cancelled(run, reason="cancelled_before_start")
            return
        claimed = self.store.claim_run_start(run_id, tenant_id=run.tenant_id)
        if claimed is None:
            return
        run = claimed
        if run.cancel_requested and not reconcile_before_cancelling:
            self._mark_cancelled(run, reason="cancelled_after_start_claim")
            return
        try:
            runtime = self.registry.resolve(run.agent_id, run.agent_version)
            registration = self.registry.registration(run.agent_id, run.agent_version)
            if (
                run.domain_id != registration.domain_id
                or run.schema_version != registration.schema_version
            ):
                raise ValueError(
                    "Persisted run schema does not match its registered runtime: "
                    f"{run.domain_id}:{run.schema_version}"
                )
            persisted_state = self.store.load_thread_state(
                run.thread_id,
                tenant_id=run.tenant_id,
                domain_id=run.domain_id,
                schema_version=run.schema_version,
            )
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
            self.store.append_event(
                run.run_id,
                "checkpoint.loaded",
                {"source": state_source},
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
                ),
            )
            result_state = self._validate_runtime_state(
                registration,
                result.state,
                thread_id=run.thread_id,
                boundary="execute",
            )
            run.state = result_state
            run.output_message = result.message
            run.validation_errors = result.validation_errors
            run.error_code = None
            run.error = None
            if not self.store.finalize_completed_run(run):
                latest = self._require_run(run_id)
                latest.state = result_state
                latest.error_code = None
                latest.error = None
                self._mark_cancelled(latest, reason="cancelled_after_execution_boundary")
                return
        except ExternalActionReconciliationPendingError as exc:
            run = self._require_run(run_id)
            # Persist a non-terminal recovery marker.  It is sufficient on its
            # own to arbitrate restart-time cancellation even when a custom
            # RuntimeManager composition omitted the optional ledger callback.
            self.store.mark_reconciliation_pending(
                run_id,
                tenant_id=run.tenant_id,
                error_code=exc.code,
                error=f"{type(exc).__name__}: {exc}",
            )
            return
        except Exception as exc:  # pragma: no cover
            run = self._require_run(run_id)
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
                self.store.mark_reconciliation_pending(
                    run_id,
                    tenant_id=run.tenant_id,
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
            if run.cancel_requested and not external_action_safety_failure:
                self._mark_cancelled(run, reason="cancelled_during_failure_boundary")
                return
            run.status = RunStatus.FAILED
            run.error_code = (
                exc.code
                if isinstance(exc, RuntimeExecutionError)
                else "runtime_execution_failed"
            )
            run.error = f"{type(exc).__name__}: {exc}"
            run.completed_at = utc_now()
            self.store.update_run(run)
            event_payload = {
                "error_code": run.error_code,
                "error": run.error,
            }
            if not isinstance(exc, RuntimeExecutionError):
                event_payload["traceback"] = traceback.format_exc(limit=5)
            self.store.append_event(run.run_id, "run.failed", event_payload)

    def _mark_cancelled(self, run: RunRecord, *, reason: str) -> bool:
        return self.store.finalize_cancelled_run(run, reason=reason)

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
