from __future__ import annotations

import queue
import sqlite3
import threading
import traceback
from uuid import uuid4

from agent.contracts import utc_now
from domains.travel.state import AgentState
from .models import RunCreateRequest, RunRecord, RunStatus
from .registry import AgentRegistry
from .store import SQLiteRunStore


class RuntimeManager:
    """Durable run lifecycle manager with an in-process worker pool."""

    def __init__(self, store: SQLiteRunStore, registry: AgentRegistry, *, worker_count: int = 1) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        self.store = store
        self.registry = registry
        self.worker_count = worker_count
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._workers: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            for index in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"agent-runtime-worker-{index}",
                    daemon=True,
                )
                worker.start()
                self._workers.append(worker)
            for run in self.store.list_recoverable_runs():
                if run.status == RunStatus.RUNNING:
                    run.status = RunStatus.QUEUED
                    run.started_at = None
                    run.error = None
                    self.store.update_run(run)
                    self.store.append_event(run.run_id, "run.recovered", {"reason": "runtime_restart"})
                self._queue.put(run.run_id)

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

    def submit(self, request: RunCreateRequest) -> RunRecord:
        self.registry.resolve(request.agent_id, request.agent_version)
        if request.client_request_id:
            existing = self.store.get_run_by_client_request_id(request.client_request_id)
            if existing is not None:
                return existing
        run = RunRecord(
            run_id=f"run_{uuid4().hex}",
            thread_id=request.thread_id,
            agent_id=request.agent_id,
            agent_version=request.agent_version,
            status=RunStatus.QUEUED,
            input_message=request.user_message,
            state=request.state,
            client_request_id=request.client_request_id,
        )
        try:
            self.store.create_run(run)
        except sqlite3.IntegrityError:
            if not request.client_request_id:
                raise
            existing = self.store.get_run_by_client_request_id(request.client_request_id)
            if existing is None:
                raise
            return existing
        self.store.append_event(
            run.run_id,
            "run.queued",
            {
                "agent_id": run.agent_id,
                "agent_version": run.agent_version,
                "thread_id": run.thread_id,
                "client_request_id": run.client_request_id,
            },
        )
        self._queue.put(run.run_id)
        return run

    def get_run(self, run_id: str) -> RunRecord | None:
        return self.store.get_run(run_id)

    def request_cancel(self, run_id: str) -> RunRecord:
        """Request cancellation of a run.

        Delegates entirely to the store's atomic compare-and-set: an
        already-terminal run is returned unchanged (no exception, no new
        event -- the store's CAS simply does not match it), a run not
        found raises KeyError (-> 404 at the API layer), and a genuine
        QUEUED/RUNNING -> cancel-requested transition is what actually
        appends a `run.cancel_requested` event.
        """
        return self.store.request_cancel_atomically(run_id)

    def _worker_loop(self) -> None:
        while True:
            run_id = self._queue.get()
            try:
                if run_id is None:
                    return
                self._execute_run(run_id)
            finally:
                self._queue.task_done()

    def _execute_run(self, run_id: str) -> None:
        run = self._require_run(run_id)
        if run.status.is_terminal:
            return
        if run.cancel_requested:
            self._mark_cancelled(run, reason="cancelled_before_start")
            return
        run.status = RunStatus.RUNNING
        run.started_at = utc_now()
        run.attempt += 1
        self.store.update_run(run)
        self.store.append_event(run.run_id, "run.started", {"attempt": run.attempt})
        try:
            runtime = self.registry.resolve(run.agent_id, run.agent_version)
            persisted_state = self.store.load_thread_state(run.thread_id)
            state = run.state or persisted_state or AgentState(thread_id=run.thread_id)
            self.store.append_event(
                run.run_id,
                "checkpoint.loaded",
                {"source": "request" if run.state is not None else "thread_store" if persisted_state is not None else "new_state"},
            )
            result = runtime.handle_user_message(state, run.input_message)
            run.state = result.state
            run.output_message = result.message
            run.validation_errors = result.validation_errors
            if not self.store.finalize_completed_run(run):
                latest = self._require_run(run_id)
                latest.state = result.state
                self._mark_cancelled(latest, reason="cancelled_after_execution_boundary")
                return
        except Exception as exc:  # pragma: no cover
            run = self._require_run(run_id)
            if run.cancel_requested:
                self._mark_cancelled(run, reason="cancelled_during_failure_boundary")
                return
            run.status = RunStatus.FAILED
            run.error = f"{type(exc).__name__}: {exc}"
            run.completed_at = utc_now()
            self.store.update_run(run)
            self.store.append_event(run.run_id, "run.failed", {"error": run.error, "traceback": traceback.format_exc(limit=5)})

    def _mark_cancelled(self, run: RunRecord, *, reason: str) -> bool:
        return self.store.finalize_cancelled_run(run, reason=reason)

    def _require_run(self, run_id: str) -> RunRecord:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Run not found: {run_id}")
        return run
