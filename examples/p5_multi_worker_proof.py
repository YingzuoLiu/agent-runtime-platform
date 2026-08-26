from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

from runtime_service import AgentRegistry, RuntimeManager  # noqa: E402
from runtime_service.action_gateway import (  # noqa: E402
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    DurableActionInput,
    WebhookSendInput,
    action_client_request_id,
    action_thread_id,
)
from runtime_service.auth import RuntimePermission, TenantContext  # noqa: E402
from runtime_service.models import RunCreateRequest, RunRecord, RunStatus  # noqa: E402
from runtime_service.postgres_memory_store import PostgresMemoryStore  # noqa: E402
from runtime_service.postgres_schema import (  # noqa: E402
    bootstrap_postgres_application_schema,
)
from runtime_service.postgres_store import PostgresRunStore  # noqa: E402
from runtime_service.postgres_workflow_store import PostgresWorkflowStore  # noqa: E402
from runtime_service.storage import (  # noqa: E402
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)

if __package__:
    from examples.p5_postgres_probe import (
        P5RunSnapshot,
        drop_schema,
        expire_live_lease,
        idle_in_transaction_count,
        postgres_version,
        snapshot_run,
        terminate_backend,
    )
    from examples.p5_proof_worker import (
        P5_AGENT_ID,
        P5_AGENT_VERSION,
        P5ProofHooks,
        P5ProofRunStore,
        P5WorkerConfig,
        build_p5_registry,
        run_p5_worker,
    )
else:
    from p5_postgres_probe import (
        P5RunSnapshot,
        drop_schema,
        expire_live_lease,
        idle_in_transaction_count,
        postgres_version,
        snapshot_run,
        terminate_backend,
    )
    from p5_proof_worker import (
        P5_AGENT_ID,
        P5_AGENT_VERSION,
        P5ProofHooks,
        P5ProofRunStore,
        P5WorkerConfig,
        build_p5_registry,
        run_p5_worker,
    )


ARTIFACT_PATH = REPOSITORY_ROOT / "artifacts" / "p5-multi-worker-proof.json"
REPORT_VERSION = "p5-multi-worker-recovery:1"
HOOK_NAMES = (
    "claim.before",
    "claim.result",
    "run.claimed",
    "run.execution_entered",
    "checkpoint.commit_pending",
    "run.commit_result",
    "stale.mutations",
    "stale.process_paused",
    "replacement.authoritative_progress",
    "db.connection.open",
    "db.connection.failed",
)
TERMINAL_STATUSES = {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}
WAIT_SECONDS = 30.0
LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 50.0
ALL_SCENARIO_IDS = ("S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8")


class P5ProofFailure(RuntimeError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise P5ProofFailure(message)


def _git_value(*arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _wait_until(
    operation: Callable[[], Any],
    predicate: Callable[[Any], bool],
    *,
    description: str,
    workers: tuple[WorkerProcess, ...] = (),
    timeout: float = WAIT_SECONDS,
) -> Any:
    deadline = time.monotonic() + timeout
    last_value: Any = None
    while time.monotonic() < deadline:
        for worker in workers:
            worker.raise_if_failed()
        try:
            last_value = operation()
        except (P5ProofFailure, HTTPError, URLError, ConnectionError, TimeoutError):
            last_value = None
        if last_value is not None and predicate(last_value):
            return last_value
        time.sleep(0.03)
    raise P5ProofFailure(
        f"Timed out waiting for {description}; last safe observation: "
        f"{_safe_observation(last_value)}"
    )


def _safe_observation(value: Any) -> str:
    if isinstance(value, P5RunSnapshot):
        return json.dumps(value.public_dict(), sort_keys=True)
    if isinstance(value, RunRecord):
        return f"RunRecord(status={value.status.value}, attempt={value.attempt})"
    if isinstance(value, dict):
        allowed = {
            key: item
            for key, item in value.items()
            if key in {"scenario", "attempt_count", "effect_count", "waiting_for_release"}
        }
        return json.dumps(allowed, sort_keys=True)
    return type(value).__name__


@dataclass
class WorkerProcess:
    context: Any
    config: P5WorkerConfig
    hooks: P5ProofHooks
    process: Any | None = None
    start_event: Any | None = None
    ready_event: Any | None = None
    shutdown_event: Any | None = None
    failures: Any | None = None

    @classmethod
    def create(
        cls,
        context: Any,
        *,
        worker_id: str,
        dsn: str,
        schema: str,
        provider_url: str,
    ) -> WorkerProcess:
        return cls(
            context=context,
            config=P5WorkerConfig(
                worker_id=worker_id,
                dsn=dsn,
                schema=schema,
                provider_url=provider_url,
                lease_duration_seconds=LEASE_SECONDS,
                heartbeat_interval_seconds=HEARTBEAT_SECONDS,
            ),
            hooks=P5ProofHooks.create(context, HOOK_NAMES),
        )

    @property
    def worker_id(self) -> str:
        return self.config.worker_id

    @property
    def pid(self) -> int:
        if self.process is None or self.process.pid is None:
            raise P5ProofFailure(f"{self.worker_id} has no process PID")
        return int(self.process.pid)

    def spawn(self, *, start_claiming: bool) -> None:
        if self.process is not None and self.process.is_alive():
            raise P5ProofFailure(f"{self.worker_id} is already running")
        self.hooks = P5ProofHooks.create(self.context, HOOK_NAMES)
        self.start_event = self.context.Event()
        self.ready_event = self.context.Event()
        self.shutdown_event = self.context.Event()
        self.failures = self.context.Queue()
        self.process = self.context.Process(
            target=run_p5_worker,
            args=(
                self.config,
                self.hooks,
                self.start_event,
                self.ready_event,
                self.shutdown_event,
                self.failures,
            ),
            name=self.worker_id,
        )
        self.process.start()
        deadline = time.monotonic() + WAIT_SECONDS
        while not self.ready_event.wait(0.05):
            self.raise_if_failed()
            if time.monotonic() >= deadline:
                raise P5ProofFailure(f"{self.worker_id} did not initialize")
        self.raise_if_failed()
        if start_claiming:
            self.start_event.set()

    def start_claiming(self) -> None:
        if self.start_event is None:
            raise P5ProofFailure(f"{self.worker_id} start event is missing")
        self.start_event.set()

    def arm(self, *names: str) -> None:
        self.hooks.arm(*names)

    def wait_hook(self, name: str, *, timeout: float = WAIT_SECONDS) -> dict[str, Any]:
        hook = self.hooks.hooks[name]
        generation = hook.current_generation()
        deadline = time.monotonic() + timeout
        while hook.reached_generation() < generation:
            hook.reached.wait(0.05)
            self.raise_if_failed()
            if time.monotonic() >= deadline:
                self.dump_thread_stacks(
                    reason=f"timeout waiting for {name}",
                    hook_states={
                        hook_name: candidate.generation_state()
                        for hook_name, candidate in self.hooks.hooks.items()
                    },
                )
                raise P5ProofFailure(f"{self.worker_id} did not reach {name}")
        self.raise_if_failed()
        try:
            payload_generation, payload = hook.metadata.get(timeout=1)
        except queue.Empty:
            raise P5ProofFailure(f"{self.worker_id} {name} metadata is missing") from None
        _require(
            payload_generation == generation,
            f"{self.worker_id} {name} metadata generation is invalid",
        )
        _require(isinstance(payload, dict), f"{self.worker_id} {name} metadata is invalid")
        return payload

    def dump_thread_stacks(
        self,
        *,
        reason: str,
        hook_states: dict[str, dict[str, int]] | None = None,
    ) -> None:
        """Ask a live proof worker for bounded, locals-free stack diagnostics."""

        if self.process is None or not self.process.is_alive():
            return
        print(
            json.dumps(
                {
                    "diagnostic": "p5-worker-thread-stacks",
                    "hook_states": hook_states or {},
                    "reason": reason,
                    "worker": self.worker_id,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        os.kill(self.pid, signal.SIGUSR1)
        time.sleep(0.1)

    def release(self, name: str) -> None:
        self.hooks.hooks[name].release_current()

    def release_all(self) -> None:
        for hook in self.hooks.hooks.values():
            hook.release_current()

    def suspend(self) -> None:
        os.kill(self.pid, signal.SIGSTOP)

    def resume(self) -> None:
        os.kill(self.pid, signal.SIGCONT)

    def kill(self) -> None:
        if self.process is None:
            return
        self.process.kill()
        self.process.join(timeout=5)
        _require(not self.process.is_alive(), f"{self.worker_id} did not stop after SIGKILL")

    def stop(self) -> None:
        if self.process is None or not self.process.is_alive():
            return
        self.release_all()
        if self.shutdown_event is not None:
            self.shutdown_event.set()
        self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5)

    def raise_if_failed(self) -> None:
        if self.failures is not None:
            try:
                failure = self.failures.get_nowait()
            except queue.Empty:
                failure = None
            if failure is not None:
                error_type = failure.get("error_type", "unknown")
                raise P5ProofFailure(f"{self.worker_id} failed with {error_type}")
        if self.process is not None and not self.process.is_alive():
            if self.process.exitcode not in {None, 0, -signal.SIGKILL}:
                raise P5ProofFailure(
                    f"{self.worker_id} exited unexpectedly with code {self.process.exitcode}"
                )


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    accepted_statuses: frozenset[int] = frozenset({200}),
) -> tuple[int, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = Request(url, method=method, data=body, headers=headers)
    try:
        with urlopen(request, timeout=5) as response:
            status = response.status
            raw = response.read()
    except HTTPError as exc:
        status = exc.code
        raw = exc.read()
    if status not in accepted_statuses:
        raise P5ProofFailure(f"Provider proof endpoint returned HTTP {status}")
    try:
        return status, json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise P5ProofFailure("Provider proof endpoint returned invalid JSON") from None


@dataclass
class ProviderProcess:
    database_path: Path
    port: int
    process: subprocess.Popen[str] | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        environment = os.environ.copy()
        environment["DEMO_PROVIDER_DB_PATH"] = str(self.database_path)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "demo_provider.app:create_demo_provider_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--no-access-log",
                "--log-level",
                "warning",
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        _wait_until(
            lambda: _http_json(f"{self.url}/health")[1],
            lambda payload: payload == {"status": "ok"},
            description="independent provider startup",
            timeout=15,
        )

    def snapshot(self, run_id: str) -> dict[str, Any] | None:
        status, payload = _http_json(
            f"{self.url}/proof/actions/{run_id}",
            accepted_statuses=frozenset({200, 404}),
        )
        return payload if status == 200 and isinstance(payload, dict) else None

    def release(self, run_id: str) -> None:
        status, _ = _http_json(
            f"{self.url}/proof/actions/{run_id}/release",
            method="POST",
            payload={},
            accepted_statuses=frozenset({200, 404}),
        )
        _require(status == 200, "Provider did not release the held action")

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


class ProofController:
    def __init__(self, *, dsn: str, schema: str, provider: ProviderProcess) -> None:
        self.dsn = dsn
        self.schema = schema
        self.provider = provider
        config = resolve_runtime_storage_config(
            backend="postgres",
            postgres_dsn=dsn,
            postgres_schema=schema,
            environment={},
        )
        self.bundle = build_runtime_store_bundle(config)
        _require(
            isinstance(self.bundle.run_store, PostgresRunStore)
            and isinstance(self.bundle.workflow_store, PostgresWorkflowStore)
            and isinstance(self.bundle.memory_store, PostgresMemoryStore),
            "P5 controller did not receive the PostgreSQL store bundle",
        )
        context = multiprocessing.get_context("spawn")
        self.workers = (
            WorkerProcess.create(
                context,
                worker_id="p5-worker-a",
                dsn=dsn,
                schema=schema,
                provider_url=provider.url,
            ),
            WorkerProcess.create(
                context,
                worker_id="p5-worker-b",
                dsn=dsn,
                schema=schema,
                provider_url=provider.url,
            ),
        )
        controller_hooks = P5ProofHooks.create(context, HOOK_NAMES)
        controller_store = P5ProofRunStore(
            self.bundle.run_store,
            worker_id="p5-controller",
            hooks=controller_hooks,
        )
        self.registry: AgentRegistry = build_p5_registry(
            worker_id="p5-controller",
            hooks=controller_hooks,
            run_store=controller_store,
            workflow_store=self.bundle.workflow_store,
            memory_store=self.bundle.memory_store,
            provider_url=provider.url,
        )
        self.manager = RuntimeManager(
            controller_store,
            self.registry,
            worker_count=1,
            owner_id="p5-controller-non-worker",
        )
        self.tenant = TenantContext(
            tenant_id="p5-proof-tenant",
            subject_id="p5-proof-subject",
            permissions=tuple(permission.value for permission in RuntimePermission),
        )

    @property
    def worker_a(self) -> WorkerProcess:
        return self.workers[0]

    @property
    def worker_b(self) -> WorkerProcess:
        return self.workers[1]

    def start_workers(self) -> None:
        for worker in self.workers:
            worker.spawn(start_claiming=False)

    def stop_workers(self) -> None:
        for worker in self.workers:
            try:
                worker.resume()
            except (ProcessLookupError, P5ProofFailure):
                pass
        for worker in self.workers:
            worker.stop()

    def restart_worker(self, worker: WorkerProcess) -> None:
        worker.spawn(start_claiming=True)

    def submit_basic(
        self,
        marker: str,
        *,
        thread_id: str | None = None,
        scenario: str = "basic",
    ) -> RunRecord:
        return self.manager.submit(
            RunCreateRequest(
                thread_id=thread_id or f"p5-thread-{uuid.uuid4().hex}",
                agent_id=P5_AGENT_ID,
                agent_version=P5_AGENT_VERSION,
                input={"scenario": scenario, "marker": marker},
                client_request_id=f"p5-request-{uuid.uuid4().hex}",
            ),
            tenant_context=self.tenant,
        )

    def submit_action(self, destination: str) -> RunRecord:
        idempotency_key = f"p5-action-{destination}-{uuid.uuid4().hex}"
        runtime_input = DurableActionInput(
            action_type="webhook.send",
            destination=destination,
            idempotency_key=idempotency_key,
            input=WebhookSendInput(payload={"proof": REPORT_VERSION}),
        )
        return self.manager.submit(
            RunCreateRequest(
                thread_id=action_thread_id(
                    self.tenant.tenant_id,
                    runtime_input.action_type,
                    idempotency_key,
                ),
                agent_id=ACTION_AGENT_ID,
                agent_version=ACTION_AGENT_VERSION,
                input=runtime_input.model_dump(mode="json"),
                client_request_id=action_client_request_id(idempotency_key),
            ),
            tenant_context=self.tenant,
        )

    def wait_terminal(self, run_id: str) -> RunRecord:
        return _wait_until(
            lambda: self.bundle.run_store.get_run_internal(run_id),
            lambda run: run.status in TERMINAL_STATUSES,
            description=f"terminal Run {run_id}",
            workers=self.workers,
        )

    def designated_claim(
        self,
        worker: WorkerProcess,
        submit: Callable[[], RunRecord],
        *,
        hold_at: str = "run.claimed",
    ) -> tuple[RunRecord, dict[str, Any]]:
        other = self.worker_b if worker is self.worker_a else self.worker_a
        worker.arm("claim.before", hold_at)
        other.arm("claim.before", "claim.result")
        worker.wait_hook("claim.before")
        other.wait_hook("claim.before")
        run = submit()
        worker.release("claim.before")
        metadata = worker.wait_hook(hold_at)
        _require(metadata.get("run_id") == run.run_id, "Designated worker claimed wrong Run")
        other.release("claim.before")
        competing = other.wait_hook("claim.result")
        _require(
            competing.get("claimed") is False,
            "Competing worker acquired the same live Run",
        )
        other.release("claim.result")
        observed = snapshot_run(self.dsn, self.schema, run.run_id)
        _require(
            observed.attempt == 1
            and observed.lease_owner_id == worker.worker_id
            and observed.lease_live,
            "Designated claim did not create one live attempt",
        )
        return run, metadata


def _terminal_summary(run: RunRecord) -> dict[str, Any]:
    state = run.state.model_dump(mode="json") if run.state is not None else None
    safe_state = None
    if isinstance(state, dict):
        safe_state = {
            key: state.get(key)
            for key in ("completed_by", "completed_attempt", "marker")
            if key in state
        }
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "attempt": run.attempt,
        "error_code": run.error_code,
        "state": safe_state,
    }


def _claim_counts(snapshot: P5RunSnapshot) -> dict[str, Any]:
    return {
        "attempt": snapshot.attempt,
        "run_started_count": snapshot.run_started_count,
        "run_recovered_count": snapshot.run_recovered_count,
        "recovery_reasons": list(snapshot.recovery_reasons),
    }


def _scenario_s1(controller: ProofController) -> dict[str, Any]:
    run, _ = controller.designated_claim(
        controller.worker_a,
        lambda: controller.submit_basic("s1-single-claim"),
    )
    pre_commit = snapshot_run(controller.dsn, controller.schema, run.run_id)
    controller.worker_a.release("run.claimed")
    terminal = controller.wait_terminal(run.run_id)
    final = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        terminal.status == RunStatus.COMPLETED
        and final.attempt == 1
        and final.run_started_count == 1,
        "S1 violated single-claim safety",
    )
    return {
        "id": "S1",
        "result": "passed",
        "assertion": "one Run was claimed by exactly one process",
        "invariants": {"single_live_owner": True, "only_owner_executed": True},
        "owner": pre_commit.lease_owner_id,
        "terminal": _terminal_summary(terminal),
        "claims": _claim_counts(final),
    }


def _scenario_s2(controller: ProofController) -> dict[str, Any]:
    worker_a, worker_b = controller.workers
    worker_a.arm("claim.before", "run.claimed", "run.execution_entered")
    worker_b.arm("claim.before")
    worker_a.wait_hook("claim.before")
    worker_b.wait_hook("claim.before")
    thread_id = f"p5-shared-thread-{uuid.uuid4().hex}"
    first = controller.submit_basic("s2-first", thread_id=thread_id)
    second = controller.submit_basic("s2-second", thread_id=thread_id)
    worker_a.release("claim.before")
    claimed = worker_a.wait_hook("run.claimed")
    _require(claimed.get("run_id") == first.run_id, "S2 did not claim the oldest Run")
    worker_b.release("claim.before")
    worker_a.release("run.claimed")
    worker_a.wait_hook("run.execution_entered")
    second_while_first_runs = snapshot_run(
        controller.dsn,
        controller.schema,
        second.run_id,
    )
    _require(
        second_while_first_runs.status == "queued" and second_while_first_runs.attempt == 0,
        "S2 allowed two active Runs for one tenant/thread",
    )
    worker_a.release("run.execution_entered")
    first_terminal = controller.wait_terminal(first.run_id)
    second_terminal = controller.wait_terminal(second.run_id)
    _require(
        first_terminal.status == RunStatus.COMPLETED
        and second_terminal.status == RunStatus.COMPLETED
        and first_terminal.checkpoint_base_revision == 0
        and second_terminal.checkpoint_base_revision == 1,
        "S2 Runs did not serialize to completion",
    )
    return {
        "id": "S2",
        "result": "passed",
        "assertion": "same tenant/thread had at most one active Run",
        "invariants": {"same_thread_serialized": True, "checkpoint_revision_advanced": True},
        "second_while_first_running": second_while_first_runs.public_dict(),
        "terminal_order": [first.run_id, second.run_id],
        "checkpoint_base_revisions": [
            first_terminal.checkpoint_base_revision,
            second_terminal.checkpoint_base_revision,
        ],
    }


def _scenario_s3(controller: ProofController) -> dict[str, Any]:
    worker_a, worker_b = controller.workers
    worker_a.arm("claim.before", "run.claimed", "run.execution_entered")
    worker_b.arm("claim.before", "run.claimed", "run.execution_entered")
    worker_a.wait_hook("claim.before")
    worker_b.wait_hook("claim.before")
    first = controller.submit_basic("s3-overlap-a")
    second = controller.submit_basic("s3-overlap-b")
    worker_a.release("claim.before")
    first_claim = worker_a.wait_hook("run.claimed")
    worker_b.release("claim.before")
    second_claim = worker_b.wait_hook("run.claimed")
    _require(
        first_claim.get("run_id") == first.run_id
        and second_claim.get("run_id") == second.run_id,
        "S3 workers did not claim the two different Threads independently",
    )
    worker_a.release("run.claimed")
    worker_b.release("run.claimed")
    entered_a = worker_a.wait_hook("run.execution_entered")
    entered_b = worker_b.wait_hook("run.execution_entered")
    running_a = snapshot_run(controller.dsn, controller.schema, first.run_id)
    running_b = snapshot_run(controller.dsn, controller.schema, second.run_id)
    _require(
        running_a.status == "running"
        and running_a.lease_live
        and running_b.status == "running"
        and running_b.lease_live,
        "S3 did not observe both different-Thread executions live concurrently",
    )
    worker_a.release("run.execution_entered")
    worker_b.release("run.execution_entered")
    first_terminal = controller.wait_terminal(first.run_id)
    second_terminal = controller.wait_terminal(second.run_id)
    _require(
        first_terminal.status == RunStatus.COMPLETED
        and second_terminal.status == RunStatus.COMPLETED,
        "S3 overlapping Runs did not complete",
    )

    # Both worker Managers now return to their process-local wait events. A
    # submission through the controller Manager cannot signal either event;
    # bounded durable polling must still discover it.
    time.sleep(0.1)
    poll_started = time.monotonic()
    poll_run = controller.submit_basic("s3-durable-poll-control")
    try:
        poll_terminal = controller.wait_terminal(poll_run.run_id)
    except P5ProofFailure:
        raise P5ProofFailure(
            "S3 durable polling did not discover a cross-process submission"
        ) from None
    poll_elapsed_ms = int((time.monotonic() - poll_started) * 1000)
    _require(
        poll_terminal.status == RunStatus.COMPLETED and poll_elapsed_ms < 5_000,
        "S3 durable polling did not discover a cross-process submission",
    )
    return {
        "id": "S3",
        "result": "passed",
        "assertion": "different Threads overlapped and durable polling remained authoritative",
        "invariants": {"different_threads_overlapped": True, "durable_polling_progressed": True},
        "overlap": {
            "both_execution_barriers_reached_before_either_release": True,
            "workers": [entered_a.get("worker"), entered_b.get("worker")],
            "runs": [first.run_id, second.run_id],
            "both_live_in_postgres": True,
        },
        "durable_poll_control": {
            "run_id": poll_run.run_id,
            "elapsed_ms": poll_elapsed_ms,
            "completed_without_cross_process_wake": True,
        },
    }


def _scenario_s4(controller: ProofController) -> dict[str, Any]:
    worker_a = controller.worker_a
    run, _ = controller.designated_claim(
        worker_a,
        lambda: controller.submit_basic("s4-crash-before-commit"),
        hold_at="checkpoint.commit_pending",
    )
    before_kill = snapshot_run(controller.dsn, controller.schema, run.run_id)
    worker_a.kill()
    live_after_kill = snapshot_run(controller.dsn, controller.schema, run.run_id)
    time.sleep(0.2)
    still_live = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        live_after_kill.attempt == 1
        and live_after_kill.lease_live
        and still_live.attempt == 1
        and still_live.run_recovered_count == 0,
        "S4 stole a live lease after worker death",
    )
    expired = expire_live_lease(
        controller.dsn,
        controller.schema,
        run.run_id,
        expected_attempt=1,
    )
    terminal = controller.wait_terminal(run.run_id)
    recovered = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        terminal.status == RunStatus.COMPLETED
        and recovered.attempt == 2
        and recovered.recovery_reasons == ("lease_expired",)
        and recovered.checkpoint_saved_count == 1
        and recovered.run_completed_count == 1,
        "S4 did not recover exactly once after exact lease expiry",
    )
    controller.restart_worker(worker_a)
    return {
        "id": "S4",
        "result": "passed",
        "assertion": "SIGKILL before commit produced one expiry takeover and one checkpoint",
        "invariants": {
            "live_lease_not_stolen": True,
            "exact_expiry_takeover_once": True,
            "coherent_terminal_commit": True,
        },
        "before_kill": _claim_counts(before_kill),
        "exact_expiry": expired.public_dict(),
        "after_recovery": _claim_counts(recovered),
        "checkpoint_saved_count": recovered.checkpoint_saved_count,
        "terminal": _terminal_summary(terminal),
    }


def _scenario_s5(controller: ProofController) -> dict[str, Any]:
    worker_a, worker_b = controller.workers
    run, _ = controller.designated_claim(
        worker_a,
        lambda: controller.submit_basic(
            "s5-stale-writer",
            scenario="stale_mutations",
        ),
        hold_at="stale.process_paused",
    )
    worker_b.arm("replacement.authoritative_progress")
    expired = expire_live_lease(
        controller.dsn,
        controller.schema,
        run.run_id,
        expected_attempt=1,
    )
    replacement_progress = worker_b.wait_hook("replacement.authoritative_progress")
    replacement_snapshot = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        replacement_progress.get("run_id") == run.run_id
        and replacement_snapshot.status == "running"
        and replacement_snapshot.attempt == 2
        and replacement_snapshot.lease_live
        and replacement_snapshot.lease_owner_id == worker_b.worker_id,
        "S5 replacement attempt did not persist authoritative live progress",
    )
    worker_a.arm("stale.mutations", "run.commit_result")
    worker_a.resume()
    stale = worker_a.wait_hook("stale.mutations")
    results = stale.get("results")
    expected_keys = {
        "run_event",
        "workflow",
        "memory_snapshot",
        "memory_mutation",
        "evidence_mirror",
        "external_action",
    }
    _require(
        isinstance(results, dict)
        and set(results) == expected_keys
        and set(results.values()) == {"run_lease_lost"},
        "S5 stale writer crossed a lease-fenced mutation boundary",
    )
    worker_a.release("stale.mutations")
    commit = worker_a.wait_hook("run.commit_result")
    _require(
        commit.get("outcome") == "lease_lost",
        "S5 stale terminal commit was not rejected by the current lease",
    )
    worker_a.release("run.commit_result")
    worker_b.release("replacement.authoritative_progress")
    replacement = controller.wait_terminal(run.run_id)
    final = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        replacement.status == RunStatus.FAILED
        and replacement.error_code == "external_action_outcome_unknown"
        and final.run_completed_count == 0
        and final.run_failed_count == 1,
        "S5 stale attempt changed terminal evidence",
    )
    return {
        "id": "S5",
        "result": "passed",
        "assertion": "resumed stale writer was fenced at every durable mutation",
        "invariants": {
            "replacement_lease_remained_live": True,
            "all_sampled_stale_mutations_fenced": True,
            "replacement_evidence_preserved": True,
        },
        "exact_expiry": expired.public_dict(),
        "mutation_results": results,
        "stale_commit_outcome": commit.get("outcome"),
        "terminal": _terminal_summary(replacement),
    }


def _provider_summary(state: dict[str, Any]) -> dict[str, Any]:
    events = state.get("events")
    if not isinstance(events, list):
        raise P5ProofFailure("Provider event evidence is missing")
    event_types = [
        event.get("event_type")
        for event in events
        if isinstance(event, dict)
    ]
    return {
        "scenario": state.get("scenario"),
        "attempt_count": state.get("attempt_count"),
        "effect_count": state.get("effect_count"),
        "request_identity_count": state.get("request_identity_count"),
        "idempotency_identity_count": state.get("idempotency_identity_count"),
        "event_types": event_types,
    }


def _action_evidence(controller: ProofController, run_id: str) -> dict[str, Any]:
    action = controller.bundle.workflow_store.get_external_action(run_id, "dispatch")
    if action is None:
        raise P5ProofFailure("Durable Action ledger row is missing")
    workflow_events = controller.bundle.workflow_store.list_events(run_id)
    run_events = controller.bundle.run_store.list_events(run_id)
    return {
        "provider_name": action.provider_name,
        "status": action.status.value,
        "retry_mode": action.retry_mode.value,
        "dispatch_count": action.dispatch_count,
        "workflow_event_types": [
            event.event_type
            for event in workflow_events
            if event.event_type.startswith("external_action.")
        ],
        "run_mirror_event_types": [
            event.event_type
            for event in run_events
            if event.event_type.startswith("external_action.")
        ],
    }


def _crash_action(
    controller: ProofController,
    *,
    destination: str,
    expected_status: RunStatus,
    expected_error: str | None,
    expected_attempts: int,
    expected_retry_mode: str,
    expected_action_status: str,
) -> dict[str, Any]:
    worker_a = controller.worker_a
    run, _ = controller.designated_claim(
        worker_a,
        lambda: controller.submit_action(destination),
    )
    worker_a.release("run.claimed")
    held = _wait_until(
        lambda: controller.provider.snapshot(run.run_id),
        lambda state: state.get("waiting_for_release") is True
        and state.get("effect_count") == 1,
        description=f"{destination} provider effect",
        workers=controller.workers,
    )
    _require(held.get("attempt_count") == 1, f"{destination} dispatched too early")
    worker_a.kill()
    live = snapshot_run(controller.dsn, controller.schema, run.run_id)
    controller.provider.release(run.run_id)
    _wait_until(
        lambda: controller.provider.snapshot(run.run_id),
        lambda state: any(
            event.get("event_type") == "response.ambiguous"
            for event in state.get("events", [])
            if isinstance(event, dict)
        ),
        description=f"{destination} ambiguous response",
    )
    pre_expiry = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        live.lease_live and pre_expiry.attempt == 1 and pre_expiry.run_recovered_count == 0,
        f"{destination} was recovered before lease expiry",
    )
    expire_live_lease(
        controller.dsn,
        controller.schema,
        run.run_id,
        expected_attempt=1,
    )

    def observe_terminal() -> RunRecord | None:
        provider_state = controller.provider.snapshot(run.run_id)
        if (
            isinstance(provider_state, dict)
            and provider_state.get("waiting_for_release") is True
            and isinstance(provider_state.get("attempt_count"), int)
            and provider_state["attempt_count"] > expected_attempts
        ):
            # Baseline unsafe recovery never enters this branch. If policy is
            # mutated to replay it, release the injected second ambiguity so
            # the proof reaches its exact effect/ledger assertions instead of
            # timing out inside the fault provider.
            controller.provider.release(run.run_id)
        return controller.bundle.run_store.get_run_internal(run.run_id)

    terminal = _wait_until(
        observe_terminal,
        lambda candidate: candidate.status in TERMINAL_STATUSES,
        description=f"terminal Run {run.run_id}",
        workers=controller.workers,
    )
    provider_state = _wait_until(
        lambda: controller.provider.snapshot(run.run_id),
        lambda state: state.get("attempt_count", 0) >= expected_attempts,
        description=f"{destination} provider terminal evidence",
    )
    snapshot = snapshot_run(controller.dsn, controller.schema, run.run_id)
    ledger = _action_evidence(controller, run.run_id)
    _require(
        terminal.status == expected_status
        and terminal.error_code == expected_error
        and snapshot.attempt == 2
        and snapshot.recovery_reasons == ("lease_expired",)
        and provider_state.get("effect_count") == 1
        and provider_state.get("request_identity_count") == 1
        and provider_state.get("idempotency_identity_count") == 1
        and ledger["retry_mode"] == expected_retry_mode
        and ledger["status"] == expected_action_status
        and ledger["dispatch_count"] == expected_attempts
        and ledger["workflow_event_types"] == ledger["run_mirror_event_types"],
        f"{destination} recovery semantics are wrong",
    )
    controller.restart_worker(worker_a)
    return {
        "destination": destination,
        "terminal": _terminal_summary(terminal),
        "provider": _provider_summary(provider_state),
        "durable_action_ledger": ledger,
        "claims": _claim_counts(snapshot),
    }


def _control_action(
    controller: ProofController,
    *,
    destination: str,
    expected_status: RunStatus,
    expected_error: str | None,
    expected_effects: int,
    expected_retry_mode: str,
    expected_action_status: str,
) -> dict[str, Any]:
    run = controller.submit_action(destination)
    terminal = controller.wait_terminal(run.run_id)
    provider_state = _wait_until(
        lambda: controller.provider.snapshot(run.run_id),
        lambda state: state.get("attempt_count") == 1,
        description=f"{destination} control evidence",
    )
    ledger = _action_evidence(controller, run.run_id)
    _require(
        terminal.status == expected_status
        and terminal.error_code == expected_error
        and provider_state.get("effect_count") == expected_effects
        and provider_state.get("request_identity_count") == 1
        and provider_state.get("idempotency_identity_count") == 1
        and ledger["retry_mode"] == expected_retry_mode
        and ledger["status"] == expected_action_status
        and ledger["workflow_event_types"] == ledger["run_mirror_event_types"],
        f"{destination} definitive control semantics are wrong",
    )
    return {
        "destination": destination,
        "terminal": _terminal_summary(terminal),
        "provider": _provider_summary(provider_state),
        "durable_action_ledger": ledger,
    }


def _scenario_s6(controller: ProofController) -> dict[str, Any]:
    idempotent = _crash_action(
        controller,
        destination="p5-idempotent",
        expected_status=RunStatus.COMPLETED,
        expected_error=None,
        expected_attempts=2,
        expected_retry_mode="provider_idempotent",
        expected_action_status="succeeded",
    )
    unsafe = _crash_action(
        controller,
        destination="p5-unsafe",
        expected_status=RunStatus.FAILED,
        expected_error="external_action_outcome_unknown",
        expected_attempts=1,
        expected_retry_mode="unsafe",
        expected_action_status="outcome_unknown",
    )
    known_success = _control_action(
        controller,
        destination="p5-known-success",
        expected_status=RunStatus.COMPLETED,
        expected_error=None,
        expected_effects=1,
        expected_retry_mode="provider_idempotent",
        expected_action_status="succeeded",
    )
    known_failure = _control_action(
        controller,
        destination="p5-known-failure",
        expected_status=RunStatus.FAILED,
        expected_error="external_action_failed",
        expected_effects=0,
        expected_retry_mode="unsafe",
        expected_action_status="failed",
    )
    return {
        "id": "S6",
        "result": "passed",
        "assertion": "provider ambiguity obeyed idempotent/unsafe policy with controls",
        "invariants": {
            "idempotent_effect_exactly_once": True,
            "unsafe_effect_not_replayed": True,
            "provider_request_identity_stable": True,
            "workflow_and_run_mirror_agree": True,
        },
        "paths": [idempotent, unsafe, known_success, known_failure],
    }


def _scenario_s7(controller: ProofController) -> dict[str, Any]:
    worker_a, worker_b = controller.workers
    connection_policy_before = dict(controller.bundle.metadata.connection_policy)
    worker_a.arm(
        "db.connection.open",
        "db.connection.failed",
        "run.claimed",
        "run.execution_entered",
    )
    worker_b.arm("claim.before")
    worker_b.wait_hook("claim.before")
    run = controller.submit_basic("s7-connection-recovery")
    opened = worker_a.wait_hook("db.connection.open")
    backend_pid = opened.get("backend_pid")
    _require(isinstance(backend_pid, int), "S7 did not expose a bounded backend PID")
    _require(terminate_backend(controller.dsn, backend_pid), "S7 backend was not terminated")
    worker_a.release("db.connection.open")
    failed = worker_a.wait_hook("db.connection.failed")
    # Assert the SQLSTATE rather than the driver class name. A class-name set
    # wide enough to tolerate Psycopg naming changes would also pass while the
    # 57P01 allowlist entry went unexercised, silently losing this coverage.
    _require(
        failed.get("sqlstate") == "57P01",
        "S7 did not observe the 57P01 AdminShutdown that the retry allowlist admits",
    )
    worker_a.release("db.connection.failed")
    claimed = worker_a.wait_hook("run.claimed")
    _require(claimed.get("run_id") == run.run_id, "S7 retry did not claim the queued Run")
    worker_b.release("claim.before")
    worker_a.release("run.claimed")
    worker_a.wait_hook("run.execution_entered")
    worker_a.kill()
    controller.restart_worker(worker_a)
    live_after_restart = snapshot_run(controller.dsn, controller.schema, run.run_id)
    time.sleep(0.2)
    still_live = snapshot_run(controller.dsn, controller.schema, run.run_id)
    _require(
        live_after_restart.attempt == 1
        and live_after_restart.lease_live
        and still_live.run_recovered_count == 0,
        "S7 restarted worker stole its old live lease",
    )
    expire_live_lease(
        controller.dsn,
        controller.schema,
        run.run_id,
        expected_attempt=1,
    )
    terminal = controller.wait_terminal(run.run_id)
    final = snapshot_run(controller.dsn, controller.schema, run.run_id)
    connection_policy_after = dict(controller.bundle.metadata.connection_policy)
    _require(
        terminal.status == RunStatus.COMPLETED
        and final.attempt == 2
        and final.recovery_reasons == ("lease_expired",)
        and connection_policy_after == connection_policy_before,
        "S7 did not recover once after exact expiry",
    )
    return {
        "id": "S7",
        "result": "passed",
        "assertion": "terminated connection retried safely; restart respected live lease",
        "invariants": {
            "real_backend_terminated": True,
            "retry_bounded": True,
            "live_lease_not_stolen": True,
            "expiry_takeover_once": True,
        },
        "failure_type": failed.get("error_type"),
        "failure_sqlstate": failed.get("sqlstate"),
        "live_after_restart": live_after_restart.public_dict(),
        "terminal": _terminal_summary(terminal),
        "claims": _claim_counts(final),
        "connection_policy": connection_policy_after,
        "connection_policy_unchanged": True,
    }


def _scenario_s8(controller: ProofController) -> dict[str, Any]:
    schedules: list[dict[str, Any]] = []
    for index, designated in enumerate(
        (controller.worker_a, controller.worker_b, controller.worker_b, controller.worker_a),
        start=1,
    ):
        marker = f"s8-round-{index}"
        run, _ = controller.designated_claim(
            designated,
            lambda: controller.submit_basic(marker),
        )
        designated.release("run.claimed")
        terminal = controller.wait_terminal(run.run_id)
        state = terminal.state.model_dump(mode="json") if terminal.state else {}
        winner = state.get("completed_by") if isinstance(state, dict) else None
        _require(
            terminal.status == RunStatus.COMPLETED
            and winner == designated.worker_id
            and terminal.attempt == 1,
            f"S8 deterministic round {index} diverged",
        )
        schedules.append(
            {
                "schedule_id": f"s8-{index}",
                "release_order": (
                    "a-first" if designated is controller.worker_a else "b-first"
                ),
                "winning_worker": winner,
                "attempts": terminal.attempt,
                "recovery_path": "initial_claim",
                "single_owner": True,
                "terminal": terminal.status.value,
            }
        )
    release_classes = sorted({str(schedule["release_order"]) for schedule in schedules})
    _require(
        release_classes == ["a-first", "b-first"],
        "S8 did not cover both deterministic release classes",
    )
    return {
        "id": "S8",
        "result": "passed",
        "assertion": "repeated fixed release schedules reproduced the same winners",
        "invariants": {"both_release_classes_observed": True, "all_attempts_single_owner": True},
        "run_count": len(schedules),
        "release_classes": release_classes,
        "schedules": schedules,
    }


def validate_report(
    report: dict[str, Any],
    *,
    expected_scenario_ids: tuple[str, ...] = ALL_SCENARIO_IDS,
) -> None:
    _require(report.get("proof") == REPORT_VERSION, "Report version is wrong")
    _require(report.get("result") == "passed", "Report result is not passed")
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        raise P5ProofFailure("Report scenarios are missing")
    _require(
        [scenario.get("id") for scenario in scenarios if isinstance(scenario, dict)]
        == list(expected_scenario_ids),
        "Report does not contain the requested ordered scenario evidence",
    )
    _require(
        all(
            isinstance(scenario, dict) and scenario.get("result") == "passed"
            for scenario in scenarios
        ),
        "Every report scenario must pass",
    )


def assert_secret_safe(report: dict[str, Any], *, dsn: str) -> None:
    encoded = json.dumps(report, sort_keys=True)
    forbidden_literals = {dsn, os.getenv("P5_SECRET_CANARY", "")}
    try:
        password = conninfo_to_dict(dsn).get("password", "")
    except Exception:
        password = ""
    forbidden_literals.add(password)
    leaked = [literal for literal in forbidden_literals if literal and literal in encoded]
    _require(not leaked, "P5 report contains configured secret material")
    forbidden_patterns = (
        r"lease_[0-9a-f]{8,}",
        r"dispatch_[0-9a-f]{8,}",
        r"(?i)authorization.{0,20}bearer",
        r"(?i)postgres(?:ql)?://[^\s\"]+",
        r"(?i)traceback \(most recent call last\)",
    )
    _require(
        not any(re.search(pattern, encoded) for pattern in forbidden_patterns),
        "P5 report contains forbidden secret or unbounded diagnostic material",
    )


def _log_scenario(scenario: dict[str, Any]) -> None:
    print(
        json.dumps(
            {
                "proof": REPORT_VERSION,
                "scenario": scenario["id"],
                "result": scenario["result"],
                "assertion": scenario["assertion"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def run_proof(
    dsn: str,
    *,
    scenario_ids: tuple[str, ...] = ALL_SCENARIO_IDS,
) -> Path:
    ARTIFACT_PATH.unlink(missing_ok=True)
    schema = f"p5_{uuid.uuid4().hex[:20]}"
    proof_dsn = make_conninfo(dsn, application_name="p5_multi_worker_proof")
    provider_temp = tempfile.TemporaryDirectory(prefix="p5-provider-")
    provider = ProviderProcess(
        database_path=Path(provider_temp.name) / "ledger.db",
        port=_free_loopback_port(),
    )
    controller: ProofController | None = None
    workers_stopped = False
    scenarios: list[dict[str, Any]] = []
    cleanup_error: Exception | None = None
    try:
        provider.start()
        bootstrap_postgres_application_schema(proof_dsn, schema=schema)
        controller = ProofController(dsn=proof_dsn, schema=schema, provider=provider)
        controller.start_workers()
        controller.worker_a.start_claiming()
        controller.worker_b.start_claiming()
        scenario_functions = {
            "S1": _scenario_s1,
            "S2": _scenario_s2,
            "S3": _scenario_s3,
            "S4": _scenario_s4,
            "S5": _scenario_s5,
            "S6": _scenario_s6,
            "S7": _scenario_s7,
            "S8": _scenario_s8,
        }
        for scenario_id in scenario_ids:
            scenario = scenario_functions[scenario_id]
            result = scenario(controller)
            scenarios.append(result)
            _log_scenario(result)
        controller.stop_workers()
        workers_stopped = True
        idle_sessions = idle_in_transaction_count(proof_dsn)
        _require(idle_sessions == 0, "P5 left an idle-in-transaction PostgreSQL session")
        schedule_result = next(
            (scenario for scenario in scenarios if scenario["id"] == "S8"),
            None,
        )
        report = {
            "proof": REPORT_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "result": "passed",
            "code": {
                "head": _git_value("rev-parse", "HEAD"),
                "tree": _git_value("rev-parse", "HEAD^{tree}"),
            },
            "environment": {
                "postgres_version": postgres_version(proof_dsn),
                "worker_processes": 2,
                "schema_versions": dict(controller.bundle.metadata.schema_versions),
            },
            "topology": {
                "worker_ids": [worker.worker_id for worker in controller.workers],
                "provider_independent_process": True,
            },
            "schedule_coverage": {
                "run_count": schedule_result["run_count"] if schedule_result else 0,
                "release_classes": (
                    schedule_result["release_classes"] if schedule_result else []
                ),
            },
            "database_postconditions": {
                "idle_in_transaction_sessions": idle_sessions,
                "isolated_schema_cleaned_after_report": True,
            },
            "scenarios": scenarios,
            "summary": {"passed": len(scenarios), "failed": 0},
        }
        validate_report(report, expected_scenario_ids=scenario_ids)
        assert_secret_safe(report, dsn=proof_dsn)
        ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ARTIFACT_PATH.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    finally:
        if controller is not None and not workers_stopped:
            controller.stop_workers()
        provider.stop()
        provider_temp.cleanup()
        try:
            drop_schema(proof_dsn, schema)
        except Exception as exc:  # retain the primary proof failure, if any
            cleanup_error = exc
    if cleanup_error is not None:
        ARTIFACT_PATH.unlink(missing_ok=True)
        raise P5ProofFailure("P5 isolated PostgreSQL schema cleanup failed") from cleanup_error
    return ARTIFACT_PATH


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the deterministic P5 two-process PostgreSQL recovery proof."
    )
    parser.add_argument(
        "--dsn-env",
        default="P5_POSTGRES_DSN",
        help="Environment variable containing the PostgreSQL DSN (never printed).",
    )
    parser.add_argument(
        "--scenarios",
        default="S1,S2,S3,S4,S5,S6,S7,S8",
        help="Comma-separated deterministic scenario subset for mutation sampling.",
    )
    args = parser.parse_args()
    dsn = os.getenv(args.dsn_env)
    if not dsn:
        parser.error(f"{args.dsn_env} is required; P5 proof never skips")
    scenario_ids = tuple(item.strip().upper() for item in args.scenarios.split(",") if item)
    valid_ids = {f"S{index}" for index in range(1, 9)}
    if not scenario_ids or len(set(scenario_ids)) != len(scenario_ids):
        parser.error("--scenarios must contain a nonempty unique scenario list")
    if any(scenario_id not in valid_ids for scenario_id in scenario_ids):
        parser.error("--scenarios contains an unknown scenario ID")
    try:
        artifact = run_proof(dsn, scenario_ids=scenario_ids)
    except Exception as exc:
        message = (
            str(exc)
            if isinstance(exc, P5ProofFailure)
            else "P5 proof failed; inspect bounded scenario output."
        )
        print(
            json.dumps(
                {
                    "proof": REPORT_VERSION,
                    "result": "failed",
                    "error_type": type(exc).__name__,
                    "message": message,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    print(f"P5 MULTI-WORKER RECOVERY PROOF: PASSED ({artifact})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
