from __future__ import annotations

import os
import queue
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import (
    BaseRuntimeState,
    RuntimeExecutionContext,
    RuntimeExecutionError,
    RuntimeResponse,
)
from domains.durable_action.runtime import DurableActionGatewayRuntime, DurableActionState
from runtime_service import AgentRegistry, RuntimeManager
from runtime_service.action_gateway import (
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    DurableActionInput,
)
from runtime_service.evidence import EvidenceProjector
from runtime_service.external_actions import (
    ExternalActionDispatcher,
    ExternalActionProviderRegistry,
)
from runtime_service.http_external_action import HttpExternalActionProvider
from runtime_service.memory import MemoryKind, MemoryWrite
from runtime_service.models import RunCommitOutcome, RunLeaseClaim, RunRecord
from runtime_service.postgres_memory_store import PostgresMemoryStore
from runtime_service.postgres_store import PostgresRunStore
from runtime_service.postgres_workflow_store import PostgresWorkflowStore
from runtime_service.sandbox import ToolRetryMode
from runtime_service.storage import (
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)
from runtime_service.store import RunLeaseLostError
from runtime_service.workflow_store import (
    ClaimOutcome,
    ExternalActionDispatchOutcome,
    ExternalActionPrepareOutcome,
    WorkflowStatus,
)


P5_AGENT_ID = "p5-proof-agent"
P5_AGENT_VERSION = "1.0.0"
P5_WORKFLOW_TYPE = "p5-stale-mutation-matrix:1"
P5_STEP_ID = "stale-dispatch"
P5_HOOK_TIMEOUT_SECONDS = 45.0


class P5ProofInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scenario: Literal["basic", "stale_mutations"] = "basic"
    marker: str = Field(min_length=1, max_length=200)


class P5ProofState(BaseRuntimeState):
    domain_id = "p5-proof"
    schema_version = "1"

    completed_by: str | None = None
    completed_attempt: int | None = None
    marker: str | None = None


@dataclass
class ProcessHook:
    """One one-shot, spawn-safe proof barrier.

    The hook carries only sanitized metadata. It is constructed by the
    controller and inherited by one worker process; production code never
    imports or discovers it.
    """

    enabled: Any
    reached: Any
    release: Any
    metadata: Any

    @classmethod
    def create(cls, context: Any) -> ProcessHook:
        return cls(
            enabled=context.Event(),
            reached=context.Event(),
            release=context.Event(),
            metadata=context.Queue(maxsize=1),
        )

    def arm(self) -> None:
        self.reached.clear()
        self.release.clear()
        while True:
            try:
                self.metadata.get_nowait()
            except queue.Empty:
                break
        self.enabled.set()

    def consume(self) -> bool:
        if not self.enabled.is_set():
            return False
        self.enabled.clear()
        return True

    def block_consumed(self, payload: dict[str, Any]) -> None:
        self.metadata.put(payload)
        self.reached.set()
        if not self.release.wait(P5_HOOK_TIMEOUT_SECONDS):
            raise TimeoutError("P5 controller did not release a reached proof hook")

    def hit(self, payload: dict[str, Any]) -> bool:
        if not self.consume():
            return False
        self.block_consumed(payload)
        return True

    def pause_process(self, payload: dict[str, Any]) -> bool:
        """Stop this proof process after publishing one bounded observation."""

        if not self.consume():
            return False
        self.metadata.put(payload)
        self.reached.set()
        os.kill(os.getpid(), signal.SIGSTOP)
        return True


@dataclass
class P5ProofHooks:
    hooks: dict[str, ProcessHook]

    @classmethod
    def create(cls, context: Any, names: tuple[str, ...]) -> P5ProofHooks:
        return cls({name: ProcessHook.create(context) for name in names})

    def arm(self, *names: str) -> None:
        for name in names:
            self.hooks[name].arm()

    def hit(self, name: str, payload: dict[str, Any]) -> bool:
        hook = self.hooks.get(name)
        return hook.hit(payload) if hook is not None else False

    def pause_process(self, name: str, payload: dict[str, Any]) -> bool:
        hook = self.hooks.get(name)
        return hook.pause_process(payload) if hook is not None else False


@dataclass(frozen=True)
class P5WorkerConfig:
    worker_id: str
    schema: str
    provider_url: str | None = None
    dsn: str = field(repr=False, default="")
    lease_duration_seconds: int = 60
    heartbeat_interval_seconds: float = 50.0
    poll_interval_seconds: float = 0.05


class _TerminatedConnection:
    """Proof-only connection proxy paused before its first store query."""

    def __init__(
        self,
        connection: Any,
        hook: ProcessHook,
        failure_hook: ProcessHook | None,
        *,
        worker_id: str,
        backend_pid: int,
    ) -> None:
        self._connection = connection
        self._hook = hook
        self._failure_hook = failure_hook
        self._worker_id = worker_id
        self._backend_pid = backend_pid
        self._first_execute = True

    def execute(self, query: Any, params: Any = None) -> Any:
        if self._first_execute:
            self._first_execute = False
            self._hook.block_consumed(
                {
                    "point": "db.connection.open",
                    "worker": self._worker_id,
                    "backend_pid": self._backend_pid,
                }
            )
        try:
            if params is None:
                return self._connection.execute(query)
            return self._connection.execute(query, params)
        except Exception as exc:
            if self._failure_hook is not None:
                self._failure_hook.hit(
                    {
                        "point": "db.connection.failed",
                        "worker": self._worker_id,
                        "error_type": type(exc).__name__,
                    }
                )
            raise

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class P5ProofRunStore:
    """Narrow proof adapter around the accepted PostgreSQL Run store."""

    def __init__(
        self,
        delegate: PostgresRunStore,
        *,
        worker_id: str,
        hooks: P5ProofHooks,
    ) -> None:
        self._delegate = delegate
        self.worker_id = worker_id
        self.hooks = hooks
        self._install_connection_fault_hook()

    def _install_connection_fault_hook(self) -> None:
        original = self._delegate._lease_connect

        def connect_with_optional_fault() -> Any:
            connection = original()
            hook = self.hooks.hooks.get("db.connection.open")
            if hook is None or not hook.consume():
                return connection
            row = connection.execute("SELECT pg_backend_pid() AS pid").fetchone()
            if row is None:
                connection.close()
                raise RuntimeError("PostgreSQL did not return a backend PID")
            return _TerminatedConnection(
                connection,
                hook,
                self.hooks.hooks.get("db.connection.failed"),
                worker_id=self.worker_id,
                backend_pid=int(row["pid"]),
            )

        setattr(self._delegate, "_lease_connect", connect_with_optional_fault)

    @property
    def lease_operation_timeout_seconds(self) -> float:
        return self._delegate.lease_operation_timeout_seconds

    def claim_next_run(
        self,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        reconciliation_pending_code: str | None = None,
    ) -> RunLeaseClaim | None:
        self.hooks.hit(
            "claim.before",
            {"point": "claim.before", "worker": self.worker_id},
        )
        try:
            claim = self._delegate.claim_next_run(
                owner_id=owner_id,
                lease_duration_seconds=lease_duration_seconds,
                reconciliation_pending_code=reconciliation_pending_code,
            )
        except Exception as exc:
            self.hooks.hit(
                "db.connection.failed",
                {
                    "point": "db.connection.failed",
                    "worker": self.worker_id,
                    "error_type": type(exc).__name__,
                },
            )
            raise
        if claim is not None:
            self.hooks.hit(
                "run.claimed",
                {
                    "point": "run.claimed",
                    "worker": self.worker_id,
                    "run_id": claim.run.run_id,
                    "thread_id": claim.run.thread_id,
                    "attempt": claim.run.attempt,
                    "recovered": claim.recovery_reason is not None,
                },
            )
        self.hooks.hit(
            "claim.result",
            {
                "point": "claim.result",
                "worker": self.worker_id,
                "run_id": claim.run.run_id if claim is not None else None,
                "claimed": claim is not None,
            },
        )
        return claim

    def commit_completed_run(
        self,
        run: RunRecord,
        *,
        lease_token: str,
    ) -> RunCommitOutcome:
        self.hooks.hit(
            "checkpoint.commit_pending",
            {
                "point": "checkpoint.commit_pending",
                "worker": self.worker_id,
                "run_id": run.run_id,
                "thread_id": run.thread_id,
                "attempt": run.attempt,
            },
        )
        outcome = self._delegate.commit_completed_run(
            run,
            lease_token=lease_token,
        )
        self.hooks.hit(
            "run.commit_result",
            {
                "point": "run.commit_result",
                "worker": self.worker_id,
                "run_id": run.run_id,
                "attempt": run.attempt,
                "outcome": outcome.value,
            },
        )
        return outcome

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class P5ProofRuntime:
    def __init__(
        self,
        *,
        worker_id: str,
        hooks: P5ProofHooks,
        run_store: P5ProofRunStore,
        workflow_store: PostgresWorkflowStore,
        memory_store: PostgresMemoryStore,
    ) -> None:
        self.worker_id = worker_id
        self.hooks = hooks
        self.run_store = run_store
        self.workflow_store = workflow_store
        self.memory_store = memory_store
        self.evidence = EvidenceProjector(
            workflow_store=workflow_store,
            run_event_sink=run_store,
        )

    def initial_state(self, thread_id: str) -> P5ProofState:
        return P5ProofState(thread_id=thread_id)

    def execute(
        self,
        state: P5ProofState,
        runtime_input: P5ProofInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[P5ProofState]:
        run = self.run_store.get_run_internal(context.run_id)
        if run is None:
            raise RuntimeError("P5 proof Run disappeared")
        if runtime_input.scenario == "stale_mutations":
            if context.recovered_after_restart:
                self._resolve_stale_mutation_fixture(context)
            else:
                self._prepare_stale_mutation_fixture(context)
                self.hooks.pause_process(
                    "stale.process_paused",
                    {
                        "point": "stale.process_paused",
                        "worker": self.worker_id,
                        "run_id": context.run_id,
                        "thread_id": context.thread_id,
                        "attempt": run.attempt,
                    },
                )

        self.hooks.hit(
            "run.execution_entered",
            {
                "point": "run.execution_entered",
                "worker": self.worker_id,
                "run_id": context.run_id,
                "thread_id": context.thread_id,
                "attempt": run.attempt,
                "recovered": context.recovered_after_restart,
            },
        )

        if runtime_input.scenario == "stale_mutations" and not context.recovered_after_restart:
            results = self._exercise_stale_mutations(context)
            self.hooks.hit(
                "stale.mutations",
                {
                    "point": "stale.mutations",
                    "worker": self.worker_id,
                    "run_id": context.run_id,
                    "attempt": run.attempt,
                    "results": results,
                },
            )

        updated = state.model_copy(
            deep=True,
            update={
                "completed_by": self.worker_id,
                "completed_attempt": run.attempt,
                "marker": runtime_input.marker,
            },
        )
        return RuntimeResponse(
            message=f"completed by {self.worker_id}",
            state=updated,
            validation_errors=[],
        )

    def _prepare_stale_mutation_fixture(self, context: RuntimeExecutionContext) -> None:
        execution = self.workflow_store.create_or_get_execution(
            context.run_id,
            P5_WORKFLOW_TYPE,
            "stale-mutation-input",
            lease_token=context.lease_token,
        )
        if execution.execution.status == WorkflowStatus.PENDING:
            self.workflow_store.mark_running(
                context.run_id,
                lease_token=context.lease_token,
            )
        claim = self.workflow_store.claim_step(
            context.run_id,
            P5_STEP_ID,
            "p5.synthetic.external",
            "stale-step-input",
            max_attempts=1,
            lease_token=context.lease_token,
        )
        if claim.outcome != ClaimOutcome.CLAIMED or claim.attempt_token is None:
            raise RuntimeError("P5 stale fixture did not claim its tool step")
        prepared = self.workflow_store.prepare_external_action(
            run_id=context.run_id,
            step_id=P5_STEP_ID,
            tool_attempt_token=claim.attempt_token,
            tenant_id=context.authority.tenant_id,
            subject_id=context.authority.subject_id,
            workflow_type=P5_WORKFLOW_TYPE,
            tool_name="p5.synthetic.external",
            provider_name="p5-proof-provider",
            provider_identity="p5-proof-provider-v1",
            input_hash="stale-step-input",
            arguments_json='{"probe":true}',
            retry_mode=ToolRetryMode.UNSAFE.value,
            idempotency_key=f"p5-{context.run_id}",
            lease_token=context.lease_token,
        )
        if prepared.outcome != ExternalActionPrepareOutcome.CREATED:
            raise RuntimeError("P5 stale fixture did not prepare its external action")
        self.memory_store.get_or_create_run_snapshot_for_run(
            lease_token=self._require_lease(context),
            run_id=context.run_id,
            tenant_id=context.authority.tenant_id,
            subject_id=context.authority.subject_id,
            domain_id=P5ProofState.domain_id,
            allowed_keys=("p5.marker",),
        )

    def _resolve_stale_mutation_fixture(self, context: RuntimeExecutionContext) -> None:
        step = self.workflow_store.get_step(context.run_id, P5_STEP_ID)
        if step is None or step.attempt_token is None:
            raise RuntimeError("P5 replacement attempt cannot read its tool fixture")
        dispatch = self.workflow_store.begin_external_action_dispatch(
            context.run_id,
            P5_STEP_ID,
            tool_attempt_token=step.attempt_token,
            lease_token=context.lease_token,
        )
        if (
            dispatch.outcome != ExternalActionDispatchOutcome.CLAIMED
            or dispatch.dispatch_token is None
        ):
            raise RuntimeError("P5 replacement attempt cannot claim fixture dispatch")
        self.workflow_store.finalize_unsafe_interrupted_action(
            context.run_id,
            P5_STEP_ID,
            dispatch_token=dispatch.dispatch_token,
            tool_attempt_token=step.attempt_token,
            lease_token=context.lease_token,
        )
        self.workflow_store.finalize_failed(
            context.run_id,
            "external_action_outcome_unknown",
            lease_token=context.lease_token,
        )
        self.hooks.hit(
            "replacement.authoritative_progress",
            {
                "point": "replacement.authoritative_progress",
                "worker": self.worker_id,
                "run_id": context.run_id,
                "attempt": 2,
                "workflow_finalized": True,
            },
        )
        raise RuntimeExecutionError(
            "external_action_outcome_unknown",
            "P5 replacement attempt closed the synthetic ambiguous action.",
        )

    def _exercise_stale_mutations(
        self,
        context: RuntimeExecutionContext,
    ) -> dict[str, str]:
        step = self.workflow_store.get_step(context.run_id, P5_STEP_ID)
        if step is None or step.attempt_token is None:
            raise RuntimeError("P5 stale mutation fixture lost its tool step")
        attempt_token = step.attempt_token
        operations = {
            "run_event": lambda: self.run_store.append_attempt_event(
                context.run_id,
                lease_token=context.lease_token,
                event_type="p5.stale.run_event",
                payload={"probe": True},
            ),
            "workflow": lambda: self.workflow_store.append_event(
                context.run_id,
                "p5.stale.workflow_event",
                {"probe": True},
                lease_token=context.lease_token,
            ),
            "memory_snapshot": lambda: self.memory_store.get_or_create_run_snapshot_for_run(
                lease_token=self._require_lease(context),
                run_id=context.run_id,
                tenant_id=context.authority.tenant_id,
                subject_id=context.authority.subject_id,
                domain_id=P5ProofState.domain_id,
                allowed_keys=("p5.marker",),
            ),
            "memory_mutation": lambda: self.memory_store.upsert_from_run(
                lease_token=self._require_lease(context),
                tenant_id=context.authority.tenant_id,
                subject_id=context.authority.subject_id,
                domain_id=P5ProofState.domain_id,
                write=MemoryWrite(
                    kind=MemoryKind.FACT,
                    key="p5.marker",
                    value="stale",
                ),
                source_run_id=context.run_id,
                source_thread_id=context.thread_id,
                actor_subject_id=context.authority.subject_id,
            ),
            "evidence_mirror": lambda: self.evidence.record(
                context.run_id,
                "planner.decision",
                {"evidence_id": "p5-stale-evidence", "probe": True},
                lease_token=context.lease_token,
            ),
            "external_action": lambda: self.workflow_store.begin_external_action_dispatch(
                context.run_id,
                P5_STEP_ID,
                tool_attempt_token=attempt_token,
                lease_token=context.lease_token,
            ),
        }
        results: dict[str, str] = {}
        for name, operation in operations.items():
            try:
                operation()
            except RunLeaseLostError:
                results[name] = "run_lease_lost"
            except Exception as exc:
                results[name] = f"unexpected:{type(exc).__name__}"
            else:
                results[name] = "unexpected:committed"
        return results

    @staticmethod
    def _require_lease(context: RuntimeExecutionContext) -> str:
        if context.lease_token is None:
            raise RunLeaseLostError(f"Run lease token is required: {context.run_id}")
        return context.lease_token


def _provider_registry(provider_url: str | None) -> ExternalActionProviderRegistry:
    registry = ExternalActionProviderRegistry()
    if provider_url is None:
        return registry
    definitions = (
        ("p5-idempotent", "idempotent", True, ()),
        ("p5-unsafe", "unsafe", False, ()),
        ("p5-known-success", "known-success", True, ()),
        ("p5-known-failure", "known-failure", False, (422,)),
    )
    for alias, route, supports_idempotency, definitive_codes in definitions:
        registry.register(
            alias,
            HttpExternalActionProvider(
                endpoint=f"{provider_url}/actions/{route}",
                provider_identity=f"{alias}-provider-v1",
                allow_insecure_localhost=True,
                supports_idempotency=supports_idempotency,
                definitive_status_codes=definitive_codes,
                timeout_seconds=60,
            ),
        )
    return registry


def build_p5_registry(
    *,
    worker_id: str,
    hooks: P5ProofHooks,
    run_store: P5ProofRunStore,
    workflow_store: PostgresWorkflowStore,
    memory_store: PostgresMemoryStore,
    provider_url: str | None,
) -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(
        P5_AGENT_ID,
        P5_AGENT_VERSION,
        lambda: P5ProofRuntime(
            worker_id=worker_id,
            hooks=hooks,
            run_store=run_store,
            workflow_store=workflow_store,
            memory_store=memory_store,
        ),
        description="P5 deterministic multi-process proof runtime",
        input_model=P5ProofInput,
        state_model=P5ProofState,
    )
    providers = _provider_registry(provider_url)
    registry.register(
        ACTION_AGENT_ID,
        ACTION_AGENT_VERSION,
        lambda: DurableActionGatewayRuntime(
            workflow_store=workflow_store,
            run_event_sink=run_store,
            dispatcher=ExternalActionDispatcher(providers),
        ),
        description="P5 durable external-action recovery proof runtime",
        input_model=DurableActionInput,
        state_model=DurableActionState,
        public_runs_api=False,
    )
    return registry


def run_p5_worker(
    config: P5WorkerConfig,
    hooks: P5ProofHooks,
    start: Any,
    ready: Any,
    shutdown: Any,
    failures: Any,
) -> None:
    """Start one independently killable RuntimeManager process."""

    manager: RuntimeManager | None = None
    previous_thread_excepthook = threading.excepthook

    def report_thread_failure(args: threading.ExceptHookArgs) -> None:
        failures.put(
            {
                "worker": config.worker_id,
                "pid": os.getpid(),
                "error_type": type(args.exc_value).__name__,
                "message": "P5 worker thread failed; inspect bounded CI diagnostics.",
            }
        )
        shutdown.set()

    threading.excepthook = report_thread_failure
    try:
        storage_config = resolve_runtime_storage_config(
            backend="postgres",
            postgres_dsn=config.dsn,
            postgres_schema=config.schema,
            environment={},
        )
        bundle = build_runtime_store_bundle(storage_config)
        if not (
            isinstance(bundle.run_store, PostgresRunStore)
            and isinstance(bundle.workflow_store, PostgresWorkflowStore)
            and isinstance(bundle.memory_store, PostgresMemoryStore)
        ):
            raise RuntimeError("P5 worker did not receive the PostgreSQL store bundle")
        run_store = P5ProofRunStore(
            bundle.run_store,
            worker_id=config.worker_id,
            hooks=hooks,
        )
        registry = build_p5_registry(
            worker_id=config.worker_id,
            hooks=hooks,
            run_store=run_store,
            workflow_store=bundle.workflow_store,
            memory_store=bundle.memory_store,
            provider_url=config.provider_url,
        )
        manager = RuntimeManager(
            run_store,
            registry,
            worker_count=1,
            owner_id=config.worker_id,
            recovery_reconciliation_required=(
                bundle.workflow_store.has_external_action_requiring_reconciliation
            ),
            lease_duration_seconds=config.lease_duration_seconds,
            heartbeat_interval_seconds=config.heartbeat_interval_seconds,
            poll_interval_seconds=config.poll_interval_seconds,
            shutdown_grace_seconds=2,
        )
        ready.set()
        if not start.wait(P5_HOOK_TIMEOUT_SECONDS):
            raise TimeoutError("P5 worker was never released to start claiming")
        manager.start()
        shutdown.wait()
    except BaseException as exc:
        failures.put(
            {
                "worker": config.worker_id,
                "pid": os.getpid(),
                "error_type": type(exc).__name__,
                "message": "P5 worker failed; inspect bounded CI diagnostics.",
            }
        )
    finally:
        if manager is not None:
            manager.stop()
        threading.excepthook = previous_thread_excepthook
        time.sleep(0.01)
