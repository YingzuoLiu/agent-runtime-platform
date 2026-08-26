from __future__ import annotations

import multiprocessing
import json
import os
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import examples.p5_multi_worker_proof as p5_proof
from examples.p5_multi_worker_proof import (
    REPORT_VERSION,
    P5ProofFailure,
    assert_secret_safe,
    validate_report,
)
from examples.p5_postgres_probe import P5RunSnapshot
from examples.p5_proof_worker import (
    P5ProofHooks,
    P5WorkerConfig,
    ProcessHook,
    _TerminatedConnection,
)
from tests.conformance.p5_mutation_proof import p5_mutants


ROOT = Path(__file__).resolve().parents[1]


def _pause_hook_in_child(hooks: P5ProofHooks) -> None:
    hooks.pause_process("paused", {"point": "paused", "attempt": 1})


def _repeat_hook_in_child(hook: ProcessHook, count: int) -> None:
    for expected in range(1, count + 1):
        deadline = time.monotonic() + 5
        while not hook.hit({"generation": expected}):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"generation {expected} was never armed")
            time.sleep(0.001)


def _hit_hook_sequence_in_child(hooks: P5ProofHooks, names: tuple[str, ...]) -> None:
    for name in names:
        deadline = time.monotonic() + 5
        while not hooks.hit(name, {"point": name}):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"hook {name} was never armed")
            time.sleep(0.001)


def _straddle_claim_cycle_in_child(
    hooks: P5ProofHooks,
    between_hooks: Any,
    continue_cycle: Any,
    start_next_cycle: Any,
) -> None:
    with hooks.claim_cycle:
        assert hooks.hit("claim.before", {"cycle": 1}) is False
        between_hooks.set()
        if not continue_cycle.wait(5):
            raise TimeoutError("controller did not release the in-flight claim cycle")
        assert hooks.hit("claim.result", {"cycle": 1}) is False

    if not start_next_cycle.wait(5):
        raise TimeoutError("controller did not arm the next claim cycle")
    with hooks.claim_cycle:
        assert hooks.hit("claim.before", {"cycle": 2}) is True
        assert hooks.hit("claim.result", {"cycle": 2}) is True


def _valid_report() -> dict:
    return {
        "proof": REPORT_VERSION,
        "result": "passed",
        "scenarios": [
            {"id": f"S{index}", "result": "passed"} for index in range(1, 9)
        ],
    }


def test_report_contract_requires_ordered_passing_s1_through_s8() -> None:
    report = _valid_report()
    validate_report(report)

    report["scenarios"][4]["result"] = "failed"
    with pytest.raises(P5ProofFailure, match="Every report scenario"):
        validate_report(report)


def test_committed_report_fixture_matches_the_versioned_contract() -> None:
    fixture = json.loads(
        (ROOT / "examples" / "p5-multi-worker-proof.example.json").read_text(
            encoding="utf-8"
        )
    )

    validate_report(fixture)
    assert_secret_safe(fixture, dsn="postgresql://proof:canary-password@localhost/proof")


def test_every_p5_mutant_has_one_exact_source_target() -> None:
    mutants = p5_mutants()

    assert [mutant.number for mutant in mutants] == list(range(1, 10))
    for mutant in mutants:
        for replacement in mutant.replacements:
            source = replacement.path.read_text(encoding="utf-8")
            assert source.count(replacement.old) == replacement.count, mutant.description


@pytest.mark.parametrize(
    "leak",
    (
        "postgresql://proof:canary-password@localhost/proof",
        "lease_0123456789abcdef",
        "dispatch_0123456789abcdef",
        "Authorization: Bearer secret",
        "Traceback (most recent call last):",
    ),
)
def test_report_secret_gate_rejects_credentials_tokens_and_tracebacks(leak: str) -> None:
    dsn = "postgresql://proof:canary-password@localhost/proof"
    report = _valid_report()
    report["leak"] = leak
    with pytest.raises(P5ProofFailure, match="secret|diagnostic"):
        assert_secret_safe(report, dsn=dsn)


def test_postgres_snapshot_projection_excludes_lease_tokens() -> None:
    snapshot = P5RunSnapshot(
        run_id="run_safe",
        status="running",
        attempt=2,
        lease_owner_id="p5-worker-b",
        lease_present=True,
        lease_live=True,
        run_started_count=2,
        run_recovered_count=1,
        recovery_reasons=("lease_expired",),
        checkpoint_saved_count=0,
        run_completed_count=0,
        run_failed_count=0,
    )
    payload = snapshot.public_dict()

    assert "lease_token" not in payload
    assert payload["lease_present"] is True
    assert payload["recovery_reasons"] == ["lease_expired"]


def test_worker_config_repr_redacts_postgres_dsn() -> None:
    config = P5WorkerConfig(
        worker_id="p5-worker-a",
        schema="p5_test",
        dsn="postgresql://proof:canary-password@localhost/proof",
    )

    assert "canary-password" not in repr(config)
    assert "dsn=" not in repr(config)


def test_worker_timeout_diagnostic_requests_locals_free_stack_dump(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class LiveProcess:
        pid = 4242

        @staticmethod
        def is_alive() -> bool:
            return True

    worker = p5_proof.WorkerProcess(
        context=None,
        config=P5WorkerConfig(
            worker_id="p5-worker-a",
            schema="p5_test",
            dsn="postgresql://proof:canary-password@localhost/proof",
        ),
        hooks=P5ProofHooks({}),
        process=LiveProcess(),
    )
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(p5_proof.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(p5_proof.time, "sleep", lambda _seconds: None)

    worker.dump_thread_stacks(reason="timeout waiting for claim.before")

    diagnostic = capsys.readouterr().err
    assert signals == [(4242, signal.SIGUSR1)]
    assert '"worker": "p5-worker-a"' in diagnostic
    assert '"reason": "timeout waiting for claim.before"' in diagnostic
    assert "canary-password" not in diagnostic


def test_process_hook_is_one_shot_and_carries_only_controller_payload() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("point",))
    generation = hooks.hooks["point"].arm()
    completed = threading.Event()

    def hit() -> None:
        assert hooks.hit("point", {"point": "point", "attempt": 1}) is True
        assert hooks.hit("point", {"point": "point", "attempt": 2}) is False
        completed.set()

    thread = threading.Thread(target=hit)
    thread.start()
    hook = hooks.hooks["point"]
    assert hook.reached.wait(2)
    assert hook.metadata.get(timeout=1) == (
        generation,
        {"point": "point", "attempt": 1},
    )
    hook.release_current()
    thread.join(timeout=2)
    assert completed.is_set()


def test_process_hook_rearm_waits_for_previous_consumer_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = multiprocessing.get_context("spawn")
    hook = ProcessHook.create(context)
    generation = hook.arm()
    completion_entered = threading.Event()
    allow_completion = threading.Event()
    original_mark_completed = hook._mark_completed

    def delayed_completion(completed_generation: int) -> None:
        completion_entered.set()
        assert allow_completion.wait(2)
        original_mark_completed(completed_generation)

    monkeypatch.setattr(hook, "_mark_completed", delayed_completion)
    consumer = threading.Thread(target=hook.hit, args=({"generation": 1},))
    consumer.start()
    assert hook.reached.wait(2)
    assert hook.metadata.get(timeout=1) == (generation, {"generation": 1})

    hook.release_current()
    assert completion_entered.wait(2)
    rearmed = threading.Event()

    def rearm() -> None:
        hook.arm()
        rearmed.set()

    controller = threading.Thread(target=rearm)
    controller.start()
    try:
        assert not rearmed.wait(0.05)
    finally:
        allow_completion.set()
        consumer.join(timeout=2)
        controller.join(timeout=2)
    assert not consumer.is_alive()
    assert rearmed.is_set()


def test_process_hook_stale_release_signal_cannot_release_new_generation() -> None:
    context = multiprocessing.get_context("spawn")
    hook = ProcessHook.create(context)

    first_generation = hook.arm()
    first = threading.Thread(target=hook.hit, args=({"generation": 1},))
    first.start()
    assert hook.reached.wait(2)
    assert hook.metadata.get(timeout=1) == (first_generation, {"generation": 1})
    hook.release_current()
    first.join(timeout=2)
    assert not first.is_alive()

    second_generation = hook.arm()
    second = threading.Thread(target=hook.hit, args=({"generation": 2},))
    second.start()
    assert hook.reached.wait(2)
    assert hook.metadata.get(timeout=1) == (second_generation, {"generation": 2})

    hook.release.set()
    second.join(timeout=0.05)
    assert second.is_alive()

    hook.release_current()
    second.join(timeout=2)
    assert not second.is_alive()


def test_process_hook_rearms_across_spawned_process_generations() -> None:
    context = multiprocessing.get_context("spawn")
    hook = ProcessHook.create(context)
    count = 200
    process = context.Process(target=_repeat_hook_in_child, args=(hook, count))
    process.start()

    try:
        for expected in range(1, count + 1):
            generation = hook.arm()
            assert hook.reached.wait(5)
            assert hook.reached_generation() == generation
            assert hook.metadata.get(timeout=1) == (
                generation,
                {"generation": expected},
            )
            hook.release_current()
        process.join(timeout=5)
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=2)

    assert process.exitcode == 0


def test_arming_new_schedule_drains_worker_blocked_on_different_hook() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("previous", "next"))
    hooks.arm("previous")
    process = context.Process(
        target=_hit_hook_sequence_in_child,
        args=(hooks, ("previous", "next")),
    )
    process.start()

    try:
        previous = hooks.hooks["previous"]
        assert previous.reached.wait(5)
        previous_generation, previous_payload = previous.metadata.get(timeout=1)
        assert previous_generation == previous.current_generation()
        assert previous_payload == {"point": "previous"}

        hooks.arm("next")
        following = hooks.hooks["next"]
        assert following.reached.wait(5)
        following_generation, following_payload = following.metadata.get(timeout=1)
        assert following_generation == following.current_generation()
        assert following_payload == {"point": "next"}
        following.release_current()
        process.join(timeout=5)
    finally:
        for hook in hooks.hooks.values():
            hook.release_current()
        if process.is_alive():
            process.kill()
            process.join(timeout=2)

    assert process.exitcode == 0


def test_arming_claim_schedule_cannot_straddle_inflight_claim_cycle() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("claim.before", "claim.result"))
    between_hooks = context.Event()
    continue_cycle = context.Event()
    start_next_cycle = context.Event()
    process = context.Process(
        target=_straddle_claim_cycle_in_child,
        args=(hooks, between_hooks, continue_cycle, start_next_cycle),
    )
    process.start()
    armed = threading.Event()

    def arm_next_cycle() -> None:
        hooks.arm("claim.before", "claim.result")
        armed.set()

    controller = threading.Thread(target=arm_next_cycle)
    try:
        assert between_hooks.wait(5)
        controller.start()
        assert not armed.wait(0.05)
        continue_cycle.set()
        controller.join(timeout=5)
        assert armed.is_set()
        start_next_cycle.set()

        before = hooks.hooks["claim.before"]
        assert before.reached.wait(5)
        before_generation, before_payload = before.metadata.get(timeout=1)
        assert before_generation == before.current_generation()
        assert before_payload == {"cycle": 2}
        before.release_current()

        result = hooks.hooks["claim.result"]
        assert result.reached.wait(5)
        result_generation, result_payload = result.metadata.get(timeout=1)
        assert result_generation == result.current_generation()
        assert result_payload == {"cycle": 2}
        result.release_current()
        process.join(timeout=5)
    finally:
        continue_cycle.set()
        start_next_cycle.set()
        for hook in hooks.hooks.values():
            hook.release_current()
        controller.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)

    assert process.exitcode == 0


def test_run_proof_measures_session_hygiene_after_worker_polling_stops(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class Worker:
        def __init__(self, worker_id: str) -> None:
            self.worker_id = worker_id

        def start_claiming(self) -> None:
            events.append(f"{self.worker_id}-started")

    class Controller:
        def __init__(self, **_kwargs: object) -> None:
            self.workers = (Worker("worker-a"), Worker("worker-b"))
            self.bundle = SimpleNamespace(
                metadata=SimpleNamespace(schema_versions={"run": "test"})
            )

        @property
        def worker_a(self) -> Worker:
            return self.workers[0]

        @property
        def worker_b(self) -> Worker:
            return self.workers[1]

        def start_workers(self) -> None:
            events.append("workers-started")

        def stop_workers(self) -> None:
            events.append("workers-stopped")

    class Provider:
        url = "http://127.0.0.1:1"

        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            events.append("provider-started")

        def stop(self) -> None:
            events.append("provider-stopped")

    def count_idle_sessions(dsn: str) -> int:
        assert dsn == "sanitized-dsn"
        assert events[-1] == "workers-stopped"
        events.append("sessions-measured")
        return 0

    monkeypatch.setattr(p5_proof, "ARTIFACT_PATH", tmp_path / "proof.json")
    monkeypatch.setattr(p5_proof, "make_conninfo", lambda dsn, **_kwargs: dsn)
    monkeypatch.setattr(p5_proof, "ProviderProcess", Provider)
    monkeypatch.setattr(p5_proof, "ProofController", Controller)
    monkeypatch.setattr(
        p5_proof,
        "bootstrap_postgres_application_schema",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(p5_proof, "idle_in_transaction_count", count_idle_sessions)
    monkeypatch.setattr(p5_proof, "postgres_version", lambda _dsn: "test")
    monkeypatch.setattr(p5_proof, "drop_schema", lambda _dsn, _schema: None)
    monkeypatch.setattr(p5_proof, "_git_value", lambda *_args: "test")
    monkeypatch.setattr(p5_proof, "assert_secret_safe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        p5_proof.faulthandler,
        "dump_traceback_later",
        lambda *_args, **_kwargs: events.append("watchdog-armed"),
    )
    monkeypatch.setattr(
        p5_proof.faulthandler,
        "cancel_dump_traceback_later",
        lambda: events.append("watchdog-cancelled"),
    )

    artifact = p5_proof.run_proof("sanitized-dsn", scenario_ids=())

    assert artifact == tmp_path / "proof.json"
    assert events.index("workers-stopped") < events.index("sessions-measured")
    assert events.count("workers-stopped") == 1
    assert events.index("watchdog-armed") < events.index("provider-started")
    assert events.index("sessions-measured") < events.index("watchdog-cancelled")


def test_process_pause_hook_is_inert_until_explicitly_armed() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("paused",))

    assert hooks.pause_process("paused", {"point": "paused"}) is False
    assert hooks.hooks["paused"].reached.is_set() is False


def test_process_pause_hook_flushes_metadata_before_sigstop() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("paused",))
    hooks.arm("paused")
    process = context.Process(target=_pause_hook_in_child, args=(hooks,))
    process.start()

    try:
        hook = hooks.hooks["paused"]
        assert hook.reached.wait(5)
        assert hook.metadata.get(timeout=1) == (
            hook.current_generation(),
            {"point": "paused", "attempt": 1},
        )
    finally:
        if process.is_alive() and process.pid is not None:
            os.kill(process.pid, signal.SIGCONT)
            process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)

    assert process.exitcode == 0


def test_connection_proxy_reports_failure_for_any_polling_operation() -> None:
    class AdminShutdown(RuntimeError):
        sqlstate = "57P01"

    class BrokenConnection:
        def execute(self, _query, _params=None):
            raise AdminShutdown("sanitized connection termination")

    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(
        context,
        ("db.connection.open", "db.connection.failed"),
    )
    open_hook = hooks.hooks["db.connection.open"]
    failure_hook = hooks.hooks["db.connection.failed"]
    open_generation = open_hook.arm()
    assert open_hook.consume() == open_generation
    open_hook.release_current()
    failure_generation = failure_hook.arm()
    proxy = _TerminatedConnection(
        BrokenConnection(),
        open_hook,
        open_generation,
        failure_hook,
        worker_id="p5-worker-a",
        backend_pid=123,
    )
    observed = threading.Event()

    def execute() -> None:
        with pytest.raises(AdminShutdown):
            proxy.execute("SELECT 1")
        observed.set()

    thread = threading.Thread(target=execute)
    thread.start()
    assert failure_hook.reached.wait(2)
    assert failure_hook.metadata.get(timeout=1) == (
        failure_generation,
        {
            "point": "db.connection.failed",
            "worker": "p5-worker-a",
            "error_type": "AdminShutdown",
            "sqlstate": "57P01",
        },
    )
    failure_hook.release_current()
    thread.join(timeout=2)
    assert observed.is_set()
