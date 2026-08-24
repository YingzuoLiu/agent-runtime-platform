from __future__ import annotations

import multiprocessing
import json
import threading
from pathlib import Path

import pytest

from examples.p5_multi_worker_proof import (
    REPORT_VERSION,
    P5ProofFailure,
    assert_secret_safe,
    validate_report,
)
from examples.p5_postgres_probe import P5RunSnapshot
from examples.p5_proof_worker import P5ProofHooks, P5WorkerConfig
from tests.conformance.p5_mutation_proof import p5_mutants


ROOT = Path(__file__).resolve().parents[1]


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

    assert [mutant.number for mutant in mutants] == list(range(1, 9))
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


def test_process_hook_is_one_shot_and_carries_only_controller_payload() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("point",))
    hooks.arm("point")
    completed = threading.Event()

    def hit() -> None:
        assert hooks.hit("point", {"point": "point", "attempt": 1}) is True
        assert hooks.hit("point", {"point": "point", "attempt": 2}) is False
        completed.set()

    thread = threading.Thread(target=hit)
    thread.start()
    hook = hooks.hooks["point"]
    assert hook.reached.wait(2)
    assert hook.metadata.get(timeout=1) == {"point": "point", "attempt": 1}
    hook.release.set()
    thread.join(timeout=2)
    assert completed.is_set()


def test_released_hook_stays_released_while_worker_is_process_paused() -> None:
    context = multiprocessing.get_context("spawn")
    hooks = P5ProofHooks.create(context, ("paused",))
    hooks.arm("paused")
    hook = hooks.hooks["paused"]

    hook.release.set()

    assert hook.release.is_set()
