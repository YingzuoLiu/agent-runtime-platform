from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

import examples.action_recovery_proof as proof_module
from examples.action_recovery_proof import (
    ComposeRuntime,
    ProofFailure,
    SCENARIOS,
    _verify_lease_transition,
    _verify_scenario,
)
from examples.runtime_lease_probe import (
    LeaseProbeFailure,
    LeaseProbeSnapshot,
    arm_live_lease,
    expire_live_lease,
    read_snapshot,
)
from runtime_service.models import RunRecord, RunStatus
from runtime_service.store import SQLiteRunStore


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def runtime_events(*, retry_mode: str, attempts: int, terminal_status: str) -> list[dict]:
    events = [
        {
            "sequence": 1,
            "event_type": "external_action.prepared",
            "status": "prepared",
            "dispatch_count": 0,
            "retry_mode": retry_mode,
        }
    ]
    for attempt in range(1, attempts + 1):
        events.append(
            {
                "sequence": len(events) + 1,
                "event_type": "external_action.dispatch_started",
                "status": "dispatching",
                "dispatch_count": attempt,
                "retry_mode": retry_mode,
            }
        )
    events.append(
        {
            "sequence": len(events) + 1,
            "event_type": f"external_action.{terminal_status}",
            "status": terminal_status,
            "dispatch_count": attempts,
            "retry_mode": retry_mode,
        }
    )
    return events


def provider_state(*, scenario: str, attempts: int, include_receipt: bool) -> dict:
    event_types = [
        "attempt.received",
        "effect.committed",
        "fault.release_requested",
        "response.ambiguous",
    ]
    if include_receipt:
        event_types.extend(["attempt.received", "receipt.replayed"])
    return {
        "scenario": scenario,
        "attempt_count": attempts,
        "effect_count": 1,
        "provider_reference": "delivery_reference",
        "waiting_for_release": False,
        "events": [
            {"event_sequence": index, "event_type": event_type}
            for index, event_type in enumerate(event_types, start=1)
        ],
    }


def lease_snapshot(
    *,
    attempt: int = 1,
    lease_live: bool = True,
    started: int = 1,
    recovered: int = 0,
    recovery_reasons: tuple[str, ...] = (),
) -> LeaseProbeSnapshot:
    return LeaseProbeSnapshot(
        status="running",
        attempt=attempt,
        lease_present=True,
        lease_live=lease_live,
        run_started_count=started,
        run_recovered_count=recovered,
        recovery_reasons=recovery_reasons,
    )


def test_proof_verifier_accepts_both_capability_paths():
    idempotent, unsafe = SCENARIOS
    _verify_scenario(
        idempotent,
        action={
            "action_id": "action-idempotent",
            "status": "succeeded",
            "result": {"provider_reference": "delivery_reference"},
        },
        action_events=runtime_events(
            retry_mode="provider_idempotent",
            attempts=2,
            terminal_status="succeeded",
        ),
        provider_state=provider_state(
            scenario="idempotent",
            attempts=2,
            include_receipt=True,
        ),
        repeated={"action_id": "action-idempotent", "status": "succeeded"},
        runtime_started_before="first",
        runtime_started_after="second",
    )
    _verify_scenario(
        unsafe,
        action={
            "action_id": "action-unsafe",
            "status": "outcome_unknown",
            "result": None,
            "error_code": "external_action_outcome_unknown",
        },
        action_events=runtime_events(
            retry_mode="unsafe",
            attempts=1,
            terminal_status="outcome_unknown",
        ),
        provider_state=provider_state(
            scenario="unsafe",
            attempts=1,
            include_receipt=False,
        ),
        repeated={"action_id": "action-unsafe", "status": "outcome_unknown"},
        runtime_started_before="second",
        runtime_started_after="third",
    )


def test_proof_verifier_rejects_a_second_unsafe_dispatch():
    unsafe = SCENARIOS[1]
    with pytest.raises(ProofFailure, match="attempt count"):
        _verify_scenario(
            unsafe,
            action={
                "action_id": "action-unsafe",
                "status": "outcome_unknown",
                "error_code": "external_action_outcome_unknown",
            },
            action_events=runtime_events(
                retry_mode="unsafe",
                attempts=2,
                terminal_status="outcome_unknown",
            ),
            provider_state=provider_state(
                scenario="unsafe",
                attempts=2,
                include_receipt=False,
            ),
            repeated={"action_id": "action-unsafe", "status": "outcome_unknown"},
            runtime_started_before="first",
            runtime_started_after="second",
        )


def test_lease_probe_arms_expires_and_observes_exactly_one_recovery(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = SQLiteRunStore(database_path)
    store.create_run_with_event(
        RunRecord(
            run_id="run-proof-lease",
            tenant_id="tenant-proof",
            thread_id="thread-proof",
            agent_id="durable-action-gateway",
            agent_version="1.0.0",
            status=RunStatus.QUEUED,
            input={"proof": True},
        ),
        event_type="run.queued",
    )
    first_claim = store.claim_next_run(
        owner_id="manager-proof-first",
        lease_duration_seconds=30,
    )
    assert first_claim is not None

    before_kill = read_snapshot(database_path, first_claim.run.run_id)
    armed = arm_live_lease(
        database_path,
        first_claim.run.run_id,
        expected_attempt=1,
    )
    expired = expire_live_lease(
        database_path,
        first_claim.run.run_id,
        expected_attempt=1,
    )
    second_claim = store.claim_next_run(
        owner_id="manager-proof-second",
        lease_duration_seconds=30,
    )
    assert second_claim is not None
    recovered = read_snapshot(database_path, first_claim.run.run_id)

    _verify_lease_transition(
        "idempotent",
        before_kill=before_kill,
        armed_while_stopped=armed,
        live_after_restart=(armed, armed),
        expired=expired,
        recovered=recovered,
    )
    serialized = json.dumps(armed.to_payload(), sort_keys=True)
    assert "lease_token" not in serialized
    assert "lease_owner_id" not in serialized
    assert first_claim.lease_token not in serialized
    assert first_claim.owner_id not in serialized
    with pytest.raises(LeaseProbeFailure, match="allowlisted"):
        LeaseProbeSnapshot.from_payload(
            {**armed.to_payload(), "lease_token": first_claim.lease_token}
        )


@pytest.mark.parametrize(
    ("live_after_restart", "recovered", "message"),
    [
        (
            lease_snapshot(attempt=2, started=2, recovered=1),
            lease_snapshot(
                attempt=2,
                lease_live=False,
                started=2,
                recovered=1,
                recovery_reasons=("lease_expired",),
            ),
            "stole or lost",
        ),
        (
            lease_snapshot(),
            lease_snapshot(attempt=1, lease_live=False),
            "not recovered exactly once",
        ),
        (
            lease_snapshot(),
            lease_snapshot(
                attempt=2,
                lease_live=False,
                started=2,
                recovered=1,
                recovery_reasons=("legacy_unleased",),
            ),
            "not recovered exactly once",
        ),
    ],
)
def test_lease_transition_verifier_rejects_invalid_recovery_evidence(
    live_after_restart,
    recovered,
    message,
):
    before_kill = lease_snapshot()
    with pytest.raises(ProofFailure, match=message):
        _verify_lease_transition(
            "idempotent",
            before_kill=before_kill,
            armed_while_stopped=before_kill,
            live_after_restart=(before_kill, live_after_restart),
            expired=replace(before_kill, lease_live=False),
            recovered=recovered,
        )


def test_lease_transition_verifier_requires_expiry_before_recovery_evidence():
    before_kill = lease_snapshot()
    with pytest.raises(ProofFailure, match="inside the expiry transaction"):
        _verify_lease_transition(
            "idempotent",
            before_kill=before_kill,
            armed_while_stopped=before_kill,
            live_after_restart=(before_kill, before_kill),
            expired=lease_snapshot(
                attempt=1,
                lease_live=False,
                started=2,
                recovered=1,
                recovery_reasons=("lease_expired",),
            ),
            recovered=lease_snapshot(
                attempt=2,
                lease_live=False,
                started=2,
                recovered=1,
                recovery_reasons=("lease_expired",),
            ),
        )


def test_compose_lease_probe_uses_one_off_container_only_while_runtime_is_stopped(
    monkeypatch,
):
    calls: list[tuple[tuple[str, ...], bool]] = []
    payload = json.dumps(lease_snapshot().to_payload())

    def fake_run(*arguments: str, capture: bool = False) -> str:
        calls.append((arguments, capture))
        return payload

    monkeypatch.setattr(ComposeRuntime, "_run", staticmethod(fake_run))
    compose = ComposeRuntime(build=False)

    compose.lease_probe("snapshot", "run-proof")
    compose.lease_probe(
        "arm",
        "run-proof",
        expected_attempt=1,
        runtime_stopped=True,
    )

    assert calls == [
        (
            (
                "exec",
                "-T",
                "runtime",
                "python",
                "examples/runtime_lease_probe.py",
                "snapshot",
                "/app/runtime_data/runtime.db",
                "run-proof",
            ),
            True,
        ),
        (
            (
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "python",
                "runtime",
                "examples/runtime_lease_probe.py",
                "arm",
                "/app/runtime_data/runtime.db",
                "run-proof",
                "--expected-attempt",
                "1",
            ),
            True,
        ),
    ]


def test_documented_proof_script_entrypoint_can_import_its_probe():
    completed = subprocess.run(
        [sys.executable, "examples/action_recovery_proof.py", "--help"],
        cwd=REPOSITORY_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    assert "Durable Action Gateway restart proof" in completed.stdout


def test_failed_proof_cannot_leave_a_stale_pass_artifact(tmp_path, monkeypatch):
    artifact_path = tmp_path / "artifacts" / "action-recovery-proof.json"
    artifact_path.parent.mkdir()
    artifact_path.write_text('{"result":"passed"}\n', encoding="utf-8")

    def fail_start(_self, *, include_build=None):
        raise ProofFailure("injected startup failure")

    monkeypatch.setattr(proof_module, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(ComposeRuntime, "start", fail_start)

    with pytest.raises(ProofFailure, match="injected startup failure"):
        proof_module.run_proof(
            build=False,
            runtime_url="http://127.0.0.1:8000",
            provider_url="http://127.0.0.1:8100",
        )

    assert not artifact_path.exists()
