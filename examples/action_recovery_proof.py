from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if __package__:
    from examples.runtime_lease_probe import LeaseProbeFailure, LeaseProbeSnapshot
else:
    from runtime_lease_probe import LeaseProbeFailure, LeaseProbeSnapshot


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_URL = "http://127.0.0.1:8000"
DEFAULT_PROVIDER_URL = "http://127.0.0.1:8100"
RUNTIME_DATABASE_PATH = "/app/runtime_data/runtime.db"
LEASE_OBSERVATION_SECONDS = 1.0
TERMINAL_ACTION_STATUSES = frozenset({"succeeded", "failed", "cancelled", "outcome_unknown"})


class ProofFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    destination: str
    expected_status: str
    expected_attempts: int
    expected_retry_mode: str


SCENARIOS = (
    ScenarioSpec(
        name="idempotent",
        destination="safe-retry",
        expected_status="succeeded",
        expected_attempts=2,
        expected_retry_mode="provider_idempotent",
    ),
    ScenarioSpec(
        name="unsafe",
        destination="unsafe-no-retry",
        expected_status="outcome_unknown",
        expected_attempts=1,
        expected_retry_mode="unsafe",
    ),
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProofFailure(message)


def _json_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    accepted_statuses: frozenset[int] = frozenset({200}),
    timeout: float = 10,
) -> tuple[int, Any]:
    body = None
    resolved_headers = {"Accept": "application/json", **(headers or {})}
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        resolved_headers["Content-Type"] = "application/json"
    request = Request(
        url,
        data=body,
        headers=resolved_headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            raw = response.read()
    except HTTPError as exc:
        status = exc.code
        raw = exc.read()
    except (URLError, TimeoutError, ConnectionError) as exc:
        reason = getattr(exc, "reason", exc)
        raise ProofFailure(f"Could not reach {url}: {reason}") from None
    if status not in accepted_statuses:
        detail = raw.decode("utf-8", errors="replace")[:500]
        raise ProofFailure(f"Unexpected HTTP {status} from {url}: {detail}")
    try:
        return status, json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ProofFailure(f"Non-JSON response from {url}") from None


def _wait_until(
    operation,
    predicate,
    *,
    description: str,
    timeout: float = 20,
    interval: float = 0.1,
):
    deadline = time.monotonic() + timeout
    last_value = None
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            last_value = operation()
            last_error = None
        except ProofFailure as exc:
            last_value = None
            last_error = str(exc)
        if last_value is not None and predicate(last_value):
            return last_value
        time.sleep(interval)
    detail = repr(last_value) if last_value is not None else (last_error or "no observation")
    raise ProofFailure(f"Timed out waiting for {description}; last observation: {detail}")


class ComposeRuntime:
    def __init__(self, *, build: bool) -> None:
        self.build = build

    @staticmethod
    def _run(*arguments: str, capture: bool = False) -> str:
        command = ["docker", "compose", *arguments]
        try:
            completed = subprocess.run(
                command,
                cwd=REPOSITORY_ROOT,
                check=True,
                text=True,
                capture_output=capture,
            )
        except FileNotFoundError:
            raise ProofFailure("Docker CLI was not found.") from None
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            raise ProofFailure(
                f"Command failed: {' '.join(command)}" + (f"\n{detail}" if detail else "")
            ) from None
        return completed.stdout.strip() if capture else ""

    def start(self, *, include_build: bool | None = None) -> None:
        should_build = self.build if include_build is None else include_build
        arguments = ["up", "-d"]
        if should_build:
            arguments.append("--build")
        arguments.extend(["--wait", "--wait-timeout", "120", "runtime"])
        self._run(*arguments)

    def restart_runtime(self) -> None:
        # Keep the provider outside Compose's convergence graph and prevent
        # replacement of the existing killed Runtime container.
        self._run(
            "up",
            "-d",
            "--no-deps",
            "--no-recreate",
            "--wait",
            "--wait-timeout",
            "120",
            "runtime",
        )

    def kill_runtime(self) -> None:
        self._run("kill", "-s", "SIGKILL", "runtime")

    def started_at(self, service: str) -> str:
        container_id = self._run("ps", "-q", service, capture=True)
        if not container_id:
            raise ProofFailure(f"Compose service {service!r} has no container.")
        try:
            completed = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{.State.StartedAt}}",
                    container_id,
                ],
                cwd=REPOSITORY_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ProofFailure(
                f"Could not inspect the {service!r} container: {exc.stderr.strip()}"
            ) from None
        return completed.stdout.strip()

    def lease_probe(
        self,
        operation: str,
        run_id: str,
        *,
        expected_attempt: int | None = None,
        runtime_stopped: bool = False,
    ) -> LeaseProbeSnapshot:
        probe_arguments = [
            "examples/runtime_lease_probe.py",
            operation,
            RUNTIME_DATABASE_PATH,
            run_id,
        ]
        if expected_attempt is not None:
            probe_arguments.extend(["--expected-attempt", str(expected_attempt)])
        if runtime_stopped:
            output = self._run(
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "python",
                "runtime",
                *probe_arguments,
                capture=True,
            )
        else:
            output = self._run(
                "exec",
                "-T",
                "runtime",
                "python",
                *probe_arguments,
                capture=True,
            )
        try:
            payload = json.loads(output)
            return LeaseProbeSnapshot.from_payload(payload)
        except (json.JSONDecodeError, LeaseProbeFailure) as exc:
            raise ProofFailure(f"Invalid lease probe output: {exc}") from None


def _demo_headers(runtime_url: str) -> dict[str, str]:
    _, session = _json_request(f"{runtime_url}/demo/session")
    api_key = session.get("api_key") if isinstance(session, dict) else None
    _require(isinstance(api_key, str) and bool(api_key), "Demo session did not return an API key.")
    return {"Authorization": f"Bearer {api_key}"}


def _action_payload(spec: ScenarioSpec, *, proof_id: str) -> dict[str, Any]:
    return {
        "action_type": "webhook.send",
        "destination": spec.destination,
        "idempotency_key": f"action-recovery-proof-{proof_id}-{spec.name}",
        "input": {
            "payload": {
                "proof_id": proof_id,
                "scenario": spec.name,
                "message": "durable action recovery proof",
            }
        },
    }


def _provider_state(provider_url: str, action_id: str) -> dict[str, Any]:
    _, state = _json_request(
        f"{provider_url}/proof/actions/{action_id}",
        accepted_statuses=frozenset({200, 404}),
    )
    if not isinstance(state, dict) or "scenario" not in state:
        raise ProofFailure("Provider proof state is not available yet.")
    return state


def _action_state(runtime_url: str, action_id: str, headers: dict[str, str]) -> dict[str, Any]:
    _, action = _json_request(
        f"{runtime_url}/actions/{action_id}",
        headers=headers,
    )
    _require(isinstance(action, dict), "Runtime Action response is not an object.")
    return action


def _sanitized_action(action: dict[str, Any]) -> dict[str, Any]:
    return {
        field: action.get(field)
        for field in (
            "action_id",
            "action_type",
            "destination",
            "status",
            "result",
            "error_code",
            "error_message",
            "created_at",
            "updated_at",
            "completed_at",
        )
    }


def _event_types(provider_state: dict[str, Any]) -> list[str]:
    events = provider_state.get("events")
    if not isinstance(events, list):
        raise ProofFailure("Provider proof events are missing.")
    return [str(event.get("event_type")) for event in events if isinstance(event, dict)]


def _verify_lease_transition(
    scenario_name: str,
    *,
    before_kill: LeaseProbeSnapshot,
    armed_while_stopped: LeaseProbeSnapshot,
    live_after_restart: tuple[LeaseProbeSnapshot, LeaseProbeSnapshot],
    expired: LeaseProbeSnapshot,
    recovered: LeaseProbeSnapshot,
) -> None:
    prefix = f"{scenario_name}:"
    _require(
        before_kill.status == "running"
        and before_kill.attempt == 1
        and before_kill.lease_present
        and before_kill.lease_live,
        f"{prefix} initial Run attempt did not hold a live lease.",
    )
    _require(
        before_kill.run_started_count == 1
        and before_kill.run_recovered_count == 0
        and not before_kill.recovery_reasons,
        f"{prefix} initial Run event evidence is wrong.",
    )
    for snapshot in (armed_while_stopped, *live_after_restart):
        _require(
            snapshot.status == "running"
            and snapshot.attempt == before_kill.attempt
            and snapshot.lease_present
            and snapshot.lease_live,
            f"{prefix} restarted Runtime stole or lost the live lease.",
        )
        _require(
            snapshot.run_started_count == before_kill.run_started_count
            and snapshot.run_recovered_count == before_kill.run_recovered_count
            and snapshot.recovery_reasons == before_kill.recovery_reasons,
            f"{prefix} recovery evidence appeared before lease expiry.",
        )
    _require(
        expired.status == "running"
        and expired.attempt == before_kill.attempt
        and expired.lease_present
        and not expired.lease_live,
        f"{prefix} exact store-time lease expiry was not injected.",
    )
    _require(
        expired.run_started_count == before_kill.run_started_count
        and expired.run_recovered_count == before_kill.run_recovered_count
        and expired.recovery_reasons == before_kill.recovery_reasons,
        f"{prefix} recovery evidence changed inside the expiry transaction.",
    )
    _require(
        recovered.attempt == before_kill.attempt + 1
        and recovered.run_started_count == before_kill.run_started_count + 1
        and recovered.run_recovered_count == before_kill.run_recovered_count + 1
        and recovered.recovery_reasons == ("lease_expired",),
        f"{prefix} expired Run was not recovered exactly once by a new attempt.",
    )


def _lease_transition_artifact(
    before_kill: LeaseProbeSnapshot,
    recovered: LeaseProbeSnapshot,
) -> dict[str, Any]:
    return {
        "pre_expiry_no_takeover": True,
        "post_expiry_takeover": True,
        "attempt_before": before_kill.attempt,
        "attempt_after": recovered.attempt,
        "run_started_count": recovered.run_started_count,
        "run_recovered_count": recovered.run_recovered_count,
        "recovery_reason": recovered.recovery_reasons[-1],
        "exact_store_time_expiry_injected": True,
    }


def _verify_scenario(
    spec: ScenarioSpec,
    *,
    action: dict[str, Any],
    action_events: list[dict[str, Any]],
    provider_state: dict[str, Any],
    repeated: dict[str, Any],
    runtime_started_before: str,
    runtime_started_after: str,
) -> None:
    _require(action.get("status") == spec.expected_status, f"{spec.name}: wrong Action status.")
    _require(
        repeated.get("action_id") == action.get("action_id"),
        f"{spec.name}: duplicate submission created a new Action.",
    )
    _require(
        repeated.get("status") == spec.expected_status,
        f"{spec.name}: duplicate submission changed the terminal status.",
    )
    _require(
        provider_state.get("attempt_count") == spec.expected_attempts,
        f"{spec.name}: unexpected provider attempt count.",
    )
    _require(
        provider_state.get("effect_count") == 1,
        f"{spec.name}: provider effect was not exactly once.",
    )
    _require(
        runtime_started_before != runtime_started_after,
        f"{spec.name}: Runtime restart was not observed.",
    )
    _require(bool(action_events), f"{spec.name}: Action events are missing.")
    raw_sequences = [event.get("sequence") for event in action_events]
    _require(
        all(isinstance(sequence, int) for sequence in raw_sequences),
        f"{spec.name}: Action event sequence contains a non-integer.",
    )
    sequences = [cast(int, sequence) for sequence in raw_sequences]
    _require(
        sequences == sorted(sequences) and len(sequences) == len(set(sequences)),
        f"{spec.name}: Action event sequence is not strictly ordered.",
    )
    retry_modes = {event.get("retry_mode") for event in action_events}
    _require(
        retry_modes == {spec.expected_retry_mode},
        f"{spec.name}: Action retry-mode evidence is wrong.",
    )
    dispatch_counts = [
        int(event["dispatch_count"])
        for event in action_events
        if isinstance(event.get("dispatch_count"), int)
    ]
    _require(
        max(dispatch_counts, default=0) == spec.expected_attempts,
        f"{spec.name}: durable dispatch count is wrong.",
    )
    provider_event_types = _event_types(provider_state)
    _require(
        "effect.committed" in provider_event_types
        and "fault.release_requested" in provider_event_types
        and "response.ambiguous" in provider_event_types,
        f"{spec.name}: provider fault evidence is incomplete.",
    )
    provider_events = provider_state.get("events")
    if not isinstance(provider_events, list):
        raise ProofFailure(f"{spec.name}: provider events are missing.")
    raw_provider_sequences = [
        event.get("event_sequence") for event in provider_events if isinstance(event, dict)
    ]
    _require(
        all(isinstance(sequence, int) for sequence in raw_provider_sequences),
        f"{spec.name}: provider event_sequence contains a non-integer.",
    )
    provider_sequences = [cast(int, sequence) for sequence in raw_provider_sequences]
    _require(
        provider_sequences == sorted(provider_sequences)
        and len(provider_sequences) == len(set(provider_sequences)),
        f"{spec.name}: provider event_sequence is not strictly ordered.",
    )
    if spec.name == "idempotent":
        _require(
            "receipt.replayed" in provider_event_types,
            "idempotent: provider did not replay the persisted receipt.",
        )
        reference = provider_state.get("provider_reference")
        _require(
            action.get("result") == {"provider_reference": reference},
            "idempotent: Runtime result does not match the provider receipt.",
        )
    else:
        _require(
            "receipt.replayed" not in provider_event_types,
            "unsafe: Runtime unexpectedly requested a receipt replay.",
        )
        _require(
            action.get("error_code") == "external_action_outcome_unknown",
            "unsafe: terminal uncertainty was not projected explicitly.",
        )


def _run_scenario(
    spec: ScenarioSpec,
    *,
    proof_id: str,
    runtime_url: str,
    provider_url: str,
    compose: ComposeRuntime,
) -> dict[str, Any]:
    print(f"\nRunning {spec.name} recovery path...")
    headers = _demo_headers(runtime_url)
    payload = _action_payload(spec, proof_id=proof_id)
    _, submitted = _json_request(
        f"{runtime_url}/actions?wait=0",
        method="POST",
        payload=payload,
        headers=headers,
        accepted_statuses=frozenset({200, 202}),
    )
    _require(isinstance(submitted, dict), f"{spec.name}: submission response is invalid.")
    action_id = submitted.get("action_id")
    _require(isinstance(action_id, str) and bool(action_id), f"{spec.name}: no Action ID.")

    waiting_state = _wait_until(
        lambda: _provider_state(provider_url, action_id),
        lambda state: state.get("waiting_for_release") is True and state.get("effect_count") == 1,
        description=f"{spec.name} provider effect commit",
    )
    print("  provider effect committed; Runtime response is still unresolved")
    _require(
        waiting_state.get("attempt_count") == 1,
        f"{spec.name}: more than one attempt occurred before restart.",
    )
    before_kill = compose.lease_probe("snapshot", action_id)

    runtime_started_before = compose.started_at("runtime")
    runtime_was_killed = False
    try:
        compose.kill_runtime()
        runtime_was_killed = True
        print("  Runtime killed after provider commit")
        armed_while_stopped = compose.lease_probe(
            "arm",
            action_id,
            expected_attempt=before_kill.attempt,
            runtime_stopped=True,
        )
        _json_request(
            f"{provider_url}/proof/actions/{action_id}/release",
            method="POST",
            payload={},
        )
        _wait_until(
            lambda: _provider_state(provider_url, action_id),
            lambda state: "response.ambiguous" in _event_types(state),
            description=f"{spec.name} injected ambiguous response",
        )
        compose.restart_runtime()
        runtime_was_killed = False
    finally:
        if runtime_was_killed:
            try:
                _json_request(
                    f"{provider_url}/proof/actions/{action_id}/release",
                    method="POST",
                    payload={},
                    accepted_statuses=frozenset({200, 404}),
                )
            except ProofFailure:
                pass
            compose.restart_runtime()

    runtime_started_after = compose.started_at("runtime")
    _require(
        runtime_started_before != runtime_started_after,
        f"{spec.name}: Runtime container did not restart.",
    )
    print("  Runtime restarted; live lease remains owned by the killed attempt")

    headers = _demo_headers(runtime_url)
    first_live_after_restart = compose.lease_probe("snapshot", action_id)
    action_before_expiry = _action_state(runtime_url, action_id, headers)
    provider_before_expiry = _provider_state(provider_url, action_id)
    time.sleep(LEASE_OBSERVATION_SECONDS)
    second_live_after_restart = compose.lease_probe("snapshot", action_id)
    action_after_observation = _action_state(runtime_url, action_id, headers)
    provider_after_observation = _provider_state(provider_url, action_id)
    _require(
        action_before_expiry.get("status") not in TERMINAL_ACTION_STATUSES
        and action_after_observation.get("status") not in TERMINAL_ACTION_STATUSES,
        f"{spec.name}: Action became terminal before lease expiry.",
    )
    _require(
        provider_before_expiry.get("attempt_count") == 1
        and provider_after_observation.get("attempt_count") == 1,
        f"{spec.name}: provider was called again before lease expiry.",
    )
    expired = compose.lease_probe(
        "expire",
        action_id,
        expected_attempt=before_kill.attempt,
    )
    recovered = _wait_until(
        lambda: compose.lease_probe("snapshot", action_id),
        lambda snapshot: (
            snapshot.attempt == before_kill.attempt + 1
            and snapshot.run_started_count == before_kill.run_started_count + 1
            and snapshot.run_recovered_count == before_kill.run_recovered_count + 1
        ),
        description=f"{spec.name} lease-expiry takeover",
        interval=0.25,
    )
    _verify_lease_transition(
        spec.name,
        before_kill=before_kill,
        armed_while_stopped=armed_while_stopped,
        live_after_restart=(first_live_after_restart, second_live_after_restart),
        expired=expired,
        recovered=recovered,
    )
    print("  live lease was not stolen; exact expiry produced attempt 2")

    action = _wait_until(
        lambda: _action_state(runtime_url, action_id, headers),
        lambda state: state.get("status") in TERMINAL_ACTION_STATUSES,
        description=f"{spec.name} terminal Action",
    )
    _, raw_events = _json_request(
        f"{runtime_url}/actions/{action_id}/events?after_sequence=0",
        headers=headers,
    )
    _require(isinstance(raw_events, list), f"{spec.name}: Action event response is invalid.")
    action_events = [event for event in raw_events if isinstance(event, dict)]
    provider_state = _provider_state(provider_url, action_id)

    _, repeated = _json_request(
        f"{runtime_url}/actions?wait=5",
        method="POST",
        payload=payload,
        headers=headers,
        accepted_statuses=frozenset({200, 202}),
    )
    _require(isinstance(repeated, dict), f"{spec.name}: duplicate response is invalid.")
    provider_after_duplicate = _provider_state(provider_url, action_id)
    _require(
        provider_after_duplicate.get("attempt_count") == provider_state.get("attempt_count")
        and provider_after_duplicate.get("effect_count") == provider_state.get("effect_count"),
        f"{spec.name}: duplicate POST caused another provider call.",
    )

    _verify_scenario(
        spec,
        action=action,
        action_events=action_events,
        provider_state=provider_after_duplicate,
        repeated=repeated,
        runtime_started_before=runtime_started_before,
        runtime_started_after=runtime_started_after,
    )
    print(f"  PASS: status={spec.expected_status}, attempts={spec.expected_attempts}, effects=1")
    return {
        "scenario": spec.name,
        "action": _sanitized_action(action),
        "runtime_events": action_events,
        "provider_evidence": provider_after_duplicate,
        "restart": {
            "runtime_started_before": runtime_started_before,
            "runtime_started_after": runtime_started_after,
            "observed": True,
        },
        "run_lease_recovery": _lease_transition_artifact(before_kill, recovered),
        "duplicate_submission_reused_action": True,
    }


def run_proof(*, build: bool, runtime_url: str, provider_url: str) -> Path:
    artifact_path = REPOSITORY_ROOT / "artifacts" / "action-recovery-proof.json"
    artifact_path.unlink(missing_ok=True)
    compose = ComposeRuntime(build=build)
    print("Starting the local Runtime and provider sidecar...")
    compose.start()
    provider_started_before = compose.started_at("demo-provider")
    proof_id = uuid.uuid4().hex[:12]
    results = [
        _run_scenario(
            spec,
            proof_id=proof_id,
            runtime_url=runtime_url,
            provider_url=provider_url,
            compose=compose,
        )
        for spec in SCENARIOS
    ]
    provider_started_after = compose.started_at("demo-provider")
    _require(
        provider_started_before == provider_started_after,
        "Provider sidecar restarted; independent-lifecycle proof is invalid.",
    )

    artifact = {
        "proof": "durable-action-recovery:2",
        "generated_at": datetime.now(UTC).isoformat(),
        "proof_id": proof_id,
        "result": "passed",
        "provider_lifecycle": {
            "started_at": provider_started_before,
            "unchanged_across_runtime_restarts": True,
        },
        "scenarios": results,
    }
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the deterministic Durable Action Gateway restart proof."
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Reuse the existing Compose image after a successful warm-up run.",
    )
    parser.add_argument("--runtime-url", default=DEFAULT_RUNTIME_URL)
    parser.add_argument("--provider-url", default=DEFAULT_PROVIDER_URL)
    args = parser.parse_args()
    try:
        artifact_path = run_proof(
            build=not args.no_build,
            runtime_url=args.runtime_url.rstrip("/"),
            provider_url=args.provider_url.rstrip("/"),
        )
    except ProofFailure as exc:
        print(f"\nACTION GATEWAY RECOVERY PROOF: FAILED\n{exc}", file=sys.stderr)
        return 1

    print("\nACTION GATEWAY RECOVERY PROOF: PASSED")
    print("  safe-retry:       2 attempts, 1 effect, succeeded with stored receipt")
    print("  unsafe-no-retry:  1 attempt, 1 effect, outcome_unknown without replay")
    print("  each live lease resisted takeover, then recovered once at exact expiry")
    print("  Runtime restarted twice; provider lifecycle stayed unchanged")
    print(f"  Sanitized artifact: {artifact_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
