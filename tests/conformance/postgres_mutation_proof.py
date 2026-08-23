from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POSTGRES_STORE = ROOT / "runtime_service" / "postgres_store.py"
POSTGRES_SCHEMA = ROOT / "runtime_service" / "postgres_schema.py"
POSTGRES_WORKFLOW_STORE = ROOT / "runtime_service" / "postgres_workflow_store.py"
EXTERNAL_ACTION_COORDINATOR = ROOT / "runtime_service" / "external_action_coordinator.py"

RUN_CONTRACT = "tests/conformance/test_run_store_contract.py"
EXECUTION_CONTRACT = "tests/conformance/test_execution_plane_contract.py"
QUARANTINE_EVIDENCE_CONTRACT = "tests/conformance/test_quarantine_evidence_contract.py"
POSTGRES_MECHANICS = "tests/conformance/test_postgres_backend.py"


@dataclass(frozen=True)
class Replacement:
    path: Path
    old: str
    new: str
    count: int = 1


@dataclass(frozen=True)
class Mutant:
    number: int
    description: str
    test_target: str
    replacements: tuple[Replacement, ...]


def _pytest(target: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["STORE_CONFORMANCE_BACKENDS"] = "postgres"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", target],
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _replace_exact(replacement: Replacement) -> None:
    original = replacement.path.read_text(encoding="utf-8")
    observed = original.count(replacement.old)
    if observed != replacement.count:
        relative = replacement.path.relative_to(ROOT)
        raise RuntimeError(
            f"mutation source drift for {relative}: expected {replacement.count} "
            f"occurrence(s), observed {observed}"
        )
    replacement.path.write_text(
        original.replace(replacement.old, replacement.new, replacement.count),
        encoding="utf-8",
    )


def _run_mutant(mutant: Mutant) -> None:
    paths = {replacement.path for replacement in mutant.replacements}
    originals = {path: path.read_bytes() for path in paths}
    try:
        for replacement in mutant.replacements:
            _replace_exact(replacement)
        result = _pytest(mutant.test_target)
    finally:
        for path, content in originals.items():
            path.write_bytes(content)

    # Pytest exit 1 means the selected semantic test failed. Collection,
    # command-line, and internal errors use different codes and are not accepted
    # as mutation evidence.
    if result.returncode != 1:
        print(result.stdout)
        raise RuntimeError(
            f"mutant {mutant.number} did not fail its target test with pytest exit 1; "
            f"observed exit {result.returncode}"
        )
    print(
        f"M{mutant.number:02d} KILLED | {mutant.description} | "
        f"{mutant.test_target}"
    )


def main() -> int:
    if not os.environ.get("TEST_POSTGRES_DSN"):
        raise RuntimeError("TEST_POSTGRES_DSN is required for PostgreSQL mutation proof")

    atomic_target = (
        f"{RUN_CONTRACT}::test_i6_completion_checkpoint_and_required_events_commit_atomically"
    )
    mutants = (
        Mutant(
            1,
            "remove sampled Run lease-token predicate",
            f"{RUN_CONTRACT}::test_i1_i2_one_live_owner_and_stale_run_attempt_is_fenced",
            (
                Replacement(
                    POSTGRES_STORE,
                    "WHERE run_id = %s AND status = %s AND lease_token = %s\n"
                    "                AND lease_expires_at > %s\n"
                    "            FOR UPDATE",
                    "WHERE run_id = %s AND status = %s AND lease_token = lease_token\n"
                    "                AND %s::text IS NOT NULL AND lease_expires_at > %s\n"
                    "            FOR UPDATE",
                ),
            ),
        ),
        Mutant(
            2,
            "make exact lease expiry non-claimable (<= to <)",
            f"{RUN_CONTRACT}::test_lease_expiry_uses_the_injected_store_clock_exactly",
            (
                Replacement(
                    POSTGRES_STORE,
                    "OR candidate.lease_expires_at <= %s",
                    "OR candidate.lease_expires_at < %s",
                ),
            ),
        ),
        Mutant(
            3,
            "remove one-running-Run-per-Thread uniqueness",
            f"{POSTGRES_MECHANICS}::test_postgres_expected_unique_and_fk_constraints_are_enforced",
            (
                Replacement(
                    POSTGRES_SCHEMA,
                    '"CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_running_per_thread "',
                    '"CREATE INDEX IF NOT EXISTS idx_runs_one_running_per_thread "',
                ),
            ),
        ),
        Mutant(
            4,
            "remove tenant qualification from running-Thread uniqueness",
            f"{RUN_CONTRACT}::test_i4_thread_scope_allows_independent_claims",
            (
                Replacement(
                    POSTGRES_SCHEMA,
                    '"ON runs(tenant_id, thread_id) WHERE status = \'running\'"',
                    '"ON runs(thread_id) WHERE status = \'running\'"',
                ),
            ),
        ),
        Mutant(
            5,
            "remove checkpoint revision CAS predicate",
            f"{POSTGRES_MECHANICS}::test_postgres_checkpoint_write_rejects_stale_revision",
            (
                Replacement(
                    POSTGRES_STORE,
                    "WHERE thread_states.revision = %s\n"
                    "            RETURNING revision",
                    "WHERE TRUE OR thread_states.revision = %s\n"
                    "            RETURNING revision",
                ),
            ),
        ),
        Mutant(
            6,
            "persist full execution trace into Thread checkpoint",
            atomic_target,
            (
                Replacement(
                    POSTGRES_STORE,
                    "checkpoint_state = project_thread_checkpoint_state(run.state)",
                    "checkpoint_state = run.state.model_copy(deep=True)",
                ),
            ),
        ),
        Mutant(
            7,
            "remove the completion transaction boundary",
            atomic_target,
            (
                Replacement(
                    POSTGRES_STORE,
                    "import json\n",
                    "import contextlib\nimport json\n",
                ),
                Replacement(
                    POSTGRES_STORE,
                    "        cas_changed = False\n"
                    "        connection = self._lease_connect()\n"
                    "        try:\n"
                    "            try:\n"
                    "                with connection.transaction():",
                    "        cas_changed = False\n"
                    "        connection = self._lease_connect()\n"
                    "        try:\n"
                    "            try:\n"
                    "                with contextlib.nullcontext():",
                ),
            ),
        ),
        Mutant(
            8,
            "append run.completed outside the completion transaction",
            atomic_target,
            (
                Replacement(
                    POSTGRES_STORE,
                    "                    self._append_event_with_connection(\n"
                    "                        connection,\n"
                    "                        run.run_id,\n"
                    "                        \"run.completed\",\n"
                    "                        {\"validation_errors\": run.validation_errors},\n"
                    "                    )\n"
                    "            except _CheckpointCASChanged:\n"
                    "                cas_changed = True",
                    "            except _CheckpointCASChanged:\n"
                    "                cas_changed = True\n"
                    "            if not cas_changed:\n"
                    "                self._append_event_with_connection(\n"
                    "                    connection,\n"
                    "                    run.run_id,\n"
                    "                    \"run.completed\",\n"
                    "                    {\"validation_errors\": run.validation_errors},\n"
                    "                )",
                ),
            ),
        ),
        Mutant(
            9,
            "retry an unsafe provider after ambiguous dispatch",
            f"{EXECUTION_CONTRACT}::test_i7_reconciliation_precedes_successor_and_never_retries_unsafe_effect",
            (
                Replacement(
                    EXTERNAL_ACTION_COORDINATOR,
                    "if spec.retry_mode == ToolRetryMode.UNSAFE:\n"
                    "            self._raise_external_outcome_unknown(",
                    "if False and spec.retry_mode == ToolRetryMode.UNSAFE:\n"
                    "            self._raise_external_outcome_unknown(",
                ),
                Replacement(
                    POSTGRES_WORKFLOW_STORE,
                    "if action.retry_mode == ToolRetryMode.UNSAFE:\n"
                    "                    return ExternalActionDispatchResult(",
                    "if False and action.retry_mode == ToolRetryMode.UNSAFE:\n"
                    "                    return ExternalActionDispatchResult(",
                ),
            ),
        ),
        Mutant(
            10,
            "ignore the re-derived quarantine plan identity",
            f"{QUARANTINE_EVIDENCE_CONTRACT}::test_i9_workflow_evidence_change_after_plan_makes_plan_stale",
            (
                Replacement(
                    POSTGRES_STORE,
                    "if not plan.eligible or plan.plan_id != expected_plan_id:",
                    "if not plan.eligible:",
                ),
            ),
        ),
        Mutant(
            11,
            "accept same-revision checkpoint evidence drift",
            f"{QUARANTINE_EVIDENCE_CONTRACT}::test_i9_same_revision_checkpoint_evidence_drift_is_detected_after_commit",
            (
                Replacement(
                    POSTGRES_STORE,
                    "if observed_revision == baseline_revision:\n"
                    "            return observed_fingerprint == baseline_fingerprint",
                    "if observed_revision == baseline_revision:\n"
                    "            return True",
                ),
            ),
        ),
        Mutant(
            12,
            "reject legal later successor checkpoint progress",
            f"{EXECUTION_CONTRACT}::test_i9_unchanged_eligible_plan_releases_quarantine_preserving_evidence",
            (
                Replacement(
                    POSTGRES_STORE,
                    "if observed_revision < baseline_revision:\n"
                    "            return False",
                    "if observed_revision != baseline_revision:\n"
                    "            return False",
                ),
            ),
        ),
    )

    failures: list[str] = []
    for mutant in mutants:
        try:
            _run_mutant(mutant)
        except Exception as exc:
            message = f"M{mutant.number:02d} SURVIVED/INVALID | {exc}"
            failures.append(message)
            print(message)

    if failures:
        print("POSTGRES STORE MUTATION PROOF: FAILED")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("POSTGRES STORE MUTATION PROOF: 12/12 KILLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
