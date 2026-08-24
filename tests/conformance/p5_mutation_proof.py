from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POSTGRES_STORE = ROOT / "runtime_service" / "postgres_store.py"
POSTGRES_SCHEMA = ROOT / "runtime_service" / "postgres_schema.py"
POSTGRES_MEMORY_STORE = ROOT / "runtime_service" / "postgres_memory_store.py"
RUNTIME_MANAGER = ROOT / "runtime_service" / "manager.py"
P5_WORKER = ROOT / "examples" / "p5_proof_worker.py"
P5_CONTROLLER = ROOT / "examples" / "p5_multi_worker_proof.py"


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
    scenario: str
    expected_diagnostic: tuple[str, ...]
    replacements: tuple[Replacement, ...]


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
    with tempfile.TemporaryDirectory(prefix=f"p5-mutant-{mutant.number}-") as pycache:
        try:
            for replacement in mutant.replacements:
                _replace_exact(replacement)
            environment = os.environ.copy()
            environment["PYTHONPYCACHEPREFIX"] = pycache
            result = subprocess.run(
                [
                    sys.executable,
                    "examples/p5_multi_worker_proof.py",
                    "--scenarios",
                    mutant.scenario,
                ],
                cwd=ROOT,
                env=environment,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
            )
        finally:
            for path, content in originals.items():
                path.write_bytes(content)

    if result.returncode != 1:
        print(result.stdout[-4_000:])
        raise RuntimeError(
            f"mutant {mutant.number} did not die with proof exit 1; "
            f"observed exit {result.returncode}"
        )
    if not any(fragment in result.stdout for fragment in mutant.expected_diagnostic):
        print(result.stdout[-4_000:])
        raise RuntimeError(
            f"mutant {mutant.number} died without its intended semantic diagnostic"
        )
    print(
        f"P5M{mutant.number:02d} KILLED | {mutant.description} | "
        f"scenario={mutant.scenario}"
    )


def p5_mutants() -> tuple[Mutant, ...]:
    live_takeover = (
        Replacement(
            POSTGRES_STORE,
            "OR candidate.lease_expires_at <= %s",
            "OR candidate.lease_expires_at >= %s",
        ),
        Replacement(
            POSTGRES_STORE,
            "OR lease_expires_at <= %s",
            "OR lease_expires_at >= %s",
        ),
    )
    return (
        Mutant(
            1,
            "allow a competing process to replace the same live Run owner",
            "S1",
            ("Competing worker acquired the same live Run",),
            live_takeover,
        ),
        Mutant(
            2,
            "make the process-local wake event required by disabling bounded polling",
            "S3",
            (
                "did not reach claim.before",
                "durable polling did not discover a cross-process submission",
            ),
            (
                Replacement(
                    RUNTIME_MANAGER,
                    "self._wake.wait(self.poll_interval_seconds)",
                    "self._wake.wait()",
                    count=2,
                ),
            ),
        ),
        Mutant(
            3,
            "remove same-Thread claim exclusion and its unique-index backstop",
            "S2",
            ("S2 allowed two active Runs for one tenant/thread",),
            (
                Replacement(
                    POSTGRES_SCHEMA,
                    '"CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_running_per_thread "',
                    '"CREATE INDEX IF NOT EXISTS idx_runs_one_running_per_thread "',
                ),
                Replacement(
                    POSTGRES_STORE,
                    """AND NOT EXISTS (
                                    SELECT 1 FROM runs AS thread_run
                                    WHERE thread_run.tenant_id = candidate.tenant_id
                                        AND thread_run.thread_id = candidate.thread_id
                                        AND thread_run.status = %s
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM runs AS earlier""",
                    """AND %s::text IS NOT NULL
                                AND NOT EXISTS (
                                    SELECT 1 FROM runs AS earlier""",
                ),
                Replacement(
                    POSTGRES_STORE,
                    """AND NOT EXISTS (
                                        SELECT 1 FROM runs AS thread_run
                                        WHERE thread_run.tenant_id = runs.tenant_id
                                            AND thread_run.thread_id = runs.thread_id
                                            AND thread_run.run_id != runs.run_id
                                            AND thread_run.status = %s
                                    )
                                )
                                OR (""",
                    """AND %s::text IS NOT NULL
                                )
                                OR (""",
                ),
            ),
        ),
        Mutant(
            4,
            "turn the Thread exclusion into an accidental global running-Run lock",
            "S3",
            ("p5-worker-b did not reach run.claimed",),
            (
                Replacement(
                    POSTGRES_STORE,
                    """WHERE thread_run.tenant_id = candidate.tenant_id
                                        AND thread_run.thread_id = candidate.thread_id
                                        AND thread_run.status = %s""",
                    """WHERE thread_run.status = %s""",
                ),
                Replacement(
                    POSTGRES_STORE,
                    """WHERE thread_run.tenant_id = runs.tenant_id
                                            AND thread_run.thread_id = runs.thread_id
                                            AND thread_run.run_id != runs.run_id
                                            AND thread_run.status = %s""",
                    """WHERE thread_run.status = %s""",
                ),
            ),
        ),
        Mutant(
            5,
            "permit takeover before the old owner's lease expiry",
            "S4",
            ("Competing worker acquired the same live Run",),
            live_takeover,
        ),
        Mutant(
            6,
            "drop the selected governed-Memory lease-token equality predicate",
            "S5",
            ("S5 stale writer crossed a lease-fenced mutation boundary",),
            (
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    """WHERE run_id = %s AND status = 'running' AND lease_token = %s
                AND lease_expires_at > %s
            FOR UPDATE""",
                    """WHERE run_id = %s AND status = 'running' AND lease_token = lease_token
                AND %s::text IS NOT NULL AND lease_expires_at > %s
            FOR UPDATE""",
                ),
            ),
        ),
        Mutant(
            7,
            "misdeclare the unsafe provider as idempotent and blindly replay it",
            "S6",
            ("p5-unsafe recovery semantics are wrong",),
            (
                Replacement(
                    P5_WORKER,
                    '("p5-unsafe", "unsafe", False, ())',
                    '("p5-unsafe", "unsafe", True, ())',
                ),
            ),
        ),
        Mutant(
            8,
            "write the seeded PostgreSQL DSN into the machine report",
            "S1",
            ("P5 report contains configured secret material",),
            (
                Replacement(
                    P5_CONTROLLER,
                    'report = {\n            "proof": REPORT_VERSION,\n'
                    '            "generated_at": datetime.now(UTC).isoformat(),',
                    'report = {\n            "proof": REPORT_VERSION,\n'
                    '            "debug_dsn": proof_dsn,\n'
                    '            "generated_at": datetime.now(UTC).isoformat(),',
                ),
            ),
        ),
    )


def main() -> int:
    if not os.environ.get("P5_POSTGRES_DSN"):
        print(
            "P5 MUTATION PROOF: FAILED | P5_POSTGRES_DSN is required; never skipped",
            file=sys.stderr,
        )
        return 2

    mutants = p5_mutants()

    failures: list[str] = []
    for mutant in mutants:
        try:
            _run_mutant(mutant)
        except Exception as exc:
            failure = f"P5M{mutant.number:02d} SURVIVED/INVALID | {exc}"
            failures.append(failure)
            print(failure)
    if failures:
        print("P5 MUTATION PROOF: FAILED")
        return 1
    print("P5 MUTATION PROOF: 8/8 KILLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
