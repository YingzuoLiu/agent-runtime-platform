from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POSTGRES_MEMORY_STORE = ROOT / "runtime_service" / "postgres_memory_store.py"
POSTGRES_SCHEMA = ROOT / "runtime_service" / "postgres_schema.py"

MEMORY_CONTRACT = "tests/conformance/test_memory_store_contract.py"
MEMORY_MECHANICS = "tests/conformance/test_postgres_memory_backend.py"


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

    if result.returncode != 1:
        print(result.stdout)
        raise RuntimeError(
            f"mutant {mutant.number} did not fail its target test with pytest exit 1; "
            f"observed exit {result.returncode}"
        )
    print(
        f"MM{mutant.number:02d} KILLED | {mutant.description} | "
        f"{mutant.test_target}"
    )


def main() -> int:
    if not os.environ.get("TEST_POSTGRES_DSN"):
        raise RuntimeError("TEST_POSTGRES_DSN is required for PostgreSQL mutation proof")

    mutants = (
        Mutant(
            1,
            "remove the governed Memory lease-token predicate",
            f"{MEMORY_CONTRACT}::test_stale_and_exactly_expired_leases_cannot_snapshot_or_mutate",
            (
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    "WHERE run_id = %s AND status = 'running' AND lease_token = %s\n"
                    "                AND lease_expires_at > %s\n"
                    "            FOR UPDATE",
                    "WHERE run_id = %s AND status = 'running' AND lease_token = lease_token\n"
                    "                AND %s::text IS NOT NULL AND lease_expires_at > %s\n"
                    "            FOR UPDATE",
                ),
            ),
        ),
        Mutant(
            2,
            "ignore the persisted Run subject identity",
            f"{MEMORY_CONTRACT}::test_run_identity_mismatch_cannot_snapshot_or_mutate",
            (
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    'if authority.get("subject_id") != subject_id:',
                    'if False and authority.get("subject_id") != subject_id:',
                ),
            ),
        ),
        Mutant(
            3,
            "remove one-active-Memory-key database uniqueness",
            f"{MEMORY_MECHANICS}::test_postgres_active_key_unique_index_is_an_independent_backstop",
            (
                Replacement(
                    POSTGRES_SCHEMA,
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_records_active_key",
                    "CREATE INDEX IF NOT EXISTS idx_memory_records_active_key",
                ),
            ),
        ),
        Mutant(
            4,
            "ignore an existing sealed Run Memory snapshot",
            f"{MEMORY_CONTRACT}::test_nonempty_and_empty_run_snapshots_remain_sealed",
            (
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    "if existing is not None:\n"
                    "                    return self._row_to_snapshot(",
                    "if False and existing is not None:\n"
                    "                    return self._row_to_snapshot(",
                ),
            ),
        ),
        Mutant(
            5,
            "remove the Memory mutation transaction boundary",
            f"{MEMORY_CONTRACT}::test_record_supersession_and_events_roll_back_as_one_transaction",
            (
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    "import json\n",
                    "import contextlib\nimport json\n",
                ),
                Replacement(
                    POSTGRES_MEMORY_STORE,
                    "        connection = self._connect()\n"
                    "        try:\n"
                    "            with connection.transaction():\n"
                    "                if lease_token is not None:",
                    "        connection = self._connect()\n"
                    "        try:\n"
                    "            with contextlib.nullcontext():\n"
                    "                if lease_token is not None:",
                ),
            ),
        ),
    )

    failures: list[str] = []
    for mutant in mutants:
        try:
            _run_mutant(mutant)
        except Exception as exc:
            message = f"MM{mutant.number:02d} SURVIVED/INVALID | {exc}"
            failures.append(message)
            print(message)

    if failures:
        print("POSTGRES MEMORY MUTATION PROOF: FAILED")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("POSTGRES MEMORY MUTATION PROOF: 5/5 KILLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
