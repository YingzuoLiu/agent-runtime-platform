from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


STORE_NOW_SQL = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
DEFAULT_ARM_DURATION_MS = 180_000
SNAPSHOT_FIELDS = frozenset(
    {
        "status",
        "attempt",
        "lease_present",
        "lease_live",
        "run_started_count",
        "run_recovered_count",
        "recovery_reasons",
    }
)


class LeaseProbeFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class LeaseProbeSnapshot:
    """Allowlisted proof evidence; lease authority is never selected from storage."""

    status: str
    attempt: int
    lease_present: bool
    lease_live: bool
    run_started_count: int
    run_recovered_count: int
    recovery_reasons: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["recovery_reasons"] = list(self.recovery_reasons)
        return payload

    @classmethod
    def from_payload(cls, payload: object) -> LeaseProbeSnapshot:
        if not isinstance(payload, dict) or set(payload) != SNAPSHOT_FIELDS:
            raise LeaseProbeFailure("Lease probe output is not an allowlisted object")
        try:
            recovery_reasons = payload["recovery_reasons"]
            if not isinstance(recovery_reasons, list) or not all(
                isinstance(reason, str) for reason in recovery_reasons
            ):
                raise TypeError
            status = payload["status"]
            attempt = payload["attempt"]
            lease_present = payload["lease_present"]
            lease_live = payload["lease_live"]
            run_started_count = payload["run_started_count"]
            run_recovered_count = payload["run_recovered_count"]
            if (
                not isinstance(status, str)
                or not isinstance(attempt, int)
                or isinstance(attempt, bool)
                or not isinstance(lease_present, bool)
                or not isinstance(lease_live, bool)
                or not isinstance(run_started_count, int)
                or isinstance(run_started_count, bool)
                or not isinstance(run_recovered_count, int)
                or isinstance(run_recovered_count, bool)
            ):
                raise TypeError
            return cls(
                status=status,
                attempt=attempt,
                lease_present=lease_present,
                lease_live=lease_live,
                run_started_count=run_started_count,
                run_recovered_count=run_recovered_count,
                recovery_reasons=tuple(recovery_reasons),
            )
        except (KeyError, TypeError, ValueError):
            raise LeaseProbeFailure("Lease probe output has an invalid shape") from None


@contextmanager
def _connect(database_path: str | Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(database_path, timeout=1)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    try:
        yield connection
    finally:
        connection.close()


def _read_snapshot_with_connection(
    connection: sqlite3.Connection,
    run_id: str,
) -> LeaseProbeSnapshot:
    row = connection.execute(
        f"""
        SELECT
            status,
            attempt,
            lease_token IS NOT NULL AS lease_present,
            COALESCE(lease_expires_at > {STORE_NOW_SQL}, 0) AS lease_live
        FROM runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise LeaseProbeFailure(f"Run not found: {run_id}")

    event_rows = connection.execute(
        """
        SELECT event_type, payload_json
        FROM run_events
        WHERE run_id = ? AND event_type IN ('run.started', 'run.recovered')
        ORDER BY sequence
        """,
        (run_id,),
    ).fetchall()
    recovery_reasons: list[str] = []
    for event in event_rows:
        if event["event_type"] != "run.recovered":
            continue
        try:
            payload = json.loads(event["payload_json"])
        except json.JSONDecodeError:
            raise LeaseProbeFailure("Run recovery evidence is not valid JSON") from None
        reason = payload.get("reason") if isinstance(payload, dict) else None
        if not isinstance(reason, str):
            raise LeaseProbeFailure("Run recovery evidence has no reason")
        recovery_reasons.append(reason)

    return LeaseProbeSnapshot(
        status=str(row["status"]),
        attempt=int(row["attempt"]),
        lease_present=bool(row["lease_present"]),
        lease_live=bool(row["lease_live"]),
        run_started_count=sum(
            event["event_type"] == "run.started" for event in event_rows
        ),
        run_recovered_count=len(recovery_reasons),
        recovery_reasons=tuple(recovery_reasons),
    )


def read_snapshot(database_path: str | Path, run_id: str) -> LeaseProbeSnapshot:
    with _connect(database_path) as connection:
        connection.execute("BEGIN")
        snapshot = _read_snapshot_with_connection(connection, run_id)
        connection.commit()
        return snapshot


def arm_live_lease(
    database_path: str | Path,
    run_id: str,
    *,
    expected_attempt: int,
    duration_ms: int = DEFAULT_ARM_DURATION_MS,
) -> LeaseProbeSnapshot:
    """Extend a live, stopped attempt to create a deterministic observation window."""

    if expected_attempt < 1:
        raise ValueError("expected_attempt must be positive")
    if duration_ms <= 0:
        raise ValueError("duration_ms must be positive")
    with _connect(database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        now_ms = int(connection.execute(f"SELECT {STORE_NOW_SQL}").fetchone()[0])
        cursor = connection.execute(
            """
            UPDATE runs
            SET lease_expires_at = ?
            WHERE run_id = ?
                AND status = 'running'
                AND attempt = ?
                AND lease_token IS NOT NULL
                AND lease_expires_at > ?
            """,
            (now_ms + duration_ms, run_id, expected_attempt, now_ms),
        )
        if cursor.rowcount != 1:
            raise LeaseProbeFailure(
                "Could not arm the expected live Run attempt for restart observation"
            )
        snapshot = _read_snapshot_with_connection(connection, run_id)
        connection.commit()
        return snapshot


def expire_live_lease(
    database_path: str | Path,
    run_id: str,
    *,
    expected_attempt: int,
) -> LeaseProbeSnapshot:
    """Move one known-live attempt to the executor's exact store-time expiry boundary."""

    if expected_attempt < 1:
        raise ValueError("expected_attempt must be positive")
    with _connect(database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        now_ms = int(connection.execute(f"SELECT {STORE_NOW_SQL}").fetchone()[0])
        cursor = connection.execute(
            """
            UPDATE runs
            SET lease_expires_at = ?
            WHERE run_id = ?
                AND status = 'running'
                AND attempt = ?
                AND lease_token IS NOT NULL
                AND lease_expires_at > ?
            """,
            (now_ms, run_id, expected_attempt, now_ms),
        )
        if cursor.rowcount != 1:
            raise LeaseProbeFailure(
                "Could not expire the expected live Run attempt at the store-time boundary"
            )
        snapshot = _read_snapshot_with_connection(connection, run_id)
        connection.commit()
        return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read or fault-inject sanitized Run-lease evidence for the local proof."
    )
    parser.add_argument("operation", choices=("snapshot", "arm", "expire"))
    parser.add_argument("database_path")
    parser.add_argument("run_id")
    parser.add_argument("--expected-attempt", type=int)
    parser.add_argument("--duration-ms", type=int, default=DEFAULT_ARM_DURATION_MS)
    args = parser.parse_args()

    try:
        if args.operation == "snapshot":
            snapshot = read_snapshot(args.database_path, args.run_id)
        elif args.operation == "arm":
            if args.expected_attempt is None:
                parser.error("arm requires --expected-attempt")
            snapshot = arm_live_lease(
                args.database_path,
                args.run_id,
                expected_attempt=args.expected_attempt,
                duration_ms=args.duration_ms,
            )
        else:
            if args.expected_attempt is None:
                parser.error("expire requires --expected-attempt")
            snapshot = expire_live_lease(
                args.database_path,
                args.run_id,
                expected_attempt=args.expected_attempt,
            )
    except (LeaseProbeFailure, ValueError, sqlite3.Error) as exc:
        print(f"LEASE PROBE FAILED: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(snapshot.to_payload(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
