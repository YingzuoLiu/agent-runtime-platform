from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

import psycopg  # noqa: E402
from psycopg import sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from runtime_service.postgres_schema import validate_postgres_schema_name  # noqa: E402


P5_APPLICATION_NAME = "p5_multi_worker_proof"


class P5PostgresProbeFailure(RuntimeError):
    """Secret-safe failure from an allowlisted P5 database operation."""


@dataclass(frozen=True, slots=True)
class P5RunSnapshot:
    run_id: str
    status: str
    attempt: int
    lease_owner_id: str | None
    lease_present: bool
    lease_live: bool
    run_started_count: int
    run_recovered_count: int
    recovery_reasons: tuple[str, ...]
    checkpoint_saved_count: int
    run_completed_count: int
    run_failed_count: int

    def public_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["recovery_reasons"] = list(self.recovery_reasons)
        return payload


def _connect(dsn: str, *, schema: str | None = None):
    try:
        connection = psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
            application_name=P5_APPLICATION_NAME,
        )
        if schema is not None:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
        connection.execute("SET statement_timeout = '5s'")
        connection.execute("SET lock_timeout = '2s'")
        connection.execute("SET idle_in_transaction_session_timeout = '5s'")
        return connection
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("PostgreSQL probe connection failed") from exc


def snapshot_run(dsn: str, schema: str, run_id: str) -> P5RunSnapshot:
    validate_postgres_schema_name(schema)
    connection = _connect(dsn, schema=schema)
    try:
        with connection.transaction():
            row = connection.execute(
                """
                SELECT
                    run_id,
                    status,
                    attempt,
                    lease_owner_id,
                    lease_token IS NOT NULL AS lease_present,
                    COALESCE(
                        lease_token IS NOT NULL
                        AND lease_expires_at IS NOT NULL
                        AND lease_expires_at > floor(
                            extract(epoch FROM transaction_timestamp()) * 1000
                        )::bigint,
                        FALSE
                    ) AS lease_live
                FROM runs
                WHERE run_id = %s
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise P5PostgresProbeFailure("P5 probe Run was not found")
            event_rows = connection.execute(
                """
                SELECT event_type, payload_json
                FROM run_events
                WHERE run_id = %s
                ORDER BY sequence
                """,
                (run_id,),
            ).fetchall()
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("PostgreSQL snapshot failed") from exc
    finally:
        connection.close()

    event_types = [str(event["event_type"]) for event in event_rows]
    reasons: list[str] = []
    for event in event_rows:
        if event["event_type"] != "run.recovered":
            continue
        try:
            payload = json.loads(str(event["payload_json"]))
        except json.JSONDecodeError as exc:
            raise P5PostgresProbeFailure("Recovery event payload is invalid") from exc
        reason = payload.get("reason") if isinstance(payload, dict) else None
        if not isinstance(reason, str):
            raise P5PostgresProbeFailure("Recovery event reason is invalid")
        reasons.append(reason)
    return P5RunSnapshot(
        run_id=str(row["run_id"]),
        status=str(row["status"]),
        attempt=int(row["attempt"]),
        lease_owner_id=(
            str(row["lease_owner_id"])
            if row["lease_owner_id"] is not None
            else None
        ),
        lease_present=bool(row["lease_present"]),
        lease_live=bool(row["lease_live"]),
        run_started_count=event_types.count("run.started"),
        run_recovered_count=event_types.count("run.recovered"),
        recovery_reasons=tuple(reasons),
        checkpoint_saved_count=event_types.count("checkpoint.saved"),
        run_completed_count=event_types.count("run.completed"),
        run_failed_count=event_types.count("run.failed"),
    )


def expire_live_lease(
    dsn: str,
    schema: str,
    run_id: str,
    *,
    expected_attempt: int,
) -> P5RunSnapshot:
    """Expire exactly one expected live attempt at PostgreSQL server time."""

    validate_postgres_schema_name(schema)
    connection = _connect(dsn, schema=schema)
    try:
        with connection.transaction():
            row = connection.execute(
                """
                UPDATE runs
                SET lease_expires_at = floor(
                    extract(epoch FROM transaction_timestamp()) * 1000
                )::bigint
                WHERE run_id = %s
                    AND status = 'running'
                    AND attempt = %s
                    AND lease_token IS NOT NULL
                    AND lease_expires_at > floor(
                        extract(epoch FROM transaction_timestamp()) * 1000
                    )::bigint
                RETURNING run_id
                """,
                (run_id, expected_attempt),
            ).fetchone()
            if row is None:
                raise P5PostgresProbeFailure(
                    "Expected live Run attempt could not be expired"
                )
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("Exact PostgreSQL lease expiry failed") from exc
    finally:
        connection.close()
    return snapshot_run(dsn, schema, run_id)


def terminate_backend(dsn: str, backend_pid: int) -> bool:
    if backend_pid <= 0:
        raise P5PostgresProbeFailure("Backend PID must be positive")
    connection = _connect(dsn)
    try:
        row = connection.execute(
            "SELECT pg_terminate_backend(%s) AS terminated",
            (backend_pid,),
        ).fetchone()
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("PostgreSQL backend termination failed") from exc
    finally:
        connection.close()
    return bool(row and row["terminated"])


def idle_in_transaction_count(dsn: str) -> int:
    connection = _connect(dsn)
    try:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM pg_stat_activity
            WHERE datname = current_database()
                AND application_name = %s
                AND state = 'idle in transaction'
                AND pid != pg_backend_pid()
            """,
            (P5_APPLICATION_NAME,),
        ).fetchone()
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("PostgreSQL session inspection failed") from exc
    finally:
        connection.close()
    if row is None:  # pragma: no cover - aggregate SELECT always returns a row
        raise P5PostgresProbeFailure("PostgreSQL session count is missing")
    return int(row["count"])


def postgres_version(dsn: str) -> str:
    connection = _connect(dsn)
    try:
        row = connection.execute("SHOW server_version").fetchone()
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("PostgreSQL version inspection failed") from exc
    finally:
        connection.close()
    if row is None or not isinstance(row.get("server_version"), str):
        raise P5PostgresProbeFailure("PostgreSQL version is missing")
    return str(row["server_version"])


def drop_schema(dsn: str, schema: str) -> None:
    validate_postgres_schema_name(schema)
    connection = _connect(dsn)
    try:
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
    except psycopg.Error as exc:
        raise P5PostgresProbeFailure("P5 PostgreSQL schema cleanup failed") from exc
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one allowlisted P5 PostgreSQL probe.")
    parser.add_argument(
        "operation",
        choices=(
            "snapshot",
            "expire",
            "terminate",
            "idle-count",
            "version",
            "drop-schema",
        ),
    )
    parser.add_argument("--schema")
    parser.add_argument("--run-id")
    parser.add_argument("--expected-attempt", type=int)
    parser.add_argument("--backend-pid", type=int)
    args = parser.parse_args()
    dsn = os.getenv("P5_POSTGRES_DSN")
    if not dsn:
        parser.error("P5_POSTGRES_DSN is required")

    try:
        if args.operation == "snapshot":
            if not args.schema or not args.run_id:
                parser.error("snapshot requires --schema and --run-id")
            payload: Any = snapshot_run(dsn, args.schema, args.run_id).public_dict()
        elif args.operation == "expire":
            if not args.schema or not args.run_id or args.expected_attempt is None:
                parser.error("expire requires --schema, --run-id, and --expected-attempt")
            payload = expire_live_lease(
                dsn,
                args.schema,
                args.run_id,
                expected_attempt=args.expected_attempt,
            ).public_dict()
        elif args.operation == "terminate":
            if args.backend_pid is None:
                parser.error("terminate requires --backend-pid")
            payload = {"terminated": terminate_backend(dsn, args.backend_pid)}
        elif args.operation == "idle-count":
            payload = {"idle_in_transaction": idle_in_transaction_count(dsn)}
        elif args.operation == "version":
            payload = {"postgres_version": postgres_version(dsn)}
        else:
            if not args.schema:
                parser.error("drop-schema requires --schema")
            drop_schema(dsn, args.schema)
            payload = {"schema_dropped": True}
    except P5PostgresProbeFailure as exc:
        print(f"P5 POSTGRES PROBE FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
