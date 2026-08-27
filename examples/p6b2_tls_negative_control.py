"""Classify the P6B.2 TLS hostname negative control without leaking libpq errors."""

from __future__ import annotations

import json
import os
import sys

import psycopg
from psycopg.conninfo import conninfo_to_dict


EXPECTED_HOST = "postgres-wrong-host"
EXPECTED_SSLMODE = "verify-full"


def classify_tls_failure(message: str) -> str:
    """Reduce a client error to one safe, deterministic proof classification."""

    normalized = " ".join(message.lower().split())
    hostname_mismatch = (
        "does not match host name" in normalized
        or "does not match hostname" in normalized
        or "hostname mismatch" in normalized
    )
    if "certificate" in normalized and hostname_mismatch:
        return "tls_hostname_rejected"
    return "unexpected_connection_failure"


def _emit(*, status: str, result: str) -> None:
    print(
        json.dumps(
            {
                "result": result,
                "sslmode": EXPECTED_SSLMODE,
                "status": status,
                "target": EXPECTED_HOST,
            },
            sort_keys=True,
        )
    )


def main() -> int:
    dsn = os.environ.get("RUNTIME_POSTGRES_DSN", "")
    if not dsn:
        _emit(status="failed", result="missing_dsn")
        return 2

    try:
        parameters = conninfo_to_dict(dsn)
    except psycopg.Error:
        _emit(status="failed", result="invalid_dsn")
        return 2
    if (
        parameters.get("host") != EXPECTED_HOST
        or parameters.get("sslmode") != EXPECTED_SSLMODE
        or not parameters.get("sslrootcert")
    ):
        _emit(status="failed", result="invalid_negative_control")
        return 2

    try:
        connection = psycopg.connect(dsn, connect_timeout=5)
    except psycopg.Error as exc:
        classification = classify_tls_failure(str(exc))
        expected = classification == "tls_hostname_rejected"
        _emit(status="ok" if expected else "failed", result=classification)
        return 0 if expected else 2

    connection.close()
    _emit(status="failed", result="wrong_hostname_accepted")
    return 3


if __name__ == "__main__":
    sys.exit(main())
