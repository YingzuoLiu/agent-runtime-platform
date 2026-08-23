from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from .backends import (
    PostgresConformanceBackend,
    SQLiteConformanceBackend,
    StoreConformanceBackend,
)


def _selected_backends() -> list[str]:
    raw = os.environ.get("STORE_CONFORMANCE_BACKENDS", "sqlite")
    selected = [value.strip().lower() for value in raw.split(",") if value.strip()]
    if not selected:
        raise pytest.UsageError("STORE_CONFORMANCE_BACKENDS must select at least one backend")
    unknown = sorted(set(selected) - {"sqlite", "postgres"})
    if unknown:
        raise pytest.UsageError(
            "Unknown STORE_CONFORMANCE_BACKENDS value(s): " + ", ".join(unknown)
        )
    return list(dict.fromkeys(selected))


@pytest.fixture(
    params=[pytest.param(value, id=value) for value in _selected_backends()],
)
def store_backend(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    manual_store_clock,
) -> StoreConformanceBackend:
    """Create one isolated backend adapter per semantic-contract scenario.

    SQLite remains the local/default contract run. PostgreSQL is opt-in by an
    explicit selector, and selecting it without a DSN is a collection error,
    never a silent skip. Every PostgreSQL test receives a fresh validated
    schema and drops it after the scenario to prove isolation between tests.
    """

    backend: StoreConformanceBackend
    if request.param == "sqlite":
        backend = SQLiteConformanceBackend(
            database_path=tmp_path / "store-conformance.db",
            clock=manual_store_clock,
        )
    elif request.param == "postgres":
        dsn = os.environ.get("TEST_POSTGRES_DSN")
        if not dsn:
            raise pytest.UsageError(
                "PostgreSQL conformance was selected but TEST_POSTGRES_DSN is not set"
            )
        backend = PostgresConformanceBackend(
            dsn=dsn,
            schema=f"arp_test_{uuid4().hex}",
            clock=manual_store_clock,
        )
    else:  # pragma: no cover - guarded by collection validation
        raise AssertionError(f"Unknown conformance backend: {request.param}")

    try:
        yield backend
    finally:
        backend.close()
