from __future__ import annotations

from pathlib import Path

import pytest

from .backends import ManualStoreClock, SQLiteConformanceBackend


@pytest.fixture(
    params=[pytest.param("sqlite", id="sqlite")],
)
def store_backend(request: pytest.FixtureRequest, tmp_path: Path):
    """Select the backend adapter that executes every conformance scenario."""

    if request.param == "sqlite":
        return SQLiteConformanceBackend(
            database_path=tmp_path / "store-conformance.db",
            clock=ManualStoreClock(),
        )
    raise AssertionError(f"Unknown conformance backend: {request.param}")
