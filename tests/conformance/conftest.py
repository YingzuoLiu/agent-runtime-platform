from __future__ import annotations

from pathlib import Path

import pytest

from .backends import SQLiteConformanceBackend


@pytest.fixture(
    params=[pytest.param("sqlite", id="sqlite")],
)
def store_backend(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    manual_store_clock,
):
    """Select the current reference adapter for each contract scenario."""

    if request.param == "sqlite":
        return SQLiteConformanceBackend(
            database_path=tmp_path / "store-conformance.db",
            clock=manual_store_clock,
        )
    raise AssertionError(f"Unknown conformance backend: {request.param}")
