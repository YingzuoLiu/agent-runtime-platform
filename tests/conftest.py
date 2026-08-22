from __future__ import annotations

import threading
import time

import pytest


class ManualStoreClock:
    """Thread-safe store clock advanced explicitly by tests."""

    def __init__(self, now_ms: int | None = None) -> None:
        self._now_ms = int(time.time() * 1_000) if now_ms is None else now_ms
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            return self._now_ms

    def advance(self, delta_ms: int) -> int:
        if delta_ms < 0:
            raise ValueError("Manual store clock cannot move backwards")
        with self._lock:
            self._now_ms += delta_ms
            return self._now_ms


@pytest.fixture
def manual_store_clock() -> ManualStoreClock:
    return ManualStoreClock()
