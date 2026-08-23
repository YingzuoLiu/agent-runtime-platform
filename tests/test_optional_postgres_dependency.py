from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_default_runtime_import_does_not_require_psycopg() -> None:
    script = r'''
import importlib.abc
import sys

class BlockPsycopg(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "psycopg" or fullname.startswith("psycopg."):
            raise ModuleNotFoundError("psycopg intentionally blocked")
        return None

sys.meta_path.insert(0, BlockPsycopg())
import runtime_service
assert runtime_service.SQLiteRunStore.__name__ == "SQLiteRunStore"
assert "psycopg" not in sys.modules
'''
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    assert completed.returncode == 0, completed.stdout
