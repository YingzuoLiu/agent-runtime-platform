from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any


def apply_resource_limits() -> None:
    if os.name != "posix":
        return

    import resource

    cpu_seconds = int(os.environ["SANDBOX_MAX_CPU_SECONDS"])
    memory_bytes = int(os.environ["SANDBOX_MAX_MEMORY_MB"]) * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
    resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def load_handler(entrypoint: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    module_name, separator, function_name = entrypoint.partition(":")
    if not separator or not module_name or not function_name or ":" in function_name:
        raise ValueError("handler entrypoint must use 'module:function' format")

    # The worker runs from a fresh temporary directory. Add only this checked-out
    # repository root so server-registered domain handlers remain importable;
    # caller-controlled PYTHONPATH is intentionally absent from the environment.
    repository_root = str(Path(__file__).resolve().parents[1])
    if repository_root not in sys.path:
        sys.path.insert(0, repository_root)
    module = importlib.import_module(module_name)
    handler = getattr(module, function_name)
    if not callable(handler):
        raise TypeError("registered handler is not callable")
    return handler


def main() -> int:
    apply_resource_limits()

    if len(sys.argv) != 2:
        print("exactly one registered handler entrypoint is required", file=sys.stderr)
        return 2

    try:
        handler = load_handler(sys.argv[1])
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("tool input must be a JSON object")
        result = handler(payload)
        if not isinstance(result, dict):
            raise TypeError("tool result must be a JSON object")
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 4

    sys.stdout.write(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
