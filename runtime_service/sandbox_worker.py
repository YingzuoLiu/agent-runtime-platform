from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any


NETWORK_DENIED_MESSAGE = "Tool network access is denied by process policy."
CAPABILITY_UNSUPPORTED_EXIT_CODE = 5
CAPABILITY_UNSUPPORTED_PREFIX = "CAPABILITY_UNSUPPORTED: "
PROCESS_CAPABILITY_SUPPORT = {
    "filesystem": frozenset({("readwrite", "process")}),
    "network": frozenset({("none", "process"), ("host", "process")}),
    "environment": frozenset(
        {("restricted", "process"), ("inherited", "process")}
    ),
}


class UnsupportedCapabilityError(RuntimeError):
    pass


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


def apply_capability_policy() -> None:
    """Apply the subprocess executor's supported capability requirements.

    This validates the parent preflight again inside the worker. The network
    guard prevents accidental socket use by trusted Python handlers; it is not
    designed to contain hostile code using lower-level modules or child processes.
    """

    try:
        requirements = {
            "filesystem": (
                os.environ["SANDBOX_FILESYSTEM_MODE"],
                os.environ["SANDBOX_FILESYSTEM_ENFORCEMENT"],
            ),
            "network": (
                os.environ["SANDBOX_NETWORK_MODE"],
                os.environ["SANDBOX_NETWORK_ENFORCEMENT"],
            ),
            "environment": (
                os.environ["SANDBOX_ENVIRONMENT_MODE"],
                os.environ["SANDBOX_ENVIRONMENT_ENFORCEMENT"],
            ),
        }
    except KeyError as exc:
        raise UnsupportedCapabilityError(
            f"missing worker capability requirement: {exc.args[0]}"
        ) from exc

    for capability, requirement in requirements.items():
        if requirement not in PROCESS_CAPABILITY_SUPPORT[capability]:
            mode, enforcement = requirement
            raise UnsupportedCapabilityError(
                "unsupported worker capability requirement: "
                f"{capability}={mode}, enforcement={enforcement}"
            )

    if requirements["network"] == ("none", "process"):
        install_python_network_guard()


def install_python_network_guard() -> None:
    import socket

    class NetworkDeniedSocket(socket.socket):
        def __new__(cls, *_args: Any, **_kwargs: Any) -> NetworkDeniedSocket:
            raise PermissionError(NETWORK_DENIED_MESSAGE)

    def deny_network(*_args: Any, **_kwargs: Any) -> None:
        raise PermissionError(NETWORK_DENIED_MESSAGE)

    socket.socket = NetworkDeniedSocket
    socket.SocketType = NetworkDeniedSocket
    for function_name in (
        "create_connection",
        "create_server",
        "dup",
        "fromfd",
        "fromshare",
        "getaddrinfo",
        "gethostbyaddr",
        "gethostbyname",
        "gethostbyname_ex",
        "getnameinfo",
        "socketpair",
    ):
        if hasattr(socket, function_name):
            setattr(socket, function_name, deny_network)


def load_handler(entrypoint: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    module_name, separator, function_name = entrypoint.partition(":")
    if not separator or not module_name or not function_name or ":" in function_name:
        raise ValueError("handler entrypoint must use 'module:function' format")

    # The worker runs from a fresh temporary directory and Python isolated mode
    # ignores interpreter-control environment variables such as PYTHONPATH. Add
    # only this checked-out repository root so registered handlers are resolved
    # from server-owned application code.
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
        apply_capability_policy()
    except UnsupportedCapabilityError as exc:
        print(f"{CAPABILITY_UNSUPPORTED_PREFIX}{exc}", file=sys.stderr)
        return CAPABILITY_UNSUPPORTED_EXIT_CODE

    try:
        handler = load_handler(sys.argv[1])
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("tool input must be a JSON object")
        result = handler(payload)
        if not isinstance(result, dict):
            raise TypeError("tool result must be a JSON object")
    except BaseException as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 4

    sys.stdout.write(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
