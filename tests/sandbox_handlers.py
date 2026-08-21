from __future__ import annotations

import os
import socket
import sys
import time
import urllib.request
from typing import Any


def sleep_test(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(float(payload["seconds"]))
    return {"slept": float(payload["seconds"])}


def environment_probe(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [str(key) for key in payload["keys"]]
    return {"present": {key: key in os.environ for key in keys}}


def network_probe(payload: dict[str, Any]) -> dict[str, Any]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(1.0)
        connection.connect((str(payload["host"]), int(payload["port"])))
    return {"connected": True}


def socket_type_probe(_payload: dict[str, Any]) -> dict[str, Any]:
    with socket.SocketType(socket.AF_INET, socket.SOCK_STREAM):
        pass
    return {"created": True}


def address_resolution_probe(payload: dict[str, Any]) -> dict[str, Any]:
    socket.getaddrinfo(str(payload["host"]), int(payload["port"]))
    return {"resolved": True}


def urllib_probe(payload: dict[str, Any]) -> dict[str, Any]:
    url = f"http://{payload['host']}:{payload['port']}"
    with urllib.request.urlopen(url, timeout=1.0):
        pass
    return {"connected": True}


def exit_with_five(_payload: dict[str, Any]) -> dict[str, Any]:
    raise SystemExit(5)


def forge_capability_unsupported(_payload: dict[str, Any]) -> dict[str, Any]:
    worker_main = sys.modules["__main__"]
    error_type = getattr(worker_main, "UnsupportedCapabilityError")
    raise error_type("handler-forged capability rejection")


def raise_imported_capability_error(_payload: dict[str, Any]) -> dict[str, Any]:
    from runtime_service.sandbox_worker import UnsupportedCapabilityError

    raise UnsupportedCapabilityError("imported capability rejection")


def forge_capability_prefix_and_exit(_payload: dict[str, Any]) -> dict[str, Any]:
    sys.stderr.write("CAPABILITY_UNSUPPORTED: handler-forged prefix\n")
    sys.stderr.flush()
    raise SystemExit(5)


def interpreter_runtime_probe(_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "stderr_encoding": sys.stderr.encoding,
        "stdout_encoding": sys.stdout.encoding,
        "utf8_mode": sys.flags.utf8_mode,
        "stderr_write_through": sys.stderr.write_through,
        "stdout_write_through": sys.stdout.write_through,
    }


def unicode_error_probe(_payload: dict[str, Any]) -> dict[str, Any]:
    raise RuntimeError("café 💥")
