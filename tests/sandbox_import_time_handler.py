from __future__ import annotations

import socket
from typing import Any


with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
    pass


def import_time_socket_probe(_payload: dict[str, Any]) -> dict[str, Any]:
    return {"imported": True}
