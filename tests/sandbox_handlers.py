from __future__ import annotations

import os
import time
from typing import Any


def sleep_test(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(float(payload["seconds"]))
    return {"slept": float(payload["seconds"])}


def environment_probe(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [str(key) for key in payload["keys"]]
    return {"present": {key: key in os.environ for key in keys}}
