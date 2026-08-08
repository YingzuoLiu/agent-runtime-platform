"""Opt-in live model-driven Phase 5A demo.

Usage:
    pip install -r requirements-model-demo.txt
    RUNTIME_PLANNER_PROVIDER=openai OPENAI_API_KEY=... OPENAI_MODEL=... \
        python examples/model_driven_travel_demo.py

The Travel tools remain deterministic synthetic reference tools. Only the
Planner decision is model-driven; this script does not search live inventory or
book anything.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from api.main import create_app  # noqa: E402
from runtime_service import (  # noqa: E402
    ApiKeyCredential,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)


DEMO_API_KEY = "local-phase5a-model-demo"


def main() -> int:
    if os.getenv("RUNTIME_PLANNER_PROVIDER", "").strip().lower() != "openai":
        print("Set RUNTIME_PLANNER_PROVIDER=openai to opt into the live model demo.")
        return 2
    if not os.getenv("OPENAI_API_KEY") or not os.getenv("OPENAI_MODEL"):
        print("OPENAI_API_KEY and OPENAI_MODEL are required.")
        return 2

    message = (
        " ".join(sys.argv[1:]).strip()
        or "I want a 5-day Tokyo trip under 9000 SGD and avoid red-eye flights."
    )
    authenticator = StaticApiKeyAuthenticator(
        [
            ApiKeyCredential(
                credential_id="local-model-demo",
                api_key=DEMO_API_KEY,
                tenant_id="local-demo",
                subject_id="local-demo-user",
                role=RuntimeRole.OPERATOR,
            )
        ]
    )

    with tempfile.TemporaryDirectory(prefix="phase5a-model-demo-") as directory:
        app = create_app(
            database_path=Path(directory) / "runtime.db",
            authenticator=authenticator,
        )
        with TestClient(
            app,
            headers={"Authorization": f"Bearer {DEMO_API_KEY}"},
        ) as client:
            submitted = client.post(
                "/runs",
                json={
                    "thread_id": "live-model-demo",
                    "agent_id": "travel-agent",
                    "agent_version": "1.0.0",
                    "user_message": message,
                },
            )
            submitted.raise_for_status()
            run_id = submitted.json()["run_id"]
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                run = client.get(f"/runs/{run_id}").json()
                if run["status"] in {"completed", "failed", "cancelled"}:
                    break
                time.sleep(0.1)
            else:
                print("The model-driven run did not finish within 90 seconds.")
                return 1

            events = client.get(f"/runs/{run_id}/events").json()
            evidence = [
                event
                for event in events
                if event["event_type"]
                in {
                    "planner.decision",
                    "policy.decision",
                    "tool.result",
                    "loop.outcome",
                }
            ]
            print(json.dumps({"run": run, "evidence": evidence}, indent=2))
            return 0 if run["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
