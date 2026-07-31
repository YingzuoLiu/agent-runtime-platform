from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from domains.travel.state import AgentState  # noqa: E402
from domains.travel.runtime import TravelAgentRuntime  # noqa: E402


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    runtime = TravelAgentRuntime(retry_limit=2)
    state = AgentState(thread_id="tokyo_trip_001")

    messages = [
        "I want a 5-day Tokyo trip under 7000 SGD. Make it relaxed.",
        "Change the budget to 9000 and avoid red-eye flights. Also keep hotel near subway.",
    ]

    for idx, message in enumerate(messages, start=1):
        print_section(f"USER TURN {idx}")
        print("User:", message)

        result = runtime.handle_user_message(state, message)
        state = result.state

        print("\nAssistant:")
        print(result.message)

        print("\nCurrent state summary:")
        print(json.dumps(state.model_dump(), indent=2, ensure_ascii=False, default=str))

    trace_path = ROOT / "traces" / "sample_trace.json"
    trace_path.write_text(
        json.dumps(
            [event.model_dump() for event in state.execution_trace],
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    print_section("TRACE WRITTEN")
    print(f"Execution trace saved to: {trace_path}")


if __name__ == "__main__":
    main()
