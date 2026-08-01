from __future__ import annotations

import json
import os
import sys
import time
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


def route_cost_summary(payload: dict[str, Any]) -> dict[str, Any]:
    total = int(payload["transport_cost"]) + int(payload["hotel_cost"]) + int(
        payload["activity_cost"]
    )
    budget = int(payload["budget"])
    return {
        "total_cost": total,
        "budget": budget,
        "remaining_budget": budget - total,
        "within_budget": total <= budget,
    }


def rank_trip_options(payload: dict[str, Any]) -> dict[str, Any]:
    options = payload["options"]
    cost_weight = float(payload["cost_weight"])
    duration_weight = float(payload["duration_weight"])

    normalized: list[dict[str, Any]] = []
    max_cost = max(float(item.get("cost", 0)) for item in options) or 1.0
    max_duration = max(float(item.get("duration_hours", 0)) for item in options) or 1.0

    for index, item in enumerate(options):
        cost = float(item.get("cost", 0))
        duration = float(item.get("duration_hours", 0))
        score = cost_weight * (1 - cost / max_cost) + duration_weight * (
            1 - duration / max_duration
        )
        normalized.append(
            {
                "index": index,
                "name": str(item.get("name", f"option-{index}")),
                "score": round(score, 6),
            }
        )

    normalized.sort(key=lambda item: (-float(item["score"]), int(item["index"])))
    return {"ranking": normalized}


def load_release_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    return {"release_id": str(payload["release_id"]), "manifest_valid": True}


def _looks_like_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value.lower())


def inspect_build_artifacts(payload: dict[str, Any]) -> dict[str, Any]:
    required = [str(name) for name in payload["required_artifacts"]]
    available = {
        str(item["name"]): str(item["checksum"]) for item in payload["available_artifacts"]
    }
    missing = [name for name in required if name not in available]
    invalid_checksums = [
        name
        for name in required
        if name in available and not _looks_like_sha256(available[name])
    ]
    return {
        "missing_artifacts": missing,
        "invalid_checksums": invalid_checksums,
        "all_present": not missing and not invalid_checksums,
    }


def run_unit_test_check(payload: dict[str, Any]) -> dict[str, Any]:
    executed_suite = str(payload["executed_suite"])
    matches_required_suite = executed_suite == str(payload["required_test_suite"])
    return {
        "suite_name": executed_suite,
        "matches_required_suite": matches_required_suite,
        "passed": bool(payload["tests_passed"]),
    }


def run_compatibility_check(payload: dict[str, Any]) -> dict[str, Any]:
    required = [str(version) for version in payload["required_python_versions"]]
    tested = {str(version) for version in payload["tested_python_versions"]}
    missing = [version for version in required if version not in tested]
    return {"missing_versions": missing, "all_covered": not missing}


def inspect_deployment_configuration(payload: dict[str, Any]) -> dict[str, Any]:
    required = [str(key) for key in payload["configuration_requirements"]]
    actual = {str(key) for key in payload["actual_configuration_keys"]}
    missing = [key for key in required if key not in actual]
    return {"missing_keys": missing, "all_present": not missing}


def generate_release_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    required_checks = {"artifacts", "tests", "compatibility", "deployment"}
    referenced = [str(check) for check in payload["include_checks"]]
    return {
        "release_id": str(payload["release_id"]),
        "referenced_checks": referenced,
        "evidence_complete": required_checks.issubset(set(referenced)),
    }


def _sleep_test(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(float(payload["seconds"]))
    return {"slept": float(payload["seconds"])}


def _environment_probe(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [str(key) for key in payload["keys"]]
    return {"present": {key: key in os.environ for key in keys}}


TOOLS = {
    "route_cost_summary": route_cost_summary,
    "rank_trip_options": rank_trip_options,
    "load_release_manifest": load_release_manifest,
    "inspect_build_artifacts": inspect_build_artifacts,
    "run_unit_test_check": run_unit_test_check,
    "run_compatibility_check": run_compatibility_check,
    "inspect_deployment_configuration": inspect_deployment_configuration,
    "generate_release_evidence": generate_release_evidence,
    "_sleep_test": _sleep_test,
    "_environment_probe": _environment_probe,
}


def main() -> int:
    apply_resource_limits()

    if len(sys.argv) != 2:
        print("exactly one tool name is required", file=sys.stderr)
        return 2

    tool_name = sys.argv[1]
    tool = TOOLS.get(tool_name)
    if tool is None:
        print("tool is not available in the sandbox worker", file=sys.stderr)
        return 3

    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("tool input must be a JSON object")
        result = tool(payload)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 4

    sys.stdout.write(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
