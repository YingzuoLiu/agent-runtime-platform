from __future__ import annotations

from typing import Any


def load_release_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    return {"release_id": str(payload["release_id"]), "manifest_valid": True}


def _looks_like_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value.lower())


def inspect_build_artifacts(payload: dict[str, Any]) -> dict[str, Any]:
    required = [str(name) for name in payload["required_artifacts"]]
    available = {
        str(item["name"]): str(item["checksum"])
        for item in payload["available_artifacts"]
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
