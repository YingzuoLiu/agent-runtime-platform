from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict

from runtime_service.sandbox import ToolPolicy, ToolRegistry, ToolSpec


class LoadReleaseManifestInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    release_id: str
    application_name: str
    release_version: str
    required_artifacts: List[str]
    required_test_suite: str
    required_python_versions: List[str]
    deployment_environment: str
    configuration_requirements: List[str]


class BuildArtifactInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    checksum: str


class InspectBuildArtifactsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_artifacts: List[str]
    available_artifacts: List[BuildArtifactInput]


class RunUnitTestCheckInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_test_suite: str
    executed_suite: str
    tests_passed: bool


class RunCompatibilityCheckInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_python_versions: List[str]
    tested_python_versions: List[str]


class InspectDeploymentConfigurationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    configuration_requirements: List[str]
    actual_configuration_keys: List[str]


class GenerateReleaseEvidenceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    release_id: str
    include_checks: List[str]


def build_release_validation_tool_registry() -> ToolRegistry:
    """A second, independent `ToolRegistry` instance for this domain.

    `ToolRegistry`/`ToolPolicy`/`ToolSpec` are already domain-agnostic in
    `runtime_service/sandbox.py`; this only supplies release-validation
    shaped input schemas, unchanged from the Travel registry's classes.
    """
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="load_release_manifest",
            description="Validate and echo a synthetic release manifest.",
            input_model=LoadReleaseManifestInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:load_release_manifest"
            ),
        )
    )
    registry.register(
        ToolSpec(
            name="inspect_build_artifacts",
            description="Check required build artifacts are present with plausible checksums.",
            input_model=InspectBuildArtifactsInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:inspect_build_artifacts"
            ),
        )
    )
    registry.register(
        ToolSpec(
            name="run_unit_test_check",
            description="Confirm the required unit test suite executed and passed.",
            input_model=RunUnitTestCheckInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:run_unit_test_check"
            ),
        )
    )
    registry.register(
        ToolSpec(
            name="run_compatibility_check",
            description="Confirm all required Python versions have compatibility evidence.",
            input_model=RunCompatibilityCheckInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:run_compatibility_check"
            ),
        )
    )
    registry.register(
        ToolSpec(
            name="inspect_deployment_configuration",
            description="Confirm required deployment configuration keys are present.",
            input_model=InspectDeploymentConfigurationInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:inspect_deployment_configuration"
            ),
        )
    )
    registry.register(
        ToolSpec(
            name="generate_release_evidence",
            description="Generate a deterministic evidence record referencing prior checks.",
            input_model=GenerateReleaseEvidenceInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.release_validation.tool_handlers:generate_release_evidence"
            ),
        )
    )
    return registry
