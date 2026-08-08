from __future__ import annotations

from enum import Enum
from typing import ClassVar, List

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agent.contracts import BaseRuntimeState


class BuildArtifact(BaseModel):
    """One build output the manifest expects to exist, with its checksum."""

    name: str
    checksum: str


class ReleaseManifest(BaseModel):
    """A synthetic release manifest.

    This offline demo has no real build system, test runner, or
    deployment target to inspect, so the manifest deliberately carries
    both what a release *requires* (`required_*`) and the *observed*
    facts a real pipeline would otherwise fetch separately
    (`available_artifacts`, `executed_test_suite`, `tested_python_versions`,
    `actual_configuration_keys`). A fixture author controls both sides,
    which is what makes it possible to construct the mismatch scenarios
    (unmet compatibility, missing evidence, invalid configuration) as
    plain data instead of a simulated external system.
    """

    release_id: str
    application_name: str
    release_version: str
    required_artifacts: List[str]
    available_artifacts: List[BuildArtifact]
    required_test_suite: str
    executed_test_suite: str
    tests_passed: bool
    required_python_versions: List[str]
    tested_python_versions: List[str]
    deployment_environment: str
    configuration_requirements: List[str]
    actual_configuration_keys: List[str]
    evidence_checks_included: List[str] = Field(default_factory=lambda: [
        "artifacts", "tests", "compatibility", "deployment"
    ])


class ReleaseEvidence(BaseModel):
    """The typed shape of `generate_release_evidence`'s tool result."""

    release_id: str
    referenced_checks: List[str]
    evidence_complete: bool


class ValidationFinding(BaseModel):
    """One independent readiness-checklist rule that did not pass."""

    rule_id: str
    message: str


class ReleaseValidationStatus(str, Enum):
    READY = "ready"
    BLOCKED = "blocked"
    FAILED = "failed"


class SelectiveReplayRequest(BaseModel):
    """Replay selected DAG nodes into a new run from terminal source evidence."""

    model_config = ConfigDict(extra="forbid")

    source_run_id: str = Field(min_length=1)
    step_ids: List[str] = Field(min_length=1)

    @field_validator("step_ids")
    @classmethod
    def reject_duplicate_step_ids(cls, value: List[str]) -> List[str]:
        if any(not step_id for step_id in value):
            raise ValueError("replay step_ids must not contain empty values")
        if len(set(value)) != len(value):
            raise ValueError("replay step_ids must be unique")
        return sorted(value)


class SelectiveReplaySummary(BaseModel):
    source_run_id: str
    requested_step_ids: List[str]
    replayed_step_ids: List[str]
    reused_step_ids: List[str]
    automatically_invalidated_step_ids: List[str]


class ReleaseValidationResult(BaseModel):
    run_id: str
    status: ReleaseValidationStatus
    findings: List[ValidationFinding] = Field(default_factory=list)
    replay: SelectiveReplaySummary | None = None


class ReleaseValidationInput(BaseModel):
    """Typed input for one managed release-validation run."""

    model_config = ConfigDict(extra="forbid")

    manifest: ReleaseManifest
    resume_interrupted: bool = False
    replay: SelectiveReplayRequest | None = None


class ReleaseValidationState(BaseRuntimeState):
    """Checkpoint stored by the generic manager for this domain."""

    domain_id: ClassVar[str] = "release-validation"
    schema_version: ClassVar[str] = "1"

    manifest: ReleaseManifest | None = None
    result: ReleaseValidationResult | None = None
    current_stage: str = "initialized"
