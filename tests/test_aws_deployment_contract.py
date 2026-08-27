from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AWS_DEPLOYMENT = ROOT / "deploy" / "aws"


def _terraform_sources() -> dict[Path, str]:
    return {
        path: path.read_text(encoding="utf-8")
        for path in sorted(AWS_DEPLOYMENT.rglob("*.tf"))
    }


def test_aws_adapter_has_no_live_lookup_or_plaintext_secret_authority() -> None:
    sources = _terraform_sources()
    combined = "\n".join(sources.values())

    assert sources
    assert re.search(r'\bdata\s+"aws_', combined) is None
    assert 'resource "aws_secretsmanager_secret_version"' not in combined
    assert 'resource "aws_ssm_parameter"' not in combined
    assert re.search(r'\bprovisioner\s+"(?:local-exec|remote-exec)"', combined) is None
    assert re.search(r"AKIA[0-9A-Z]{16}", combined) is None


def test_state_backend_is_externalized_and_native_locking_is_declared() -> None:
    backend = (AWS_DEPLOYMENT / "proof" / "backend.tf").read_text(encoding="utf-8")
    bootstrap_outputs = (
        AWS_DEPLOYMENT / "bootstrap" / "outputs.tf"
    ).read_text(encoding="utf-8")

    assert 'backend "s3" {}' in backend
    assert re.search(r"^\s*(?:bucket|key|region)\s*=", backend, re.MULTILINE) is None
    assert "use_lockfile = true" in bootstrap_outputs


def test_offline_plan_uses_mock_provider_and_fail_closed_controls() -> None:
    proof = (
        AWS_DEPLOYMENT / "proof" / "tests" / "proof.tftest.hcl"
    ).read_text(encoding="utf-8")

    assert 'mock_provider "aws"' in proof
    assert 'command = plan' in proof
    assert "unseeded_secret_blocks_runtime_start" in proof
    assert "short_ecs_stop_budget_is_rejected" in proof
    assert "retained_rds_snapshot_requires_unique_name" in proof
    assert proof.count("expect_failures = [aws_ecs_service.runtime]") == 2
    assert proof.count("expect_failures = [aws_db_instance.runtime]") == 1


def test_application_and_bootstrap_tasks_keep_separate_roles() -> None:
    compute = (AWS_DEPLOYMENT / "proof" / "compute.tf").read_text(encoding="utf-8")
    iam = (AWS_DEPLOYMENT / "proof" / "iam.tf").read_text(encoding="utf-8")

    assert re.search(
        r"task_role_arn\s*=\s*aws_iam_role\.runtime_task\.arn", compute
    )
    assert re.search(
        r"task_role_arn\s*=\s*aws_iam_role\.bootstrap_task\.arn", compute
    )
    assert 'resource "aws_iam_role" "runtime_task"' in iam
    assert 'resource "aws_iam_role" "bootstrap_task"' in iam
    assert 'resource "aws_iam_role_policy" "runtime_task"' not in iam
    assert 'resource "aws_iam_role_policy" "bootstrap_task"' not in iam
