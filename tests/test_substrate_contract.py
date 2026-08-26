from pathlib import Path

import pytest

from scripts.check_substrate_contract import (
    APPLICATION_REQUIREMENT_FILES,
    find_python_violations,
    find_requirement_violations,
    main,
    scan_repository,
)


ROOT = Path(__file__).resolve().parents[1]
PORTABLE_APPLICATION_ROOTS = ("agent", "api", "domains", "runtime_service")


def _categories(source: str) -> set[str]:
    return {
        violation.category
        for violation in find_python_violations(Path("runtime_service/candidate.py"), source)
    }


def test_repository_satisfies_portable_substrate_contract() -> None:
    violations, scanned_python_files = scan_repository(ROOT)

    assert scanned_python_files > 0
    assert violations == []


@pytest.mark.parametrize(
    "source",
    [
        "import boto3\n",
        "from botocore.config import Config\n",
        "from google import api_core\n",
        "from google import auth\n",
        "from google import cloud\n",
        "from google.cloud import storage\n",
        "from azure.identity import DefaultAzureCredential\n",
        "from opentelemetry.instrumentation import botocore\n",
        "from opentelemetry.propagators import aws\n",
        "from opentelemetry.sdk.extension import aws\n",
        (
            "from opentelemetry.sdk.extension.aws.resource "
            "import AwsEcsResourceDetector\n"
        ),
        'import importlib\nclient = importlib.import_module("boto3")\n',
        'import importlib as loader\nclient = loader.import_module("boto3")\n',
        'from importlib import import_module as load\nclient = load("boto3")\n',
        'client = __import__("kubernetes.client")\n',
    ],
)
def test_guard_rejects_static_and_dynamic_cloud_sdk_imports(source: str) -> None:
    assert "cloud-sdk-import" in _categories(source)


@pytest.mark.parametrize(
    "source",
    [
        "from deploy.aws.runtime import task_identity\n",
        "from scripts.aws_evidence import collect\n",
    ],
)
def test_guard_rejects_reverse_dependency_on_deployment_code(source: str) -> None:
    assert "reverse-deployment-import" in _categories(source)


@pytest.mark.parametrize(
    "source",
    [
        'endpoint = "http://169.254.170.2/v4/task"\n',
        'owner_id = environ["ECS_CONTAINER_METADATA_URI_V4"]\n',
        'queue_field = "ReceiptHandle"\n',
        'queue_setting = "VisibilityTimeout"\n',
        'permission = "rds-db:connect"\n',
        'metrics = {"CloudWatchMetrics": []}\n',
    ],
)
def test_guard_rejects_provider_specific_runtime_semantics(source: str) -> None:
    assert "vendor-runtime-semantic" in _categories(source)


def test_guard_allows_portable_runtime_protocols() -> None:
    source = """
import os
from uuid import uuid4

postgres_dsn = os.environ["RUNTIME_POSTGRES_DSN"]
owner_id = f"manager_{uuid4().hex}"
log_record = {"trace_id": "trace-1", "message": "ready"}
"""

    assert find_python_violations(Path("runtime_service/candidate.py"), source) == []


@pytest.mark.parametrize(
    "requirement",
    [
        "boto3>=1.40\n",
        "google-cloud-storage==3.0\n",
        "azure-identity~=1.0\n",
        "aws_xray_sdk>=2\n",
        "opentelemetry-propagator-aws-xray==1.0.2\n",
        "opentelemetry-sdk-extension-aws==2.1.0\n",
    ],
)
def test_guard_rejects_cloud_sdk_production_dependencies(requirement: str) -> None:
    violations = find_requirement_violations(Path("requirements.txt"), requirement)

    assert [violation.category for violation in violations] == ["cloud-sdk-dependency"]


def test_guard_allows_portable_postgres_and_telemetry_dependencies() -> None:
    requirements = """
psycopg[binary]>=3.2,<4
opentelemetry-exporter-otlp>=1.0
"""

    assert find_requirement_violations(Path("requirements.txt"), requirements) == []


@pytest.mark.parametrize("violating_directory", PORTABLE_APPLICATION_ROOTS)
def test_main_returns_nonzero_for_violation_in_each_portable_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    violating_directory: str,
) -> None:
    for directory_name in PORTABLE_APPLICATION_ROOTS:
        directory = tmp_path / directory_name
        directory.mkdir()
        (directory / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / violating_directory / "candidate.py").write_text(
        "import boto3\n",
        encoding="utf-8",
    )
    for filename in APPLICATION_REQUIREMENT_FILES:
        (tmp_path / filename).write_text("", encoding="utf-8")

    assert main(tmp_path) == 1
    captured = capsys.readouterr()
    assert "portable substrate contract: FAIL" in captured.err
    assert f"{violating_directory}/candidate.py:1: cloud-sdk-import: boto3" in (
        captured.err
    )
