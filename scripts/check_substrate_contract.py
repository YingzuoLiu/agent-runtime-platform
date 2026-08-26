from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTABLE_SOURCE_DIRECTORIES = ("agent", "api", "domains", "runtime_service")
APPLICATION_REQUIREMENT_FILES = ("requirements.txt", "requirements-postgres.txt")

FORBIDDEN_IMPORT_PREFIXES = (
    "aioboto3",
    "aiobotocore",
    "aws_embedded_metrics",
    "aws_lambda_powertools",
    "aws_xray_sdk",
    "awscrt",
    "azure",
    "boto3",
    "botocore",
    "google.api_core",
    "google.auth",
    "google.cloud",
    "kubernetes",
    "opentelemetry.instrumentation.botocore",
    "opentelemetry.propagators.aws",
    "opentelemetry.sdk.extension.aws",
    "s3fs",
    "sagemaker",
)
FORBIDDEN_DEPLOYMENT_IMPORT_PREFIXES = ("deploy", "scripts")
FORBIDDEN_DEPENDENCY_PREFIXES = (
    "aioboto3",
    "aiobotocore",
    "aws-embedded-metrics",
    "aws-lambda-powertools",
    "aws-xray-sdk",
    "awscrt",
    "azure-",
    "boto3",
    "botocore",
    "google-api-core",
    "google-auth",
    "google-cloud-",
    "kubernetes",
    "opentelemetry-propagator-aws-xray",
    "opentelemetry-sdk-extension-aws",
    "s3fs",
    "sagemaker",
)
FORBIDDEN_RUNTIME_MARKERS = {
    "169.254.169.254": "cloud instance metadata endpoint",
    "169.254.170.2": "ECS task metadata endpoint",
    "arn:aws:": "AWS ARN as application data",
    "aws_access_key_id": "AWS credential environment",
    "aws_container_credentials_": "AWS container credential environment",
    "aws_default_region": "AWS region environment",
    "aws_execution_env": "AWS execution environment",
    "aws_region": "AWS region environment",
    "aws_secret_access_key": "AWS credential environment",
    "aws_web_identity_token_file": "AWS workload identity environment",
    "cloudwatchmetrics": "CloudWatch embedded metric envelope",
    "ecs_container_metadata_uri": "ECS task metadata environment",
    "generate_db_auth_token": "RDS IAM-only authentication path",
    "rds-db:connect": "RDS IAM-only authentication path",
    "receipthandle": "SQS receipt authority",
    "states:startexecution": "Step Functions execution authority",
    "visibilitytimeout": "queue visibility authority",
}


@dataclass(frozen=True, slots=True, order=True)
class ContractViolation:
    path: str
    line: int
    category: str
    detail: str

    def render(self) -> str:
        return f"{self.path}:{self.line}: {self.category}: {self.detail}"


def _matches_module_prefix(module_name: str, prefix: str) -> bool:
    return module_name == prefix or module_name.startswith(f"{prefix}.")


def _import_names(tree: ast.AST) -> list[tuple[int, str]]:
    names: list[tuple[int, str]] = []
    importlib_module_names = {"importlib"}
    import_module_function_names = {"__import__"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend((node.lineno, alias.name) for alias in node.names)
            for alias in node.names:
                if alias.name == "importlib":
                    importlib_module_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.extend(
                (
                    node.lineno,
                    (
                        node.module
                        if alias.name == "*"
                        else f"{node.module}.{alias.name}"
                    ),
                )
                for alias in node.names
            )
            if node.module == "importlib":
                for alias in node.names:
                    if alias.name == "import_module":
                        import_module_function_names.add(alias.asname or alias.name)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and node.args:
            is_dynamic_import = False
            if (
                isinstance(node.func, ast.Name)
                and node.func.id in import_module_function_names
            ):
                is_dynamic_import = True
            elif (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in importlib_module_names
                and node.func.attr == "import_module"
            ):
                is_dynamic_import = True
            first_argument = node.args[0]
            if (
                is_dynamic_import
                and isinstance(first_argument, ast.Constant)
                and isinstance(first_argument.value, str)
            ):
                names.append((node.lineno, first_argument.value))
    return names


def _docstring_constant_ids(tree: ast.AST) -> set[int]:
    docstrings: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
        ):
            continue
        body = getattr(node, "body", None)
        if not body or not isinstance(body, list):
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstrings.add(id(first.value))
    return docstrings


def find_python_violations(path: Path, source: str) -> list[ContractViolation]:
    tree = ast.parse(source, filename=str(path))
    display_path = path.as_posix()
    violations: list[ContractViolation] = []

    for line, module_name in _import_names(tree):
        for prefix in FORBIDDEN_IMPORT_PREFIXES:
            if _matches_module_prefix(module_name, prefix):
                violations.append(
                    ContractViolation(
                        display_path,
                        line,
                        "cloud-sdk-import",
                        module_name,
                    )
                )
        for prefix in FORBIDDEN_DEPLOYMENT_IMPORT_PREFIXES:
            if _matches_module_prefix(module_name, prefix):
                violations.append(
                    ContractViolation(
                        display_path,
                        line,
                        "reverse-deployment-import",
                        module_name,
                    )
                )

    docstring_ids = _docstring_constant_ids(tree)
    for node in ast.walk(tree):
        if (
            not isinstance(node, ast.Constant)
            or not isinstance(node.value, str)
            or id(node) in docstring_ids
        ):
            continue
        normalized = node.value.lower()
        for marker, reason in FORBIDDEN_RUNTIME_MARKERS.items():
            if marker in normalized:
                violations.append(
                    ContractViolation(
                        display_path,
                        node.lineno,
                        "vendor-runtime-semantic",
                        reason,
                    )
                )

    return sorted(set(violations))


def _normalized_requirement_name(line: str) -> str | None:
    candidate = line.split("#", 1)[0].strip()
    if not candidate or candidate.startswith("-"):
        return None
    name = re.split(r"[\s\[<>=!~;@]", candidate, maxsplit=1)[0]
    return name.lower().replace("_", "-")


def find_requirement_violations(path: Path, source: str) -> list[ContractViolation]:
    violations: list[ContractViolation] = []
    for line_number, line in enumerate(source.splitlines(), start=1):
        requirement_name = _normalized_requirement_name(line)
        if requirement_name is None:
            continue
        if any(
            requirement_name == prefix.rstrip("-")
            or requirement_name.startswith(prefix)
            for prefix in FORBIDDEN_DEPENDENCY_PREFIXES
        ):
            violations.append(
                ContractViolation(
                    path.as_posix(),
                    line_number,
                    "cloud-sdk-dependency",
                    requirement_name,
                )
            )
    return violations


def scan_repository(root: Path = ROOT) -> tuple[list[ContractViolation], int]:
    violations: list[ContractViolation] = []
    scanned_python_files = 0

    for directory_name in PORTABLE_SOURCE_DIRECTORIES:
        directory = root / directory_name
        if not directory.is_dir():
            violations.append(
                ContractViolation(
                    directory.relative_to(root).as_posix(),
                    0,
                    "missing-portable-source-root",
                    directory_name,
                )
            )
            continue
        for path in sorted(directory.rglob("*.py")):
            scanned_python_files += 1
            relative_path = path.relative_to(root)
            violations.extend(
                find_python_violations(
                    relative_path,
                    path.read_text(encoding="utf-8"),
                )
            )

    for filename in APPLICATION_REQUIREMENT_FILES:
        path = root / filename
        if not path.is_file():
            violations.append(
                ContractViolation(filename, 0, "missing-dependency-manifest", filename)
            )
            continue
        violations.extend(
            find_requirement_violations(
                Path(filename),
                path.read_text(encoding="utf-8"),
            )
        )

    return sorted(set(violations)), scanned_python_files


def main(root: Path = ROOT) -> int:
    violations, scanned_python_files = scan_repository(root)
    if violations:
        print("portable substrate contract: FAIL", file=sys.stderr)
        for violation in violations:
            print(violation.render(), file=sys.stderr)
        return 1
    print(
        "portable substrate contract: PASS "
        f"({scanned_python_files} Python files, "
        f"{len(APPLICATION_REQUIREMENT_FILES)} dependency manifests)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
