from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class ToolExecutionStatus(str, Enum):
    COMPLETED = "completed"
    DENIED = "denied"
    INVALID_INPUT = "invalid_input"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class ToolPolicy(BaseModel):
    timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    max_output_bytes: int = Field(default=16_384, ge=256, le=1_048_576)
    max_memory_mb: int = Field(default=256, ge=32, le=1024)
    max_cpu_seconds: int = Field(default=2, ge=1, le=30)
    network_mode: Literal["host"] = "host"


class ToolExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    arguments: dict[str, Any] = Field(default_factory=dict)
    run_id: str | None = None


class ToolExecutionResult(BaseModel):
    execution_id: str
    tool_name: str
    status: ToolExecutionStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: int
    exit_code: int | None = None
    output_truncated: bool = False


class ToolDescriptor(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    policy: ToolPolicy


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_model: type[BaseModel]
    policy: ToolPolicy
    handler_entrypoint: str


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        if spec.name in self._tools:
            raise ValueError(f"Tool already registered: {spec.name}")
        module_name, separator, function_name = spec.handler_entrypoint.partition(":")
        if not separator or not module_name or not function_name or ":" in function_name:
            raise ValueError(
                "handler_entrypoint must use the server-controlled 'module:function' format"
            )
        self._tools[spec.name] = spec

    def resolve(self, tool_name: str) -> ToolSpec | None:
        return self._tools.get(tool_name)

    def list_tools(self) -> list[ToolDescriptor]:
        return [
            ToolDescriptor(
                name=spec.name,
                description=spec.description,
                input_schema=spec.input_model.model_json_schema(),
                policy=spec.policy,
            )
            for spec in sorted(self._tools.values(), key=lambda item: item.name)
        ]


class ToolSandbox:
    """Execute only server-registered tools in a restricted subprocess.

    This is intentionally not an arbitrary-code sandbox. The service controls
    the executable, worker script, tool allowlist, schemas, environment and
    resource policy. The process backend does not isolate host networking or
    the full host filesystem; those require a container, gVisor or microVM
    execution backend.
    """

    def __init__(self, registry: ToolRegistry) -> None:
        self.registry = registry
        self._worker_path = Path(__file__).with_name("sandbox_worker.py")

    def execute(self, tool_name: str, arguments: dict[str, Any]) -> ToolExecutionResult:
        started = time.monotonic()
        execution_id = f"exec_{uuid4().hex}"
        spec = self.registry.resolve(tool_name)
        if spec is None:
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.DENIED,
                error="Tool is not registered in the runtime allowlist.",
                duration_ms=self._duration_ms(started),
            )

        try:
            validated = spec.input_model.model_validate(arguments)
        except ValidationError as exc:
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.INVALID_INPUT,
                error=exc.json(),
                duration_ms=self._duration_ms(started),
            )

        command = [sys.executable, str(self._worker_path), spec.handler_entrypoint]
        environment = self._sanitized_environment(spec.policy)

        with tempfile.TemporaryDirectory(prefix="agent-runtime-sandbox-") as workspace:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                start_new_session=os.name == "posix",
            )
            payload = json.dumps(validated.model_dump(mode="json")).encode("utf-8")
            try:
                stdout, stderr = process.communicate(
                    input=payload,
                    timeout=spec.policy.timeout_seconds,
                )
            except subprocess.TimeoutExpired:
                self._terminate_process(process)
                stdout, stderr = process.communicate()
                return ToolExecutionResult(
                    execution_id=execution_id,
                    tool_name=tool_name,
                    status=ToolExecutionStatus.TIMED_OUT,
                    error=f"Tool exceeded {spec.policy.timeout_seconds:.2f}s timeout.",
                    duration_ms=self._duration_ms(started),
                    exit_code=process.returncode,
                    output_truncated=(
                        len(stdout) > spec.policy.max_output_bytes
                        or len(stderr) > spec.policy.max_output_bytes
                    ),
                )

        output_truncated = (
            len(stdout) > spec.policy.max_output_bytes
            or len(stderr) > spec.policy.max_output_bytes
        )
        stdout = stdout[: spec.policy.max_output_bytes]
        stderr = stderr[: spec.policy.max_output_bytes]

        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.FAILED,
                error=error or f"Tool exited with code {process.returncode}.",
                duration_ms=self._duration_ms(started),
                exit_code=process.returncode,
                output_truncated=output_truncated,
            )

        try:
            decoded = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.FAILED,
                error=f"Sandbox returned invalid JSON: {type(exc).__name__}",
                duration_ms=self._duration_ms(started),
                exit_code=process.returncode,
                output_truncated=output_truncated,
            )

        if not isinstance(decoded, dict):
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.FAILED,
                error="Sandbox result must be a JSON object.",
                duration_ms=self._duration_ms(started),
                exit_code=process.returncode,
                output_truncated=output_truncated,
            )

        return ToolExecutionResult(
            execution_id=execution_id,
            tool_name=tool_name,
            status=ToolExecutionStatus.COMPLETED,
            result=decoded,
            duration_ms=self._duration_ms(started),
            exit_code=process.returncode,
            output_truncated=output_truncated,
        )

    @staticmethod
    def _sanitized_environment(policy: ToolPolicy) -> dict[str, str]:
        environment = {
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
            "PATH": os.environ.get("PATH", ""),
            "SANDBOX_MAX_CPU_SECONDS": str(policy.max_cpu_seconds),
            "SANDBOX_MAX_MEMORY_MB": str(policy.max_memory_mb),
            "SANDBOX_NETWORK_MODE": policy.network_mode,
        }
        for key in ("LANG", "LC_ALL", "TZ", "SYSTEMROOT", "WINDIR"):
            value = os.environ.get(key)
            if value:
                environment[key] = value
        return environment

    @staticmethod
    def _terminate_process(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            return

    @staticmethod
    def _duration_ms(started: float) -> int:
        return max(0, int((time.monotonic() - started) * 1000))
