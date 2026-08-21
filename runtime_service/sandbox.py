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
from typing import Any, Callable, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError


WORKER_CAPABILITY_UNSUPPORTED_EXIT_CODE = 5
WORKER_CAPABILITY_UNSUPPORTED_PREFIX = "CAPABILITY_UNSUPPORTED: "


class ToolExecutionStatus(str, Enum):
    COMPLETED = "completed"
    DENIED = "denied"
    CAPABILITY_UNSUPPORTED = "capability_unsupported"
    INVALID_INPUT = "invalid_input"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class ToolEffect(str, Enum):
    """Server-owned classification of whether a tool changes external state."""

    READ_ONLY = "read_only"
    EXTERNAL_WRITE = "external_write"


class ToolRetryMode(str, Enum):
    """Retry contract declared by the server-controlled tool registration."""

    SAFE = "safe"
    PROVIDER_IDEMPOTENT = "provider_idempotent"
    UNSAFE = "unsafe"


class ToolPolicy(BaseModel):
    """Server-declared requirements for one registered tool execution.

    Capability declarations are requirements, not proof. ``ToolSandbox`` must
    reject an execution unless its subprocess layer supports the requested
    mode at the requested enforcement strength.
    """

    model_config = ConfigDict(extra="forbid")

    timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    max_output_bytes: int = Field(default=16_384, ge=256, le=1_048_576)
    max_memory_mb: int = Field(default=256, ge=32, le=1024)
    max_cpu_seconds: int = Field(default=2, ge=1, le=30)
    filesystem_mode: Literal["none", "readonly", "readwrite"] = "readwrite"
    filesystem_enforcement: Literal["process", "kernel"] = "process"
    network_mode: Literal["none", "host"] = "host"
    network_enforcement: Literal["process", "kernel"] = "process"
    environment_mode: Literal["restricted", "inherited"] = "restricted"
    environment_enforcement: Literal["process", "kernel"] = "process"


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
    handler_entrypoint: str | None = None
    effect: ToolEffect = ToolEffect.READ_ONLY
    retry_mode: ToolRetryMode = ToolRetryMode.SAFE
    provider_name: str | None = None
    output_model: type[BaseModel] | None = None
    runtime_input_gate: Callable[[dict[str, Any]], bool] | None = None


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        if spec.name in self._tools:
            raise ValueError(f"Tool already registered: {spec.name}")

        if spec.effect == ToolEffect.READ_ONLY:
            if spec.retry_mode != ToolRetryMode.SAFE:
                raise ValueError("read-only tools must use retry_mode='safe'")
            if spec.provider_name is not None:
                raise ValueError("read-only tools cannot declare provider_name")
            if spec.runtime_input_gate is not None:
                raise ValueError("read-only tools cannot declare runtime_input_gate")
            if not self._valid_handler_entrypoint(spec.handler_entrypoint):
                raise ValueError(
                    "handler_entrypoint must use the server-controlled "
                    "'module:function' format"
                )
        elif spec.effect == ToolEffect.EXTERNAL_WRITE:
            if spec.handler_entrypoint is not None:
                raise ValueError(
                    "external-write tools cannot declare a sandbox handler_entrypoint"
                )
            if spec.provider_name is None or not spec.provider_name.strip():
                raise ValueError("external-write tools require a non-empty provider_name")
            if spec.output_model is None:
                raise ValueError("external-write tools require an output_model")
            if spec.runtime_input_gate is not None and not callable(
                spec.runtime_input_gate
            ):
                raise ValueError("runtime_input_gate must be callable")
            if (
                not isinstance(spec.output_model, type)
                or not issubclass(spec.output_model, BaseModel)
                or spec.output_model.model_config.get("extra") != "forbid"
            ):
                raise ValueError(
                    "external-write output_model must be a BaseModel with extra='forbid'"
                )
        else:  # pragma: no cover - Enum typing makes this defensive only
            raise ValueError(
                f"unsupported tool effect: {spec.effect!r}"
            )
        self._tools[spec.name] = spec

    @staticmethod
    def _valid_handler_entrypoint(handler_entrypoint: str | None) -> bool:
        if handler_entrypoint is None:
            return False
        module_name, separator, function_name = handler_entrypoint.partition(":")
        return bool(
            separator
            and module_name
            and function_name
            and ":" not in function_name
        )

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
    resource policy. ``network_mode='none'`` installs a Python socket guard in
    the worker. That is process-level prevention for trusted registered tools,
    not a kernel security boundary. The subprocess cannot enforce a private or
    read-only filesystem; unsupported requirements fail closed before start.
    """

    _PROCESS_CAPABILITY_SUPPORT = {
        "filesystem": frozenset({("readwrite", "process")}),
        "network": frozenset({("none", "process"), ("host", "process")}),
        "environment": frozenset(
            {("restricted", "process"), ("inherited", "process")}
        ),
    }

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

        if spec.effect == ToolEffect.EXTERNAL_WRITE:
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.DENIED,
                error=(
                    "External-write tools require the durable external-action "
                    "execution path."
                ),
                duration_ms=self._duration_ms(started),
            )

        unsupported_capability = self._unsupported_capability(spec.policy)
        if unsupported_capability is not None:
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.CAPABILITY_UNSUPPORTED,
                error=unsupported_capability,
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

        assert spec.handler_entrypoint is not None
        command = [
            sys.executable,
            "-E",
            "-P",
            "-X",
            "utf8",
            "-u",
            str(self._worker_path),
            spec.handler_entrypoint,
        ]
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

        error = stderr.decode("utf-8", errors="replace").strip()
        if (
            process.returncode == WORKER_CAPABILITY_UNSUPPORTED_EXIT_CODE
            and error.startswith(WORKER_CAPABILITY_UNSUPPORTED_PREFIX)
        ):
            return ToolExecutionResult(
                execution_id=execution_id,
                tool_name=tool_name,
                status=ToolExecutionStatus.CAPABILITY_UNSUPPORTED,
                error=error,
                duration_ms=self._duration_ms(started),
                exit_code=process.returncode,
                output_truncated=output_truncated,
            )

        if process.returncode != 0:
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
        environment = (
            dict(os.environ) if policy.environment_mode == "inherited" else {}
        )
        # Preserve legacy handler-visible values. ``-E`` ignores them for
        # interpreter startup; ``-X utf8`` owns the worker's encoding contract.
        environment.update({
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
            "PATH": os.environ.get("PATH", ""),
            "SANDBOX_MAX_CPU_SECONDS": str(policy.max_cpu_seconds),
            "SANDBOX_MAX_MEMORY_MB": str(policy.max_memory_mb),
            "SANDBOX_FILESYSTEM_MODE": policy.filesystem_mode,
            "SANDBOX_FILESYSTEM_ENFORCEMENT": policy.filesystem_enforcement,
            "SANDBOX_NETWORK_MODE": policy.network_mode,
            "SANDBOX_NETWORK_ENFORCEMENT": policy.network_enforcement,
            "SANDBOX_ENVIRONMENT_MODE": policy.environment_mode,
            "SANDBOX_ENVIRONMENT_ENFORCEMENT": policy.environment_enforcement,
        })
        if policy.environment_mode == "restricted":
            for key in ("LANG", "LC_ALL", "TZ", "SYSTEMROOT", "WINDIR"):
                value = os.environ.get(key)
                if value:
                    environment[key] = value
        return environment

    @classmethod
    def _unsupported_capability(cls, policy: ToolPolicy) -> str | None:
        requirements = (
            (
                "filesystem",
                policy.filesystem_mode,
                policy.filesystem_enforcement,
            ),
            ("network", policy.network_mode, policy.network_enforcement),
            (
                "environment",
                policy.environment_mode,
                policy.environment_enforcement,
            ),
        )
        for capability, mode, enforcement in requirements:
            if (mode, enforcement) not in cls._PROCESS_CAPABILITY_SUPPORT[capability]:
                return (
                    "Tool capability cannot be enforced by the subprocess executor: "
                    f"{capability}={mode}, enforcement={enforcement}."
                )
        return None

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
