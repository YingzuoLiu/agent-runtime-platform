from __future__ import annotations

import socket

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from domains.travel.tools import build_travel_tool_registry
from runtime_service import (
    ToolExecutionStatus,
    ToolPolicy,
    ToolRegistry,
    ToolSandbox,
    ToolSpec,
    build_default_tool_registry,
)
from runtime_service.sandbox import ToolEffect, ToolRetryMode
from runtime_service.sandbox_worker import (
    CAPABILITY_UNSUPPORTED_EXIT_CODE,
    NETWORK_DENIED_MESSAGE,
    PROCESS_CAPABILITY_SUPPORT,
)


class SleepInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seconds: float = Field(gt=0, le=5)


class EnvironmentProbeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keys: list[str] = Field(min_length=1, max_length=20)


class NetworkProbeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = Field(ge=1, le=65535)


class ExternalWriteOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool


class PermissiveExternalWriteOutput(BaseModel):
    model_config = ConfigDict(extra="allow")

    ok: bool


def test_registered_tool_executes_in_subprocess():
    sandbox = ToolSandbox(build_travel_tool_registry())

    result = sandbox.execute(
        "route_cost_summary",
        {
            "transport_cost": 2000,
            "hotel_cost": 3000,
            "activity_cost": 1000,
            "budget": 7000,
        },
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result == {
        "total_cost": 6000,
        "budget": 7000,
        "remaining_budget": 1000,
        "within_budget": True,
    }


def test_unregistered_tool_is_denied_before_process_start():
    sandbox = ToolSandbox(build_travel_tool_registry())

    result = sandbox.execute("python", {"code": "print('not allowed')"})

    assert result.status == ToolExecutionStatus.DENIED
    assert result.exit_code is None


def test_existing_read_only_specs_keep_legacy_defaults_and_descriptor_shape():
    registry = build_travel_tool_registry()
    spec = registry.resolve("search_trip_options")
    assert spec is not None

    assert spec.effect == ToolEffect.READ_ONLY
    assert spec.retry_mode == ToolRetryMode.SAFE
    assert spec.provider_name is None
    assert spec.policy.filesystem_mode == "readwrite"
    assert spec.policy.network_mode == "host"
    assert spec.policy.environment_mode == "restricted"
    assert set(registry.list_tools()[0].model_dump()) == {
        "name",
        "description",
        "input_schema",
        "policy",
    }


def test_capability_declaration_is_strict_and_worker_support_matches_preflight():
    with pytest.raises(ValidationError):
        ToolPolicy(network="none")

    assert ToolSandbox._PROCESS_CAPABILITY_SUPPORT == PROCESS_CAPABILITY_SUPPORT


def test_legacy_fifth_positional_argument_still_binds_handler_entrypoint():
    spec = ToolSpec(
        "legacy-positional",
        "Legacy positional construction.",
        SleepInput,
        ToolPolicy(),
        "tests.sandbox_handlers:sleep_test",
    )

    ToolRegistry().register(spec)
    assert spec.handler_entrypoint == "tests.sandbox_handlers:sleep_test"
    assert spec.output_model is None


@pytest.mark.parametrize("retry_mode", list(ToolRetryMode))
def test_external_write_registration_accepts_every_explicit_retry_contract(retry_mode):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_external_write_test",
            description="Test-only external action.",
            input_model=SleepInput,
            output_model=ExternalWriteOutput,
            policy=ToolPolicy(),
            effect=ToolEffect.EXTERNAL_WRITE,
            retry_mode=retry_mode,
            provider_name="test-provider",
        )
    )

    spec = registry.resolve("_external_write_test")
    assert spec is not None
    assert spec.handler_entrypoint is None
    assert spec.retry_mode == retry_mode


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (
            ToolSpec(
                name="read-only-without-handler",
                description="Invalid read-only tool.",
                input_model=SleepInput,
                policy=ToolPolicy(),
            ),
            "handler_entrypoint",
        ),
        (
            ToolSpec(
                name="read-only-provider",
                description="Invalid read-only tool.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                handler_entrypoint="tests.sandbox_handlers:sleep_test",
                provider_name="unexpected",
            ),
            "provider_name",
        ),
        (
            ToolSpec(
                name="external-without-output-model",
                description="Invalid external action.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
                provider_name="test-provider",
            ),
            "output_model",
        ),
        (
            ToolSpec(
                name="external-with-permissive-output-model",
                description="Invalid external action.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
                provider_name="test-provider",
                output_model=PermissiveExternalWriteOutput,
            ),
            "extra='forbid'",
        ),
        (
            ToolSpec(
                name="external-with-invalid-runtime-gate",
                description="Invalid external action.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
                provider_name="test-provider",
                output_model=ExternalWriteOutput,
                runtime_input_gate=True,  # type: ignore[arg-type]
            ),
            "runtime_input_gate",
        ),
        (
            ToolSpec(
                name="read-only-unsafe",
                description="Invalid read-only tool.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                handler_entrypoint="tests.sandbox_handlers:sleep_test",
                retry_mode=ToolRetryMode.UNSAFE,
            ),
            "retry_mode",
        ),
        (
            ToolSpec(
                name="external-with-handler",
                description="Invalid external action.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                handler_entrypoint="tests.sandbox_handlers:sleep_test",
                effect=ToolEffect.EXTERNAL_WRITE,
                provider_name="test-provider",
            ),
            "handler_entrypoint",
        ),
        (
            ToolSpec(
                name="external-without-provider",
                description="Invalid external action.",
                input_model=SleepInput,
                policy=ToolPolicy(),
                effect=ToolEffect.EXTERNAL_WRITE,
            ),
            "provider_name",
        ),
    ],
)
def test_registry_rejects_incoherent_execution_metadata(spec, message):
    with pytest.raises(ValueError, match=message):
        ToolRegistry().register(spec)


def test_sandbox_denies_external_write_before_validation_or_process_start(
    monkeypatch,
):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_external_write_test",
            description="Test-only external action.",
            input_model=SleepInput,
            output_model=ExternalWriteOutput,
            policy=ToolPolicy(filesystem_mode="none"),
            effect=ToolEffect.EXTERNAL_WRITE,
            retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
            provider_name="test-provider",
        )
    )

    def unexpected_process_start(*_args, **_kwargs):
        raise AssertionError("external-write tool started a subprocess")

    monkeypatch.setattr("runtime_service.sandbox.subprocess.Popen", unexpected_process_start)
    result = ToolSandbox(registry).execute(
        "_external_write_test",
        {"seconds": "this would fail schema validation"},
    )

    assert result.status == ToolExecutionStatus.DENIED
    assert result.exit_code is None
    assert result.error == (
        "External-write tools require the durable external-action execution path."
    )


def test_invalid_arguments_are_rejected_by_schema():
    sandbox = ToolSandbox(build_travel_tool_registry())

    result = sandbox.execute(
        "route_cost_summary",
        {
            "transport_cost": -1,
            "hotel_cost": 3000,
            "activity_cost": 1000,
            "budget": 7000,
            "unexpected": "field",
        },
    )

    assert result.status == ToolExecutionStatus.INVALID_INPUT
    assert result.exit_code is None


def test_tool_timeout_terminates_the_process():
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_sleep_test",
            description="Test-only blocking tool.",
            input_model=SleepInput,
            policy=ToolPolicy(timeout_seconds=0.2),
            handler_entrypoint="tests.sandbox_handlers:sleep_test",
        )
    )
    sandbox = ToolSandbox(registry)

    result = sandbox.execute("_sleep_test", {"seconds": 2.0})

    assert result.status == ToolExecutionStatus.TIMED_OUT
    assert result.exit_code is not None


def test_parent_secrets_are_not_forwarded_to_worker(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-cross-process-boundary")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_environment_probe",
            description="Test-only environment visibility probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(),
            handler_entrypoint="tests.sandbox_handlers:environment_probe",
        )
    )
    sandbox = ToolSandbox(registry)

    result = sandbox.execute(
        "_environment_probe",
        {"keys": ["OPENAI_API_KEY", "PYTHONIOENCODING"]},
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result == {
        "present": {
            "OPENAI_API_KEY": False,
            "PYTHONIOENCODING": True,
        }
    }


def test_explicit_inherited_environment_is_forwarded_to_worker(monkeypatch):
    monkeypatch.setenv("SANDBOX_TEST_INHERITED", "visible-by-explicit-policy")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_inherited_environment_probe",
            description="Test-only inherited environment probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(environment_mode="inherited"),
            handler_entrypoint="tests.sandbox_handlers:environment_probe",
        )
    )

    result = ToolSandbox(registry).execute(
        "_inherited_environment_probe",
        {"keys": ["SANDBOX_TEST_INHERITED"]},
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result == {"present": {"SANDBOX_TEST_INHERITED": True}}


def test_inherited_pythonpath_cannot_redirect_handler_import(
    monkeypatch,
    tmp_path,
):
    module_name = "_sandbox_injected_handler"
    (tmp_path / f"{module_name}.py").write_text(
        "def run(_payload):\n    return {'injected': True}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_injected_handler_probe",
            description="Test-only interpreter isolation probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(environment_mode="inherited"),
            handler_entrypoint=f"{module_name}:run",
        )
    )

    result = ToolSandbox(registry).execute(
        "_injected_handler_probe",
        {"keys": ["PYTHONPATH"]},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error == f"ModuleNotFoundError: No module named '{module_name}'"


def test_worker_errors_use_utf8_independently_of_host_locale():
    module_name = "café_mödule"
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_utf8_error_probe",
            description="Test-only worker error encoding probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(),
            handler_entrypoint=f"{module_name}:run",
        )
    )

    result = ToolSandbox(registry).execute(
        "_utf8_error_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error == f"ModuleNotFoundError: No module named '{module_name}'"


def test_worker_flags_preserve_user_site_and_override_inherited_python_settings(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("PYTHONIOENCODING", "cp1252")
    monkeypatch.setenv("PYTHONUTF8", "0")
    monkeypatch.setenv("PYTHONHOME", str(tmp_path / "invalid-python-home"))
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_interpreter_runtime_probe",
            description="Test-only interpreter runtime probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(environment_mode="inherited"),
            handler_entrypoint="tests.sandbox_handlers:interpreter_runtime_probe",
        )
    )

    result = ToolSandbox(registry).execute(
        "_interpreter_runtime_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result == {
        "ignore_environment": 1,
        "isolated": 0,
        "no_user_site": 0,
        "safe_path": True,
        "stderr_encoding": "utf-8",
        "stdout_encoding": "utf-8",
        "utf8_mode": 1,
        "stderr_write_through": True,
        "stdout_write_through": True,
    }


def test_worker_unicode_error_round_trips_exactly():
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_unicode_error_probe",
            description="Test-only Unicode error probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(),
            handler_entrypoint="tests.sandbox_handlers:unicode_error_probe",
        )
    )

    result = ToolSandbox(registry).execute(
        "_unicode_error_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error == "RuntimeError: café 💥"


def test_declared_network_deny_is_enforced_inside_worker():
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_network_denied_probe",
            description="Test-only denied network probe.",
            input_model=NetworkProbeInput,
            policy=ToolPolicy(network_mode="none", network_enforcement="process"),
            handler_entrypoint="tests.sandbox_handlers:network_probe",
        )
    )

    result = ToolSandbox(registry).execute(
        "_network_denied_probe",
        {"host": "127.0.0.1", "port": 9},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error == (
        "PermissionError: Tool network access is denied by process policy."
    )


@pytest.mark.parametrize(
    "handler_entrypoint",
    [
        "tests.sandbox_handlers:socket_type_probe",
        "tests.sandbox_handlers:address_resolution_probe",
        "tests.sandbox_handlers:urllib_probe",
        "tests.sandbox_import_time_handler:import_time_socket_probe",
    ],
)
def test_network_guard_blocks_public_python_apis_and_import_time_access(
    handler_entrypoint,
):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_network_api_probe",
            description="Test-only denied network API probe.",
            input_model=NetworkProbeInput,
            policy=ToolPolicy(network_mode="none"),
            handler_entrypoint=handler_entrypoint,
        )
    )

    result = ToolSandbox(registry).execute(
        "_network_api_probe",
        {"host": "127.0.0.1", "port": 9},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error is not None
    assert NETWORK_DENIED_MESSAGE in result.error


def test_explicit_host_network_capability_allows_connection():
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_network_allowed_probe",
            description="Test-only allowed network probe.",
            input_model=NetworkProbeInput,
            policy=ToolPolicy(network_mode="host", network_enforcement="process"),
            handler_entrypoint="tests.sandbox_handlers:network_probe",
        )
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        host, port = listener.getsockname()

        result = ToolSandbox(registry).execute(
            "_network_allowed_probe",
            {"host": host, "port": port},
        )

        assert result.status == ToolExecutionStatus.COMPLETED
        assert result.result == {"connected": True}

        listener.settimeout(1.0)
        connection, _address = listener.accept()
        connection.close()


@pytest.mark.parametrize(
    ("policy", "capability"),
    [
        (ToolPolicy(filesystem_mode="none"), "filesystem=none"),
        (ToolPolicy(filesystem_mode="readonly"), "filesystem=readonly"),
        (
            ToolPolicy(filesystem_enforcement="kernel"),
            "filesystem=readwrite",
        ),
        (
            ToolPolicy(network_mode="none", network_enforcement="kernel"),
            "network=none",
        ),
        (
            ToolPolicy(network_mode="host", network_enforcement="kernel"),
            "network=host",
        ),
        (
            ToolPolicy(environment_mode="restricted", environment_enforcement="kernel"),
            "environment=restricted",
        ),
        (
            ToolPolicy(environment_mode="inherited", environment_enforcement="kernel"),
            "environment=inherited",
        ),
    ],
)
def test_unsupported_capability_requirement_fails_closed_before_process_start(
    monkeypatch,
    policy,
    capability,
):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_unsupported_capability_probe",
            description="Test-only unsupported capability probe.",
            input_model=EnvironmentProbeInput,
            policy=policy,
            handler_entrypoint="tests.sandbox_handlers:environment_probe",
        )
    )

    def unexpected_process_start(*_args, **_kwargs):
        raise AssertionError("unsupported capability started a subprocess")

    monkeypatch.setattr("runtime_service.sandbox.subprocess.Popen", unexpected_process_start)
    result = ToolSandbox(registry).execute(
        "_unsupported_capability_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.CAPABILITY_UNSUPPORTED
    assert result.exit_code is None
    assert result.error is not None
    assert capability in result.error
    assert "cannot be enforced" in result.error


def test_worker_capability_rejection_preserves_unsupported_status(monkeypatch):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_worker_capability_rejection_probe",
            description="Test-only worker capability rejection probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(
                network_mode="none",
                network_enforcement="kernel",
            ),
            handler_entrypoint="tests.sandbox_handlers:environment_probe",
        )
    )
    parent_support = dict(ToolSandbox._PROCESS_CAPABILITY_SUPPORT)
    parent_support["network"] = frozenset(
        {*parent_support["network"], ("none", "kernel")}
    )
    monkeypatch.setattr(ToolSandbox, "_PROCESS_CAPABILITY_SUPPORT", parent_support)

    result = ToolSandbox(registry).execute(
        "_worker_capability_rejection_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.CAPABILITY_UNSUPPORTED
    assert result.exit_code == CAPABILITY_UNSUPPORTED_EXIT_CODE
    assert result.error is not None
    assert "unsupported worker capability requirement" in result.error


def test_handler_exit_five_is_not_misclassified_as_capability_unsupported():
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_exit_five_probe",
            description="Test-only worker exit-code collision probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(),
            handler_entrypoint="tests.sandbox_handlers:exit_with_five",
        )
    )

    result = ToolSandbox(registry).execute(
        "_exit_five_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4


@pytest.mark.parametrize(
    "handler_entrypoint",
    [
        "tests.sandbox_handlers:forge_capability_unsupported",
        "tests.sandbox_handlers:raise_imported_capability_error",
        "tests.sandbox_handlers:forge_capability_prefix_and_exit",
    ],
)
def test_handler_cannot_forge_capability_unsupported_status(handler_entrypoint):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="_forged_capability_rejection_probe",
            description="Test-only capability-status forgery probe.",
            input_model=EnvironmentProbeInput,
            policy=ToolPolicy(),
            handler_entrypoint=handler_entrypoint,
        )
    )

    result = ToolSandbox(registry).execute(
        "_forged_capability_rejection_probe",
        {"keys": ["PATH"]},
    )

    assert result.status == ToolExecutionStatus.FAILED
    assert result.exit_code == 4
    assert result.error is not None


def test_travel_registry_owns_all_phase_5a_tool_schemas_and_handlers():
    registry = build_travel_tool_registry()

    assert {descriptor.name for descriptor in registry.list_tools()} == {
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
    }
    for tool_name in (
        "search_trip_options",
        "rank_trip_options",
        "route_cost_summary",
    ):
        spec = registry.resolve(tool_name)
        assert spec is not None
        assert spec.handler_entrypoint.startswith("domains.travel.tools.handlers:")


def test_search_trip_options_returns_deterministic_costs_for_dynamic_planning():
    sandbox = ToolSandbox(build_travel_tool_registry())

    result = sandbox.execute(
        "search_trip_options",
        {
            "destination": "Tokyo",
            "days": 5,
            "avoid_red_eye": True,
            "hotel_near_subway": True,
            "travel_style": "relaxed",
        },
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result is not None
    assert result.result["source"] == "synthetic_reference_catalog"
    assert result.result["destination"] == "Tokyo"
    assert len(result.result["options"]) == 2
    for option in result.result["options"]:
        assert option["cost"] == (
            option["transport_cost"]
            + option["hotel_cost"]
            + option["activity_cost"]
        )


def test_legacy_registry_builder_and_rank_weight_defaults_remain_compatible():
    sandbox = ToolSandbox(build_default_tool_registry())

    result = sandbox.execute(
        "rank_trip_options",
        {
            "options": [
                {"name": "lower cost", "cost": 100, "duration_hours": 4},
                {"name": "shorter", "cost": 150, "duration_hours": 2},
            ]
        },
    )

    assert result.status == ToolExecutionStatus.COMPLETED
    assert result.result is not None
    assert len(result.result["ranking"]) == 2
