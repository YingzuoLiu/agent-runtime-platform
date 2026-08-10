from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from runtime_service.external_actions import (
    AmbiguousExternalActionError,
    DefinitiveExternalActionError,
    ExternalActionDispatcher,
    ExternalActionProviderRegistry,
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.sandbox import ToolRetryMode


def action_request(**updates: Any) -> ExternalActionRequest:
    payload: dict[str, Any] = {
        "action_id": "action-1",
        "run_id": "run-1",
        "step_id": "call-0001",
        "tenant_id": "tenant-1",
        "subject_id": "subject-1",
        "workflow_type": "dynamic-tool-loop:reference:1.0.0",
        "tool_name": "create_record",
        "arguments": {"record_name": "example"},
        "idempotency_key": "action-key-1",
    }
    payload.update(updates)
    return ExternalActionRequest.model_validate(payload)


class RecordingProvider:
    def __init__(
        self,
        *,
        supports_idempotency: bool,
        outcome: ExternalActionProviderResult | BaseException | object,
        provider_identity: str = "provider-account-records",
    ) -> None:
        self.provider_identity = provider_identity
        self.supports_idempotency = supports_idempotency
        self.outcome = outcome
        self.calls: list[ExternalActionRequest] = []

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        self.calls.append(request)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome  # type: ignore[return-value]


def success_result() -> ExternalActionProviderResult:
    return ExternalActionProviderResult(
        provider_reference="provider-record-1",
        result={"status": "created"},
    )


def test_external_action_contracts_are_strict_and_forbid_unknown_fields():
    request = action_request()

    with pytest.raises(ValidationError):
        ExternalActionRequest.model_validate(
            {
                **request.model_dump(mode="python"),
                "run_id": 123,
            }
        )
    with pytest.raises(ValidationError):
        ExternalActionRequest.model_validate(
            {
                **request.model_dump(mode="python"),
                "provider_url": "https://planner-controlled.invalid",
            }
        )
    with pytest.raises(ValidationError):
        ExternalActionProviderResult.model_validate(
            {
                "provider_reference": "provider-record-1",
                "result": {},
                "raw_authorization_header": "secret",
            }
        )


def test_provider_registry_rejects_invalid_and_duplicate_configurations():
    registry = ExternalActionProviderRegistry()
    provider = RecordingProvider(
        supports_idempotency=True,
        outcome=success_result(),
    )

    with pytest.raises(ValueError, match="provider_name"):
        registry.register("   ", provider)

    class MissingCapabilityProvider:
        provider_identity = "provider-account-missing-capability"

        def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
            del request
            return success_result()

    with pytest.raises(ValueError, match="supports_idempotency"):
        registry.register("missing-capability", MissingCapabilityProvider())  # type: ignore[arg-type]

    class MissingIdentityProvider:
        supports_idempotency = True

        def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
            del request
            return success_result()

    with pytest.raises(ValueError, match="provider_identity"):
        registry.register("missing-identity", MissingIdentityProvider())  # type: ignore[arg-type]

    for index, provider_identity in enumerate(("", "   ", "x" * 201)):
        invalid_identity_provider = RecordingProvider(
            supports_idempotency=True,
            outcome=success_result(),
            provider_identity=provider_identity,
        )
        with pytest.raises(ValueError, match="provider_identity"):
            registry.register(
                f"invalid-identity-{index}",
                invalid_identity_provider,
            )

    registry.register("records", provider)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("records", provider)


def test_dispatcher_resolves_server_provider_and_forwards_typed_request():
    request = action_request()
    provider = RecordingProvider(
        supports_idempotency=True,
        outcome=success_result(),
    )
    registry = ExternalActionProviderRegistry()
    registry.register("records", provider)

    result = ExternalActionDispatcher(registry).dispatch(
        provider_name="records",
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        request=request,
    )

    assert result == success_result()
    assert provider.calls == [request]
    assert registry.resolve("records") is provider
    assert provider.provider_identity == "provider-account-records"


def test_provider_idempotent_mode_requires_declared_provider_capability():
    provider = RecordingProvider(
        supports_idempotency=False,
        outcome=success_result(),
    )
    registry = ExternalActionProviderRegistry()
    registry.register("non-idempotent", provider)

    with pytest.raises(DefinitiveExternalActionError) as raised:
        ExternalActionDispatcher(registry).dispatch(
            provider_name="non-idempotent",
            retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
            request=action_request(),
        )

    assert raised.value.code == "external_action_failed"
    assert provider.calls == []


def test_unknown_provider_is_definitive_and_never_dispatches():
    with pytest.raises(DefinitiveExternalActionError) as raised:
        ExternalActionDispatcher(ExternalActionProviderRegistry()).dispatch(
            provider_name="not-configured",
            retry_mode=ToolRetryMode.UNSAFE,
            request=action_request(),
        )

    assert raised.value.code == "external_action_failed"
    assert str(raised.value) == "External action provider definitively failed."


@pytest.mark.parametrize(
    ("provider_error", "expected_type", "expected_code"),
    [
        (
            DefinitiveExternalActionError("upstream rejected card 4111111111111111"),
            DefinitiveExternalActionError,
            "external_action_failed",
        ),
        (
            AmbiguousExternalActionError("timeout with bearer secret-token"),
            AmbiguousExternalActionError,
            "external_action_outcome_unknown",
        ),
    ],
)
def test_typed_provider_errors_preserve_only_stable_safe_classification(
    provider_error,
    expected_type,
    expected_code,
):
    provider = RecordingProvider(
        supports_idempotency=True,
        outcome=provider_error,
    )
    registry = ExternalActionProviderRegistry()
    registry.register("records", provider)

    with pytest.raises(expected_type) as raised:
        ExternalActionDispatcher(registry).dispatch(
            provider_name="records",
            retry_mode=ToolRetryMode.SAFE,
            request=action_request(),
        )

    assert raised.value.code == expected_code
    assert "4111111111111111" not in str(raised.value)
    assert "secret-token" not in str(raised.value)


def test_unclassified_provider_exception_becomes_sanitized_ambiguous_outcome():
    provider = RecordingProvider(
        supports_idempotency=False,
        outcome=RuntimeError("response included secret-token"),
    )
    registry = ExternalActionProviderRegistry()
    registry.register("records", provider)

    with pytest.raises(AmbiguousExternalActionError) as raised:
        ExternalActionDispatcher(registry).dispatch(
            provider_name="records",
            retry_mode=ToolRetryMode.UNSAFE,
            request=action_request(),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert str(raised.value) == "External action provider outcome is unknown."
    assert provider.calls == [action_request()]


def test_invalid_provider_result_is_ambiguous_after_dispatch():
    provider = RecordingProvider(
        supports_idempotency=True,
        outcome={"result": {"status": "created"}},
    )
    registry = ExternalActionProviderRegistry()
    registry.register("records", provider)

    with pytest.raises(AmbiguousExternalActionError) as raised:
        ExternalActionDispatcher(registry).dispatch(
            provider_name="records",
            retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
            request=action_request(),
        )

    assert raised.value.code == "external_action_outcome_unknown"
    assert provider.calls == [action_request()]
