from __future__ import annotations

from typing import Any, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .sandbox import ToolRetryMode


class ExternalActionRequest(BaseModel):
    """One immutable, provider-facing external action invocation.

    Provider credentials and endpoints are deliberately absent. The dispatcher
    resolves those from its server-controlled provider registry; a Planner can
    supply only arguments that already passed the registered tool schema.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    action_id: str = Field(min_length=1, max_length=200)
    run_id: str = Field(min_length=1, max_length=200)
    step_id: str = Field(min_length=1, max_length=200)
    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)
    workflow_type: str = Field(min_length=1, max_length=500)
    tool_name: str = Field(min_length=1, max_length=200)
    arguments: dict[str, Any]
    idempotency_key: str = Field(min_length=1, max_length=255)


class ExternalActionProviderResult(BaseModel):
    """Sanitized successful result returned by a registered provider."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    provider_reference: str = Field(min_length=1, max_length=500)
    result: dict[str, Any]


class ExternalActionProviderError(RuntimeError):
    """Base for safe, stable provider failures exposed to the runtime."""

    code: ClassVar[str]
    safe_message: ClassVar[str]

    def __init__(self, _unsafe_detail: str | None = None) -> None:
        # Provider errors can contain credentials, request bodies, or upstream
        # responses. Keep those out of exception text that may become durable
        # run evidence. Adapters may pass a detail for local debugging, but this
        # boundary deliberately does not retain or chain it.
        del _unsafe_detail
        super().__init__(self.safe_message)


class DefinitiveExternalActionError(ExternalActionProviderError):
    """The action is known not to have completed and may be treated as failed."""

    code = "external_action_failed"
    safe_message = "External action provider definitively failed."


class AmbiguousExternalActionError(ExternalActionProviderError):
    """The provider may have completed the action; blind retry is unsafe."""

    code = "external_action_outcome_unknown"
    safe_message = "External action provider outcome is unknown."


class ExternalActionProvider(Protocol):
    """Server-configured adapter for one external system boundary."""

    supports_idempotency: bool

    @property
    def provider_identity(self) -> str:
        ...

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        ...


class ExternalActionProviderRegistry:
    """Explicit allowlist of server-owned external action providers."""

    def __init__(self) -> None:
        self._providers: dict[str, ExternalActionProvider] = {}

    def register(self, provider_name: str, provider: ExternalActionProvider) -> None:
        if not provider_name or not provider_name.strip():
            raise ValueError("provider_name must not be empty")
        if len(provider_name) > 200:
            raise ValueError("provider_name must contain at most 200 characters")
        if provider_name in self._providers:
            raise ValueError(f"External action provider already registered: {provider_name}")
        provider_identity = getattr(provider, "provider_identity", None)
        if (
            not isinstance(provider_identity, str)
            or not provider_identity
            or len(provider_identity) > 200
            or provider_identity != provider_identity.strip()
        ):
            raise ValueError(
                "external action provider must declare provider_identity "
                "as a non-empty string of at most 200 characters"
            )
        if not isinstance(getattr(provider, "supports_idempotency", None), bool):
            raise ValueError("external action provider must declare supports_idempotency as bool")
        if not callable(getattr(provider, "execute", None)):
            raise ValueError("external action provider must define execute(request)")
        self._providers[provider_name] = provider

    def resolve(self, provider_name: str) -> ExternalActionProvider | None:
        return self._providers.get(provider_name)


class ExternalActionDispatcher:
    """Resolve and invoke providers without trusting Planner-selected routing."""

    def __init__(self, registry: ExternalActionProviderRegistry) -> None:
        self.registry = registry

    def dispatch(
        self,
        *,
        provider_name: str,
        retry_mode: ToolRetryMode,
        request: ExternalActionRequest,
    ) -> ExternalActionProviderResult:
        provider = self.registry.resolve(provider_name)
        if provider is None:
            # Nothing was dispatched, so this is a definitive failure rather
            # than an uncertain external outcome.
            raise DefinitiveExternalActionError()
        if (
            retry_mode == ToolRetryMode.PROVIDER_IDEMPOTENT
            and not provider.supports_idempotency
        ):
            # The registered Tool contract promised a capability the provider
            # cannot supply. Fail before calling it.
            raise DefinitiveExternalActionError()

        try:
            raw_result = provider.execute(request)
        except (DefinitiveExternalActionError, AmbiguousExternalActionError):
            raise
        except Exception:
            # Once provider code has been entered, an unclassified exception
            # cannot prove that the external action did not happen.
            raise AmbiguousExternalActionError() from None

        try:
            return ExternalActionProviderResult.model_validate(raw_result)
        except (TypeError, ValidationError):
            # An invalid response arrived after provider dispatch. It cannot be
            # downgraded to a definitive failure without risking a duplicate.
            raise AmbiguousExternalActionError() from None
