from __future__ import annotations

import json
from collections.abc import Collection
from typing import Any

import httpx
from pydantic import ValidationError

from .external_actions import (
    AmbiguousExternalActionError,
    DefinitiveExternalActionError,
    ExternalActionProviderResult,
    ExternalActionRequest,
)


_INSECURE_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class HttpExternalActionProvider:
    """Server-configured synchronous JSON-over-HTTP action adapter.

    Provider routing and credentials are constructor-only configuration. The
    Planner-controlled request supplies neither, and failures are collapsed to
    the stable external-action errors without retaining endpoint, header,
    token, transport, or upstream-response details.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        provider_identity: str,
        bearer_token: str | None = None,
        allow_insecure_localhost: bool = False,
        supports_idempotency: bool = False,
        definitive_status_codes: Collection[int] = (),
        timeout_seconds: float = 5.0,
        max_response_bytes: int = 65_536,
    ) -> None:
        try:
            parsed_endpoint = httpx.URL(endpoint)
        except (TypeError, ValueError):
            raise ValueError("endpoint must be an absolute HTTP(S) URL") from None
        if (
            parsed_endpoint.scheme not in {"http", "https"}
            or not parsed_endpoint.host
            or parsed_endpoint.userinfo
        ):
            raise ValueError("endpoint must be an absolute HTTP(S) URL without credentials")
        if bearer_token is not None and not bearer_token:
            raise ValueError("bearer_token must be non-empty when configured")
        if not isinstance(allow_insecure_localhost, bool):
            raise ValueError("allow_insecure_localhost must be a bool")
        if parsed_endpoint.scheme == "http":
            insecure_loopback_is_allowed = (
                allow_insecure_localhost
                and parsed_endpoint.host.lower() in _INSECURE_LOOPBACK_HOSTS
            )
            if not insecure_loopback_is_allowed:
                raise ValueError(
                    "HTTP providers require an explicitly enabled loopback "
                    "development endpoint"
                )
        if (
            not isinstance(provider_identity, str)
            or not provider_identity
            or len(provider_identity) > 200
            or provider_identity != provider_identity.strip()
        ):
            raise ValueError(
                "provider_identity must be a non-empty string of at most 200 characters"
            )
        if not isinstance(supports_idempotency, bool):
            raise ValueError("supports_idempotency must be a bool")
        if isinstance(definitive_status_codes, (str, bytes)) or not isinstance(
            definitive_status_codes,
            Collection,
        ):
            raise ValueError("definitive_status_codes must be a collection of 4xx codes")
        if any(
            isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or not 400 <= status_code < 500
            for status_code in definitive_status_codes
        ):
            raise ValueError("definitive_status_codes must contain only 4xx codes")
        if not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds must be greater than zero and at most 60")
        if not 256 <= max_response_bytes <= 1_048_576:
            raise ValueError("max_response_bytes must be between 256 and 1048576 bytes")

        self._endpoint = parsed_endpoint
        self._provider_identity = provider_identity
        self._bearer_token = bearer_token
        # Sending an Idempotency-Key does not prove that an arbitrary endpoint
        # honors it. Capability must be asserted by server configuration before
        # a provider-idempotent ToolSpec is allowed to dispatch or retry.
        self.supports_idempotency = supports_idempotency
        self._definitive_status_codes = frozenset(definitive_status_codes)
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes

    @property
    def provider_identity(self) -> str:
        return self._provider_identity

    def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Idempotency-Key": request.idempotency_key,
        }
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"

        try:
            with httpx.Client(
                timeout=httpx.Timeout(self._timeout_seconds),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                with client.stream(
                    "POST",
                    self._endpoint,
                    headers=headers,
                    json=request.model_dump(mode="json"),
                ) as response:
                    status_code = response.status_code
                    if status_code in {200, 201}:
                        payload = self._read_bounded_response(response)
                    elif status_code in self._definitive_status_codes:
                        raise DefinitiveExternalActionError()
                    else:
                        # Redirects are deliberately not followed. A redirect,
                        # retry-later response, server failure, or unexpected
                        # status cannot prove that the provider did not apply
                        # the action.
                        raise AmbiguousExternalActionError()
        except (DefinitiveExternalActionError, AmbiguousExternalActionError):
            raise
        except httpx.HTTPError:
            raise AmbiguousExternalActionError() from None
        except (TypeError, ValueError):
            # JSON serialization and local protocol validation happen before a
            # valid provider response exists. The dispatcher treats any
            # unclassified provider exception as ambiguous, but returning the
            # stable error here ensures no library detail can escape even when
            # this adapter is called directly.
            raise AmbiguousExternalActionError() from None

        result = self._decode_success(payload)
        if self._bearer_token is not None and self._contains_string(
            (result.provider_reference, result.result),
            self._bearer_token,
        ):
            # A schema-valid upstream response can still reflect credentials
            # into fields that the runtime would otherwise persist as durable
            # action evidence. Collapse it to the same safe ambiguous outcome
            # without retaining or rendering the credential-bearing payload.
            raise AmbiguousExternalActionError()
        return result

    @staticmethod
    def _contains_string(value: Any, needle: str) -> bool:
        pending = [value]
        while pending:
            current = pending.pop()
            if isinstance(current, str):
                if needle in current:
                    return True
            elif isinstance(current, dict):
                pending.extend(current.keys())
                pending.extend(current.values())
            elif isinstance(current, (list, tuple)):
                pending.extend(current)
        return False

    def _read_bounded_response(self, response: httpx.Response) -> bytes:
        media_type = response.headers.get("Content-Type", "").partition(";")[0].strip().lower()
        if media_type != "application/json":
            raise AmbiguousExternalActionError()

        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                raise AmbiguousExternalActionError() from None
            if declared_length < 0 or declared_length > self._max_response_bytes:
                raise AmbiguousExternalActionError()

        payload = bytearray()
        try:
            for chunk in response.iter_bytes(chunk_size=8_192):
                if len(payload) + len(chunk) > self._max_response_bytes:
                    raise AmbiguousExternalActionError()
                payload.extend(chunk)
        except AmbiguousExternalActionError:
            raise
        except httpx.HTTPError:
            raise AmbiguousExternalActionError() from None
        return bytes(payload)

    @staticmethod
    def _decode_success(payload: bytes) -> ExternalActionProviderResult:
        try:
            decoded: Any = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise AmbiguousExternalActionError() from None
        if not isinstance(decoded, dict):
            raise AmbiguousExternalActionError()
        try:
            return ExternalActionProviderResult.model_validate(decoded)
        except ValidationError:
            raise AmbiguousExternalActionError() from None
