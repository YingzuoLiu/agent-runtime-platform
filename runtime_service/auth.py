from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Iterable
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter, ValidationError


class AuthenticationError(ValueError):
    """The supplied credential did not authenticate a principal."""


class Authenticator(Protocol):
    def authenticate(self, api_key: str | None) -> "Principal":
        ...


class TenantContext(BaseModel):
    """Trusted tenant identity passed from authentication into the runtime manager."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)


class Principal(BaseModel):
    """Authenticated caller identity with no authorization policy attached yet."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject_id: str = Field(min_length=1, max_length=200)
    tenant_id: str = Field(min_length=1, max_length=200)
    credential_id: str = Field(min_length=1, max_length=200)
    authentication_method: Literal["api_key"] = "api_key"

    @property
    def tenant_context(self) -> TenantContext:
        return TenantContext(tenant_id=self.tenant_id, subject_id=self.subject_id)


class ApiKeyCredential(BaseModel):
    """Local API-key configuration; the plaintext secret is never retained after loading."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    credential_id: str = Field(min_length=1, max_length=200)
    api_key: SecretStr = Field(min_length=1)
    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)


class StaticApiKeyAuthenticator:
    """Offline-first API-key authenticator backed by SHA-256 credential digests."""

    def __init__(self, credentials: Iterable[ApiKeyCredential] = ()) -> None:
        self._principals_by_digest: dict[bytes, Principal] = {}
        credential_ids: set[str] = set()
        for credential in credentials:
            if credential.credential_id in credential_ids:
                raise ValueError(
                    f"Duplicate API credential id: {credential.credential_id}"
                )
            credential_ids.add(credential.credential_id)
            digest = self._digest(credential.api_key.get_secret_value())
            if digest in self._principals_by_digest:
                raise ValueError("Duplicate API key material")
            self._principals_by_digest[digest] = Principal(
                subject_id=credential.subject_id,
                tenant_id=credential.tenant_id,
                credential_id=credential.credential_id,
            )

    @classmethod
    def from_environment(
        cls,
        variable_name: str = "RUNTIME_API_KEYS_JSON",
    ) -> "StaticApiKeyAuthenticator":
        raw_value = os.getenv(variable_name)
        if raw_value is None or not raw_value.strip():
            return cls()
        try:
            credentials = TypeAdapter(list[ApiKeyCredential]).validate_json(raw_value)
        except ValidationError as exc:
            raise ValueError(
                f"{variable_name} must be a JSON array of API-key credential records"
            ) from exc
        return cls(credentials)

    def authenticate(self, api_key: str | None) -> Principal:
        if api_key is None or not api_key:
            raise AuthenticationError("Invalid or missing API key")
        candidate = self._digest(api_key)
        matched: Principal | None = None
        for digest, principal in self._principals_by_digest.items():
            if hmac.compare_digest(candidate, digest):
                matched = principal
        if matched is None:
            raise AuthenticationError("Invalid or missing API key")
        return matched

    @staticmethod
    def _digest(api_key: str) -> bytes:
        return hashlib.sha256(api_key.encode("utf-8")).digest()
