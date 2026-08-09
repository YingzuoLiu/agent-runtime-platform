from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Iterable
from enum import Enum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter, ValidationError

from agent.contracts import RuntimeExecutionAuthority


class AuthenticationError(ValueError):
    """The supplied credential did not authenticate a principal."""


class AuthorizationError(PermissionError):
    """The authenticated principal is not allowed to perform an operation."""


class RuntimeRole(str, Enum):
    VIEWER = "viewer"
    OPERATOR = "operator"


class RuntimePermission(str, Enum):
    AGENTS_READ = "agents:read"
    TOOLS_READ = "tools:read"
    TOOLS_EXECUTE = "tools:execute"
    AGENT_MESSAGE_EXECUTE = "agent-message:execute"
    RUNS_CREATE = "runs:create"
    RUNS_READ = "runs:read"
    RUNS_CANCEL = "runs:cancel"
    RUN_EVENTS_READ = "run-events:read"
    THREAD_STATE_READ = "thread-state:read"
    MEMORY_READ = "memory:read"
    MEMORY_WRITE = "memory:write"
    MEMORY_DELETE = "memory:delete"


class Authenticator(Protocol):
    def authenticate(self, api_key: str | None) -> "Principal":
        ...


class Authorizer(Protocol):
    def authorize(self, principal: "Principal", permission: RuntimePermission) -> None:
        ...


class TenantContext(BaseModel):
    """Trusted tenant identity passed from authentication into the runtime manager."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str = Field(min_length=1, max_length=200)
    subject_id: str = Field(min_length=1, max_length=200)
    permissions: tuple[str, ...] = ()

    @property
    def execution_authority(self) -> RuntimeExecutionAuthority:
        return RuntimeExecutionAuthority(
            tenant_id=self.tenant_id,
            subject_id=self.subject_id,
            permissions=self.permissions,
        )


class Principal(BaseModel):
    """Authenticated caller identity and trusted configured runtime role."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject_id: str = Field(min_length=1, max_length=200)
    tenant_id: str = Field(min_length=1, max_length=200)
    credential_id: str = Field(min_length=1, max_length=200)
    role: RuntimeRole
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
    role: RuntimeRole


class RoleAuthorizer:
    """Small default-deny policy for the local Viewer/Operator roles."""

    _VIEWER_PERMISSIONS = frozenset(
        {
            RuntimePermission.AGENTS_READ,
            RuntimePermission.TOOLS_READ,
            RuntimePermission.RUNS_READ,
            RuntimePermission.RUN_EVENTS_READ,
            RuntimePermission.THREAD_STATE_READ,
            RuntimePermission.MEMORY_READ,
        }
    )
    _OPERATOR_PERMISSIONS = _VIEWER_PERMISSIONS | frozenset(
        {
            RuntimePermission.TOOLS_EXECUTE,
            RuntimePermission.AGENT_MESSAGE_EXECUTE,
            RuntimePermission.RUNS_CREATE,
            RuntimePermission.RUNS_CANCEL,
            RuntimePermission.MEMORY_WRITE,
            RuntimePermission.MEMORY_DELETE,
        }
    )
    _PERMISSIONS_BY_ROLE = {
        RuntimeRole.VIEWER: _VIEWER_PERMISSIONS,
        RuntimeRole.OPERATOR: _OPERATOR_PERMISSIONS,
    }

    def authorize(self, principal: Principal, permission: RuntimePermission) -> None:
        allowed_permissions = self._PERMISSIONS_BY_ROLE.get(principal.role, frozenset())
        if permission not in allowed_permissions:
            raise AuthorizationError("Operation not permitted")


def effective_execution_authority(
    principal: Principal,
    authorizer: Authorizer,
) -> RuntimeExecutionAuthority:
    """Snapshot server-evaluated permissions for asynchronous execution."""

    allowed: list[str] = []
    for permission in RuntimePermission:
        try:
            authorizer.authorize(principal, permission)
        except AuthorizationError:
            continue
        allowed.append(permission.value)
    return RuntimeExecutionAuthority(
        tenant_id=principal.tenant_id,
        subject_id=principal.subject_id,
        permissions=tuple(allowed),
    )


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
                role=credential.role,
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
        except ValidationError:
            credentials = None
        if credentials is None:
            raise ValueError(
                f"{variable_name} must be a JSON array of API-key credential records"
            )
        return cls(credentials)

    def authenticate(self, api_key: str | None) -> Principal:
        if api_key is None or not api_key:
            raise AuthenticationError("Invalid or missing API key")
        candidate = self._digest(api_key)
        for digest, principal in self._principals_by_digest.items():
            if hmac.compare_digest(candidate, digest):
                return principal
        raise AuthenticationError("Invalid or missing API key")

    @staticmethod
    def _digest(api_key: str) -> bytes:
        return hashlib.sha256(api_key.encode("utf-8")).digest()
