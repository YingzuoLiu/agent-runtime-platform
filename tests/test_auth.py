import traceback

import pytest

from runtime_service import (
    ApiKeyCredential,
    AuthenticationError,
    AuthorizationError,
    RoleAuthorizer,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)


def credential(
    credential_id: str = "credential-a",
    api_key: str = "test-key-a",
    tenant_id: str = "tenant-a",
    role: RuntimeRole = RuntimeRole.OPERATOR,
) -> ApiKeyCredential:
    return ApiKeyCredential(
        credential_id=credential_id,
        api_key=api_key,
        tenant_id=tenant_id,
        subject_id="subject-a",
        role=role,
    )


def test_static_api_key_authenticator_builds_typed_tenant_context():
    authenticator = StaticApiKeyAuthenticator([credential()])

    principal = authenticator.authenticate("test-key-a")

    assert principal.credential_id == "credential-a"
    assert principal.authentication_method == "api_key"
    assert principal.tenant_id == "tenant-a"
    assert principal.role == RuntimeRole.OPERATOR
    assert principal.tenant_context.tenant_id == "tenant-a"
    assert principal.tenant_context.subject_id == "subject-a"


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
def test_static_api_key_authenticator_rejects_missing_or_invalid_key(api_key):
    authenticator = StaticApiKeyAuthenticator([credential()])

    with pytest.raises(AuthenticationError, match="Invalid or missing API key"):
        authenticator.authenticate(api_key)


def test_static_api_key_authenticator_rejects_duplicate_identity_or_secret():
    with pytest.raises(ValueError, match="Duplicate API credential id"):
        StaticApiKeyAuthenticator(
            [
                credential(),
                credential(api_key="different-key", tenant_id="tenant-b"),
            ]
        )

    with pytest.raises(ValueError, match="Duplicate API key material"):
        StaticApiKeyAuthenticator(
            [
                credential(),
                credential(
                    credential_id="credential-b",
                    tenant_id="tenant-b",
                ),
            ]
        )


def test_environment_authenticator_is_fail_closed_when_unconfigured(monkeypatch):
    monkeypatch.delenv("RUNTIME_API_KEYS_JSON", raising=False)

    authenticator = StaticApiKeyAuthenticator.from_environment()

    with pytest.raises(AuthenticationError):
        authenticator.authenticate("any-key")


def test_environment_authenticator_loads_json_without_exposing_secret(monkeypatch):
    secret = "environment-secret-key"
    monkeypatch.setenv(
        "RUNTIME_API_KEYS_JSON",
        (
            '[{"credential_id":"env-a","api_key":"'
            + secret
            + '","tenant_id":"tenant-env","subject_id":"subject-env","role":"viewer"}]'
        ),
    )

    authenticator = StaticApiKeyAuthenticator.from_environment()
    principal = authenticator.authenticate(secret)

    assert principal.tenant_id == "tenant-env"
    assert principal.role == RuntimeRole.VIEWER
    assert secret not in repr(authenticator.__dict__)


def test_environment_authenticator_rejects_invalid_configuration(monkeypatch):
    monkeypatch.setenv("RUNTIME_API_KEYS_JSON", '{"not":"a-list"}')

    with pytest.raises(ValueError, match="must be a JSON array"):
        StaticApiKeyAuthenticator.from_environment()


def test_environment_authenticator_does_not_expose_secret_on_validation_failure(
    monkeypatch,
):
    secret = "sk-live-VALIDATION-FAILURE-SENTINEL"
    monkeypatch.setenv(
        "RUNTIME_API_KEYS_JSON",
        (
            '[{"credential_id":"env-a","apikey":"'
            + secret
            + '","tenant_id":"tenant-env","subject_id":"subject-env","role":"operator"}]'
        ),
    )

    with pytest.raises(ValueError, match="must be a JSON array") as caught:
        StaticApiKeyAuthenticator.from_environment()

    rendered_traceback = "".join(
        traceback.format_exception(
            type(caught.value),
            caught.value,
            caught.value.__traceback__,
        )
    )
    assert caught.value.__context__ is None
    assert secret not in str(caught.value)
    assert secret not in repr(caught.value)
    assert secret not in rendered_traceback


@pytest.mark.parametrize(
    "role_fragment",
    ["", ',"role":"administrator"'],
)
def test_environment_authenticator_rejects_missing_or_unknown_role_without_secret(
    monkeypatch,
    role_fragment,
):
    secret = "sk-live-ROLE-VALIDATION-SENTINEL"
    monkeypatch.setenv(
        "RUNTIME_API_KEYS_JSON",
        (
            '[{"credential_id":"env-a","api_key":"'
            + secret
            + '","tenant_id":"tenant-env","subject_id":"subject-env"'
            + role_fragment
            + "}]"
        ),
    )

    with pytest.raises(ValueError, match="must be a JSON array") as caught:
        StaticApiKeyAuthenticator.from_environment()

    rendered_traceback = "".join(
        traceback.format_exception(
            type(caught.value),
            caught.value,
            caught.value.__traceback__,
        )
    )
    assert caught.value.__context__ is None
    assert secret not in str(caught.value)
    assert secret not in repr(caught.value)
    assert secret not in rendered_traceback


def test_static_api_key_authenticator_returns_after_first_digest_match(monkeypatch):
    authenticator = StaticApiKeyAuthenticator(
        [
            credential(),
            credential(
                credential_id="credential-b",
                api_key="test-key-b",
                tenant_id="tenant-b",
            ),
        ]
    )
    compare_calls = 0

    def counting_compare_digest(candidate: bytes, digest: bytes) -> bool:
        nonlocal compare_calls
        compare_calls += 1
        return candidate == digest

    monkeypatch.setattr("runtime_service.auth.hmac.compare_digest", counting_compare_digest)

    principal = authenticator.authenticate("test-key-a")

    assert principal.credential_id == "credential-a"
    assert compare_calls == 1


@pytest.mark.parametrize(
    "permission",
    [
        RuntimePermission.AGENTS_READ,
        RuntimePermission.TOOLS_READ,
        RuntimePermission.RUNS_READ,
        RuntimePermission.RUN_EVENTS_READ,
        RuntimePermission.THREAD_STATE_READ,
        RuntimePermission.MEMORY_READ,
    ],
)
def test_viewer_role_allows_only_read_permissions(permission):
    principal = StaticApiKeyAuthenticator(
        [credential(role=RuntimeRole.VIEWER)]
    ).authenticate("test-key-a")

    RoleAuthorizer().authorize(principal, permission)


@pytest.mark.parametrize(
    "permission",
    [
        RuntimePermission.TOOLS_EXECUTE,
        RuntimePermission.AGENT_MESSAGE_EXECUTE,
        RuntimePermission.RUNS_CREATE,
        RuntimePermission.RUNS_CANCEL,
        RuntimePermission.MEMORY_WRITE,
        RuntimePermission.MEMORY_DELETE,
    ],
)
def test_viewer_role_denies_mutating_permissions_by_default(permission):
    principal = StaticApiKeyAuthenticator(
        [credential(role=RuntimeRole.VIEWER)]
    ).authenticate("test-key-a")

    with pytest.raises(AuthorizationError, match="Operation not permitted"):
        RoleAuthorizer().authorize(principal, permission)


def test_operator_role_allows_every_declared_permission():
    principal = StaticApiKeyAuthenticator([credential()]).authenticate("test-key-a")
    authorizer = RoleAuthorizer()

    for permission in RuntimePermission:
        authorizer.authorize(principal, permission)
