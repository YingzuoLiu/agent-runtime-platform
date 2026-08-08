import pytest

from runtime_service import (
    ApiKeyCredential,
    AuthenticationError,
    StaticApiKeyAuthenticator,
)


def credential(
    credential_id: str = "credential-a",
    api_key: str = "test-key-a",
    tenant_id: str = "tenant-a",
) -> ApiKeyCredential:
    return ApiKeyCredential(
        credential_id=credential_id,
        api_key=api_key,
        tenant_id=tenant_id,
        subject_id="subject-a",
    )


def test_static_api_key_authenticator_builds_typed_tenant_context():
    authenticator = StaticApiKeyAuthenticator([credential()])

    principal = authenticator.authenticate("test-key-a")

    assert principal.credential_id == "credential-a"
    assert principal.authentication_method == "api_key"
    assert principal.tenant_id == "tenant-a"
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
            + '","tenant_id":"tenant-env","subject_id":"subject-env"}]'
        ),
    )

    authenticator = StaticApiKeyAuthenticator.from_environment()
    principal = authenticator.authenticate(secret)

    assert principal.tenant_id == "tenant-env"
    assert secret not in repr(authenticator.__dict__)


def test_environment_authenticator_rejects_invalid_configuration(monkeypatch):
    monkeypatch.setenv("RUNTIME_API_KEYS_JSON", '{"not":"a-list"}')

    with pytest.raises(ValueError, match="must be a JSON array"):
        StaticApiKeyAuthenticator.from_environment()
