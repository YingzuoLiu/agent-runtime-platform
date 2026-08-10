from __future__ import annotations

from contextlib import contextmanager
import json
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
from typing import Iterator

import pytest

from runtime_service.external_actions import (
    AmbiguousExternalActionError,
    DefinitiveExternalActionError,
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.http_external_action import HttpExternalActionProvider


PROVIDER_IDENTITY = "provider-account-records-test"


class _ResponseState:
    def __init__(
        self,
        *,
        status: int,
        body: bytes,
        delay_seconds: float = 0,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.body = body
        self.delay_seconds = delay_seconds
        self.extra_headers = extra_headers or {}
        self.requests: list[dict] = []
        self.lock = threading.Lock()


class _ActionHttpServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, state: _ResponseState) -> None:
        super().__init__(("127.0.0.1", 0), _ActionHandler)
        self.state = state


class _ActionHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        server = self.server
        assert isinstance(server, _ActionHttpServer)
        state = server.state
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length)
        with state.lock:
            state.requests.append(
                {
                    "path": self.path,
                    "headers": dict(self.headers.items()),
                    "body": request_body,
                }
            )

        if state.delay_seconds:
            time.sleep(state.delay_seconds)
        self.send_response(state.status)
        configured_headers = {name.lower() for name in state.extra_headers}
        if "content-type" not in configured_headers:
            self.send_header("Content-Type", "application/json")
        if "content-length" not in configured_headers:
            self.send_header("Content-Length", str(len(state.body)))
        for name, value in state.extra_headers.items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(state.body)
        except (BrokenPipeError, ConnectionResetError):
            # Expected when the timeout test closes the client connection.
            return

    def log_message(self, _format: str, *args) -> None:
        del args


@contextmanager
def _http_server(
    *,
    status: int = 200,
    body: bytes | None = None,
    delay_seconds: float = 0,
    extra_headers: dict[str, str] | None = None,
) -> Iterator[tuple[str, _ResponseState]]:
    response_body = (
        json.dumps(
            {
                "provider_reference": "provider-ref-1",
                "result": {"created": True, "record_id": "record-1"},
            }
        ).encode("utf-8")
        if body is None
        else body
    )
    state = _ResponseState(
        status=status,
        body=response_body,
        delay_seconds=delay_seconds,
        extra_headers=extra_headers,
    )
    server = _ActionHttpServer(state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}/v1/actions", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _request() -> ExternalActionRequest:
    return ExternalActionRequest(
        action_id="action-1",
        run_id="run-1",
        step_id="call-0001",
        tenant_id="tenant-a",
        subject_id="subject-a",
        workflow_type="generic-external-action:1.0.0",
        tool_name="create_record",
        arguments={"label": "test", "value": 1},
        idempotency_key="idem-run-1-call-0001",
    )


def _assert_sanitized_error(
    error: BaseException,
    *,
    endpoint: str,
    token: str,
) -> None:
    rendered = str(error)
    assert endpoint not in rendered
    assert token not in rendered
    assert "Authorization" not in rendered
    assert "Idempotency-Key" not in rendered


def test_real_http_success_sends_normalized_body_idempotency_and_private_auth():
    token = "server-only-bearer-secret"
    with _http_server() as (endpoint, state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
            supports_idempotency=True,
            timeout_seconds=1,
        )
        request = _request()

        result = provider.execute(request)

    assert provider.supports_idempotency is True
    assert provider.provider_identity == PROVIDER_IDENTITY
    assert isinstance(result, ExternalActionProviderResult)
    assert result.provider_reference == "provider-ref-1"
    assert result.result == {"created": True, "record_id": "record-1"}
    assert len(state.requests) == 1
    captured = state.requests[0]
    assert captured["path"] == "/v1/actions"
    assert captured["headers"]["Idempotency-Key"] == request.idempotency_key
    assert captured["headers"]["Authorization"] == f"Bearer {token}"
    decoded_body = json.loads(captured["body"])
    assert decoded_body == request.model_dump(mode="json")
    assert token not in captured["body"].decode("utf-8")
    assert endpoint not in captured["body"].decode("utf-8")
    assert token not in repr(provider)
    assert token not in result.model_dump_json()


@pytest.mark.parametrize(
    "leak_location",
    ["provider_reference", "result_key", "nested_result_value"],
)
def test_schema_valid_success_reflecting_bearer_is_ambiguous_and_sanitized(
    leak_location,
):
    token = "reflected-provider-bearer-secret"
    response = {
        "provider_reference": "provider-ref-safe",
        "result": {"status": "created"},
    }
    if leak_location == "provider_reference":
        response["provider_reference"] = f"provider-ref-{token}"
    elif leak_location == "result_key":
        response["result"] = {f"private-{token}-key": "created"}
    else:
        response["result"] = {
            "metadata": [
                {"allowed_field": f"reflected:{token}:value"},
            ]
        }

    with _http_server(body=json.dumps(response).encode("utf-8")) as (
        endpoint,
        _state,
    ):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )

        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    assert raised.value.code == "external_action_outcome_unknown"
    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_201_is_the_only_additional_synchronous_success_status():
    with _http_server(status=201) as (endpoint, _state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            allow_insecure_localhost=True,
        )

        result = provider.execute(_request())

    assert result.provider_reference == "provider-ref-1"


@pytest.mark.parametrize("status_code", [202, 204, 206, 207])
def test_other_2xx_statuses_are_ambiguous_even_with_valid_json(status_code):
    with _http_server(status=status_code) as (endpoint, _state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            allow_insecure_localhost=True,
        )

        with pytest.raises(AmbiguousExternalActionError):
            provider.execute(_request())


def test_optional_auth_header_is_absent_when_no_token_is_configured():
    with _http_server() as (endpoint, state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            allow_insecure_localhost=True,
        )
        provider.execute(_request())

    assert len(state.requests) == 1
    assert "Authorization" not in state.requests[0]["headers"]


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:8080/v1/actions",
        "http://[::1]:8080/v1/actions",
        "http://localhost:8080/v1/actions",
    ],
)
def test_loopback_bearer_requires_explicit_insecure_development_opt_in(endpoint):
    token = "loopback-secret-token"

    with pytest.raises(ValueError) as raised:
        HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
        )

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_loopback_without_bearer_still_requires_explicit_insecure_opt_in():
    endpoint = "http://127.0.0.1:8080/v1/actions"

    with pytest.raises(ValueError) as raised:
        HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
        )

    assert endpoint not in str(raised.value)


def test_insecure_development_opt_in_never_allows_remote_plaintext_bearer():
    endpoint = "http://provider.example.test/v1/actions"
    token = "remote-plaintext-secret"

    with pytest.raises(ValueError) as raised:
        HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_insecure_opt_in_never_allows_remote_plaintext_without_bearer():
    endpoint = "http://provider.example.test/v1/actions"

    with pytest.raises(ValueError) as raised:
        HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            allow_insecure_localhost=True,
        )

    assert endpoint not in str(raised.value)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:8080/v1/actions",
        "http://[::1]:8080/v1/actions",
        "http://localhost:8080/v1/actions",
    ],
)
def test_explicit_insecure_development_opt_in_accepts_only_loopback(endpoint):
    provider = HttpExternalActionProvider(
        endpoint=endpoint,
        provider_identity=PROVIDER_IDENTITY,
        bearer_token="loopback-development-token",
        allow_insecure_localhost=True,
    )

    assert provider.provider_identity == PROVIDER_IDENTITY


def test_idempotency_capability_is_fail_closed_until_explicitly_asserted():
    provider = HttpExternalActionProvider(
        endpoint="https://provider.example.test/v1/actions",
        provider_identity=PROVIDER_IDENTITY,
    )

    assert provider.supports_idempotency is False


def test_provider_identity_is_explicit_and_independent_of_credentials():
    endpoint = "https://provider.example.test/v1/actions"
    first = HttpExternalActionProvider(
        endpoint=endpoint,
        provider_identity=PROVIDER_IDENTITY,
        bearer_token="first-token",
    )
    rotated = HttpExternalActionProvider(
        endpoint=endpoint,
        provider_identity=PROVIDER_IDENTITY,
        bearer_token="rotated-token",
    )
    other_account = HttpExternalActionProvider(
        endpoint=endpoint,
        provider_identity="provider-account-records-other",
        bearer_token="first-token",
    )

    assert first.provider_identity == rotated.provider_identity
    assert first.provider_identity != other_account.provider_identity
    with pytest.raises(TypeError):
        HttpExternalActionProvider(endpoint=endpoint)  # type: ignore[call-arg]


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 405, 413, 415, 422])
def test_only_explicitly_configured_provider_4xx_is_definitive(status_code):
    token = "definitive-secret"
    with _http_server(status=status_code, body=b'{"error":"private"}') as (
        endpoint,
        _state,
    ):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
            definitive_status_codes={status_code},
        )
        with pytest.raises(DefinitiveExternalActionError) as raised:
            provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)
    assert str(raised.value) == "External action provider definitively failed."


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 405, 413, 415, 418, 422])
def test_unconfigured_4xx_is_ambiguous(status_code):
    with _http_server(status=status_code, body=b'{"error":"private"}') as (
        endpoint,
        _state,
    ):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            allow_insecure_localhost=True,
        )

        with pytest.raises(AmbiguousExternalActionError):
            provider.execute(_request())


@pytest.mark.parametrize("status_code", [408, 409, 425, 429, 500, 503])
def test_retry_later_and_server_statuses_are_ambiguous(status_code):
    token = "ambiguous-status-secret"
    with _http_server(status=status_code, body=b'{"error":"private"}') as (
        endpoint,
        _state,
    ):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_timeout_after_actual_dispatch_is_ambiguous_and_sanitized():
    token = "timeout-secret"
    with _http_server(delay_seconds=0.2) as (endpoint, state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
            timeout_seconds=0.05,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    assert len(state.requests) == 1
    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_network_failure_is_ambiguous_and_sanitized():
    token = "network-secret"
    socket_value = socket.socket()
    socket_value.bind(("127.0.0.1", 0))
    host, port = socket_value.getsockname()
    socket_value.close()
    endpoint = f"http://{host}:{port}/v1/actions"
    provider = HttpExternalActionProvider(
        endpoint=endpoint,
        provider_identity=PROVIDER_IDENTITY,
        bearer_token=token,
        allow_insecure_localhost=True,
        timeout_seconds=0.2,
    )

    with pytest.raises(AmbiguousExternalActionError) as raised:
        provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        b"[]",
        b'{"provider_reference":"ref-only"}',
        b'{"provider_reference":"ref","result":{},"extra":true}',
        b'{"provider_reference":123,"result":{}}',
    ],
)
def test_invalid_success_payload_is_ambiguous(body):
    token = "invalid-response-secret"
    with _http_server(body=body) as (endpoint, _state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_success_with_non_json_media_type_is_ambiguous():
    token = "invalid-media-type-secret"
    valid_json = b'{"provider_reference":"ref","result":{}}'
    with _http_server(
        body=valid_json,
        extra_headers={"Content-Type": "text/plain"},
    ) as (endpoint, _state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_oversized_success_response_is_rejected_before_decoding():
    token = "oversized-response-secret"
    oversized = json.dumps(
        {
            "provider_reference": "ref-large",
            "result": {"content": "x" * 1_000},
        }
    ).encode("utf-8")
    with _http_server(body=oversized) as (endpoint, _state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
            max_response_bytes=256,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


def test_redirect_is_not_followed_and_is_ambiguous():
    token = "redirect-secret"
    with _http_server(
        status=302,
        body=b"",
        extra_headers={"Location": "/redirect-target"},
    ) as (endpoint, state):
        provider = HttpExternalActionProvider(
            endpoint=endpoint,
            provider_identity=PROVIDER_IDENTITY,
            bearer_token=token,
            allow_insecure_localhost=True,
        )
        with pytest.raises(AmbiguousExternalActionError) as raised:
            provider.execute(_request())

    assert len(state.requests) == 1
    assert state.requests[0]["path"] == "/v1/actions"
    _assert_sanitized_error(raised.value, endpoint=endpoint, token=token)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"endpoint": "not-a-url"},
        {"endpoint": "ftp://example.com/action"},
        {"endpoint": "http://user:password@example.com/action"},
        {"endpoint": "https://example.com/action", "bearer_token": ""},
        {"endpoint": "https://example.com/action", "provider_identity": ""},
        {"endpoint": "https://example.com/action", "provider_identity": "   "},
        {"endpoint": "https://example.com/action", "provider_identity": "x" * 201},
        {"endpoint": "https://example.com/action", "provider_identity": 123},
        {
            "endpoint": "https://example.com/action",
            "supports_idempotency": "true",
        },
        {
            "endpoint": "https://example.com/action",
            "allow_insecure_localhost": "true",
        },
        {
            "endpoint": "https://example.com/action",
            "definitive_status_codes": 400,
        },
        {
            "endpoint": "https://example.com/action",
            "definitive_status_codes": {200},
        },
        {
            "endpoint": "https://example.com/action",
            "definitive_status_codes": {500},
        },
        {
            "endpoint": "https://example.com/action",
            "definitive_status_codes": {"400"},
        },
        {"endpoint": "https://example.com/action", "timeout_seconds": 0},
        {"endpoint": "https://example.com/action", "max_response_bytes": 255},
    ],
)
def test_configuration_validation_never_echoes_configured_values(kwargs):
    configuration = {"provider_identity": PROVIDER_IDENTITY, **kwargs}
    rendered_values = [str(value) for value in configuration.values() if value]
    with pytest.raises(ValueError) as raised:
        HttpExternalActionProvider(**configuration)

    for value in rendered_values:
        assert value not in str(raised.value)
