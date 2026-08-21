from __future__ import annotations

import json

import runtime_service
from agent.contracts import RuntimeExecutionAuthority, RuntimeExecutionContext
from api.main import create_app
from runtime_service.external_actions import (
    ExternalActionDispatcher,
    ExternalActionProviderRegistry,
    ExternalActionProviderResult,
    ExternalActionRequest,
)
from runtime_service.models import RunLeaseClaim, RunRecord, RunStatus
from runtime_service.sandbox import ToolRetryMode
from runtime_service.store import SQLiteRunStore


LEASE_TOKEN = "lease_public_boundary_secret"
LEASE_OWNER = "manager_public_boundary_secret"
INTERNAL_LEASE_FIELDS = {
    "lease_owner_id",
    "lease_token",
    "lease_heartbeat_at",
    "lease_expires_at",
}


def _leased_run() -> RunRecord:
    return RunRecord(
        run_id="run-public-boundary",
        tenant_id="tenant-public-boundary",
        thread_id="thread-public-boundary",
        agent_id="travel-agent",
        agent_version="0.3.0",
        status=RunStatus.RUNNING,
        input={"user_message": "Exercise the public boundary."},
        lease_owner_id=LEASE_OWNER,
        lease_token=LEASE_TOKEN,
        lease_heartbeat_at=1_000,
        lease_expires_at=31_000,
    )


def test_lease_authority_is_absent_from_serialization_repr_and_public_exports() -> None:
    run = _leased_run()
    context = RuntimeExecutionContext(
        run_id=run.run_id,
        thread_id=run.thread_id,
        authority=RuntimeExecutionAuthority(
            tenant_id=run.tenant_id,
            subject_id="subject-public-boundary",
        ),
        lease_token=LEASE_TOKEN,
    )
    claim = RunLeaseClaim(
        run=run,
        owner_id=LEASE_OWNER,
        lease_token=LEASE_TOKEN,
    )

    for value in (run, context, claim):
        encoded = value.model_dump_json()
        assert LEASE_TOKEN not in encoded
        assert LEASE_OWNER not in encoded
        assert LEASE_TOKEN not in repr(value)
        assert LEASE_OWNER not in repr(value)

    assert INTERNAL_LEASE_FIELDS.isdisjoint(run.model_dump(mode="json"))
    assert "lease_token" not in context.model_dump(mode="json")
    assert {"owner_id", "lease_token"}.isdisjoint(claim.model_dump(mode="json"))
    assert "RunLeaseClaim" not in runtime_service.__all__


def test_run_openapi_schema_excludes_internal_lease_fields(tmp_path) -> None:
    app = create_app(database_path=tmp_path / "runtime.db")

    run_schema = app.openapi()["components"]["schemas"]["RunRecord"]

    assert INTERNAL_LEASE_FIELDS.isdisjoint(run_schema["properties"])


def test_claim_and_recovery_events_do_not_expose_lease_authority(tmp_path) -> None:
    now_ms = [1_000]
    store = SQLiteRunStore(
        tmp_path / "runtime.db",
        lease_clock_ms=lambda: now_ms[0],
    )
    run = _leased_run().model_copy(
        update={
            "status": RunStatus.QUEUED,
            "lease_owner_id": None,
            "lease_token": None,
            "lease_heartbeat_at": None,
            "lease_expires_at": None,
        }
    )
    store.create_run_with_event(run, event_type="run.queued")

    first = store.claim_next_run(
        owner_id=LEASE_OWNER,
        lease_duration_seconds=30,
    )
    assert first is not None
    now_ms[0] = 31_000
    second = store.claim_next_run(
        owner_id="manager_replacement_secret",
        lease_duration_seconds=30,
    )
    assert second is not None

    encoded_events = json.dumps(
        [event.model_dump(mode="json") for event in store.list_events(run.run_id)],
        sort_keys=True,
    )
    for private_value in (
        first.lease_token,
        second.lease_token,
        LEASE_OWNER,
        "manager_replacement_secret",
    ):
        assert private_value not in encoded_events
    assert INTERNAL_LEASE_FIELDS.isdisjoint(
        {
            key
            for event in store.list_events(run.run_id)
            for key in event.payload
        }
    )


def test_provider_request_contract_cannot_carry_internal_lease_metadata() -> None:
    captured: list[ExternalActionRequest] = []

    class CapturingProvider:
        provider_identity = "provider-public-boundary"
        supports_idempotency = True

        def execute(self, request: ExternalActionRequest) -> ExternalActionProviderResult:
            captured.append(request)
            return ExternalActionProviderResult(
                provider_reference="provider-reference",
                result={"accepted": True},
            )

    registry = ExternalActionProviderRegistry()
    registry.register("provider", CapturingProvider())
    dispatcher = ExternalActionDispatcher(registry)
    request = ExternalActionRequest(
        action_id="action-public-boundary",
        run_id="run-public-boundary",
        step_id="call-0001",
        tenant_id="tenant-public-boundary",
        subject_id="subject-public-boundary",
        workflow_type="public-boundary:1.0.0",
        tool_name="create_record",
        arguments={"record": "safe"},
        idempotency_key="idempotency-public-boundary",
    )

    dispatcher.dispatch(
        provider_name="provider",
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        request=request,
    )

    assert captured == [request]
    assert INTERNAL_LEASE_FIELDS.isdisjoint(ExternalActionRequest.model_fields)
    encoded = captured[0].model_dump_json()
    assert LEASE_TOKEN not in encoded
    assert LEASE_OWNER not in encoded
