from __future__ import annotations

import json
import sqlite3
from contextlib import closing

from fastapi.testclient import TestClient

from agent.contracts import RuntimeExecutionAuthority
from api.main import create_app
from domains.durable_action import DurableActionState
from domains.travel.state import AgentState
from runtime_service import (
    ApiKeyCredential,
    RunRecord,
    RunStatus,
    RuntimeRole,
    StaticApiKeyAuthenticator,
)
from runtime_service.action_gateway import (
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    ACTION_DOMAIN_ID,
    ACTION_SCHEMA_VERSION,
    ACTION_STEP_ID,
    ACTION_WORKFLOW_TYPE,
    DurableActionInput,
    action_client_request_id,
    action_fingerprint,
    action_thread_id,
    canonical_action_json,
)
from runtime_service.canonical import stable_hash
from runtime_service.external_actions import ExternalActionProviderResult
from runtime_service.sandbox import ToolRetryMode
from runtime_service.store import THREAD_CHECKPOINT_CONFLICT_CODE


OPERATOR_A_KEY = "quarantine-operator-a"
VIEWER_A_KEY = "quarantine-viewer-a"
OPERATOR_B_KEY = "quarantine-operator-b"
TENANT_A = "quarantine-tenant-a"
TENANT_B = "quarantine-tenant-b"
SUBJECT_A = "quarantine-subject-a"
RESOLUTION = "terminalize_failed_preserving_checkpoint"


AUTHENTICATOR = StaticApiKeyAuthenticator(
    [
        ApiKeyCredential(
            credential_id="quarantine-credential-a",
            api_key=OPERATOR_A_KEY,
            tenant_id=TENANT_A,
            subject_id=SUBJECT_A,
            role=RuntimeRole.OPERATOR,
        ),
        ApiKeyCredential(
            credential_id="quarantine-viewer-credential-a",
            api_key=VIEWER_A_KEY,
            tenant_id=TENANT_A,
            subject_id="quarantine-viewer-a",
            role=RuntimeRole.VIEWER,
        ),
        ApiKeyCredential(
            credential_id="quarantine-credential-b",
            api_key=OPERATOR_B_KEY,
            tenant_id=TENANT_B,
            subject_id="quarantine-subject-b",
            role=RuntimeRole.OPERATOR,
        ),
    ]
)


class NeverCalledProvider:
    supports_idempotency = True
    provider_identity = "private-provider-identity"

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _request) -> ExternalActionProviderResult:
        self.calls += 1
        raise AssertionError("Quarantine resolution must not call a provider")


def headers(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def create_run(
    store,
    *,
    run_id: str,
    thread_id: str,
    agent_id: str,
    agent_version: str,
    domain_id: str,
    schema_version: str,
    input_payload: dict,
    client_request_id: str | None = None,
) -> RunRecord:
    run = RunRecord(
        run_id=run_id,
        tenant_id=TENANT_A,
        thread_id=thread_id,
        agent_id=agent_id,
        agent_version=agent_version,
        domain_id=domain_id,
        schema_version=schema_version,
        status=RunStatus.QUEUED,
        input=input_payload,
        execution_authority=RuntimeExecutionAuthority(
            tenant_id=TENANT_A,
            subject_id=SUBJECT_A,
            permissions=("external-actions:execute", "tools:execute"),
        ),
        client_request_id=client_request_id,
    )
    store.create_run_with_event(run, event_type="run.queued")
    return run


def create_terminal_workflow_evidence(
    workflow_store,
    *,
    claim,
    workflow_type: str,
    tool_name: str,
    step_id: str,
    input_hash: str,
    execution_input_hash: str | None = None,
    arguments: dict,
    provider_name: str,
    provider_identity: str,
    idempotency_key: str,
    result_json: str,
    provider_reference: str,
) -> None:
    workflow_store.create_or_get_execution(
        claim.run.run_id,
        workflow_type,
        execution_input_hash or input_hash,
        lease_token=claim.lease_token,
    )
    workflow_store.mark_running(
        claim.run.run_id,
        lease_token=claim.lease_token,
    )
    step = workflow_store.claim_step(
        claim.run.run_id,
        step_id,
        tool_name,
        input_hash,
        max_attempts=2,
        lease_token=claim.lease_token,
    )
    assert step.attempt_token is not None
    prepared = workflow_store.prepare_external_action(
        run_id=claim.run.run_id,
        step_id=step_id,
        tool_attempt_token=step.attempt_token,
        tenant_id=TENANT_A,
        subject_id=SUBJECT_A,
        workflow_type=workflow_type,
        tool_name=tool_name,
        provider_name=provider_name,
        provider_identity=provider_identity,
        input_hash=input_hash,
        arguments_json=json.dumps(arguments),
        retry_mode=ToolRetryMode.PROVIDER_IDEMPOTENT,
        idempotency_key=idempotency_key,
        lease_token=claim.lease_token,
    )
    assert prepared.action is not None
    dispatch = workflow_store.begin_external_action_dispatch(
        claim.run.run_id,
        step_id,
        tool_attempt_token=step.attempt_token,
        lease_token=claim.lease_token,
    )
    assert dispatch.dispatch_token is not None
    workflow_store.finalize_external_action_succeeded(
        claim.run.run_id,
        step_id,
        dispatch_token=dispatch.dispatch_token,
        tool_attempt_token=step.attempt_token,
        result_json=result_json,
        provider_reference=provider_reference,
    )
    workflow_store.finalize_ready(
        claim.run.run_id,
        result_json=result_json,
        lease_token=claim.lease_token,
    )


def quarantine_with_checkpoint(
    database_path,
    store,
    claim,
    *,
    state_json: str,
) -> None:
    with closing(sqlite3.connect(database_path, timeout=30)) as connection, connection:
        connection.execute(
            """
            INSERT INTO thread_states (
                tenant_id, thread_id, domain_id, schema_version,
                state_json, updated_at, revision
            ) VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (
                claim.run.tenant_id,
                claim.run.thread_id,
                claim.run.domain_id,
                claim.run.schema_version,
                state_json,
                "2026-08-23T00:00:00+00:00",
            ),
        )
    store.quarantine_checkpoint_conflict_for_reconciliation(
        claim.run,
        lease_token=claim.lease_token,
        phase="load",
    )


def seed_public_quarantine(client: TestClient, database_path) -> tuple[str, str]:
    store = client.app.state.run_store
    workflow_store = client.app.state.workflow_store
    run_id = "run_public_quarantine_api"
    thread_id = "public-quarantine-thread"
    create_run(
        store,
        run_id=run_id,
        thread_id=thread_id,
        agent_id="travel-agent",
        agent_version="0.3.0",
        domain_id="travel",
        schema_version="1",
        input_payload={"user_message": "hold a terminal external result"},
    )
    claim = store.claim_next_run(
        owner_id="public-quarantine-owner",
        lease_duration_seconds=30,
        reconciliation_pending_code="external_action_reconciliation_pending",
    )
    assert claim is not None and claim.run.run_id == run_id
    create_terminal_workflow_evidence(
        workflow_store,
        claim=claim,
        workflow_type="api-quarantine-workflow:1",
        tool_name="api_external_write",
        step_id="call-0001",
        input_hash="sha256:" + "a" * 64,
        arguments={"secret_argument": "must-not-leak"},
        provider_name="api-provider",
        provider_identity="api-provider-private-identity",
        idempotency_key="api-provider-private-idempotency-key",
        result_json='{"provider_reference":"public-ref"}',
        provider_reference="public-ref",
    )
    quarantine_with_checkpoint(
        database_path,
        store,
        claim,
        state_json=AgentState(
            thread_id=thread_id,
            destination="Tokyo",
            budget=7_777,
        ).model_dump_json(),
    )
    return run_id, thread_id


def seed_private_action_quarantine(
    client: TestClient,
    database_path,
    provider: NeverCalledProvider,
) -> tuple[str, str, str]:
    store = client.app.state.run_store
    workflow_store = client.app.state.workflow_store
    action_id = "run_private_action_quarantine_api"
    caller_idempotency_key = "caller-private-idempotency-key"
    runtime_input = DurableActionInput(
        destination="demo",
        idempotency_key=caller_idempotency_key,
        input={"payload": {"secret": "private-action-payload"}},
    )
    thread_id = action_thread_id(
        TENANT_A,
        runtime_input.action_type,
        caller_idempotency_key,
    )
    create_run(
        store,
        run_id=action_id,
        thread_id=thread_id,
        agent_id=ACTION_AGENT_ID,
        agent_version=ACTION_AGENT_VERSION,
        domain_id=ACTION_DOMAIN_ID,
        schema_version=ACTION_SCHEMA_VERSION,
        input_payload=runtime_input.model_dump(mode="json"),
        client_request_id=action_client_request_id(caller_idempotency_key),
    )
    claim = store.claim_next_run(
        owner_id="private-action-quarantine-owner",
        lease_duration_seconds=30,
        reconciliation_pending_code="external_action_reconciliation_pending",
    )
    assert claim is not None and claim.run.run_id == action_id
    arguments = runtime_input.input.model_dump(mode="json")
    input_hash = stable_hash(arguments)
    internal_provider_key = "external_action_" + stable_hash(
        {
            "tenant_id": TENANT_A,
            "run_id": action_id,
            "workflow_type": ACTION_WORKFLOW_TYPE,
            "step_id": ACTION_STEP_ID,
            "tool_name": "webhook.send",
            "input_hash": input_hash,
        }
    )
    result_json = canonical_action_json(
        {"provider_reference": "private-action-reference"}
    )
    create_terminal_workflow_evidence(
        workflow_store,
        claim=claim,
        workflow_type=ACTION_WORKFLOW_TYPE,
        tool_name="webhook.send",
        step_id=ACTION_STEP_ID,
        input_hash=input_hash,
        execution_input_hash=action_fingerprint(runtime_input),
        arguments=arguments,
        provider_name="demo",
        provider_identity=provider.provider_identity,
        idempotency_key=internal_provider_key,
        result_json=result_json,
        provider_reference="private-action-reference",
    )
    assert workflow_store.get_execution(action_id).input_hash == action_fingerprint(
        runtime_input
    )
    quarantine_with_checkpoint(
        database_path,
        store,
        claim,
        state_json=DurableActionState(thread_id=thread_id).model_dump_json(),
    )
    return action_id, thread_id, caller_idempotency_key


def test_public_run_resolution_enforces_permission_tenant_and_exact_replay(tmp_path) -> None:
    database_path = tmp_path / "public-quarantine-api.db"
    provider = NeverCalledProvider()
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )
    command = {
        "target": {"run_id": "run_public_quarantine_api"},
        "resolution": RESOLUTION,
        "dry_run": True,
    }

    with TestClient(app) as client:
        client.app.state.runtime_manager.stop()
        run_id, _thread_id = seed_public_quarantine(client, database_path)
        unauthenticated = client.post("/operator/quarantine-resolutions", json=command)
        viewer = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(VIEWER_A_KEY),
            json=command,
        )
        other_tenant = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_B_KEY),
            json=command,
        )
        unknown = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json={**command, "target": {"run_id": "missing"}},
        )
        wrong_private_target = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json={**command, "target": {"action_id": run_id}},
        )
        dry_run = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json=command,
        )
        stale_plan_id = dry_run.json()["plan"]["plan_id"]
        client.app.state.run_store.request_cancel_atomically(
            run_id,
            tenant_id=TENANT_A,
        )
        stale_apply = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json={
                **command,
                "dry_run": False,
                "expected_plan_id": stale_plan_id,
            },
        )
        refreshed_dry_run = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json=command,
        )
        plan_id = refreshed_dry_run.json()["plan"]["plan_id"]
        apply_command = {
            **command,
            "dry_run": False,
            "expected_plan_id": plan_id,
        }
        applied = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json=apply_command,
        )
        replayed = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json=apply_command,
        )
        resolved_run = client.get(
            f"/runs/{run_id}",
            headers=headers(OPERATOR_A_KEY),
        )

    assert unauthenticated.status_code == 401
    assert viewer.status_code == 403
    assert viewer.json()["error"]["code"] == "operation_not_permitted"
    assert other_tenant.status_code == unknown.status_code == 404
    assert other_tenant.json()["error"]["code"] == "quarantine_target_not_found"
    assert unknown.json() == other_tenant.json()
    assert wrong_private_target.status_code == 404
    assert dry_run.status_code == 200
    assert dry_run.json()["outcome"] == "dry_run"
    assert dry_run.json()["plan"]["eligible"]
    assert stale_apply.status_code == 409
    assert stale_apply.json()["error"]["code"] == (
        "quarantine_resolution_plan_stale"
    )
    stale_current_plan = stale_apply.json()["error"]["details"]["current_plan"]
    assert stale_current_plan["cancel_requested"] is True
    assert refreshed_dry_run.status_code == 200
    assert refreshed_dry_run.json()["plan"]["eligible"]
    assert refreshed_dry_run.json()["plan"] == stale_current_plan
    assert plan_id != stale_plan_id
    assert applied.status_code == 200
    assert applied.json()["outcome"] == "applied"
    assert applied.json()["verified"]
    assert replayed.status_code == 200
    assert replayed.json()["outcome"] == "reused"
    assert replayed.json()["reused"]
    assert resolved_run.json()["status"] == "failed"
    assert resolved_run.json()["error_code"] == THREAD_CHECKPOINT_CONFLICT_CODE
    assert provider.calls == 0


def test_private_action_target_stays_sanitized_and_hidden_from_run_routes(tmp_path) -> None:
    database_path = tmp_path / "private-action-quarantine-api.db"
    provider = NeverCalledProvider()
    app = create_app(
        database_path=database_path,
        authenticator=AUTHENTICATOR,
        action_providers={"demo": provider},
    )

    with TestClient(app) as client:
        client.app.state.runtime_manager.stop()
        action_id, private_thread_id, caller_idempotency_key = (
            seed_private_action_quarantine(client, database_path, provider)
        )
        command = {
            "target": {"action_id": action_id},
            "resolution": RESOLUTION,
            "dry_run": True,
        }
        hidden_before = client.get(
            f"/runs/{action_id}",
            headers=headers(OPERATOR_A_KEY),
        )
        wrong_run_target = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json={**command, "target": {"run_id": action_id}},
        )
        dry_run = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json=command,
        )
        plan_id = dry_run.json()["plan"]["plan_id"]
        applied = client.post(
            "/operator/quarantine-resolutions",
            headers=headers(OPERATOR_A_KEY),
            json={
                **command,
                "dry_run": False,
                "expected_plan_id": plan_id,
            },
        )
        hidden_after = client.get(
            f"/runs/{action_id}",
            headers=headers(OPERATOR_A_KEY),
        )
        public_action = client.get(
            f"/actions/{action_id}",
            headers=headers(OPERATOR_A_KEY),
        )
        internal_audit = json.dumps(
            [
                event.payload
                for event in client.app.state.run_store.list_events(action_id)
                if event.event_type == "quarantine.resolution_applied"
            ]
        )

    assert hidden_before.status_code == hidden_after.status_code == 404
    assert wrong_run_target.status_code == 404
    assert dry_run.status_code == 200, dry_run.text
    assert dry_run.json()["plan"]["eligible"]
    assert dry_run.json()["plan"]["target"] == {"run_id": None, "action_id": action_id}
    assert dry_run.json()["plan"]["thread"] == {
        "tenant_id": TENANT_A,
        "reference_kind": "action_id",
        "reference": action_id,
    }
    encoded = dry_run.text + applied.text
    for private_value in (
        private_thread_id,
        caller_idempotency_key,
        "private-action-payload",
        provider.provider_identity,
        "private-action-reference",
    ):
        assert private_value not in encoded
        assert private_value not in internal_audit
    assert applied.status_code == 200, applied.text
    assert applied.json()["outcome"] == "applied"
    assert public_action.status_code == 200
    assert public_action.json()["status"] == "succeeded"
    assert provider.calls == 0
