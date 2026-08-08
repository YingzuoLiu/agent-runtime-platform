from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from domains.release_validation.runtime import ReleaseValidationWorkflow
from domains.release_validation.tools import build_release_validation_tool_registry
from domains.travel.dynamic_runtime import DynamicTravelRuntime
from domains.travel.planner import build_travel_planner_from_environment
from domains.travel.state import AgentState
from domains.travel.runtime import TravelAgentRuntime
from domains.travel.tools import build_travel_tool_registry
from runtime_service import (
    AgentDescriptor,
    AuthenticationError,
    Authenticator,
    AuthorizationError,
    Authorizer,
    DynamicToolLoop,
    Planner,
    Principal,
    ReferencedRunNotFoundError,
    RoleAuthorizer,
    RunCreateRequest,
    RunEvent,
    RunRecord,
    RuntimeManager,
    RuntimePermission,
    SQLiteRunStore,
    StaticApiKeyAuthenticator,
    TenantContext,
    ToolDescriptor,
    ToolExecutionRequest,
    ToolExecutionResult,
    ToolSandbox,
    build_default_registry,
    effective_execution_authority,
)
from runtime_service.workflow_store import SQLiteWorkflowStore


class AgentMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thread_id: str = Field(..., description="Conversation or task thread id.")
    user_message: str = Field(..., description="User message to process.")
    state: Optional[AgentState] = Field(
        default=None,
        description="Optional client-provided state. If omitted, server loads the durable thread checkpoint.",
    )


class AgentMessageResponse(BaseModel):
    assistant_message: str
    updated_state: AgentState
    validation_errors: list[str]


def create_app(
    *,
    database_path: str | Path | None = None,
    worker_count: int | None = None,
    authenticator: Authenticator | None = None,
    authorizer: Authorizer | None = None,
    travel_planner: Planner | None = None,
) -> FastAPI:
    database_value = database_path
    if database_value is None:
        database_value = os.getenv("RUNTIME_DB_PATH") or "runtime_data/runtime.db"
    resolved_database_path = Path(database_value)
    resolved_worker_count = worker_count or int(os.getenv("RUNTIME_WORKER_COUNT", "1"))
    resolved_authenticator = (
        authenticator
        if authenticator is not None
        else StaticApiKeyAuthenticator.from_environment()
    )
    resolved_authorizer = authorizer if authorizer is not None else RoleAuthorizer()
    resolved_travel_planner = (
        travel_planner
        if travel_planner is not None
        else build_travel_planner_from_environment()
    )
    tool_registry = build_travel_tool_registry()
    bearer_scheme = HTTPBearer(auto_error=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = SQLiteRunStore(resolved_database_path)
        workflow_store = SQLiteWorkflowStore(resolved_database_path)
        release_workflow = ReleaseValidationWorkflow(
            workflow_store,
            ToolSandbox(build_release_validation_tool_registry()),
        )
        dynamic_travel_loop = DynamicToolLoop(
            planner=resolved_travel_planner,
            tool_registry=tool_registry,
            tool_sandbox=ToolSandbox(tool_registry),
            workflow_store=workflow_store,
            run_event_sink=store,
            workflow_type="dynamic-tool-loop:travel-agent:1.0.0",
            max_tool_calls=3,
        )
        registry = build_default_registry(
            release_validation_workflow=release_workflow,
            dynamic_travel_runtime_factory=(
                lambda: DynamicTravelRuntime(dynamic_travel_loop)
            ),
        )
        store.bind_state_registry(registry)
        manager = RuntimeManager(
            store=store,
            registry=registry,
            worker_count=resolved_worker_count,
        )
        manager.start()
        app.state.run_store = store
        app.state.runtime_manager = manager
        app.state.agent_registry = registry
        app.state.tool_registry = tool_registry
        app.state.tool_sandbox = ToolSandbox(tool_registry)
        app.state.authenticator = resolved_authenticator
        app.state.authorizer = resolved_authorizer
        yield
        manager.stop()

    app = FastAPI(
        title="Agent Runtime Reliability Platform",
        description=(
            "Typed domain runtimes sharing one durable run lifecycle and unified API, "
            "plus policy-enforced sandboxed tool execution."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    def get_manager(request: Request) -> RuntimeManager:
        return request.app.state.runtime_manager

    def require_principal(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    ) -> Principal:
        api_key = None
        if credentials is not None and credentials.scheme.lower() == "bearer":
            api_key = credentials.credentials
        try:
            return resolved_authenticator.authenticate(api_key)
        except AuthenticationError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key",
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    def require_permission(
        principal: Principal,
        permission: RuntimePermission,
    ) -> None:
        try:
            resolved_authorizer.authorize(principal, permission)
        except AuthorizationError as exc:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operation not permitted",
            ) from exc

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready(request: Request) -> dict[str, str]:
        request.app.state.run_store.ping()
        return {"status": "ready"}

    @app.get("/agents", response_model=list[AgentDescriptor])
    def list_agents(
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> list[AgentDescriptor]:
        require_permission(principal, RuntimePermission.AGENTS_READ)
        return request.app.state.agent_registry.list_agents()

    @app.get("/tools", response_model=list[ToolDescriptor])
    def list_tools(
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> list[ToolDescriptor]:
        require_permission(principal, RuntimePermission.TOOLS_READ)
        return request.app.state.tool_registry.list_tools()

    @app.post("/tools/{tool_name}/execute", response_model=ToolExecutionResult)
    def execute_tool(
        tool_name: str,
        payload: ToolExecutionRequest,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> ToolExecutionResult:
        store: SQLiteRunStore = request.app.state.run_store
        if (
            payload.run_id is not None
            and store.get_run_for_tenant(payload.run_id, principal.tenant_id) is None
        ):
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.TOOLS_EXECUTE)
        if payload.run_id is not None:
            store.append_event(
                payload.run_id,
                "sandbox.execution_started",
                {"tool_name": tool_name},
            )

        sandbox: ToolSandbox = request.app.state.tool_sandbox
        result = sandbox.execute(tool_name, payload.arguments)

        if payload.run_id is not None:
            store.append_event(
                payload.run_id,
                "sandbox.execution_finished",
                {
                    "tool_name": tool_name,
                    "execution_id": result.execution_id,
                    "status": result.status.value,
                    "duration_ms": result.duration_ms,
                },
            )
        return result

    @app.post("/agent/message", response_model=AgentMessageResponse)
    def handle_agent_message(
        payload: AgentMessageRequest,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> AgentMessageResponse:
        """Backward-compatible synchronous endpoint backed by the durable thread store."""
        require_permission(principal, RuntimePermission.AGENT_MESSAGE_EXECUTE)
        store: SQLiteRunStore = request.app.state.run_store
        runtime = TravelAgentRuntime(retry_limit=2)
        if payload.state is not None and payload.state.thread_id != payload.thread_id:
            raise HTTPException(
                status_code=422,
                detail="state.thread_id must match request.thread_id",
            )
        try:
            persisted_state = store.load_thread_state(
                payload.thread_id,
                tenant_id=principal.tenant_id,
                domain_id=AgentState.domain_id,
                schema_version=AgentState.schema_version,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        state_value = payload.state or persisted_state or AgentState(thread_id=payload.thread_id)
        result = runtime.handle_user_message(state_value, payload.user_message)
        try:
            store.save_thread_state(result.state, tenant_id=principal.tenant_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return AgentMessageResponse(
            assistant_message=result.message,
            updated_state=result.state,
            validation_errors=result.validation_errors,
        )

    @app.post("/runs", response_model=RunRecord, status_code=status.HTTP_202_ACCEPTED)
    def create_run(
        payload: RunCreateRequest,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> RunRecord:
        require_permission(principal, RuntimePermission.RUNS_CREATE)
        authority = effective_execution_authority(principal, resolved_authorizer)
        try:
            return get_manager(request).submit(
                payload,
                tenant_context=TenantContext(
                    tenant_id=authority.tenant_id,
                    subject_id=authority.subject_id,
                    permissions=authority.permissions,
                ),
            )
        except ReferencedRunNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Referenced run not found") from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/runs/{run_id}", response_model=RunRecord)
    def get_run(
        run_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> RunRecord:
        run = get_manager(request).get_run(
            run_id,
            tenant_context=principal.tenant_context,
        )
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.RUNS_READ)
        return run

    @app.post("/runs/{run_id}/cancel", response_model=RunRecord)
    def cancel_run(
        run_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> RunRecord:
        if (
            get_manager(request).get_run(
                run_id,
                tenant_context=principal.tenant_context,
            )
            is None
        ):
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.RUNS_CANCEL)
        try:
            return get_manager(request).request_cancel(
                run_id,
                tenant_context=principal.tenant_context,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Run not found") from exc

    @app.get("/runs/{run_id}/events", response_model=list[RunEvent])
    def list_run_events(
        run_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
        after_sequence: int = Query(default=0, ge=0),
    ) -> list[RunEvent]:
        if (
            get_manager(request).get_run(
                run_id,
                tenant_context=principal.tenant_context,
            )
            is None
        ):
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.RUN_EVENTS_READ)
        return request.app.state.run_store.list_events_for_tenant(
            run_id,
            principal.tenant_id,
            after_sequence=after_sequence,
        )

    @app.get("/runs/{run_id}/events/stream")
    async def stream_run_events(
        run_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
        after_sequence: int = Query(default=0, ge=0),
    ) -> StreamingResponse:
        manager = get_manager(request)
        if (
            manager.get_run(run_id, tenant_context=principal.tenant_context)
            is None
        ):
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.RUN_EVENTS_READ)

        async def event_stream():
            sequence = after_sequence
            while True:
                if await request.is_disconnected():
                    return
                events = request.app.state.run_store.list_events_for_tenant(
                    run_id,
                    principal.tenant_id,
                    after_sequence=sequence,
                )
                for event in events:
                    sequence = event.sequence
                    data = json.dumps(event.model_dump(mode="json"))
                    yield f"event: {event.event_type}\ndata: {data}\n\n"
                run = manager.get_run(
                    run_id,
                    tenant_context=principal.tenant_context,
                )
                if run is None or (run.status.is_terminal and not events):
                    return
                await asyncio.sleep(0.2)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.get("/threads/{thread_id}/state")
    def get_thread_state(
        thread_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
        domain_id: str = Query(default="travel"),
        schema_version: str = Query(default="1"),
    ) -> dict:
        require_permission(principal, RuntimePermission.THREAD_STATE_READ)
        try:
            state_value = request.app.state.run_store.load_thread_state(
                thread_id,
                tenant_id=principal.tenant_id,
                domain_id=domain_id,
                schema_version=schema_version,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if state_value is None:
            raise HTTPException(status_code=404, detail="Thread state not found")
        return state_value.model_dump(mode="json")

    return app


app = create_app()
