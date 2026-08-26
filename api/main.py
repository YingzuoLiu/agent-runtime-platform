from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Literal, Optional

import anyio
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.types import Scope

from api.demo import (
    DemoSession,
    build_demo_authenticator,
    create_demo_session,
    demo_assets_path,
    resolve_demo_mode,
)
from domains.release_validation.runtime import ReleaseValidationWorkflow
from domains.release_validation.tools import build_release_validation_tool_registry
from domains.durable_action import DurableActionGatewayRuntime, DurableActionState
from domains.travel.dynamic_runtime import DynamicTravelRuntime
from domains.travel.external_action_runtime import (
    DurableActionTravelPlanner,
    DurableActionTravelRuntime,
)
from domains.travel.memory import TravelMemoryPolicy
from domains.travel.planner import build_travel_planner_from_environment
from domains.travel.preferences import (
    parse_explicit_travel_preferences,
    parse_legacy_travel_preferences,
)
from domains.travel.state import AgentState
from domains.travel.tools import (
    SQLiteTripHoldProvider,
    build_travel_external_action_tool_registry,
    build_travel_tool_registry,
)
from runtime_service import (
    AgentDescriptor,
    AuthenticationError,
    Authenticator,
    AuthorizationError,
    Authorizer,
    DisabledExternalActionProvider,
    DynamicToolLoop,
    GovernedMemory,
    HttpExternalActionProvider,
    MemoryAdminStore,
    MemoryKind,
    MemoryRecord,
    Planner,
    Principal,
    QuarantineResolutionCommand,
    QuarantineResolutionEvidenceIncompleteError,
    QuarantineResolutionResponse,
    QuarantineResolutionService,
    QuarantineResolutionStalePlanError,
    QuarantineTargetNotFoundError,
    ReferencedRunNotFoundError,
    RoleAuthorizer,
    RunCreateRequest,
    RunEvent,
    RunRecord,
    RunStatus,
    RunStore,
    RuntimeManager,
    RuntimeExtension,
    RuntimeExtensionContext,
    RuntimePermission,
    StaticApiKeyAuthenticator,
    TenantContext,
    ThreadStateConflictError,
    ToolDescriptor,
    ToolEffect,
    ToolExecutionRequest,
    ToolExecutionResult,
    ToolSandbox,
    build_default_registry,
    effective_execution_authority,
)
from runtime_service.external_actions import (
    ExternalActionDispatcher,
    ExternalActionProvider,
    ExternalActionProviderRegistry,
)
from runtime_service.run_store import is_run_store_contention_error, is_run_store_error
from runtime_service.deployment import (
    RuntimeDeploymentConfig,
    resolve_runtime_deployment_config,
    validate_manager_shutdown_grace_seconds,
    validate_runtime_worker_count,
)
from runtime_service.storage import (
    build_runtime_store_bundle,
    resolve_runtime_storage_config,
)
from runtime_service.action_gateway import (
    ACTION_AGENT_ID,
    ACTION_AGENT_VERSION,
    ACTION_DOMAIN_ID,
    ACTION_REQUEST_NAMESPACE_PREFIX,
    ActionApiErrorBody,
    ActionApiErrorEnvelope,
    ActionCreateRequest,
    ActionEvent,
    ActionResource,
    DurableActionInput,
    load_action_providers_from_environment,
)
from runtime_service.action_gateway_service import (
    ActionEvidenceIncompleteError,
    ActionTypeNotRegisteredError,
    DestinationNotRegisteredError,
    DurableActionGateway,
    IdempotencyKeyReusedError,
)


logger = logging.getLogger(__name__)


class ActionRouteError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        headers: Mapping[str, str] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = dict(headers or {})
        self.details = dict(details) if details is not None else None


class NoStoreStaticFiles(StaticFiles):
    """Serve local demo assets without retaining stale frontend code."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-store"
        return response


class AgentMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thread_id: str = Field(..., description="Conversation or task thread id.")
    user_message: str = Field(..., description="User message to process.")
    state: Optional[AgentState] = Field(
        default=None,
        description=(
            "Optional state used only to initialize an empty thread. "
            "Omit it to continue an existing durable checkpoint."
        ),
    )


class AgentMessageResponse(BaseModel):
    assistant_message: str
    updated_state: AgentState
    validation_errors: list[str]


class AgentMessagePendingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    status: Literal["queued", "running"]


def create_app(
    *,
    database_path: str | Path | None = None,
    store_backend: str | None = None,
    postgres_dsn: str | None = None,
    postgres_schema: str | None = None,
    postgres_connect_timeout_seconds: float | None = None,
    postgres_statement_timeout_seconds: float | None = None,
    postgres_lock_timeout_seconds: float | None = None,
    postgres_idle_in_transaction_session_timeout_seconds: float | None = None,
    postgres_lease_operation_timeout_seconds: float | None = None,
    worker_count: int | None = None,
    shutdown_grace_seconds: float | None = None,
    deployment_config: RuntimeDeploymentConfig | None = None,
    authenticator: Authenticator | None = None,
    authorizer: Authorizer | None = None,
    travel_planner: Planner | None = None,
    travel_action_provider: ExternalActionProvider | None = None,
    action_providers: Mapping[str, ExternalActionProvider] | None = None,
    action_waiter_limit: int | None = None,
    agent_message_waiter_limit: int | None = None,
    demo_mode: bool | None = None,
    demo_api_key: str | None = None,
    runtime_extensions: Sequence[RuntimeExtension] = (),
) -> FastAPI:
    resolved_deployment_config = (
        deployment_config
        if deployment_config is not None
        else resolve_runtime_deployment_config()
    )
    storage_config = resolve_runtime_storage_config(
        backend=store_backend,
        database_path=database_path,
        postgres_dsn=postgres_dsn,
        postgres_schema=postgres_schema,
        postgres_connect_timeout_seconds=postgres_connect_timeout_seconds,
        postgres_statement_timeout_seconds=postgres_statement_timeout_seconds,
        postgres_lock_timeout_seconds=postgres_lock_timeout_seconds,
        postgres_idle_in_transaction_session_timeout_seconds=(
            postgres_idle_in_transaction_session_timeout_seconds
        ),
        postgres_lease_operation_timeout_seconds=(
            postgres_lease_operation_timeout_seconds
        ),
    )
    resolved_database_path = storage_config.sqlite_path
    resolved_worker_count = validate_runtime_worker_count(
        resolved_deployment_config.worker_count
        if worker_count is None
        else worker_count
    )
    resolved_shutdown_grace_seconds = validate_manager_shutdown_grace_seconds(
        resolved_deployment_config.manager_shutdown_grace_seconds
        if shutdown_grace_seconds is None
        else shutdown_grace_seconds
    )
    resolved_action_waiter_limit = (
        int(os.getenv("RUNTIME_ACTION_WAITER_LIMIT", "16"))
        if action_waiter_limit is None
        else action_waiter_limit
    )
    if not 1 <= resolved_action_waiter_limit <= 1_000:
        raise ValueError("action waiter limit must be between 1 and 1000")
    resolved_agent_message_waiter_limit = (
        int(os.getenv("RUNTIME_AGENT_MESSAGE_WAITER_LIMIT", "16"))
        if agent_message_waiter_limit is None
        else agent_message_waiter_limit
    )
    if not 1 <= resolved_agent_message_waiter_limit <= 1_000:
        raise ValueError("agent message waiter limit must be between 1 and 1000")
    configured_action_providers = dict(load_action_providers_from_environment())
    for alias, provider in (action_providers or {}).items():
        if alias in configured_action_providers:
            raise ValueError(f"Duplicate Action destination alias: {alias}")
        configured_action_providers[alias] = provider
    travel_provider_environment_names = (
        "RUNTIME_TRAVEL_ACTION_PROVIDER_URL",
        "RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY",
        "RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN",
        "RUNTIME_TRAVEL_ACTION_PROVIDER_ALLOW_INSECURE_LOCALHOST",
        "RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY",
    )
    has_travel_provider_configuration = any(
        os.getenv(name, "").strip() for name in travel_provider_environment_names
    )
    if resolved_deployment_config.external_action_mode == "disabled" and (
        travel_action_provider is not None
        or has_travel_provider_configuration
        or configured_action_providers
    ):
        raise ValueError(
            "disabled external actions cannot be combined with an injected "
            "or configured provider"
        )
    resolved_runtime_extensions = tuple(runtime_extensions)
    resolved_demo_mode = resolve_demo_mode(demo_mode)
    demo_session: DemoSession | None = None
    if resolved_demo_mode:
        if authenticator is not None:
            raise ValueError("demo mode cannot be combined with an injected authenticator")
        if os.getenv("RUNTIME_API_KEYS_JSON", "").strip():
            raise ValueError(
                "RUNTIME_DEMO_MODE cannot be combined with RUNTIME_API_KEYS_JSON"
            )
        demo_session = create_demo_session(demo_api_key)
        resolved_authenticator = build_demo_authenticator(demo_session)
        logger.warning(
            "RUNTIME_DEMO_MODE is enabled: /demo/session exposes an ephemeral "
            "Operator credential without authentication. Keep this server bound "
            "to localhost and do not deploy it."
        )
    else:
        if demo_api_key is not None:
            raise ValueError("demo_api_key requires demo mode")
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
    legacy_travel_tool_registry = build_travel_tool_registry()
    governed_memory_tool_registry = build_travel_tool_registry()
    external_action_tool_registry = build_travel_external_action_tool_registry()
    bearer_scheme = HTTPBearer(auto_error=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        stores = build_runtime_store_bundle(storage_config)
        store = stores.run_store
        memory_store = stores.memory_store
        governed_memory = GovernedMemory(memory_store, store)
        workflow_store = stores.workflow_store
        release_workflow = ReleaseValidationWorkflow(
            workflow_store,
            ToolSandbox(build_release_validation_tool_registry()),
        )
        travel_provider_registry = ExternalActionProviderRegistry()
        if resolved_deployment_config.external_action_mode == "disabled":
            resolved_action_provider = DisabledExternalActionProvider(
                "travel-external-actions-disabled"
            )
        elif travel_action_provider is not None:
            resolved_action_provider = travel_action_provider
        elif action_provider_url := os.getenv("RUNTIME_TRAVEL_ACTION_PROVIDER_URL"):
            action_provider_identity = os.getenv(
                "RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY"
            )
            if action_provider_identity is None:
                raise ValueError(
                    "RUNTIME_TRAVEL_ACTION_PROVIDER_IDENTITY is required "
                    "when the HTTP external-action provider is configured"
                )
            resolved_action_provider = HttpExternalActionProvider(
                endpoint=action_provider_url,
                provider_identity=action_provider_identity,
                bearer_token=os.getenv(
                    "RUNTIME_TRAVEL_ACTION_PROVIDER_BEARER_TOKEN"
                ),
                allow_insecure_localhost=(
                    os.getenv(
                        "RUNTIME_TRAVEL_ACTION_PROVIDER_ALLOW_INSECURE_LOCALHOST",
                        "",
                    ).strip().lower()
                    == "true"
                ),
                supports_idempotency=(
                    os.getenv(
                        "RUNTIME_TRAVEL_ACTION_PROVIDER_SUPPORTS_IDEMPOTENCY",
                        "",
                    ).strip().lower()
                    == "true"
                ),
            )
        else:
            if resolved_database_path is None:
                raise ValueError(
                    "PostgreSQL application composition requires an injected or "
                    "HTTP travel external-action provider; the SQLite provider "
                    "cannot be selected implicitly"
                )
            resolved_action_provider = SQLiteTripHoldProvider(
                resolved_database_path.with_name(
                    f"{resolved_database_path.name}.trip-hold-provider"
                )
            )
        travel_provider_registry.register("travel-trip-hold", resolved_action_provider)
        travel_action_dispatcher = ExternalActionDispatcher(travel_provider_registry)
        action_provider_registry = ExternalActionProviderRegistry()
        for alias, provider in sorted(configured_action_providers.items()):
            action_provider_registry.register(alias, provider)
        action_dispatcher = ExternalActionDispatcher(action_provider_registry)
        dynamic_travel_loop = DynamicToolLoop(
            planner=resolved_travel_planner,
            tool_registry=legacy_travel_tool_registry,
            tool_sandbox=ToolSandbox(legacy_travel_tool_registry),
            workflow_store=workflow_store,
            run_event_sink=store,
            workflow_type="dynamic-tool-loop:travel-agent:1.0.0",
            max_tool_calls=3,
        )
        governed_memory_travel_loop = DynamicToolLoop(
            planner=resolved_travel_planner,
            tool_registry=governed_memory_tool_registry,
            tool_sandbox=ToolSandbox(governed_memory_tool_registry),
            workflow_store=workflow_store,
            run_event_sink=store,
            workflow_type="dynamic-tool-loop:travel-agent:1.1.0",
            max_tool_calls=3,
        )
        external_action_travel_loop = DynamicToolLoop(
            planner=DurableActionTravelPlanner(resolved_travel_planner),
            tool_registry=external_action_tool_registry,
            tool_sandbox=ToolSandbox(external_action_tool_registry),
            workflow_store=workflow_store,
            run_event_sink=store,
            workflow_type="dynamic-tool-loop:travel-agent:1.2.0",
            max_tool_calls=4,
            external_action_dispatcher=travel_action_dispatcher,
        )
        registry = build_default_registry(
            release_validation_workflow=release_workflow,
            dynamic_travel_runtime_factory=(
                lambda: DynamicTravelRuntime(
                    dynamic_travel_loop,
                    preference_parser=parse_legacy_travel_preferences,
                )
            ),
            governed_memory_travel_runtime_factory=(
                lambda: DynamicTravelRuntime(
                    governed_memory_travel_loop,
                    governed_memory=governed_memory,
                    memory_policy=TravelMemoryPolicy(),
                    preference_parser=parse_explicit_travel_preferences,
                )
            ),
            external_action_travel_runtime_factory=(
                lambda: DurableActionTravelRuntime(
                    external_action_travel_loop,
                    governed_memory=governed_memory,
                    memory_policy=TravelMemoryPolicy(),
                    preference_parser=parse_explicit_travel_preferences,
                )
            ),
        )
        registry.register(
            ACTION_AGENT_ID,
            ACTION_AGENT_VERSION,
            lambda: DurableActionGatewayRuntime(
                workflow_store=workflow_store,
                run_event_sink=store,
                dispatcher=action_dispatcher,
            ),
            description=(
                "Private single-step domain for the durable Action API façade."
            ),
            input_model=DurableActionInput,
            state_model=DurableActionState,
            public_runs_api=False,
        )
        extension_context = RuntimeExtensionContext(
            registry=registry,
            workflow_store=workflow_store,
            run_event_sink=store,
        )
        for extension in resolved_runtime_extensions:
            extension.register(extension_context)
        store.bind_state_registry(registry)
        manager = RuntimeManager(
            store=store,
            registry=registry,
            worker_count=resolved_worker_count,
            shutdown_grace_seconds=resolved_shutdown_grace_seconds,
            recovery_reconciliation_required=(
                workflow_store.has_external_action_requiring_reconciliation
            ),
        )
        action_gateway = DurableActionGateway(
            manager=manager,
            run_store=store,
            workflow_store=workflow_store,
            provider_registry=action_provider_registry,
        )
        quarantine_resolution_service = QuarantineResolutionService(
            store=store,
            registry=registry,
        )
        manager.start()
        app.state.run_store = store
        app.state.memory_store = memory_store
        app.state.storage_metadata = stores.metadata
        app.state.release_identity = resolved_deployment_config.release_identity
        app.state.runtime_manager = manager
        app.state.agent_registry = registry
        # Preserve the published direct-sandbox catalog: external writes are
        # reachable only through the version-pinned 1.2 durable run loop.
        app.state.tool_registry = legacy_travel_tool_registry
        app.state.tool_sandbox = ToolSandbox(legacy_travel_tool_registry)
        app.state.external_action_tool_registry = external_action_tool_registry
        app.state.workflow_store = workflow_store
        app.state.travel_action_provider = resolved_action_provider
        app.state.action_provider_registry = action_provider_registry
        app.state.action_gateway = action_gateway
        app.state.quarantine_resolution_service = quarantine_resolution_service
        app.state.authenticator = resolved_authenticator
        app.state.authorizer = resolved_authorizer
        yield
        manager.stop()

    app = FastAPI(
        title="Agent Runtime Reliability Platform",
        description=(
            "Typed domain runtimes sharing one durable run lifecycle and unified API, "
            "plus policy-enforced tools, governed cross-thread memory and durable "
            "external actions."
        ),
        version="1.3.0",
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

    def require_action_principal(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    ) -> Principal:
        api_key = None
        if credentials is not None and credentials.scheme.lower() == "bearer":
            api_key = credentials.credentials
        try:
            return resolved_authenticator.authenticate(api_key)
        except AuthenticationError:
            raise ActionRouteError(
                status.HTTP_401_UNAUTHORIZED,
                "invalid_api_key",
                "Invalid or missing API key.",
                headers={"WWW-Authenticate": "Bearer"},
            ) from None

    def require_action_permission(
        principal: Principal,
        permission: RuntimePermission,
    ) -> None:
        try:
            resolved_authorizer.authorize(principal, permission)
        except AuthorizationError:
            raise ActionRouteError(
                status.HTTP_403_FORBIDDEN,
                "operation_not_permitted",
                "Operation not permitted.",
            ) from None

    def public_run_or_none(request: Request, run: RunRecord | None) -> RunRecord | None:
        if run is None:
            return None
        try:
            registration = request.app.state.agent_registry.registration(
                run.agent_id,
                run.agent_version,
            )
        except KeyError:
            return None
        return run if registration.public_runs_api else None

    @app.exception_handler(ActionRouteError)
    async def render_action_error(
        _request: Request,
        exc: ActionRouteError,
    ) -> JSONResponse:
        envelope = ActionApiErrorEnvelope(
            error=ActionApiErrorBody(
                code=exc.code,
                message=exc.message,
                details=exc.details,
            )
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=envelope.model_dump(mode="json", exclude_none=True),
            headers=exc.headers,
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready(request: Request) -> dict[str, object]:
        request.app.state.run_store.ping()
        request.app.state.memory_store.ping()
        request.app.state.workflow_store.ping()
        result: dict[str, object] = {
            "status": "ready",
            "storage": request.app.state.storage_metadata.public_dict(),
        }
        if request.app.state.release_identity is not None:
            result["release"] = request.app.state.release_identity.public_dict()
        return result

    if demo_session is not None:
        assets_path = demo_assets_path()

        @app.get("/", include_in_schema=False)
        def demo_root() -> RedirectResponse:
            return RedirectResponse("/demo")

        @app.get("/demo", include_in_schema=False)
        def runtime_console() -> FileResponse:
            return FileResponse(
                assets_path / "index.html",
                media_type="text/html",
                headers={"Cache-Control": "no-store"},
            )

        @app.get(
            "/demo/session",
            response_model=DemoSession,
            include_in_schema=False,
        )
        def demo_browser_session(response: Response) -> DemoSession:
            response.headers["Cache-Control"] = "no-store"
            return demo_session

        app.mount(
            "/demo-assets",
            NoStoreStaticFiles(directory=assets_path),
            name="demo-assets",
        )

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
        store: RunStore = request.app.state.run_store
        if (
            payload.run_id is not None
            and public_run_or_none(
                request,
                store.get_run_for_tenant(payload.run_id, principal.tenant_id),
            )
            is None
        ):
            raise HTTPException(status_code=404, detail="Run not found")
        require_permission(principal, RuntimePermission.TOOLS_EXECUTE)
        spec = request.app.state.external_action_tool_registry.resolve(tool_name)
        if spec is not None and spec.effect == ToolEffect.EXTERNAL_WRITE:
            require_permission(
                principal,
                RuntimePermission.EXTERNAL_ACTIONS_EXECUTE,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="External-write tools require the durable run lifecycle",
            )
        if payload.run_id is not None:
            store.append_control_plane_event(
                payload.run_id,
                tenant_id=principal.tenant_id,
                event_type="sandbox.execution_started",
                payload={"tool_name": tool_name},
            )

        sandbox: ToolSandbox = request.app.state.tool_sandbox
        result = sandbox.execute(tool_name, payload.arguments)

        if payload.run_id is not None:
            store.append_control_plane_event(
                payload.run_id,
                tenant_id=principal.tenant_id,
                event_type="sandbox.execution_finished",
                payload={
                    "tool_name": tool_name,
                    "execution_id": result.execution_id,
                    "status": result.status.value,
                    "duration_ms": result.duration_ms,
                },
            )
        return result

    def validate_agent_message_wait(request: Request, wait: float) -> float:
        values = request.query_params.getlist("wait")
        if len(values) > 1:
            raise HTTPException(
                status_code=422,
                detail="wait must be a number between 0 and 5 seconds",
            )
        return wait

    async def get_agent_message_run(
        request: Request,
        run_id: str,
        principal: Principal,
        *,
        operation_timeout_seconds: float,
    ) -> RunRecord | None:
        try:
            with anyio.fail_after(operation_timeout_seconds):
                current = await anyio.to_thread.run_sync(
                    lambda: get_manager(request).get_run(
                        run_id,
                        tenant_context=principal.tenant_context,
                        operation_timeout_seconds=operation_timeout_seconds,
                    ),
                    abandon_on_cancel=True,
                )
        except TimeoutError:
            # Submission already committed. Database contention or an over-budget
            # observation must end compatibility waiting, not fail or cancel the Run.
            return None
        except Exception as exc:
            if is_run_store_contention_error(exc):
                return None
            if not is_run_store_error(exc):
                raise
            raise HTTPException(
                status_code=500,
                detail="Managed Run evidence is incomplete",
                headers={"Location": f"/runs/{run_id}"},
            ) from None
        if current is None:
            raise HTTPException(
                status_code=500,
                detail="Managed Run evidence is incomplete",
                headers={"Location": f"/runs/{run_id}"},
            )
        return current

    async def wait_for_agent_message_run(
        request: Request,
        run: RunRecord,
        principal: Principal,
        wait_seconds: float,
    ) -> RunRecord:
        if wait_seconds <= 0 or run.status.is_terminal:
            return run
        semaphore: anyio.Semaphore = request.app.state.agent_message_waiter_semaphore
        try:
            semaphore.acquire_nowait()
        except anyio.WouldBlock:
            return run
        try:
            deadline = anyio.current_time() + wait_seconds
            current = run
            poll_delay = 0.05
            while not current.status.is_terminal:
                remaining = deadline - anyio.current_time()
                if remaining <= 0:
                    break
                await anyio.sleep(min(poll_delay, remaining))
                remaining = deadline - anyio.current_time()
                if remaining <= 0:
                    break
                refreshed = await get_agent_message_run(
                    request,
                    current.run_id,
                    principal,
                    operation_timeout_seconds=max(0.001, min(0.25, remaining)),
                )
                if refreshed is None:
                    break
                current = refreshed
                poll_delay = min(0.4, poll_delay * 1.5)
            return current
        finally:
            semaphore.release()

    def agent_message_pending_response(run: RunRecord) -> JSONResponse:
        pending = AgentMessagePendingResponse(
            run_id=run.run_id,
            status=run.status.value,
        )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=pending.model_dump(mode="json"),
            headers={
                "Location": f"/runs/{run.run_id}",
                "Retry-After": "1",
            },
        )

    @app.post(
        "/agent/message",
        response_model=AgentMessageResponse,
        responses={202: {"model": AgentMessagePendingResponse}},
    )
    async def handle_agent_message(
        payload: AgentMessageRequest,
        request: Request,
        wait: Annotated[
            float,
            Query(ge=0, le=5, allow_inf_nan=False),
        ] = 5.0,
        principal: Principal = Depends(require_principal),
    ) -> AgentMessageResponse | JSONResponse:
        """Bounded-wait Travel adapter backed by the managed Run lifecycle."""
        require_permission(principal, RuntimePermission.AGENT_MESSAGE_EXECUTE)
        wait_seconds = validate_agent_message_wait(request, wait)
        store: RunStore = request.app.state.run_store
        if payload.state is not None and payload.state.thread_id != payload.thread_id:
            raise HTTPException(
                status_code=422,
                detail="state.thread_id must match request.thread_id",
            )
        try:
            # Preserve the compatibility endpoint's immediate cross-domain
            # conflict while execution itself now goes through a managed Run.
            await anyio.to_thread.run_sync(
                lambda: store.load_thread_state(
                    payload.thread_id,
                    tenant_id=principal.tenant_id,
                    domain_id=AgentState.domain_id,
                    schema_version=AgentState.schema_version,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        authority = effective_execution_authority(principal, resolved_authorizer)
        try:
            submitted = await anyio.to_thread.run_sync(
                lambda: get_manager(request).submit(
                    RunCreateRequest(
                        thread_id=payload.thread_id,
                        agent_id="travel-agent",
                        agent_version="0.3.0",
                        input={"user_message": payload.user_message},
                        state=payload.state,
                    ),
                    tenant_context=TenantContext(
                        tenant_id=authority.tenant_id,
                        subject_id=authority.subject_id,
                        permissions=authority.permissions,
                    ),
                )
            )
        except ThreadStateConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        completed = await wait_for_agent_message_run(
            request,
            submitted,
            principal,
            wait_seconds,
        )
        if not completed.status.is_terminal:
            return agent_message_pending_response(completed)

        if completed.status != RunStatus.COMPLETED or completed.state is None:
            raise HTTPException(
                status_code=500,
                detail="Managed Run did not complete successfully",
                headers={"Location": f"/runs/{completed.run_id}"},
            )
        try:
            result_state = AgentState.model_validate(completed.state)
        except ValidationError:
            raise HTTPException(
                status_code=500,
                detail="Managed Run evidence is incomplete",
                headers={"Location": f"/runs/{completed.run_id}"},
            ) from None
        return AgentMessageResponse(
            assistant_message=completed.output_message or "",
            updated_state=result_state,
            validation_errors=completed.validation_errors,
        )

    @app.post("/runs", response_model=RunRecord, status_code=status.HTTP_202_ACCEPTED)
    def create_run(
        payload: RunCreateRequest,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> RunRecord | JSONResponse:
        require_permission(principal, RuntimePermission.RUNS_CREATE)
        if (
            payload.client_request_id is not None
            and payload.client_request_id.startswith(ACTION_REQUEST_NAMESPACE_PREFIX)
        ):
            return JSONResponse(
                status_code=422,
                content={
                    "error": {
                        "code": "reserved_client_request_namespace",
                        "message": (
                            "client_request_id uses a namespace reserved for the Action API."
                        ),
                    }
                },
            )
        try:
            registration = request.app.state.agent_registry.registration(
                payload.agent_id,
                payload.agent_version,
            )
        except KeyError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if not registration.public_runs_api:
            raise HTTPException(
                status_code=422,
                detail="Agent version is not available through /runs",
            )
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
        except ThreadStateConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post(
        "/operator/quarantine-resolutions",
        response_model=QuarantineResolutionResponse,
    )
    def resolve_quarantine(
        payload: QuarantineResolutionCommand,
        request: Request,
        principal: Principal = Depends(require_action_principal),
    ) -> QuarantineResolutionResponse:
        service: QuarantineResolutionService = (
            request.app.state.quarantine_resolution_service
        )
        if not service.target_is_visible(payload, tenant_id=principal.tenant_id):
            raise ActionRouteError(
                status.HTTP_404_NOT_FOUND,
                "quarantine_target_not_found",
                "Quarantine target not found.",
            )
        require_action_permission(
            principal,
            RuntimePermission.QUARANTINE_RESOLVE,
        )
        try:
            return service.execute(payload, principal=principal)
        except QuarantineTargetNotFoundError:
            raise ActionRouteError(
                status.HTTP_404_NOT_FOUND,
                "quarantine_target_not_found",
                "Quarantine target not found.",
            ) from None
        except QuarantineResolutionStalePlanError as exc:
            raise ActionRouteError(
                status.HTTP_409_CONFLICT,
                "quarantine_resolution_plan_stale",
                "The quarantine resolution plan is stale; run dry-run again.",
                details={
                    "current_plan": exc.current_plan.model_dump(mode="json"),
                },
            ) from None
        except QuarantineResolutionEvidenceIncompleteError:
            raise ActionRouteError(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "quarantine_resolution_evidence_incomplete",
                "The quarantine resolution's durable evidence is incomplete.",
            ) from None

    @app.get("/runs/{run_id}", response_model=RunRecord)
    def get_run(
        run_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> RunRecord:
        run = public_run_or_none(
            request,
            get_manager(request).get_run(
                run_id,
                tenant_context=principal.tenant_context,
            ),
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
        if public_run_or_none(
            request,
            get_manager(request).get_run(
                run_id,
                tenant_context=principal.tenant_context,
            ),
        ) is None:
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
        if public_run_or_none(
            request,
            get_manager(request).get_run(
                run_id,
                tenant_context=principal.tenant_context,
            ),
        ) is None:
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
        if public_run_or_none(
            request,
            manager.get_run(run_id, tenant_context=principal.tenant_context),
        ) is None:
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

    def action_gateway_error(exc: Exception) -> ActionRouteError:
        if isinstance(exc, ActionTypeNotRegisteredError):
            return ActionRouteError(
                422,
                "action_type_not_registered",
                "The requested Action type is not registered.",
            )
        if isinstance(exc, DestinationNotRegisteredError):
            return ActionRouteError(
                422,
                "destination_not_registered",
                "The requested destination alias is not registered.",
            )
        if isinstance(exc, IdempotencyKeyReusedError):
            return ActionRouteError(
                status.HTTP_409_CONFLICT,
                "idempotency_key_reused",
                "The idempotency key is already bound to a different action request.",
            )
        return ActionRouteError(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "action_evidence_incomplete",
            "The Action's durable evidence is incomplete.",
        )

    async def parse_action_request(request: Request) -> ActionCreateRequest:
        try:
            raw_payload = await request.json()
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "The Action request body is invalid.",
            ) from None
        if not isinstance(raw_payload, dict):
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "The Action request body is invalid.",
            )
        try:
            return ActionCreateRequest.model_validate(raw_payload)
        except ValidationError:
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "The Action request body is invalid.",
            ) from None

    def parse_wait(request: Request) -> float:
        values = request.query_params.getlist("wait")
        if len(values) > 1:
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "wait must be a number between 0 and 5 seconds.",
            )
        try:
            wait = float(values[0]) if values else 0.0
        except ValueError:
            wait = math.nan
        if not math.isfinite(wait) or not 0 <= wait <= 5:
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "wait must be a number between 0 and 5 seconds.",
            )
        return wait

    def parse_after_sequence(request: Request) -> int:
        values = request.query_params.getlist("after_sequence")
        if len(values) > 1:
            candidate = -1
        else:
            try:
                candidate = int(values[0]) if values else 0
            except ValueError:
                candidate = -1
        if candidate < 0:
            raise ActionRouteError(
                422,
                "invalid_action_input",
                "after_sequence must be a non-negative integer.",
            )
        return candidate

    async def get_action_resource(
        request: Request,
        action_id: str,
        principal: Principal,
    ) -> ActionResource | None:
        gateway: DurableActionGateway = request.app.state.action_gateway
        try:
            return await anyio.to_thread.run_sync(
                lambda: gateway.get(
                    action_id,
                    tenant_context=principal.tenant_context,
                )
            )
        except ActionEvidenceIncompleteError as exc:
            raise action_gateway_error(exc) from None

    async def action_exists(
        request: Request,
        action_id: str,
        principal: Principal,
    ) -> bool:
        gateway: DurableActionGateway = request.app.state.action_gateway
        return await anyio.to_thread.run_sync(
            lambda: gateway.exists(
                action_id,
                tenant_context=principal.tenant_context,
            )
        )

    async def wait_for_action(
        request: Request,
        action: ActionResource,
        principal: Principal,
        wait_seconds: float,
    ) -> ActionResource:
        if wait_seconds <= 0 or action.status.is_terminal:
            return action
        semaphore: anyio.Semaphore = request.app.state.action_waiter_semaphore
        try:
            semaphore.acquire_nowait()
        except anyio.WouldBlock:
            return action
        try:
            deadline = anyio.current_time() + wait_seconds
            current = action
            poll_delay = 0.05
            while not current.status.is_terminal:
                remaining = deadline - anyio.current_time()
                if remaining <= 0:
                    break
                await anyio.sleep(min(poll_delay, remaining))
                refreshed = await get_action_resource(
                    request,
                    current.action_id,
                    principal,
                )
                if refreshed is None:
                    raise ActionRouteError(
                        status.HTTP_500_INTERNAL_SERVER_ERROR,
                        "action_evidence_incomplete",
                        "The Action's durable evidence is incomplete.",
                    )
                current = refreshed
                poll_delay = min(0.4, poll_delay * 1.5)
            return current
        finally:
            semaphore.release()

    def action_response(action: ActionResource) -> JSONResponse:
        if action.status.is_terminal:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=action.model_dump(mode="json"),
            )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=action.model_dump(mode="json"),
            headers={
                "Location": f"/actions/{action.action_id}",
                "Retry-After": "1",
            },
        )

    app.state.action_waiter_semaphore = anyio.Semaphore(
        resolved_action_waiter_limit
    )
    app.state.agent_message_waiter_semaphore = anyio.Semaphore(
        resolved_agent_message_waiter_limit
    )

    @app.post("/actions", response_model=ActionResource)
    async def create_action(
        request: Request,
        principal: Principal = Depends(require_action_principal),
    ) -> JSONResponse:
        require_action_permission(principal, RuntimePermission.RUNS_CREATE)
        require_action_permission(
            principal,
            RuntimePermission.EXTERNAL_ACTIONS_EXECUTE,
        )
        require_action_permission(principal, RuntimePermission.TOOLS_EXECUTE)
        payload = await parse_action_request(request)
        wait_seconds = parse_wait(request)
        authority = effective_execution_authority(principal, resolved_authorizer)
        tenant_context = TenantContext(
            tenant_id=authority.tenant_id,
            subject_id=authority.subject_id,
            permissions=authority.permissions,
        )
        gateway: DurableActionGateway = request.app.state.action_gateway
        try:
            submitted = await anyio.to_thread.run_sync(
                lambda: gateway.submit(payload, tenant_context=tenant_context)
            )
        except (
            ActionTypeNotRegisteredError,
            DestinationNotRegisteredError,
            IdempotencyKeyReusedError,
            ActionEvidenceIncompleteError,
        ) as exc:
            raise action_gateway_error(exc) from None
        action = await wait_for_action(
            request,
            submitted.resource,
            principal,
            wait_seconds,
        )
        return action_response(action)

    @app.get("/actions/{action_id}", response_model=ActionResource)
    async def get_action(
        action_id: str,
        request: Request,
        principal: Principal = Depends(require_action_principal),
    ) -> ActionResource:
        if not await action_exists(request, action_id, principal):
            raise ActionRouteError(
                status.HTTP_404_NOT_FOUND,
                "action_not_found",
                "Action not found.",
            )
        require_action_permission(principal, RuntimePermission.RUNS_READ)
        action = await get_action_resource(request, action_id, principal)
        if action is None:  # pragma: no cover - tenant lookup was just verified
            raise ActionRouteError(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "action_evidence_incomplete",
                "The Action's durable evidence is incomplete.",
            )
        return action

    @app.get("/actions/{action_id}/events", response_model=list[ActionEvent])
    async def list_action_events(
        action_id: str,
        request: Request,
        principal: Principal = Depends(require_action_principal),
    ) -> list[ActionEvent]:
        gateway: DurableActionGateway = request.app.state.action_gateway
        if not await action_exists(request, action_id, principal):
            raise ActionRouteError(
                status.HTTP_404_NOT_FOUND,
                "action_not_found",
                "Action not found.",
            )
        require_action_permission(principal, RuntimePermission.RUN_EVENTS_READ)
        if await get_action_resource(request, action_id, principal) is None:
            raise ActionRouteError(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "action_evidence_incomplete",
                "The Action's durable evidence is incomplete.",
            )
        after_sequence = parse_after_sequence(request)
        try:
            events = await anyio.to_thread.run_sync(
                lambda: gateway.list_events(
                    action_id,
                    tenant_context=principal.tenant_context,
                    after_sequence=after_sequence,
                )
            )
        except ActionEvidenceIncompleteError as exc:
            raise action_gateway_error(exc) from None
        if events is None:  # pragma: no cover - tenant lookup was just verified
            raise ActionRouteError(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "action_evidence_incomplete",
                "The Action's durable evidence is incomplete.",
            )
        return events

    @app.get("/threads/{thread_id}/state")
    def get_thread_state(
        thread_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
        domain_id: str = Query(default="travel"),
        schema_version: str = Query(default="1"),
    ) -> dict:
        require_permission(principal, RuntimePermission.THREAD_STATE_READ)
        if domain_id == ACTION_DOMAIN_ID:
            raise HTTPException(status_code=404, detail="Thread state not found")
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

    @app.get("/memories", response_model=list[MemoryRecord])
    def list_memories(
        request: Request,
        principal: Principal = Depends(require_principal),
        domain_id: str | None = Query(default=None),
        kind: MemoryKind | None = Query(default=None),
        include_inactive: bool = Query(default=False),
    ) -> list[MemoryRecord]:
        require_permission(principal, RuntimePermission.MEMORY_READ)
        memory_store: MemoryAdminStore = request.app.state.memory_store
        return memory_store.list_memories(
            tenant_id=principal.tenant_id,
            subject_id=principal.subject_id,
            domain_id=domain_id,
            kind=kind,
            include_inactive=include_inactive,
        )

    @app.delete("/memories/{memory_id}", response_model=MemoryRecord)
    def forget_memory(
        memory_id: str,
        request: Request,
        principal: Principal = Depends(require_principal),
    ) -> MemoryRecord:
        memory_store: MemoryAdminStore = request.app.state.memory_store
        if (
            memory_store.get_memory_for_subject(
                memory_id,
                tenant_id=principal.tenant_id,
                subject_id=principal.subject_id,
            )
            is None
        ):
            raise HTTPException(status_code=404, detail="Memory not found")
        require_permission(principal, RuntimePermission.MEMORY_DELETE)
        try:
            return memory_store.forget_memory(
                memory_id,
                tenant_id=principal.tenant_id,
                subject_id=principal.subject_id,
                actor_subject_id=principal.subject_id,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Memory not found") from exc

    return app


app = create_app()
