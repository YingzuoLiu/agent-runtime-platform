from .dag import WorkflowDag, WorkflowGraphError, WorkflowNode
from .auth import (
    ApiKeyCredential,
    AuthenticationError,
    Authenticator,
    AuthorizationError,
    Authorizer,
    Principal,
    RoleAuthorizer,
    RuntimePermission,
    RuntimeRole,
    StaticApiKeyAuthenticator,
    TenantContext,
    effective_execution_authority,
)
from .dynamic_loop import (
    DynamicLoopOutcome,
    DynamicLoopResult,
    DynamicToolLoop,
    FinishEvaluation,
)
from .manager import ReferencedRunNotFoundError, RuntimeManager
from .memory import (
    GovernedMemory,
    MemoryEvent,
    MemoryKind,
    MemoryMutationAction,
    MemoryMutationResult,
    MemoryRecord,
    MemorySnapshot,
    MemoryStatus,
    MemoryStore,
    MemoryWrite,
    RetrievedMemory,
    SQLiteMemoryStore,
)
from .models import AgentDescriptor, RunCreateRequest, RunEvent, RunRecord, RunStatus
from .planner import (
    CallToolDecision,
    FinishDecision,
    Planner,
    PlannerContext,
    PlannerProviderError,
    RequestClarificationDecision,
    ToolObservation,
)
from .registry import AgentRegistry, build_default_registry
from .sandbox import (
    ToolDescriptor,
    ToolExecutionRequest,
    ToolExecutionResult,
    ToolExecutionStatus,
    ToolPolicy,
    ToolRegistry,
    ToolSandbox,
    ToolSpec,
)
from .store import SQLiteRunStore


def build_default_tool_registry() -> ToolRegistry:
    """Compatibility composition wrapper for the Travel tool registry.

    Tool schemas and handler entrypoints moved out of Core in Phase 5A. Keep
    the original package-level builder import working while new code uses the
    domain-owned ``build_travel_tool_registry`` name directly.
    """

    from domains.travel.tools import build_travel_tool_registry

    return build_travel_tool_registry()


__all__ = [
    "AgentDescriptor",
    "AgentRegistry",
    "ApiKeyCredential",
    "AuthenticationError",
    "Authenticator",
    "AuthorizationError",
    "Authorizer",
    "CallToolDecision",
    "DynamicLoopOutcome",
    "DynamicLoopResult",
    "DynamicToolLoop",
    "FinishDecision",
    "FinishEvaluation",
    "GovernedMemory",
    "MemoryEvent",
    "MemoryKind",
    "MemoryMutationAction",
    "MemoryMutationResult",
    "MemoryRecord",
    "MemorySnapshot",
    "MemoryStatus",
    "MemoryStore",
    "MemoryWrite",
    "Planner",
    "PlannerContext",
    "PlannerProviderError",
    "Principal",
    "ReferencedRunNotFoundError",
    "RunCreateRequest",
    "RunEvent",
    "RunRecord",
    "RunStatus",
    "RetrievedMemory",
    "RuntimeManager",
    "RoleAuthorizer",
    "RuntimePermission",
    "RuntimeRole",
    "RequestClarificationDecision",
    "SQLiteRunStore",
    "SQLiteMemoryStore",
    "StaticApiKeyAuthenticator",
    "TenantContext",
    "ToolDescriptor",
    "ToolExecutionRequest",
    "ToolExecutionResult",
    "ToolExecutionStatus",
    "ToolPolicy",
    "ToolRegistry",
    "ToolSandbox",
    "ToolSpec",
    "ToolObservation",
    "WorkflowDag",
    "WorkflowGraphError",
    "WorkflowNode",
    "build_default_registry",
    "build_default_tool_registry",
    "effective_execution_authority",
]
