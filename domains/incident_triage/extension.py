from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from runtime_service.dynamic_loop import DynamicToolLoop
from runtime_service.extensions import RuntimeExtensionContext
from runtime_service.planner import Planner
from runtime_service.sandbox import ToolSandbox

from .models import IncidentTriageInput, IncidentTriageState
from .planner import ScriptedIncidentTriagePlanner
from .runtime import IncidentTriageRuntime
from .tools import build_incident_triage_tool_registry


@dataclass(frozen=True)
class IncidentTriageExtension:
    """Register one trusted, optional incident-triage Agent version."""

    planner_factory: Callable[[], Planner] = ScriptedIncidentTriagePlanner

    def register(self, context: RuntimeExtensionContext) -> None:
        def runtime_factory() -> IncidentTriageRuntime:
            tool_registry = build_incident_triage_tool_registry()
            loop = DynamicToolLoop(
                planner=self.planner_factory(),
                tool_registry=tool_registry,
                tool_sandbox=ToolSandbox(tool_registry),
                workflow_store=context.workflow_store,
                run_event_sink=context.run_event_sink,
                workflow_type="dynamic-tool-loop:incident-triage:1.0.0",
                max_tool_calls=1,
            )
            return IncidentTriageRuntime(loop)

        context.registry.register(
            "incident-triage",
            "1.0.0",
            runtime_factory,
            description=(
                "Optional synthetic incident-triage reference extension with a typed "
                "Planner, allowlisted read-only evidence and deterministic validation."
            ),
            input_model=IncidentTriageInput,
            state_model=IncidentTriageState,
        )
