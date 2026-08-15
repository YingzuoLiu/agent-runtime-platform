from __future__ import annotations

from runtime_service.sandbox import ToolPolicy, ToolRegistry, ToolSpec

from .models import InspectIncidentSignalInput


def build_incident_triage_tool_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="inspect_incident_signal",
            description=(
                "Inspect a deterministic synthetic incident signal without contacting "
                "Prometheus, Alertmanager or Kubernetes."
            ),
            input_model=InspectIncidentSignalInput,
            policy=ToolPolicy(timeout_seconds=2.0),
            handler_entrypoint=(
                "domains.incident_triage.handlers:inspect_incident_signal"
            ),
        )
    )
    return registry
