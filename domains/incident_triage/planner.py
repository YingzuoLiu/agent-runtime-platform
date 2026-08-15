from __future__ import annotations

from runtime_service.planner import (
    CallToolDecision,
    FinishDecision,
    PlannerContext,
    RequestClarificationDecision,
)

from .models import IncidentSignalEvidence, IncidentTriageInput
from .policy import recommend_action


class ScriptedIncidentTriagePlanner:
    """Stateless Planner for the optional offline reference extension."""

    def decide(self, context: PlannerContext):
        runtime_input = IncidentTriageInput.model_validate(context.runtime_input)
        if runtime_input.service is None:
            return RequestClarificationDecision(
                question="Which service emitted this incident alert?",
                reason="service_is_required_for_incident_triage",
            )

        observation = next(
            (
                item
                for item in context.observations
                if item.tool_name == "inspect_incident_signal"
            ),
            None,
        )
        if observation is None:
            return CallToolDecision(
                tool_name="inspect_incident_signal",
                arguments={
                    "alert_id": runtime_input.alert_id,
                    "service": runtime_input.service,
                },
                reason="incident_triage_requires_registered_signal_evidence",
            )

        evidence = IncidentSignalEvidence.model_validate(observation.result)
        return FinishDecision(
            message="Synthetic incident evidence is ready for deterministic validation.",
            output={
                "alert_id": runtime_input.alert_id,
                "recommended_action": recommend_action(evidence),
            },
            reason="recommendation_is_grounded_in_registered_tool_evidence",
        )
