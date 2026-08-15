from __future__ import annotations

from agent.contracts import RuntimeExecutionContext, RuntimeResponse, TraceEvent
from runtime_service.dynamic_loop import (
    DynamicLoopOutcome,
    DynamicToolLoop,
    FinishEvaluation,
)
from runtime_service.planner import FinishDecision, ToolObservation

from .models import (
    IncidentFinishPayload,
    IncidentSignalEvidence,
    IncidentTriageInput,
    IncidentTriageResult,
    IncidentTriageState,
    InspectIncidentSignalInput,
)
from .policy import (
    classify_risk,
    recommend_action,
    synthetic_signal_for,
)


class IncidentTriageRuntime:
    """Managed runtime adapter for the optional incident-triage extension."""

    def __init__(self, loop: DynamicToolLoop) -> None:
        self.loop = loop

    def initial_state(self, thread_id: str) -> IncidentTriageState:
        return IncidentTriageState(thread_id=thread_id)

    def execute(
        self,
        state: IncidentTriageState,
        runtime_input: IncidentTriageInput,
        context: RuntimeExecutionContext,
    ) -> RuntimeResponse[IncidentTriageState]:
        prepared = state.model_copy(
            update={
                "alert_id": runtime_input.alert_id,
                "service": runtime_input.service,
                "claimed_severity": runtime_input.severity,
                "claimed_error_rate_percent": runtime_input.error_rate_percent,
                "claimed_recent_deployment": runtime_input.recent_deployment,
                "result": None,
                "tool_outputs": {},
                "blockers": [],
                "current_stage": "triaging",
                # Run Events and Workflow rows are the evidence authority. Do not
                # carry caller-supplied trace entries into this reference result.
                "execution_trace": [],
            },
            deep=True,
        )
        loop_result = self.loop.execute(
            runtime_input=runtime_input.model_dump(mode="json"),
            state=prepared.model_dump(mode="json"),
            context=context,
            finish_evaluator=lambda decision, observations: self._evaluate_finish(
                prepared,
                decision,
                observations,
            ),
        )
        tool_outputs = {
            observation.step_id: {
                "tool_name": observation.tool_name,
                "arguments": observation.arguments,
                "result": observation.result,
                "cached": observation.cached,
            }
            for observation in loop_result.observations
        }
        triage_payload = (
            loop_result.output.get("triage")
            if loop_result.outcome == DynamicLoopOutcome.FINISHED
            else None
        )
        triage_result = (
            IncidentTriageResult.model_validate(triage_payload)
            if isinstance(triage_payload, dict)
            else None
        )
        if loop_result.outcome == DynamicLoopOutcome.CLARIFICATION:
            current_stage = "needs_clarification"
            blockers = [loop_result.message]
        elif loop_result.outcome == DynamicLoopOutcome.BLOCKED:
            current_stage = "blocked"
            blockers = loop_result.validation_errors
        else:
            current_stage = "triaged"
            blockers = []
        updated = prepared.model_copy(
            update={
                "result": triage_result,
                "tool_outputs": tool_outputs,
                "blockers": blockers,
                "current_stage": current_stage,
                "execution_trace": [
                    *prepared.execution_trace,
                    TraceEvent(
                        event="incident_triage_finished",
                        reason=loop_result.outcome.value,
                        payload={
                            "tool_call_count": len(loop_result.observations),
                            "validation_errors": loop_result.validation_errors,
                        },
                    ),
                ],
            },
            deep=True,
        )
        return RuntimeResponse[IncidentTriageState](
            message=loop_result.message,
            state=updated,
            validation_errors=loop_result.validation_errors,
        )

    def _evaluate_finish(
        self,
        state: IncidentTriageState,
        decision: FinishDecision,
        observations: list[ToolObservation],
    ) -> FinishEvaluation:
        payload = IncidentFinishPayload.model_validate(decision.output)
        observation = next(
            (
                item
                for item in observations
                if item.tool_name == "inspect_incident_signal"
            ),
            None,
        )
        if observation is None:
            raise ValueError("FINISH requires inspect_incident_signal evidence")
        evidence = IncidentSignalEvidence.model_validate(observation.result)
        tool_input = InspectIncidentSignalInput.model_validate(observation.arguments)
        errors: list[str] = []
        if payload.alert_id != state.alert_id or evidence.alert_id != state.alert_id:
            errors.append("Incident evidence does not match the requested alert id")
        if tool_input.alert_id != state.alert_id or evidence.alert_id != tool_input.alert_id:
            errors.append("Incident tool arguments do not match the requested alert id")
        if evidence.service != state.service:
            errors.append("Incident evidence does not match the requested service")
        if tool_input.service != state.service or evidence.service != tool_input.service:
            errors.append("Incident tool arguments do not match the requested service")
        if evidence.severity != state.claimed_severity:
            errors.append("Incident evidence does not match the requested severity")
        if evidence.error_rate_percent != state.claimed_error_rate_percent:
            errors.append("Incident evidence does not match the requested error rate")
        if evidence.recent_deployment != state.claimed_recent_deployment:
            errors.append("Incident evidence does not match deployment recency")
        fixture_severity, fixture_error_rate, fixture_recent_deployment = (
            synthetic_signal_for(evidence.service)
        )
        if (
            evidence.severity,
            evidence.error_rate_percent,
            evidence.recent_deployment,
        ) != (
            fixture_severity,
            fixture_error_rate,
            fixture_recent_deployment,
        ):
            errors.append("Incident evidence does not match the server-owned fixture")
        expected_risk = classify_risk(
            severity=evidence.severity,
            error_rate_percent=evidence.error_rate_percent,
        )
        if evidence.risk_level != expected_risk:
            errors.append("Incident risk does not match deterministic policy")
        expected_action = recommend_action(evidence)
        if payload.recommended_action != expected_action:
            errors.append("Planner recommendation does not match deterministic policy")
        triage = IncidentTriageResult(
            alert_id=evidence.alert_id,
            service=evidence.service,
            risk_level=expected_risk,
            recommended_action=expected_action,
            evidence_source=evidence.source,
            action_executed=False,
        )
        output = {"triage": triage.model_dump(mode="json")}
        if errors:
            return FinishEvaluation(
                outcome=DynamicLoopOutcome.BLOCKED,
                message="Incident triage was blocked by deterministic validation.",
                output={},
                validation_errors=errors,
            )
        return FinishEvaluation(
            outcome=DynamicLoopOutcome.FINISHED,
            message=(
                f"Incident {triage.alert_id} triaged: "
                f"{triage.recommended_action.replace('_', ' ')}. "
                "No external action was executed."
            ),
            output=output,
        )
