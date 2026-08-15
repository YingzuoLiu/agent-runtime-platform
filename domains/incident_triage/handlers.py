from __future__ import annotations

from typing import Any

from .models import IncidentSignalEvidence, InspectIncidentSignalInput
from .policy import classify_risk, synthetic_signal_for


def inspect_incident_signal(payload: dict[str, Any]) -> dict[str, Any]:
    """Return deterministic evidence; no telemetry system is contacted."""

    request = InspectIncidentSignalInput.model_validate(payload)
    severity, error_rate_percent, recent_deployment = synthetic_signal_for(
        request.service
    )
    evidence = IncidentSignalEvidence(
        source="synthetic_incident_fixture",
        alert_id=request.alert_id,
        service=request.service,
        severity=severity,
        error_rate_percent=error_rate_percent,
        recent_deployment=recent_deployment,
        risk_level=classify_risk(
            severity=severity,
            error_rate_percent=error_rate_percent,
        ),
    )
    return evidence.model_dump(mode="json")
