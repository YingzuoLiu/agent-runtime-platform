from __future__ import annotations

from .models import (
    IncidentAction,
    IncidentRisk,
    IncidentSeverity,
    IncidentSignalEvidence,
    SupportedIncidentService,
)


SYNTHETIC_SIGNAL_FIXTURES: dict[
    SupportedIncidentService,
    tuple[IncidentSeverity, float, bool],
] = {
    SupportedIncidentService.CATALOG_API: ("warning", 0.8, False),
    SupportedIncidentService.CHECKOUT_API: ("critical", 8.0, True),
}

if set(SYNTHETIC_SIGNAL_FIXTURES) != set(SupportedIncidentService):
    raise RuntimeError("Every supported incident service requires one fixture")


def synthetic_signal_for(
    service: SupportedIncidentService,
) -> tuple[IncidentSeverity, float, bool]:
    """Return the server-owned offline fixture for one allowlisted service."""

    return SYNTHETIC_SIGNAL_FIXTURES[service]


def classify_risk(
    *,
    severity: IncidentSeverity,
    error_rate_percent: float,
) -> IncidentRisk:
    if severity == "critical" and error_rate_percent >= 5:
        return "high"
    if severity == "critical" or error_rate_percent >= 2:
        return "elevated"
    return "low"


def recommend_action(evidence: IncidentSignalEvidence) -> IncidentAction:
    if evidence.risk_level == "high" and evidence.recent_deployment:
        return "prepare_rollback_review"
    if evidence.risk_level in {"high", "elevated"}:
        return "investigate"
    return "observe"
