from __future__ import annotations

from .action_gateway import is_action_run
from .auth import Principal
from .quarantine import (
    QuarantineResolutionCommand,
    QuarantineResolutionResponse,
    QuarantineTargetKind,
    QuarantineTargetNotFoundError,
)
from .registry import AgentRegistry
from .store import SQLiteRunStore


class QuarantineResolutionService:
    """Deterministic operator service; it never invokes Runtime or a provider."""

    def __init__(self, *, store: SQLiteRunStore, registry: AgentRegistry) -> None:
        self.store = store
        self.registry = registry

    def target_is_visible(
        self,
        command: QuarantineResolutionCommand,
        *,
        tenant_id: str,
    ) -> bool:
        run = self.store.get_run_for_tenant(command.target.identifier, tenant_id)
        if run is None:
            return False
        if command.target.kind == QuarantineTargetKind.ACTION:
            return is_action_run(run)
        try:
            registration = self.registry.registration(run.agent_id, run.agent_version)
        except KeyError:
            return False
        return registration.public_runs_api

    def execute(
        self,
        command: QuarantineResolutionCommand,
        *,
        principal: Principal,
    ) -> QuarantineResolutionResponse:
        if not self.target_is_visible(command, tenant_id=principal.tenant_id):
            raise QuarantineTargetNotFoundError("Quarantine target not found")

        if command.dry_run:
            try:
                plan = self.store.plan_quarantine_resolution(
                    command.target.identifier,
                    tenant_id=principal.tenant_id,
                    target=command.target,
                    resolution=command.resolution,
                )
            except KeyError:
                raise QuarantineTargetNotFoundError(
                    "Quarantine target not found"
                ) from None
            return QuarantineResolutionResponse(
                outcome="dry_run",
                plan=plan,
            )

        assert command.expected_plan_id is not None
        try:
            commit = self.store.apply_quarantine_resolution(
                command.target.identifier,
                tenant_id=principal.tenant_id,
                target=command.target,
                resolution=command.resolution,
                expected_plan_id=command.expected_plan_id,
                operator_subject_id=principal.subject_id,
                operator_credential_id=principal.credential_id,
            )
        except KeyError:
            raise QuarantineTargetNotFoundError(
                "Quarantine target not found"
            ) from None
        self.store.verify_quarantine_resolution(commit)
        return QuarantineResolutionResponse(
            outcome="reused" if commit.reused else "applied",
            plan=commit.plan,
            reused=commit.reused,
            verified=True,
        )
