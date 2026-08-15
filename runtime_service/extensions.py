"""Trusted deployment-time extension seam for additional Agent domains.

Runtime extensions are supplied explicitly by the service composition root.
They are not discovered dynamically and they do not load arbitrary user code.
The context exposes only the domain-neutral services needed to register a
managed runtime backed by the same durable stores as the built-in domains.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .evidence import RunEventSink
from .registry import AgentRegistry
from .workflow_store import WorkflowStore


@dataclass(frozen=True)
class RuntimeExtensionContext:
    """Shared services available while an extension registers its runtimes."""

    registry: AgentRegistry
    workflow_store: WorkflowStore
    run_event_sink: RunEventSink


class RuntimeExtension(Protocol):
    """One trusted package that registers Agent versions during app startup."""

    def register(self, context: RuntimeExtensionContext) -> None:
        ...
