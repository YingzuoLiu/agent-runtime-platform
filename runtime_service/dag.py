from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Iterable


class WorkflowGraphError(ValueError):
    """The declared workflow graph is not a valid directed acyclic graph."""


@dataclass(frozen=True)
class WorkflowNode:
    node_id: str
    dependencies: tuple[str, ...] = ()


class WorkflowDag:
    """Validated, deterministic dependency graph for workflow node ids.

    The graph deliberately owns only topology. Domain code still owns tool
    selection, typed inputs, retries and result validation. Ready nodes are
    ordered by declaration position so identical graphs always produce the
    same serial schedule.
    """

    def __init__(self, nodes: Iterable[WorkflowNode]) -> None:
        self._nodes = tuple(nodes)
        if not self._nodes:
            raise WorkflowGraphError("workflow graph must contain at least one node")

        self._positions: dict[str, int] = {}
        for position, node in enumerate(self._nodes):
            if not node.node_id:
                raise WorkflowGraphError("workflow node_id must not be empty")
            if node.node_id in self._positions:
                raise WorkflowGraphError(f"duplicate workflow node_id: {node.node_id!r}")
            if len(set(node.dependencies)) != len(node.dependencies):
                raise WorkflowGraphError(
                    f"node {node.node_id!r} declares duplicate dependencies"
                )
            self._positions[node.node_id] = position

        known = set(self._positions)
        for node in self._nodes:
            if node.node_id in node.dependencies:
                raise WorkflowGraphError(f"node {node.node_id!r} cannot depend on itself")
            missing = set(node.dependencies) - known
            if missing:
                raise WorkflowGraphError(
                    f"node {node.node_id!r} has unknown dependencies: {sorted(missing)!r}"
                )

        self._dependencies = {
            node.node_id: frozenset(node.dependencies) for node in self._nodes
        }
        dependents: dict[str, set[str]] = {node.node_id: set() for node in self._nodes}
        for node in self._nodes:
            for dependency in node.dependencies:
                dependents[dependency].add(node.node_id)
        self._dependents = {
            node_id: frozenset(values) for node_id, values in dependents.items()
        }
        self._topological_order = self._build_topological_order()

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(node.node_id for node in self._nodes)

    @property
    def topological_order(self) -> tuple[str, ...]:
        return self._topological_order

    def dependencies_for(self, node_id: str) -> frozenset[str]:
        self._require_known({node_id})
        return self._dependencies[node_id]

    def descendants(self, node_ids: Iterable[str], *, include_roots: bool = True) -> frozenset[str]:
        roots = set(node_ids)
        self._require_known(roots)
        discovered = set(roots)
        pending = list(roots)
        while pending:
            current = pending.pop()
            for dependent in self._dependents[current]:
                if dependent not in discovered:
                    discovered.add(dependent)
                    pending.append(dependent)
        if not include_roots:
            discovered -= roots
        return frozenset(discovered)

    def _build_topological_order(self) -> tuple[str, ...]:
        remaining_dependencies = {
            node_id: len(dependencies)
            for node_id, dependencies in self._dependencies.items()
        }
        ready = [
            (self._positions[node_id], node_id)
            for node_id, count in remaining_dependencies.items()
            if count == 0
        ]
        heapq.heapify(ready)
        ordered: list[str] = []

        while ready:
            _, node_id = heapq.heappop(ready)
            ordered.append(node_id)
            for dependent in self._dependents[node_id]:
                remaining_dependencies[dependent] -= 1
                if remaining_dependencies[dependent] == 0:
                    heapq.heappush(ready, (self._positions[dependent], dependent))

        if len(ordered) != len(self._nodes):
            cyclic = sorted(
                node_id for node_id, count in remaining_dependencies.items() if count > 0
            )
            raise WorkflowGraphError(f"workflow graph contains a cycle: {cyclic!r}")
        return tuple(ordered)

    def _require_known(self, node_ids: set[str]) -> None:
        unknown = node_ids - set(self._positions)
        if unknown:
            raise WorkflowGraphError(f"unknown workflow node ids: {sorted(unknown)!r}")
