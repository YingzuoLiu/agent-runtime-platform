import pytest

from runtime_service.dag import WorkflowDag, WorkflowGraphError, WorkflowNode


def test_dag_builds_deterministic_topological_order_and_descendants():
    graph = WorkflowDag(
        [
            WorkflowNode("root_b"),
            WorkflowNode("root_a"),
            WorkflowNode("join", ("root_a", "root_b")),
            WorkflowNode("final", ("join",)),
        ]
    )

    assert graph.topological_order == ("root_b", "root_a", "join", "final")
    assert graph.dependencies_for("join") == frozenset({"root_a", "root_b"})
    assert graph.descendants(["root_a"]) == frozenset({"root_a", "join", "final"})
    assert graph.descendants(["join"], include_roots=False) == frozenset({"final"})


@pytest.mark.parametrize(
    "nodes, message",
    [
        ([WorkflowNode("a"), WorkflowNode("a")], "duplicate workflow node_id"),
        ([WorkflowNode("a", ("missing",))], "unknown dependencies"),
        ([WorkflowNode("a", ("a",))], "cannot depend on itself"),
        ([WorkflowNode("a", ("b",)), WorkflowNode("b", ("a",))], "contains a cycle"),
    ],
)
def test_dag_rejects_invalid_graphs(nodes, message):
    with pytest.raises(WorkflowGraphError, match=message):
        WorkflowDag(nodes)


def test_dag_rejects_unknown_replay_roots():
    graph = WorkflowDag([WorkflowNode("known")])

    with pytest.raises(WorkflowGraphError, match="unknown workflow node ids"):
        graph.descendants(["unknown"])
