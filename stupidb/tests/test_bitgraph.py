import pytest

from stupidb.associative.bitgraph import BitGraph
from stupidb.associative.bitset import BitSet


@pytest.fixture  # type: ignore[misc]
def g() -> BitGraph:
    return BitGraph({0: {1, 2}, 1: {}, 2: {}})


def test_from_vertices_and_edges(g: BitGraph) -> None:
    assert g == BitGraph.from_vertices_and_edges(
        vertices=[0, 1, 2], edges=[(0, 1), (0, 2)]
    )


def test_nodes(g: BitGraph) -> None:
    assert g.nodes == {0: BitSet({1, 2}), 1: BitSet(), 2: BitSet()}


def test_predecessors(g: BitGraph) -> None:
    assert g.predecessors == {1: 0, 2: 0}


def test_in_edges(g: BitGraph) -> None:
    assert set(g.in_edges) == {1, 2}
