import pytest

from stupidb.bitgraph import BitGraph
from stupidb.bitset import BitSet


@pytest.fixture
def g():
    return BitGraph({0: {1, 2}, 1: {}, 2: {}})


def test_from_vertices_and_edges(g):
    assert g == BitGraph.from_vertices_and_edges(
        vertices=[0, 1, 2], edges=[(0, 1), (0, 2)]
    )


def test_nodes(g) -> None:
    assert g.nodes == {0: BitSet({1, 2}), 1: BitSet(), 2: BitSet()}


def test_predecessors(g: BitGraph) -> None:
    assert g.predecessors == {1: 0, 2: 0}


def test_in_edges(g: BitGraph) -> None:
    assert set(g.in_edges) == {1, 2}
