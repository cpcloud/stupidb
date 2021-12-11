"""Implementation of a graph whose vertices are unsigned integers."""

from __future__ import annotations

from typing import AbstractSet, Any, Iterable, Iterator, Mapping, MutableMapping

import toolz

from .bitset import BitSet


class BitGraph:
    """An immutable graph whose vertices are unsigned integers."""

    __slots__ = "_nodes", "_predecessors"

    def __init__(self, nodes: Mapping[int, Iterable[int]]) -> None:
        self._nodes: Mapping[int, AbstractSet[int]] = toolz.valmap(BitSet, nodes)
        self._predecessors: MutableMapping[int, int] = {}

        for parent, children in self._nodes.items():
            for child in children:
                self._predecessors[child] = parent

    @classmethod
    def from_vertices_and_edges(
        cls,
        *,
        vertices: Iterable[int],
        edges: Iterable[tuple[int, int]],
    ) -> BitGraph:
        """Construct a `BitGraph` from `vertices` and `edges`."""
        nodes = {vertex: BitSet() for vertex in vertices}
        for source, dest in edges:
            nodes[source].add(dest)
        return cls(nodes)

    @property
    def nodes(self) -> Mapping[int, AbstractSet[int]]:
        """Return a mapping from node to connected nodes."""
        return self._nodes

    @property
    def predecessors(self) -> Mapping[int, int]:
        """Return a mapping of predecessors."""
        return self._predecessors

    @property
    def in_edges(self) -> Iterator[int]:
        """Return the nodes with indegree zero."""
        return (source for source, nodes in self._nodes.items() if not nodes)

    def __eq__(self, other: Any) -> bool:
        return self.nodes == other.nodes and self.predecessors == other.predecessors
