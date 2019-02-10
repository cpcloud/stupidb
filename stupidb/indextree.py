"""Module implementing an abstraction for navigation of array-backed trees."""

from typing import Iterable, Sequence, TypeVar

T = TypeVar("T", covariant=True)


def reprtree(
    nodes: Sequence[T], *, fanout: int, node_index: int = 0, level: int = 0
) -> str:
    """Return a string representation of `nodes`.

    Parameters
    ----------
    nodes
        A sequence of nodes of a tree
    fanout
        Number of child nodes per nodes
    node_index
        The current node's index
    level
        The current level of the tree

    """
    # if node_index is past the maximum possible nodes, return
    if node_index >= len(nodes):
        return ""
    node = nodes[node_index]
    assert node is not None, f"node {node_index} is None"
    subtrees = "".join(
        reprtree(
            nodes,
            fanout=fanout,
            node_index=fanout * node_index + i + 1,
            level=level + 1,
        )
        for i in range(fanout)
    )
    indent = level * 4 * " "
    return f"{indent}|-- {node}\n{subtrees}"


def first_node(level: int, *, fanout: int) -> int:
    """Return the first node at `level`."""
    return int((fanout ** (level - 1) - 1) / (fanout - 1))


def last_node(level: int, *, fanout: int) -> int:
    """Return the last node at `level`."""
    return int((fanout ** level - 1) / (fanout - 1))


class IndexTree:
    """Abstraction for navigating around array-backed trees."""

    def __init__(self, *, height: int, fanout: int) -> None:
        """Construct an :class:`~stupidb.indextree.IndexTree`."""
        self.height = height
        self.nodes = list(
            range(int((fanout ** self.height - 1) / (fanout - 1)))
        )
        self.fanout = fanout

    @property
    def leaves(self) -> Iterable[int]:
        """Return the indices of the leaves of the tree."""
        height = self.height
        first = self.first_node(height)
        last = self.last_node(height)
        return self.nodes[first:last]

    def __repr__(self) -> str:
        return reprtree(self.nodes, fanout=self.fanout).strip()

    def __len__(self) -> int:
        """Return the number of nodes in the tree."""
        return len(self.nodes)

    def first_node(self, level: int) -> int:
        """Return the first node at `level`."""
        return first_node(level, fanout=self.fanout)

    def last_node(self, level: int) -> int:
        """Return the last node at `level`."""
        return last_node(level, fanout=self.fanout)

    def parent(self, node: int) -> int:
        """Return the parent node of `node`."""
        if not node:
            # the parent's parent is itself
            return 0
        return (node - 1) // self.fanout
