"""Module implementing an abstraction for navigation of array-backed trees."""

from typing import MutableSequence, Sequence, TypeVar

from stupidb.bitset import BitSet

T = TypeVar("T", covariant=True)


def reprtree(nodes: Sequence[T], *, fanout: int, indent: str = 4 * " ") -> str:
    """Return a string representation of `nodes`.

    Parameters
    ----------
    nodes
        A sequence of nodes in a tree.
    fanout
        The number of children per node.
    indent
        The prefix number of spaces to print at each level.

    """
    # track the current level of the tree and the index of the current node
    level_index_stack = [(0, 0)]

    # store the nodes that we've seen
    seen = BitSet()
    node_repr_pieces: MutableSequence[str] = []

    while level_index_stack:
        level, node_index = level_index_stack.pop()
        if node_index not in seen:
            node = nodes[node_index]
            node_repr_pieces.append(f"{level * indent}|-- {node}")
            node_indices = (
                fanout * node_index + i + 1 for i in reversed(range(fanout))
            )
            level_index_stack.extend(
                (level + 1, index) for index in node_indices if index < len(nodes)
            )
            seen.add(node_index)
    return "\n".join(node_repr_pieces)


def first_node(level: int, *, fanout: int) -> int:
    """Return the first node at `level`."""
    return (fanout ** level - 1) // (fanout - 1)


def last_node(level: int, *, fanout: int) -> int:
    """Return the last node at `level`."""
    return (fanout ** (level + 1) - 1) // (fanout - 1)


class IndexTree:
    """Abstraction for navigating around array-backed trees."""

    __slots__ = "height", "fanout", "nodes"

    def __init__(self, *, height: int, fanout: int) -> None:
        """Construct an :class:`~stupidb.indextree.IndexTree`."""
        self.height = height
        self.fanout = fanout
        self.nodes = range((fanout ** height - 1) // (fanout - 1))

    @property
    def leaves(self) -> range:
        """Return the indices of the leaves of the tree."""
        height = self.height - 1
        first = self.first_node(height)
        last = self.last_node(height)
        return self.nodes[first:last]

    def __repr__(self) -> str:
        return reprtree(self.nodes, fanout=self.fanout)

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
        """Return the index of the parent node of `node`."""
        if not node:
            parent_node_index = 0
        else:
            parent_node_index = (node - 1) // self.fanout

        # parent should never be negative
        assert (
            parent_node_index >= 0
        ), f"parent_node_index < 0: parent_node_index == {parent_node_index}"
        return parent_node_index
