from typing import Iterable, Iterator, Sequence, TypeVar

T = TypeVar("T", covariant=True)


def reprtree(
    nodes: Sequence[T], fanout: int, node_index: int = 0, level: int = 0
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
        reprtree(nodes, fanout, fanout * node_index + i + 1, level=level + 1)
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


class Tree:
    """An array backed tree."""

    def __init__(self, *, height: int, fanout: int) -> None:
        self.height = height
        self.nodes = list(
            range(int((fanout ** self.height - 1) / (fanout - 1)))
        )
        self.fanout = fanout

    @property
    def leaves(self) -> Iterable[int]:
        height = self.height
        first = self.first_node(height)
        last = self.last_node(height)
        return self.nodes[first:last]

    def __repr__(self) -> str:
        return reprtree(self.nodes, fanout=self.fanout).strip()

    def __len__(self) -> int:
        return len(self.nodes)

    def __getitem__(self, key: int) -> int:
        return self.nodes[key]

    def __iter__(self) -> Iterator[int]:
        return iter(self.nodes)

    def first_node(self, level: int) -> int:
        """Return the first node at `level`."""
        return first_node(level, fanout=self.fanout)

    def last_node(self, level: int) -> int:
        """Return the last node at `level`."""
        return last_node(level, fanout=self.fanout)

    def child(self, node: int, i: int) -> int:
        r"""Return the :math:`i\mbox{th}` child of `node`."""
        return self.fanout * node + i + 1

    def children(self, node: int) -> Iterable[int]:
        """Iterate over the children of an node.

        Examples
        --------
        >>> list(children(parent=0, fanout=4))
        [1, 2, 3, 4]
        >>> list(children(parent=1, fanout=4))
        [5, 6, 7, 8]

        """
        return (self.child(node, i) for i in range(self.fanout))

    def parent(self, node: int) -> int:
        """Return the parent node of `node`."""
        if not node:
            # the parent has no parent
            return 0
        return (node - 1) // self.fanout
