"""Segment tree implementation.

The implementation is based on `Leis, 2015
<http://www.vldb.org/pvldb/vol8/p1058-leis.pdf>`_

"""

import math
from typing import (
    ClassVar,
    Iterator,
    List,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from stupidb.aggregator import Aggregator
from stupidb.associative import AssociativeAggregate
from stupidb.typehints import Result

T = TypeVar("T")


def build(
    tree: MutableSequence[Optional[AssociativeAggregate]],
    leaves: Sequence[Tuple[Optional[T], ...]],
    node_index: int,
    start: int,
    end: int,
    aggregate_type: Type[AssociativeAggregate],
) -> None:
    """Build a segment tree from `leaves` into `tree`.

    Parameters
    ----------
    tree
        A mutable sequence of
        :class:`~stupidb.associative.AssociativeAggregate` instances.
    leaves
        A sequence of tuples that make up the full range of the partition to
        aggregate.
    node_index
        The current node's index.
    start
        The starting node's index.
    end
        The last node's index.
    aggregate_type
        The class of the aggregate that makes up the tree.

    """
    if start == end:
        # consider all trees to be complete, and take no action if we traverse
        # a node that doesn't exist
        if start > len(leaves) - 1:
            return
        assert tree[node_index] is None, f"tree[{node_index}] is not None"
        args = leaves[start]
        agg = aggregate_type()
        agg.step(*args)
        tree[node_index] = agg
    else:
        midpoint = (start + end) // 2
        left_node_index = 2 * node_index + 1
        right_node_index = left_node_index + 1

        build(tree, leaves, left_node_index, start, midpoint, aggregate_type)
        build(
            tree, leaves, right_node_index, midpoint + 1, end, aggregate_type
        )

        if tree[node_index] is None:
            tree[node_index] = aggregate_type()

        node = tree[node_index]
        assert node is not None, f"tree[{node_index}] is None"

        left_node = tree[left_node_index]
        if left_node is not None:
            node.update(left_node)

        right_node = tree[right_node_index]
        if right_node is not None:
            node.update(right_node)


def next_power_of_2(value: int) -> int:
    """Compute the next power of two of an integer.

    Parameters
    ----------
    value
        The value whose next power of two to compute.

    """
    if not value:
        return value
    if value < 0:
        raise ValueError(f"Invalid value: {value:d}")
    assert value > 0, f"value == {value}"
    return 1 << int(math.ceil(math.log2(value)))


def make_segment_tree(
    leaves: Sequence[Tuple[T, ...]], aggregate_type: Type[AssociativeAggregate]
) -> Sequence[Optional[AssociativeAggregate]]:
    """Make a segment tree from tuples `leaves` and class `aggregate`.

    Parameters
    ----------
    leaves
        A sequence of tuples that make up the leaves of the segment tree
    aggregate_type
        The aggregate class whose instances compose the tree.

    """
    number_of_leaves = len(leaves)
    height = int(math.ceil(math.log2(number_of_leaves))) + 1
    maximum_number_of_nodes = (1 << height) - 1
    tree: MutableSequence[Optional[AssociativeAggregate]] = [
        None
    ] * maximum_number_of_nodes
    build(
        tree,
        leaves,
        node_index=0,
        start=0,
        # even if we don't have a power-of-2 number of leaves, we need to
        # traverse as if we do, to make sure that leaves don't get pushed
        # up to higher levels (thus invalidating the traversal algo) during
        # the build
        end=next_power_of_2(number_of_leaves) - 1,
        aggregate_type=aggregate_type,
    )
    return tree


def reprtree(nodes: Sequence[T], node_index: int = 0, level: int = 0) -> str:
    """Return a string representation of `tree`.

    Parameters
    ----------
    nodes
        A sequence of nodes of a tree
    node_index
        The current node's index
    level
        The current level of the tree

    """
    # if node_index is past the maximum possible nodes, return
    if node_index > len(nodes) - 1:
        return ""
    node = nodes[node_index]
    if node is None:
        # Don't print null nodes
        return ""
    left_child_index = 2 * node_index + 1
    right_child_index = left_child_index + 1
    left_subtree = reprtree(nodes, left_child_index, level=level + 1)
    right_subtree = reprtree(nodes, right_child_index, level=level + 1)
    indent = level * 4 * " "
    return f"{indent}|-- {node}\n{left_subtree}{right_subtree}"


class SegmentTree(Aggregator[AssociativeAggregate, Result]):
    """A segment tree for window aggregation.

    Attributes
    ----------
    nodes
    aggregate_type
    levels

    """

    fanout: ClassVar[int] = 2

    def __init__(
        self,
        leaves: Sequence[Tuple[T, ...]],
        aggregate_type: Type[AssociativeAggregate],
    ) -> None:
        self.nodes: Sequence[
            Optional[AssociativeAggregate]
        ] = make_segment_tree(leaves, aggregate_type)
        self.aggregate_type: Type[AssociativeAggregate] = aggregate_type
        self.levels: Sequence[Sequence[AssociativeAggregate]] = list(
            self.iterlevels(self.nodes)
        )

    @staticmethod
    def iterlevels(
        nodes: Sequence[Optional[AssociativeAggregate]]
    ) -> Iterator[List[AssociativeAggregate]]:
        """Iterate over every level in the tree starting from the bottom."""
        height = int(math.ceil(math.log2(len(nodes))))
        for level in range(1, height + 1):
            start = (1 << level - 1) - 1
            stop = (1 << level) - 1
            yield [node for node in nodes[start:stop] if node is not None]

    def __repr__(self) -> str:
        # strip because the base case is the empty string + a newline
        return reprtree(self.nodes).strip()

    def query(self, begin: int, end: int) -> Optional[Result]:
        """Aggregate the values between `begin` and `end` using `aggregate`."""
        # TODO: investigate fanout
        fanout = self.__class__.fanout
        aggregate: AssociativeAggregate = self.aggregate_type()
        for i, level in enumerate(reversed(self.levels)):
            parent_begin = begin // fanout
            parent_end = end // fanout
            if parent_begin == parent_end:
                for item in level[begin:end]:
                    aggregate.update(item)
                result = aggregate.finalize()
                return result
            group_begin = parent_begin * fanout
            if begin != group_begin:
                limit = group_begin + fanout
                for item in level[begin:limit]:
                    aggregate.update(item)
                parent_begin += 1
            group_end = parent_end * fanout
            if end != group_end:
                for item in level[group_end:end]:
                    aggregate.update(item)
            begin = parent_begin
            end = parent_end
        return None
