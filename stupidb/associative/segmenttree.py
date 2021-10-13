from __future__ import annotations

import collections
import math
from typing import Generic, Iterator, MutableSequence, Sequence

from ..aggregator import Aggregator
from ..functions.associative.core import AssociativeAggregate
from ..typehints import Result, T
from . import indextree
from .bitset import BitSet


def make_segment_tree(
    leaf_arguments: Sequence[tuple[T, ...]],
    aggregate_type: type[AssociativeAggregate],
    *,
    fanout: int,
) -> Sequence[AssociativeAggregate]:
    """Make a segment tree from tuples `leaves` and class `aggregate_type`.

    The algorithm used here traverses from the bottom of tree upward, updating
    the parent every time a new node is seen.

    Parameters
    ----------
    leaves
        A sequence of tuples that make up the leaves of the segment tree
    aggregate_type
        The aggregate class whose instances compose the tree.

    """
    number_of_leaves = len(leaf_arguments)
    index_tree = indextree.IndexTree(
        height=int(math.ceil(math.log(number_of_leaves, fanout))) + 1,
        fanout=fanout,
    )
    num_nodes = len(index_tree)
    segment_tree_nodes: MutableSequence[AssociativeAggregate] = [
        aggregate_type() for _ in range(num_nodes)
    ]
    queue = collections.deque(index_tree.leaves)

    # seed the leaves
    for leaf_index, args in zip(queue, leaf_arguments):
        segment_tree_nodes[leaf_index].step(*args)

    seen = BitSet()

    while queue:
        node = queue.popleft()
        if node not in seen:
            seen.add(node)
            node_agg = segment_tree_nodes[node]
            parent = index_tree.parent(node)
            parent_agg = segment_tree_nodes[parent]
            parent_agg.combine(node_agg)
            if parent:
                # don't append the root, since we've already aggregated into
                # that node if parent == 0
                queue.append(parent)
    return segment_tree_nodes


class SegmentTree(
    Generic[T, AssociativeAggregate, Result],
    Aggregator[AssociativeAggregate, Result],
):
    """A segment tree for window aggregation.

    Attributes
    ----------
    nodes
        The nodes of the segment tree
    aggregate_type
        The class of the aggregate to use
    levels
        A list of the nodes in each level of the tree
    fanout
        The number of leaves to aggregate into each interior node

    """

    __slots__ = "nodes", "aggregate_type", "levels", "fanout"

    def __init__(
        self,
        leaves: Sequence[tuple[T | None, ...]],
        aggregate_type: type[AssociativeAggregate],
        *,
        fanout: int,
    ) -> None:
        """Construct a segment tree."""
        self.nodes: Sequence[AssociativeAggregate] = make_segment_tree(
            leaves, aggregate_type, fanout=fanout
        )
        self.aggregate_type: type[AssociativeAggregate] = aggregate_type
        self.fanout = fanout
        self.height = int(math.ceil(math.log(len(leaves), fanout))) + 1
        self.levels: Sequence[Sequence[AssociativeAggregate]] = list(
            self.iterlevels(self.nodes, fanout=fanout)
        )

    @staticmethod
    def iterlevels(
        nodes: Sequence[AssociativeAggregate], *, fanout: int
    ) -> Iterator[Sequence[AssociativeAggregate]]:
        """Iterate over every level in the tree.

        Parameters
        ----------
        nodes
            The nodes of the tree whose levels will be yielded.
        fanout
            The number child nodes per interior node

        """
        height = int(math.ceil(math.log(len(nodes), fanout)))
        for level in range(height):
            start = indextree.first_node(level, fanout=fanout)
            stop = indextree.last_node(level, fanout=fanout)
            yield nodes[start:stop]

    def __repr__(self) -> str:
        return indextree.reprtree(self.nodes, fanout=self.fanout)

    def query(self, begin: int, end: int) -> Result | None:
        """Aggregate the values between `begin` and `end` using `aggregate`.

        Parameters
        ----------
        begin
            The start of the range to aggregate
        end
            The end of the range to aggregate

        """
        fanout = self.fanout
        aggregate: AssociativeAggregate = self.aggregate_type()

        for level in reversed(self.levels):
            parent_begin = begin // fanout
            parent_end = end // fanout
            if parent_begin == parent_end:
                for item in level[begin:end]:
                    aggregate.combine(item)
                return aggregate.finalize()
            group_begin = parent_begin * fanout
            if begin != group_begin:
                limit = group_begin + fanout
                for item in level[begin:limit]:
                    aggregate.combine(item)
                parent_begin += 1
            group_end = parent_end * fanout
            if end != group_end:
                for item in level[group_end:end]:
                    aggregate.combine(item)
            begin = parent_begin
            end = parent_end
        return None  # pragma: no cover
