"""Segment tree implementation."""

import abc
import math
from typing import (
    Any,
    ClassVar,
    Collection,
    Generic,
    Iterator,
    List,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from stupidb.typehints import Input1, Input2, Output

T = TypeVar("T")
U = TypeVar("U", bound="UnaryAggregate")


class UnaryAggregate(Generic[Input1, Output], abc.ABC):
    __slots__ = "count", "node_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        self.count = 0
        self.node_index = node_index

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: U, other: U) -> None:
        ...


B = TypeVar("B", bound="BinaryAggregate")


class BinaryAggregate(Generic[Input1, Input2, Output], abc.ABC):
    __slots__ = "count", "node_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        self.count = 0
        self.node_index = node_index

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: B, other: B) -> None:
        ...


Aggregate = TypeVar("Aggregate", UnaryAggregate, BinaryAggregate)


def build(
    tree: MutableSequence[Aggregate],
    leaves: Sequence[Tuple[Optional[T], ...]],
    node_index: int,
    start: int,
    end: int,
    offset: int,
    aggregate: Type[Aggregate],
) -> None:
    """Build a segment tree from `leaves` into `tree`."""
    if start == end:
        # consider all trees to be complete, and take no action if we traverse
        # a node that doesn't exist
        if start > len(leaves) - 1:
            return
        assert tree[node_index] is None, f"tree[{node_index}] is not None"
        args = leaves[start]
        agg = aggregate(node_index=node_index)
        agg.step(*args)
        tree[node_index] = agg
    else:
        midpoint = (start + end) // 2
        left_node_index = 2 * node_index + (1 - offset)
        right_node_index = left_node_index + 1

        build(
            tree, leaves, left_node_index, start, midpoint, offset, aggregate
        )
        build(
            tree,
            leaves,
            right_node_index,
            midpoint + 1,
            end,
            offset,
            aggregate,
        )

        if tree[node_index] is None:
            tree[node_index] = aggregate(node_index=node_index)

        node = tree[node_index]
        assert node is not None, f"tree[{node_index}] is None"

        left_node = tree[left_node_index]
        if left_node is not None:
            node.update(left_node)

        right_node = tree[right_node_index]
        if right_node is not None:
            node.update(right_node)


def next_power_of_2(value: int) -> int:
    if not value:
        return value
    assert value > 0, f"value == {value}"
    return 1 << int(math.ceil(math.log2(value)))


def make_segment_tree(
    leaves: Sequence[Tuple[T, ...]],
    aggregate: Type[Aggregate],
    starting_index: int,
) -> Sequence[Optional[Aggregate]]:
    """Make a segment tree from tuples `leaves` and class `aggregate`."""
    number_of_leaves = len(leaves)
    height = int(math.ceil(math.log2(number_of_leaves))) + 1
    maximum_number_of_nodes = (1 << height) - 1
    tree: MutableSequence = [None] * maximum_number_of_nodes
    build(
        tree,
        leaves,
        node_index=starting_index,
        start=0,
        # even if we don't have a power-of-2 number of leaves, we need to
        # traverse as if we do, to make sure that leaves don't get pushed
        # up to higher levels (thus invalidating the traversal algo) during
        # the build
        end=next_power_of_2(number_of_leaves) - 1,
        offset=starting_index,
        aggregate=aggregate,
    )
    return tree


def reprtree(
    nodes: Sequence[T], offset: int, node_index: int, level: int = 0
) -> str:
    """Return a string representation of `tree`."""
    # if node_index is past the maximum possible nodes, return
    if node_index > len(nodes) - 1:
        return ""
    node = nodes[node_index]
    if node is None:
        # Don't print null nodes
        return ""
    left_child_index = 2 * node_index + (1 - offset)
    right_child_index = left_child_index + 1
    left_subtree = reprtree(nodes, offset, left_child_index, level=level + 1)
    right_subtree = reprtree(nodes, offset, right_child_index, level=level + 1)
    indent = level * 4 * " "
    return f"{indent}|-- {node}\n{left_subtree}{right_subtree}"


class SegmentTree(Collection[Optional[Aggregate]]):
    """A segment tree with element type ``T``."""

    starting_index: ClassVar[int] = 0

    def __init__(
        self, leaves: Sequence[Tuple[T, ...]], aggregate: Type[Aggregate]
    ) -> None:
        self.nodes: Sequence[Optional[Aggregate]] = make_segment_tree(
            leaves, aggregate, self.starting_index
        )
        self.aggregate: Type[Aggregate] = aggregate
        self.levels: Sequence[Sequence[Aggregate]] = list(
            self.iterlevels(self.nodes)
        )

    @classmethod
    def iterlevels(
        cls, nodes: Sequence[Optional[Aggregate]]
    ) -> Iterator[List[Aggregate]]:
        """Iterate over every level in the tree starting from the bottom."""
        offset = 1 - cls.starting_index
        height = int(math.ceil(math.log2(len(nodes))))

        for level in range(height, 0, -1):
            start = (1 << max(level - 1, 0)) - offset
            stop = (1 << level) - offset
            yield [node for node in nodes[start:stop] if node is not None]

    def __repr__(self) -> str:
        # strip because basecase is the empty string
        return reprtree(
            self.nodes, self.starting_index, self.starting_index
        ).strip()

    def __iter__(self) -> Iterator[Optional[Aggregate]]:
        return iter(self.nodes)

    def __contains__(self, value: Optional[Aggregate]) -> bool:
        return value in self.nodes

    def __len__(self) -> int:
        """Return the number of nodes in the tree."""
        return len(self.nodes)

    def query(self, begin: int, end: int) -> Any:
        """Aggregate the values between `begin` and `end` using `aggregate`."""
        fanout = 2
        aggregate = self.aggregate()
        levels = self.levels
        for i, level in enumerate(levels):
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
