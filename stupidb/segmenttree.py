"""Segment tree implementation."""

import abc
import math
from typing import (
    Any,
    Generic,
    Iterator,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from stupidb.typehints import Input1, Input2, Output

T = TypeVar("T")
U = TypeVar("U", bound="UnaryAggregate")


class UnaryAggregate(Generic[Input1, Output], abc.ABC):
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

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
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: B, other: B) -> None:
        ...


Aggregate = Union[UnaryAggregate, BinaryAggregate]


def build(
    tree: MutableSequence,
    leaves: Sequence,
    node: int,
    start: int,
    end: int,
    aggregate: Type[Aggregate],
) -> None:
    """Build a segment tree from `leaves` into `tree`."""
    if start == end:
        assert tree[node] is None, f"tree[{node}] == {tree[node]}"
        tree[node] = aggregate()
        args = leaves[start]
        tree[node].step(*args)
    else:
        mid = (start + end) // 2
        left_node = 2 * node
        right_node = left_node + 1
        build(tree, leaves, left_node, start, mid, aggregate)
        build(tree, leaves, right_node, mid + 1, end, aggregate)
        if tree[node] is None:
            tree[node] = aggregate()
        tree[node].update(tree[left_node])
        tree[node].update(tree[right_node])


def make_segment_tree(
    leaves: Sequence[Tuple[T, ...]], aggregate: Type[Aggregate]
) -> Sequence[Aggregate]:
    num_leaves = len(leaves)
    height = int(math.ceil(math.log2(num_leaves)))
    num_nodes = 1 << height + 1
    tree: MutableSequence = [None] * num_nodes
    build(tree, leaves, 1, 0, num_leaves - 1, aggregate)
    return tree


def reprtree(tree: Sequence[T], index: int, level: int = 0) -> str:
    if index > len(tree) - 1:
        return ""
    node = tree[index]
    left_child = 2 * index
    right_child = 2 * index + 1
    left_subtree = reprtree(tree, left_child, level=level + 1)
    right_subtree = reprtree(tree, right_child, level=level + 1)
    return f"{level * '  '}* {node}\n{left_subtree}{right_subtree}"


class SegmentTree(Generic[T]):
    """A segment tree with element type ``T``."""

    def __init__(
        self, leaves: Sequence[Tuple[T, ...]], aggregate: Type[Aggregate]
    ) -> None:
        self.nodes = make_segment_tree(leaves, aggregate)
        self.aggregate = aggregate

    def __repr__(self) -> str:
        # strip because basecase is the empty string
        return reprtree(self.nodes, index=1).strip()

    @property
    def height(self) -> int:
        """Return the number of levels in the tree."""
        return int(math.log2(len(self)))

    def __len__(self) -> int:
        """Return the number of nodes in the tree."""
        return len(self.nodes)

    def iterlevels(self) -> Iterator[Sequence[Optional[Aggregate]]]:
        """Iterate over every level in the tree."""
        nodes = self.nodes
        for level in range(self.height, -1, -1):
            level_nodes = nodes[1 << level : 1 << level + 1]
            if level_nodes:
                yield level_nodes

    def traverse(self, begin: int, end: int) -> Any:
        """Aggregate the values between `begin` and `end` using `aggregate`."""
        fanout = 2
        aggregate = self.aggregate()
        for level in self.iterlevels():
            parent_begin = begin // fanout
            parent_end = end // fanout
            if parent_begin == parent_end:
                for item in filter(None, level[begin:end]):
                    aggregate.update(item)
                result = aggregate.finalize()
                return result
            group_begin = parent_begin * fanout
            if begin != group_begin:
                limit = group_begin + fanout
                for item in filter(None, level[begin:limit]):
                    aggregate.update(item)
                parent_begin += 1
            group_end = parent_end * fanout
            if end != group_end:
                for item in filter(None, level[group_end:end]):
                    aggregate.update(item)
            begin = parent_begin
            end = parent_end
