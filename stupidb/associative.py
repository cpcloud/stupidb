r"""Segment tree and corresponding aggregate function implementations.

The segment tree implementation is based on `Leis, 2015
<http://www.vldb.org/pvldb/vol8/p1058-leis.pdf>`_

The segment tree here uses :class:`~stupidb.associative.AssociativeAggregate`
instances as its nodes. The leaves are computed by call the
:meth:`~stupidb.associative.AssociativeAggregate.step` method when the tree
construction bottoms out. On the way back up the tree each aggregation instance
is updated based on its children by calling the
:meth:`~stupidb.associative.AssociativeAggregate.update` method. This method
takes another instance of the same aggregation as input and updates the calling
instance based on the value of the intermediate state of its input (the other
aggregation).

Each interior node contains an intermediate value of a given aggregation such
that it is possible to compute a range query in :math:`O\left(\log{N}\right)`
time rather than :math:`O\left(N\right)`.

Using segment trees in this way results in window aggregations having
:math:`O\left(N\log{N}\right)` worst case behavior rather than
:math:`O\left(N^{2}\right)`, which is the complexity of naive window
aggregation implementations.

A `previous iteration of stupidb
<https://github.com/cpcloud/stupidb/tree/14ef13e>`_ had :math:`O\left(N\right)`
worst case behavior for some aggregations such as those based on ``sum``. The
segment tree implementation provides a generic solution for any associative
aggregate, including ``min`` and ``max`` as well as the typical ``sum`` based
aggregations, that gives a worst case runtime complexity of
:math:`O\left(N\log{N}\right)`.

A future iteration might determine the aggregation algorithm based on the
specific aggregate to achieve optimal behavior from all aggregates.

"""

import abc
import math
import typing
from typing import (
    Callable,
    ClassVar,
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

from stupidb.aggregatetypes import Aggregate
from stupidb.aggregator import Aggregator
from stupidb.protocols import Comparable
from stupidb.typehints import R1, R2, Input1, Input2, Output, R, Result, T

AssociativeAggregate = TypeVar(
    "AssociativeAggregate",
    "UnaryAssociativeAggregate",
    "BinaryAssociativeAggregate",
)


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
        The nodes of the segment tree
    aggregate_type
        The class of the aggregate to use
    levels
        A list of the nodes in each level of the tree
    fanout
        The number of leaves to aggregate into each interior node

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
        """Iterate over every level in the tree starting from the bottom.

        Parameters
        ----------
        nodes
            The nodes of the tree whose levels will be yielded.

        """
        height = int(math.ceil(math.log2(len(nodes))))
        getitem = nodes.__getitem__
        for level in range(1, height + 1):
            start = (1 << level - 1) - 1
            stop = (1 << level) - 1
            yield list(filter(None, map(getitem, range(start, stop))))

    def __repr__(self) -> str:
        # strip because the base case is the empty string + a newline
        return reprtree(self.nodes).strip()

    def query(self, begin: int, end: int) -> Optional[Result]:
        """Aggregate the values between `begin` and `end` using `aggregate`.

        Parameters
        ----------
        begin
            The start of the range to aggregate
        end
            The end of the range to aggregate

        """
        # TODO: investigate fanout
        fanout = self.fanout
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
        return None  # pragma: no cover


class AbstractAssociativeAggregate(Aggregate[Output]):
    """Base class for aggregations with an associative binary operation."""

    __slots__ = ("count",)
    aggregator_class: ClassVar[Type[SegmentTree]] = SegmentTree

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        """Compute the value of the aggregation from its current state."""


UA = TypeVar("UA", bound="UnaryAssociativeAggregate")
BA = TypeVar("BA", bound="BinaryAssociativeAggregate")


class UnaryAssociativeAggregate(
    AbstractAssociativeAggregate[Output], Generic[Input1, Output]
):
    __slots__ = ()

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def update(self: UA, other: UA) -> None:
        """Update this aggregation based on another of the same type."""


class BinaryAssociativeAggregate(
    AbstractAssociativeAggregate[Output], Generic[Input1, Input2, Output]
):
    __slots__ = ()

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def update(self: BA, other: BA) -> None:
        """Update this aggregation based on another of the same type."""


class Count(UnaryAssociativeAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def __repr__(self) -> str:
        return f"{type(self).__name__}(count={self.count!r})"

    def finalize(self) -> Optional[int]:
        return self.count

    def update(self, other: "Count[Input1]") -> None:
        self.count += other.count


class Sum(UnaryAssociativeAggregate[R1, R2]):
    __slots__ = ("total",)

    def __init__(self) -> None:
        super().__init__()
        self.total = typing.cast(R2, 0)

    def __repr__(self) -> str:
        total = self.finalize()
        count = self.count
        name = type(self).__name__
        return f"{name}(total={total!r}, count={count!r})"

    def step(self, input1: Optional[R1]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else None

    def update(self, other: "Sum[R1, R2]") -> None:
        self.total += other.total
        self.count += other.count


class Total(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else typing.cast(R2, 0)


class Mean(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        count = self.count
        return self.total / count if count > 0 else None

    def __repr__(self) -> str:
        name = type(self).__name__
        total = super().finalize()
        count = self.count
        mean = self.finalize()
        return f"{name}(total={total!r}, count={count!r}, mean={mean!r})"


class MinMax(UnaryAssociativeAggregate[Comparable, Comparable]):
    __slots__ = "current_value", "comparator"

    def __init__(
        self, *, comparator: Callable[[Comparable, Comparable], Comparable]
    ) -> None:
        super().__init__()
        self.current_value: Optional[Comparable] = None
        self.comparator = comparator

    def step(self, input1: Optional[Comparable]) -> None:
        if input1 is not None:
            if self.current_value is None:
                self.current_value = input1
            else:
                self.current_value = self.comparator(
                    self.current_value, input1
                )

    def finalize(self) -> Optional[Comparable]:
        return self.current_value

    def update(self, other: "MinMax") -> None:
        assert self.comparator == other.comparator, (
            f"self.comparator == {self.comparator!r}, "
            f"other.comparator == {other.comparator!r}"
        )
        if other.current_value is not None:
            self.current_value = (
                other.current_value
                if self.current_value is None
                else self.comparator(self.current_value, other.current_value)
            )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(current_value={self.current_value!r})"


class Min(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=min)


class Max(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=max)


class Covariance(BinaryAssociativeAggregate[R1, R2, float]):
    __slots__ = "meanx", "meany", "cov", "ddof"

    def __init__(self, *, ddof: int) -> None:
        super().__init__()
        self.meanx = 0.0
        self.meany = 0.0
        self.cov = 0.0
        self.ddof = ddof

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f"{name}(meanx={self.meanx!r}, meany={self.meany!r}, "
            f"cov={self.cov!r}, count={self.count!r})"
        )

    def step(self, x: Optional[R1], y: Optional[R2]) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.meanx
            self.meanx += delta_x + count
            self.meany += (y - self.meany) / count
            self.cov += delta_x * (y - self.meany)

    def finalize(self) -> Optional[float]:
        denom = self.count - self.ddof
        return self.cov / denom if denom > 0 else None

    def update(self, other: "Covariance[R1, R2]") -> None:
        new_count = self.count + other.count
        self.cov += (
            other.cov
            + (self.meanx - other.meanx)
            * (self.meany - other.meany)
            * (self.count * other.count)
            / new_count
        )
        self.meanx = (
            self.count * self.meanx + other.count * other.meanx
        ) / new_count
        self.meany = (
            self.count * self.meany + other.count * other.meany
        ) / new_count
        self.count = new_count


class SampleCovariance(Covariance[R1, R2]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance[R1, R2]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class Variance(UnaryAssociativeAggregate[R, float]):
    __slots__ = ("aggregator",)

    def __init__(self, ddof: int) -> None:
        self.aggregator = Covariance[R, R](ddof=ddof)

    def step(self, x: Optional[R]) -> None:
        self.aggregator.step(x, x)

    def finalize(self) -> Optional[float]:
        return self.aggregator.finalize()

    def update(self, other: "Variance[R]") -> None:
        self.aggregator.update(other.aggregator)


class SampleVariance(Variance[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationVariance(Variance[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class StandardDeviation(Variance[R]):
    __slots__ = ("aggregator",)

    def finalize(self) -> Optional[float]:
        variance = super().finalize()
        return math.sqrt(variance) if variance is not None else None


class SampleStandardDeviation(StandardDeviation[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationStandardDeviation(StandardDeviation[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)
