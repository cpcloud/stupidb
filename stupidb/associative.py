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

Here's an example of a segment tree for the :class:`~stupidb.associative.Sum`
aggregation, constructed with leaves::

   >>> [1, 2, 3, 4]

.. graphviz::
   :align: center

   digraph segtree {
       0 [label=10];
       1 [label=3];
       2 [label=7];
       3 [label=1];
       4 [label=2];
       5 [label=3];
       6 [label=4];
       0 -> 1 [dir=back];
       0 -> 2 [dir=back];
       1 -> 3 [dir=back];
       1 -> 4 [dir=back];
       2 -> 5 [dir=back];
       2 -> 6 [dir=back];
   }

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
import collections
import functools
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
    Set,
    Tuple,
    Type,
    TypeVar,
)

import stupidb.indextree as indextree
from stupidb.aggregatetypes import Aggregate
from stupidb.aggregator import Aggregator
from stupidb.protocols import Comparable
from stupidb.typehints import R1, R2, Input1, Input2, Output, R, Result, T

AssociativeAggregate = TypeVar(
    "AssociativeAggregate",
    "UnaryAssociativeAggregate",
    "BinaryAssociativeAggregate",
)


def make_segment_tree(
    leaf_arguments: Sequence[Tuple[T, ...]],
    aggregate_type: Type[AssociativeAggregate],
    *,
    fanout: int,
) -> Sequence[AssociativeAggregate]:
    """Make a segment tree from tuples `leaves` and class `aggregate`.

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
    height = int(math.ceil(math.log(number_of_leaves, fanout))) + 1
    index_tree = indextree.IndexTree(height=height, fanout=fanout)
    segment_tree_nodes: MutableSequence[AssociativeAggregate] = [
        aggregate_type() for _ in range(len(index_tree))
    ]
    queue = collections.deque(index_tree.leaves)

    # seed the leaves
    for leaf_index, args in zip(queue, leaf_arguments):
        leaf = segment_tree_nodes[leaf_index]
        leaf.step(*args)

    seen: Set[int] = set()

    traversed = 0
    while queue:
        node = queue.popleft()
        if node not in seen:
            node_agg = segment_tree_nodes[node]
            parent = index_tree.parent(node)
            parent_agg = segment_tree_nodes[parent]
            parent_agg.update(node_agg)
            seen.add(node)
            if parent:
                # don't append the root, since we've already aggregated into
                # that node if parent == 0
                queue.append(parent)
            traversed += 1

    # assert the invariant that we have traversed N - 1 nodes (-1 because we
    # don't traverse the root)
    assert traversed == len(segment_tree_nodes) - 1, (
        f"traversed == {traversed}, "
        f"len(segment_tree_nodes) == {len(segment_tree_nodes)}"
    )
    return segment_tree_nodes


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

    __slots__ = "nodes", "aggregate_type", "levels", "fanout"

    def __init__(
        self,
        leaves: Sequence[Tuple[T, ...]],
        aggregate_type: Type[AssociativeAggregate],
        *,
        fanout: int = 4,
    ) -> None:
        self.nodes: Sequence[AssociativeAggregate] = make_segment_tree(
            leaves, aggregate_type, fanout=fanout
        )
        self.aggregate_type: Type[AssociativeAggregate] = aggregate_type
        self.fanout = fanout
        self.levels: Sequence[Sequence[AssociativeAggregate]] = list(
            self.iterlevels(self.nodes, fanout=fanout)
        )

    @classmethod
    def iterlevels(
        cls, nodes: Sequence[Optional[AssociativeAggregate]], *, fanout: int
    ) -> Iterator[List[AssociativeAggregate]]:
        """Iterate over every level in the tree starting from the bottom.

        Parameters
        ----------
        nodes
            The nodes of the tree whose levels will be yielded.
        fanout
            The number child nodes per interior node

        """
        height = int(math.ceil(math.log(len(nodes), fanout)))
        getitem = nodes.__getitem__
        for level in range(1, height + 1):
            start = indextree.first_node(level, fanout=fanout)
            stop = indextree.last_node(level, fanout=fanout)
            res = list(filter(None, map(getitem, range(start, stop))))
            yield res

    def __repr__(self) -> str:
        # strip because the base case is the empty string + a newline
        return indextree.reprtree(self.nodes, fanout=self.fanout).strip()

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

    aggregator_class: ClassVar[Callable[..., Aggregator]] = functools.partial(
        SegmentTree, fanout=4
    )

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
