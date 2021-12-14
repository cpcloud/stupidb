r"""Segment tree and corresponding aggregate function implementations.

The segment tree implementation is based on `Leis, 2015
<http://www.vldb.org/pvldb/vol8/p1058-leis.pdf>`_

This segment tree implementation uses
:class:`~stupidb.functions.associative.core.AssociativeAggregate` instances as
its nodes. The leaves of the tree are computed by calling the
:meth:`~stupidb.functions.associative.core.AssociativeAggregate.step` method
once when the tree is initialized. The fanout of the tree is adjustable.

From the leaves, the traversal continues breadth-first and bottom-up during
which each interior node's current aggregation state is combined with those of
its children by calling the
:meth:`~stupidb.functions.associative.core.AssociativeAggregate.combine`
method. This method takes another instance of the same aggregation as input and
combines the calling instance's aggregation state with the input instance's
aggregation state.

Each interior node therefore contains the combined aggregation value of all of
its children. This makes it possible to compute a range query in
:math:`O\left(\log{N}\right)` time rather than :math:`O\left(N\right)`.

Here's an example of a segment tree for the
:class:`~stupidb.functions.associative.Sum` aggregation, constructed with the
following leaves and a fanout of 2::

   [1, 2, 3, 4, 5, 6, 7, 8]

Blue indicates that a node was just aggregated into its parent, and red
indicates a node that contains the aggregate value of all of its children.

.. image:: ../_static/main.gif
   :align: center

.. note::

   You can generate this GIF yourself by typing::

       $ python -m stupidb.associative.animate > segment_tree.gif

   at the command line, after you've installed stupidb. The code used to
   generate the GIF lives in :mod:`stupidb.associative.animate`.

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

from __future__ import annotations

import math
import typing
from typing import Callable, TypeVar

from ...protocols import Comparable
from ...typehints import R1, R2, Input1, R
from ..associative.core import BinaryAssociativeAggregate, UnaryAssociativeAggregate


class Count(UnaryAssociativeAggregate[Input1, int]):
    """Count column values."""

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    def step(self, input1: Input1 | None) -> None:
        """Add one to the count if `input1` is not :data:`None`."""
        self.count += input1 is not None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(count={self.count!r})"

    def finalize(self) -> int | None:
        """Return the count."""
        return self.count

    def combine(self: Count[Input1], other: Count[Input1]) -> None:
        """Combine two :class:`Count` instances."""
        self.count += other.count


class Sum(UnaryAssociativeAggregate[R1, R2]):
    """Sum column values, ignoring nulls."""

    __slots__ = "count", "total"

    def __init__(self) -> None:
        super().__init__()
        self.total = typing.cast(R2, 0)
        self.count = 0

    def __repr__(self) -> str:
        name = type(self).__name__
        count = self.count
        total = self.finalize()
        return f"{name}(total={total!r}, count={count!r})"

    def step(self, input1: R1 | None) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> R2 | None:
        return self.total if self.count else None

    def combine(self: Sum[R1, R2], other: Sum[R1, R2]) -> None:
        self.total += other.total
        self.count += other.count


class Total(Sum[R1, R2]):
    """Sum column values, preserving nulls."""

    __slots__ = ()

    def finalize(self) -> R2 | None:
        return self.total if self.count else typing.cast(R2, 0)


class Mean(Sum[R1, R2]):
    """Average values in a column."""

    __slots__ = ()

    def finalize(self) -> R2 | None:
        count = self.count
        return self.total / count if count > 0 else None

    def __repr__(self) -> str:
        name = type(self).__name__
        total = super().finalize()
        count = self.count
        mean = self.finalize()
        return f"{name}(total={total!r}, count={count!r}, mean={mean!r})"


C = TypeVar("C", bound=Comparable)


class MinMax(UnaryAssociativeAggregate[C, C]):
    """Base class modeling min/max order statistics."""

    __slots__ = "current_value", "comparator"

    def __init__(self, *, comparator: Callable[[C, C], C]) -> None:
        super().__init__()
        self.current_value: C | None = None
        self.comparator = comparator

    def step(self, input1: C | None) -> None:
        if input1 is not None:
            if self.current_value is None:
                self.current_value = input1
            else:
                self.current_value = self.comparator(self.current_value, input1)

    def finalize(self) -> C | None:
        return self.current_value

    def combine(self, other: MinMax) -> None:
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
    """Minimum of column values."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=min)


class Max(MinMax):
    """Maximum of column values."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=max)


class Covariance(BinaryAssociativeAggregate[R1, R2, float]):
    """Base class modeling the covariance of two columns."""

    __slots__ = "count", "mean_x", "mean_y", "cov", "ddof"

    def __init__(self, *, ddof: int) -> None:
        super().__init__()
        self.count = 0
        self.mean_x = 0.0
        self.mean_y = 0.0
        self.cov = 0.0
        self.ddof = ddof

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f"{name}(mean_x={self.mean_x!r}, mean_y={self.mean_y!r}, "
            f"cov={self.cov!r}, count={self.count!r})"
        )

    def step(self, x: R1 | None, y: R2 | None) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.mean_x
            self.mean_x += delta_x + count
            self.mean_y += (y - self.mean_y) / count
            self.cov += delta_x * (y - self.mean_y)

    def finalize(self) -> float | None:
        denom = self.count - self.ddof
        return self.cov / denom if denom > 0 else None

    def combine(self, other: Covariance[R1, R2]) -> None:
        count = self.count + other.count
        self.cov += (
            other.cov
            + (self.mean_x - other.mean_x)
            * (self.mean_y - other.mean_y)
            * (self.count * other.count)
            / count
        )
        self.mean_x = (self.count * self.mean_x + other.count * other.mean_x) / count
        self.mean_y = (self.count * self.mean_y + other.count * other.mean_y) / count
        self.count = count


class SampleCovariance(Covariance[R1, R2]):
    """Sample covariance of two columns."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance[R1, R2]):
    """Population covariance of two columns."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class Variance(UnaryAssociativeAggregate[R, float]):
    """Base class modeling the variance of a column."""

    __slots__ = ("aggregator",)

    def __init__(self, ddof: int) -> None:
        self.aggregator = Covariance[R, R](ddof=ddof)

    def step(self, x: R | None) -> None:
        self.aggregator.step(x, x)

    def finalize(self) -> float | None:
        return self.aggregator.finalize()

    def combine(self, other: Variance[R]) -> None:
        self.aggregator.combine(other.aggregator)


class SampleVariance(Variance[R]):
    """Sample variance of a column."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationVariance(Variance[R]):
    """Population variance of a column."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class StandardDeviation(Variance[R]):
    """Base class modeling the standard deviation of a column."""

    __slots__ = ("aggregator",)

    def finalize(self) -> float | None:
        variance = super().finalize()
        return math.sqrt(variance) if variance is not None else None


class SampleStandardDeviation(StandardDeviation[R]):
    """Sample standard deviation of a column."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationStandardDeviation(StandardDeviation[R]):
    """Population standard deviation of a column."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)
