"""StupiDB user-facing API.

.. note::

   The join functions all take `right`, `predicate` and then `left` as
   arguments, **in that order**.

   This is intentional, and is the way the functions must be written to enable
   `currying <https://en.wikipedia.org/wiki/Currying>`_.  Currying is the
   technique that allows us to use the right shift operator (``>>``) to chain
   operations.

"""

import inspect
from typing import Any, Callable, Iterable, Mapping, Optional, TypeVar

from toolz import curry

from stupidb.aggregation import (
    AggregateSpecification,
    FrameClause,
    WindowAggregateSpecification,
)
from stupidb.associative import (
    Count,
    Max,
    Mean,
    Min,
    PopulationCovariance,
    PopulationStandardDeviation,
    PopulationVariance,
    SampleCovariance,
    SampleStandardDeviation,
    SampleVariance,
    Sum,
    Total,
)
from stupidb.navigation import First, Lag, Last, Lead, Nth
from stupidb.protocols import Comparable
from stupidb.ranking import Rank, RowNumber
from stupidb.row import AbstractRow
from stupidb.stupidb import (
    Aggregation,
    Difference,
    FullProjector,
    GroupBy,
    Intersect,
    IntersectAll,
    Join,
    LeftJoin,
    Mutate,
    PartitionBy,
    Predicate,
    Projection,
    Relation,
    RightJoin,
    Selection,
    SortBy,
    Tuple,
    Union,
    UnionAll,
)
from stupidb.typehints import R1, R2, OrderBy, R


class _shiftable(curry):
    """Shiftable curry."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.__annotations__ = self.func.__annotations__

    @property
    def __signature__(self) -> inspect.Signature:
        return inspect.signature(self.func)

    def __rrshift__(self, other: Relation) -> "_shiftable":
        return self(other)


@_shiftable
def table(rows: Iterable[Mapping[str, Any]]) -> Relation:
    """Construct a relation from an iterable of mappings.

    Parameters
    ----------
    rows
        An iterable of mappings whose keys are :class:`str` instances.

    """
    return Relation.from_iterable(rows)


@_shiftable
def cross_join(right: Relation, left: Relation) -> Join:
    """Return the Cartesian product of tuples from `left` and `right`.

    Parameters
    ----------
    right
        A relation
    left
        A relation

    """
    return Join(left, right, lambda row: True)


@_shiftable
def inner_join(right: Relation, predicate: Predicate, left: Relation) -> Join:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a :class:`bool`.

    """
    return Join(left, right, predicate)


@_shiftable
def left_join(
    right: Relation, predicate: Predicate, left: Relation
) -> LeftJoin:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `left`.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a :class:`bool`.

    """
    return LeftJoin(left, right, predicate)


@_shiftable
def right_join(
    right: Relation, predicate: Predicate, left: Relation
) -> RightJoin:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `right`.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a :class:`bool`.

    """
    return RightJoin(left, right, predicate)


@_shiftable
def _order_by(order_by: Tuple[OrderBy, ...], child: Relation) -> SortBy:
    return SortBy(child, order_by)


def order_by(*order_by: OrderBy) -> SortBy:
    """Order the rows of the child operator according to `order_by`.

    Parameters
    ----------
    order_by
        A sequence of ``OrderBy`` instances

    """
    return _order_by(order_by)


@_shiftable
def _select(
    projectors: Mapping[str, FullProjector], child: Relation
) -> Projection:
    return Projection(child, projectors)


def select(**projectors: FullProjector) -> Projection:
    """Subset or compute columns from `projectors`.

    Parameters
    ----------
    projectors
        A mapping from :class:`str` to ``FullProjector`` instances.

    """
    valid_projectors = {
        name: projector
        for name, projector in projectors.items()
        if callable(projector)
        or isinstance(projector, WindowAggregateSpecification)
    }
    if len(valid_projectors) != len(projectors):
        raise TypeError("Invalid projection")
    return _select(projectors)


@_shiftable
def _mutate(mutators: Mapping[str, FullProjector], child: Relation) -> Mutate:
    return Mutate(child, mutators)


def mutate(**mutators: FullProjector) -> Mutate:
    """Add new columns specified by `mutators`.

    Parameters
    ----------
    projectors
        A mapping from :class:`str` to ``FullProjector`` instances.

    Notes
    -----
    Columns are appended, unlike :func:`~stupidb.api.select`.

    See Also
    --------
    select

    """
    return _mutate(mutators)


@_shiftable
def sift(predicate: Predicate, child: Relation) -> Selection:
    """Filter rows in `child` according to `predicate`.

    Parameters
    ----------
    predicate
        A callable of one argument taking an :class:`~stupidb.row.AbstractRow`
        and returning a ``bool``.

    """
    return Selection(child, predicate)


def exists(relation: Relation) -> bool:
    """Compute whether any of the rows in `relation` are truthy.

    This is useful for computing semi-joins.

    """
    return any(relation)


@_shiftable
def _aggregate(
    aggregations: Mapping[str, AggregateSpecification], child: Relation
) -> Aggregation:
    return Aggregation(child, aggregations)


def aggregate(**aggregations: AggregateSpecification) -> Aggregation:
    """Aggregate values from the child operator using `aggregations`.

    Parameters
    ----------
    aggregations
        A mapping from :class:`str` column names to
        :class:`~stupidb.aggregation.AggregateSpecification` instances.

    """
    return _aggregate(aggregations)


@_shiftable
def over(
    window: FrameClause, child: AggregateSpecification
) -> WindowAggregateSpecification:
    """Construct a window aggregate.

    Parameters
    ----------
    window
        A :class:`~stupidb.aggregation.FrameClause` instance constructed from
        :class:`~stupidb.aggregation.Window.rows` or
        :class:`~stupidb.aggregation.Window.range`.
    child
        The aggregation to compute over `window`

    Notes
    -----
    This is one of the few user-facing functions that does **not** return a
    :class:`~stupidb.stupidb.Relation`. The behavior of materializing the rows
    of the result of calling this function is undefined.

    """
    return WindowAggregateSpecification(
        child.aggregate_type, child.getters, window
    )


@_shiftable
def _group_by(group_by: Mapping[str, PartitionBy], child: Relation) -> GroupBy:
    return GroupBy(child, group_by)


def group_by(**group_by: PartitionBy) -> GroupBy:
    """Group the rows of the child operator according to `group_by`.

    Parameters
    ----------
    group_by
        A mapping of :class:`str` column names to functions that compute
        grouping keys.

    Notes
    -----
    Iterating over the rows of the result of this function is not very useful,
    since its :meth:`~stupidb.stupidb.GroupBy.__iter__` method just yields
    the rows of its child. A call to this function is best followed by a call
    to :func:`~stupidb.api.aggregate`.

    See Also
    --------
    aggregate

    """
    return _group_by(group_by)


# Set operations
@_shiftable
def union(right: Relation, left: Relation) -> Union:
    """Compute the union of `left` and `right`, ignoring duplicate rows.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    See Also
    --------
    union_all

    """
    return Union(left, right)


@_shiftable
def union_all(right: Relation, left: Relation) -> UnionAll:
    """Compute the union of `left` and `right`, preserving duplicate rows.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    See Also
    --------
    union

    """
    return UnionAll(left, right)


@_shiftable
def intersect(right: Relation, left: Relation) -> Intersect:
    """Compute the intersection of `left` and `right`, ignoring duplicate rows.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    See Also
    --------
    intersect_all

    """
    return Intersect(left, right)


@_shiftable
def intersect_all(right: Relation, left: Relation) -> IntersectAll:
    """Compute the intersection of `left` and `right`, preserving duplicates.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    See Also
    --------
    intersect

    """
    return IntersectAll(left, right)


@_shiftable
def difference(right: Relation, left: Relation) -> Difference:
    """Compute the set difference of `left` and `right`.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    """
    return Difference(left, right)


V = TypeVar("V")


# Aggregate functions
def count(x: Callable[[AbstractRow], Optional[V]]) -> AggregateSpecification:
    """Count the number of non-NULL values of `x`.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(Count, (x,))


def sum(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the sum of `x`, with an empty column summing to NULL.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(Sum, (x,))


def total(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the sum of `x`, with an empty column summing to zero.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(Total, (x,))


def first(x: Callable[[AbstractRow], Optional[V]]) -> AggregateSpecification:
    """Compute the first row of `x` over a window.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(First, (x,))


def last(x: Callable[[AbstractRow], Optional[V]]) -> AggregateSpecification:
    """Compute the last row of `x` over a window.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(Last, (x,))


def nth(
    x: Callable[[AbstractRow], Optional[V]],
    i: Callable[[AbstractRow], Optional[int]],
) -> AggregateSpecification:
    """Compute the `i`th row of `x` over a window.

    Parameters
    ----------
    x
        Column selector.
    i
        Callable to compute the row offset of the window to return.

    """
    return AggregateSpecification(Nth, (x, i))


def row_number() -> AggregateSpecification:
    """Compute the row number over a window."""
    return AggregateSpecification(RowNumber, ())


def rank() -> AggregateSpecification:
    """Rank the rows of a relation based on the ordering key given in over."""
    return AggregateSpecification(Rank, ())


def lead(
    x: Callable[[AbstractRow], Optional[V]],
    n: Callable[[AbstractRow], Optional[int]] = (lambda row: 1),
    default: Callable[[AbstractRow], Optional[V]] = (lambda row: None),
) -> AggregateSpecification:
    """Lead a column `x` by `n` rows, using `default` for NULL values.

    Parameters
    ----------
    x
        A column selector.
    n
        A callable computing the number of rows to lead. Defaults to a lead of
        **1** row. The callable takes the current row as input and thus the
        lead can be computed relative to the current row.
    default
        A callable computing the default value for the lead if the row would
        produce a NULL value when led. The callable takes the current row as
        input and thus the default can be computed relative to the current row.

    """
    return AggregateSpecification(Lead, (x, n, default))


def lag(
    x: Callable[[AbstractRow], Optional[V]],
    n: Callable[[AbstractRow], Optional[int]] = (lambda row: 1),
    default: Callable[[AbstractRow], Optional[V]] = (lambda row: None),
) -> AggregateSpecification:
    """Lag a column `x` by `n` rows, using `default` for NULL values.

    Parameters
    ----------
    x
        A column selector.
    n
        A callable computing the number of rows to lag. Defaults to a lag of
        **1** row. The callable takes the current row as input and thus the lag
        can be computed relative to the current row.
    default
        A callable computing the default value for the lag if the row would
        produce a NULL value when lagged. The callable takes the current row as
        input and thus the default can be computed relative to the current row.

    """
    return AggregateSpecification(Lag, (x, n, default))


def mean(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Average of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Mean, (x,))


def min(
    x: Callable[[AbstractRow], Optional[Comparable]]
) -> AggregateSpecification:
    """Minimum of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Min, (x,))


def max(
    x: Callable[[AbstractRow], Optional[Comparable]]
) -> AggregateSpecification:
    """Maximum of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Max, (x,))


def cov_samp(
    x: Callable[[AbstractRow], R1], y: Callable[[AbstractRow], R2]
) -> AggregateSpecification:
    """Sample covariance of two columns.

    Parameters
    ----------
    x
        A column selector.
    y
        A column selector.

    """
    return AggregateSpecification(SampleCovariance, (x, y))


def var_samp(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Sample variance of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(SampleVariance, (x,))


def stdev_samp(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Sample standard deviation of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(SampleStandardDeviation, (x,))


def cov_pop(
    x: Callable[[AbstractRow], R1], y: Callable[[AbstractRow], R2]
) -> AggregateSpecification:
    """Population covariance of two columns.

    Parameters
    ----------
    x
        A column selector.
    y
        A column selector.

    """
    return AggregateSpecification(PopulationCovariance, (x, y))


def var_pop(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Population variance of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(PopulationVariance, (x,))


def stdev_pop(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Population standard deviation of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(PopulationStandardDeviation, (x,))
