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
from stupidb.navigation import First, Lag, Last, Lead, Nth, RowNumber
from stupidb.protocols import Comparable
from stupidb.row import AbstractRow
from stupidb.stupidb import (
    Aggregation,
    Aggregations,
    Difference,
    FullProjector,
    GroupBy,
    Intersection,
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
)
from stupidb.typehints import OrderBy, RealGetter


class shiftable(curry):
    def __rrshift__(self, other):
        return self(other)


@shiftable
def table(rows: Iterable[Mapping[str, Any]]) -> Relation:
    """Construct a relation from an iterable of mappings.

    Parameters
    ----------
    rows
        An iterable of mappings whose keys are strings.

    """
    return Relation.from_iterable(rows)


@shiftable
def cross_join(right: Relation, left: Relation) -> Relation:
    """Return the Cartesian product of tuples from `left` and `right`.

    Parameters
    ----------
    left
        A relation
    right
        A relation

    """
    return Join(left, right, lambda row: True)


@shiftable
def inner_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a ``bool``.

    """
    return Join(left, right, predicate)


@shiftable
def left_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `left`.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a ``bool``.

    """
    return LeftJoin(left, right, predicate)


@shiftable
def right_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `right`.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a ``bool``.

    """
    return RightJoin(left, right, predicate)


@shiftable
def _order_by(order_by: Tuple[OrderBy, ...], child: Relation) -> Relation:
    return SortBy(child, order_by)


def order_by(*order_by: OrderBy) -> Relation:
    """Order the rows of the child operator according to `order_by`.

    Parameters
    ----------
    order_by
        A sequence of ``OrderBy`` instances

    """
    return _order_by(order_by)


@shiftable
def _select(
    projectors: Mapping[str, FullProjector], child: Relation
) -> Relation:
    return Projection(child, projectors)


def select(**projectors: FullProjector) -> Relation:
    """Subset or compute columns from `projectors`.

    Parameters
    ----------
    projectors
        A mapping from ``str`` to ``FullProjector`` instances.

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


@shiftable
def _mutate(
    mutators: Mapping[str, FullProjector], child: Relation
) -> Relation:
    return Mutate(child, mutators)


def mutate(**mutators: FullProjector) -> Relation:
    """Add new columns specified by `mutators`.

    Parameters
    ----------
    projectors
        A mapping from ``str`` to ``FullProjector`` instances.

    Notes
    -----
    Columns are appended, unlike :func:`~stupidb.api.select`.

    See Also
    --------
    select

    """
    return _mutate(mutators)


@shiftable
def sift(predicate: Predicate, child: Relation) -> Relation:
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


@shiftable
def _aggregate(aggregations: Aggregations, child: Relation) -> Relation:
    return Aggregation(child, aggregations)


def aggregate(**aggregations: AggregateSpecification) -> Relation:
    """Aggregate values from the child operator using `aggregations`."""
    return _aggregate(aggregations)


@shiftable
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


@shiftable
def _group_by(
    group_by: Mapping[str, PartitionBy], child: Relation
) -> Relation:
    return GroupBy(child, group_by)


def group_by(**group_by: PartitionBy) -> Relation:
    """Group the rows of the child operator according to `group_by`.

    Parameters
    ----------
    group_by
        A mapping of ``str`` (column names) to functions that compute grouping
        keys.

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
@shiftable
def union(right: Relation, left: Relation) -> Relation:
    """Compute the set union of `left` and `right`.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    """
    return Union(left, right)


@shiftable
def intersection(right: Relation, left: Relation) -> Relation:
    """Compute the set intersection of `left` and `right`.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    """
    return Intersection(left, right)


@shiftable
def difference(right: Relation, left: Relation) -> Relation:
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


# Aggregations
def count(x: Callable[[AbstractRow], Optional[V]]) -> AggregateSpecification:
    """Count the number of non-NULL values of `x`."""
    return AggregateSpecification(Count, (x,))


def sum(x: RealGetter) -> AggregateSpecification:
    """Compute the sum of `x`, with an empty column summing to NULL.

    Parameters
    ----------
    x
        A function produce a column from an :class:`~stupidb.row.AbstractRow`.

    """
    return AggregateSpecification(Sum, (x,))


def total(x: RealGetter) -> AggregateSpecification:
    """Compute the sum of `x`, with an empty column summing to zero.

    Parameters
    ----------
    x
        A function produce a column from an :class:`~stupidb.row.AbstractRow`.

    """
    return AggregateSpecification(Total, (x,))


def first(
    getter: Callable[[AbstractRow], Optional[V]]
) -> AggregateSpecification:
    """Compute the first row of `x` over a window."""
    return AggregateSpecification(First, (getter,))


def last(x: Callable[[AbstractRow], Optional[V]]) -> AggregateSpecification:
    """Compute the last row of `x` over a window."""
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


def row_number():
    """Compute the row number over a window."""
    return AggregateSpecification(RowNumber, ())


def lead(
    x: Callable[[AbstractRow], Optional[V]],
    n: Callable[[AbstractRow], int],
    default: Optional[Callable[[AbstractRow], Optional[V]]] = None,
) -> AggregateSpecification:
    """Lead a column `x` by `n` rows, using `default` for NULL values."""
    return AggregateSpecification(
        Lead, (x, n, default if default is not None else (lambda row: None))
    )


def lag(
    x: Callable[[AbstractRow], Optional[V]],
    n: Callable[[AbstractRow], int],
    default: Optional[Callable[[AbstractRow], Optional[V]]] = None,
) -> AggregateSpecification:
    """Lag a column `x` by `n` rows, using `default` for NULL values."""
    return AggregateSpecification(
        Lag, (x, n, default if default is not None else (lambda row: None))
    )


def mean(x: RealGetter) -> AggregateSpecification:
    """Average of a column."""
    return AggregateSpecification(Mean, (x,))


def min(
    x: Callable[[AbstractRow], Optional[Comparable]]
) -> AggregateSpecification:
    """Minimum of a column."""
    return AggregateSpecification(Min, (x,))


def max(
    x: Callable[[AbstractRow], Optional[Comparable]]
) -> AggregateSpecification:
    """Maximum of a column."""
    return AggregateSpecification(Max, (x,))


def cov_samp(x: RealGetter, y: RealGetter) -> AggregateSpecification:
    """Sample covariance of two columns."""
    return AggregateSpecification(SampleCovariance, (x, y))


def var_samp(x: RealGetter) -> AggregateSpecification:
    """Sample variance of a column."""
    return AggregateSpecification(SampleVariance, (x,))


def stdev_samp(x: RealGetter) -> AggregateSpecification:
    """Sample standard deviation of a column."""
    return AggregateSpecification(SampleStandardDeviation, (x,))


def cov_pop(x: RealGetter, y: RealGetter) -> AggregateSpecification:
    """Population covariance of two columns."""
    return AggregateSpecification(PopulationCovariance, (x, y))


def var_pop(x: RealGetter) -> AggregateSpecification:
    """Population variance of a column."""
    return AggregateSpecification(PopulationVariance, (x,))


def stdev_pop(x: RealGetter) -> AggregateSpecification:
    """Population standard deviation of a column."""
    return AggregateSpecification(PopulationStandardDeviation, (x,))
