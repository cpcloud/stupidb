"""StupiDB user-facing API.

.. note::

   The join functions all take `right`, `predicate` and then `left` as
   arguments, **in that order**.

   This is intentional, and is the way the functions must be written to enable
   `currying <https://en.wikipedia.org/wiki/Currying>`_.  Currying is the
   technique that allows us to use the right shift operator (``>>``) to chain
   operations.

"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable, Mapping, Optional

from toolz import curry

from .aggregation import Window  # noqa: F401
from .aggregation import (
    AggregateSpecification,
    FrameClause,
    Nulls,
    WindowAggregateSpecification,
)
from .associative import (
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
from .core import (
    Aggregation,
    CrossJoin,
    Difference,
    DifferenceAll,
    FullProjector,
    GroupBy,
    InnerJoin,
    Intersect,
    IntersectAll,
    Join,
    JoinPredicate,
    LeftJoin,
    Limit,
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
from .navigation import First, Lag, Last, Lead, Nth
from .protocols import Comparable
from .ranking import DenseRank, Rank, RowNumber
from .row import AbstractRow
from .typehints import R1, R2, OrderBy, R, T


class _shiftable(curry):
    """Shiftable curry."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.__annotations__ = self.func.__annotations__

    @property
    def __signature__(self) -> inspect.Signature:
        return inspect.signature(self.func)  # pragma: no cover

    def __rrshift__(self, other: Relation) -> _shiftable:
        return self(other)


@_shiftable
def table(rows: Iterable[Mapping[str, Any]]) -> Relation:
    """Construct a relation from an iterable of mappings.

    Parameters
    ----------
    rows
        An iterable of mappings whose keys are :class:`str` instances.

    Examples
    --------
    >>> from stupidb.api import table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> t = table(rows)
    >>> t  # doctest: +ELLIPSIS
    <stupidb.core.Relation object at 0x...>

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

    Examples
    --------
    >>> from stupidb.api import cross_join, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> t = table(rows)
    >>> s = table(rows)
    >>> crossed = cross_join(t, s)
    >>> crossed  # doctest: +ELLIPSIS
    <stupidb.core.CrossJoin object at 0x...>

    """
    return CrossJoin(left, right)


@_shiftable
def inner_join(right: Relation, predicate: JoinPredicate, left: Relation) -> Join:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.

    Parameters
    ----------
    right
        A relation
    predicate
        A callable taking two arguments and returning a :class:`bool`.

    Examples
    --------
    >>> from stupidb.api import cross_join, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> t = table(rows)
    >>> s = table(rows)
    >>> crossed = cross_join(t, s)
    >>> crossed  # doctest: +ELLIPSIS
    <stupidb.core.CrossJoin object at 0x...>

    """
    return InnerJoin(left, right, predicate)


@_shiftable
def left_join(right: Relation, predicate: JoinPredicate, left: Relation) -> LeftJoin:
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
def right_join(right: Relation, predicate: JoinPredicate, left: Relation) -> RightJoin:
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
def _order_by(order_by: Tuple[OrderBy, ...], nulls: Nulls, child: Relation) -> SortBy:
    return SortBy(child, order_by, nulls)


def order_by(*order_by: OrderBy, nulls: Nulls = Nulls.FIRST) -> SortBy:
    """Order the rows of the child operator according to `order_by`.

    Parameters
    ----------
    order_by
        A sequence of ``OrderBy`` instances
    nulls
        One of :class:`~stupidb.aggregation.Nulls` indicating how to treat
        nulls when sorting. :attr:`~stupidb.aggregation.Nulls.FIRST` treats
        nulls as less than every other value, and
        :attr:`~stupidb.aggregation.Nulls.LAST` treats them as greater than
        every other value.

    Examples
    --------
    >>> from stupidb.api import order_by, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> ordered = table(rows) >> order_by(lambda r: r.balance)
    >>> balances = [row.balance for row in ordered]
    >>> balances
    [-300, -100, 400, 700]

    """
    return _order_by(order_by, nulls)


@_shiftable
def _select(projectors: Mapping[str, FullProjector], child: Relation) -> Projection:
    return Projection(child, projectors)


def select(**projectors: FullProjector) -> Projection:
    """Subset or compute new columns from `projectors`.

    Parameters
    ----------
    projectors
        A mapping from :class:`str` to :data:`FullProjector` instances.

    Examples
    --------
    >>> from stupidb.api import select, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> names = table(rows) >> select(lower_name=lambda r: r.name.lower())
    >>> names = [row.lower_name for row in names]
    >>> names
    ['bob', 'alice', 'bob', 'alice']

    See Also
    --------
    mutate

    """
    valid_projectors = {
        name: projector
        for name, projector in projectors.items()
        if callable(projector) or isinstance(projector, WindowAggregateSpecification)
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
        A mapping from :class:`str` to :data:`FullProjector` instances.

    Notes
    -----
    Columns are appended, unlike :func:`~stupidb.api.select`.

    Examples
    --------
    >>> from pprint import pprint
    >>> from stupidb.api import mutate, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> rows = table(rows) >> mutate(lower_name=lambda r: r.name.lower())
    >>> pprint(list(rows))
    [Row({'name': 'Bob', 'balance': -300, 'lower_name': 'bob'}),
     Row({'name': 'Alice', 'balance': 400, 'lower_name': 'alice'}),
     Row({'name': 'Bob', 'balance': -100, 'lower_name': 'bob'}),
     Row({'name': 'Alice', 'balance': 700, 'lower_name': 'alice'})]

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

    Examples
    --------
    >>> from pprint import pprint
    >>> from stupidb.api import sift, table
    >>> rows = [
    ...     dict(name="Bob", balance=-300),
    ...     dict(name="Alice", balance=400),
    ...     dict(name="Bob", balance=-100),
    ...     dict(name="Alice", balance=700),
    ... ]
    >>> rows = table(rows) >> sift(lambda r: r.name.lower().startswith("a"))
    >>> pprint(list(rows), width=79)
    [Row({'name': 'Alice', 'balance': 400}),
     Row({'name': 'Alice', 'balance': 700})]

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

    Examples
    --------
    Compute the average of a column:

    >>> from pprint import pprint
    >>> from stupidb.api import aggregate, group_by, mean, table
    >>> rows = [
    ...     dict(name="Bob", age=30, timezone="America/New_York"),
    ...     dict(name="Susan", age=20, timezone="America/New_York"),
    ...     dict(name="Joe", age=41, timezone="America/Los_Angeles"),
    ...     dict(name="Alice", age=39, timezone="America/Los_Angeles"),
    ... ]
    >>> average_age = table(rows) >> aggregate(avg_age=mean(lambda r: r.age))
    >>> pprint(list(average_age), width=79)
    [Row({'avg_age': 32.5})]

    Compute the average a column, grouped by another column:

    >>> average_age_by_timezone = (
    ...     table(rows) >> group_by(tz=lambda r: r.timezone)
    ...                 >> aggregate(avg_age=mean(lambda r: r.age))
    ... )
    >>> pprint(list(average_age_by_timezone), width=79)
    [Row({'tz': 'America/New_York', 'avg_age': 25.0}),
     Row({'tz': 'America/Los_Angeles', 'avg_age': 40.0})]

    See Also
    --------
    group_by

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
    :class:`~stupidb.core.Relation`. The behavior of materializing the rows
    of the result of calling this function is undefined.

    Examples
    --------
    >>> from stupidb.api import Window, over, mean, select, table
    >>> from datetime import date, timedelta
    >>> today = date(2019, 2, 9)
    >>> days = timedelta(days=1)
    >>> rows = [
    ...     {"name": "Alice", "balance": 400, "date": today},
    ...     {"name": "Alice", "balance": 300, "date": today + 1 * days},
    ...     {"name": "Alice", "balance": 100, "date": today + 2 * days},
    ...     {"name": "Bob", "balance": -150, "date": today - 4 * days},
    ...     {"name": "Bob", "balance": 200, "date": today - 3 * days},
    ... ]
    >>> t = table(rows)
    >>> window = Window.range(
    ...     partition_by=[lambda r: r.name],
    ...     order_by=[lambda r: r.date],
    ...     preceding=lambda r: 2 * days  # two days behind + the current row
    ... )
    >>> avg_balance_per_person = table(rows) >> select(
    ...     name=lambda r: r.name,
    ...     avg_balance=mean(lambda r: r.balance) >> over(window),
    ...     balance=lambda r: r.balance,
    ...     date=lambda r: r.date,
    ... ) >> order_by(lambda r: r.name, lambda r: r.date)
    >>> pprint(list(avg_balance_per_person), width=79)  # noqa: E501
    [Row({'name': 'Alice', 'balance': 400, 'date': datetime.date(2019, 2, 9), 'avg_balance': 400.0}),
     Row({'name': 'Alice', 'balance': 300, 'date': datetime.date(2019, 2, 10), 'avg_balance': 350.0}),
     Row({'name': 'Alice', 'balance': 100, 'date': datetime.date(2019, 2, 11), 'avg_balance': 266.6666666666667}),
     Row({'name': 'Bob', 'balance': -150, 'date': datetime.date(2019, 2, 5), 'avg_balance': -150.0}),
     Row({'name': 'Bob', 'balance': 200, 'date': datetime.date(2019, 2, 6), 'avg_balance': 25.0})]

    """
    return WindowAggregateSpecification(child.aggregate_type, child.getters, window)


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
    since its :meth:`~stupidb.core.GroupBy.__iter__` method just yields
    the rows of its child. A call to this function is best followed by a call
    to :func:`~stupidb.api.aggregate`.

    Examples
    --------
    >>> from pprint import pprint
    >>> from stupidb.api import aggregate, group_by, mean, table
    >>> rows = [
    ...     dict(name="Bob", age=30, timezone="America/New_York"),
    ...     dict(name="Susan", age=20, timezone="America/New_York"),
    ...     dict(name="Joe", age=41, timezone="America/Los_Angeles"),
    ...     dict(name="Alice", age=39, timezone="America/Los_Angeles"),
    ... ]
    >>> average_age_by_timezone = (
    ...     table(rows) >> group_by(tz=lambda r: r.timezone)
    ...                 >> aggregate(avg_age=mean(lambda r: r.age))
    ... )
    >>> pprint(list(average_age_by_timezone), width=79)
    [Row({'tz': 'America/New_York', 'avg_age': 25.0}),
     Row({'tz': 'America/Los_Angeles', 'avg_age': 40.0})]

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


@_shiftable
def difference_all(right: Relation, left: Relation) -> DifferenceAll:
    """Compute the set difference of `left` and `right`, preserving duplicates.

    Parameters
    ----------
    right
         A relation
    left
         A relation

    """
    return DifferenceAll(left, right)


@_shiftable
def limit(limit: int, relation: Relation, *, offset: int = 0) -> Limit:
    """Return the rows in `relation` starting from `offset` up to `limit`.

    Parameters
    ----------
    limit
        The number of rows starting from offset to produce
    relation
        Relation whose rows to limit
    offset
        The number of rows to skip before yielding

    """
    if offset < 0:
        raise ValueError(f"invalid offset, must be non-negative: {offset}")
    if limit < 0:
        raise ValueError(f"invalid limit, must be non-negative: {limit}")
    return Limit(relation, offset=offset, limit=limit)


# Aggregate functions
def count(x: Callable[[AbstractRow], Optional[T]]) -> AggregateSpecification:
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


def first(x: Callable[[AbstractRow], Optional[T]]) -> AggregateSpecification:
    """Compute the first row of `x` over a window.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(First, (x,))


def last(x: Callable[[AbstractRow], Optional[T]]) -> AggregateSpecification:
    """Compute the last row of `x` over a window.

    Parameters
    ----------
    x
        A column getter.

    """
    return AggregateSpecification(Last, (x,))


def nth(
    x: Callable[[AbstractRow], Optional[T]],
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


def dense_rank() -> AggregateSpecification:
    """Rank the rows of a relation based on the ordering key given in over."""
    return AggregateSpecification(DenseRank, ())


def lead(
    x: Callable[[AbstractRow], Optional[T]],
    n: Callable[[AbstractRow], Optional[int]] = (lambda row: 1),
    default: Callable[[AbstractRow], Optional[T]] = (lambda row: None),
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
    x: Callable[[AbstractRow], Optional[T]],
    n: Callable[[AbstractRow], Optional[int]] = (lambda row: 1),
    default: Callable[[AbstractRow], Optional[T]] = (lambda row: None),
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
    """Compute the average of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Mean, (x,))


def min(x: Callable[[AbstractRow], Optional[Comparable]]) -> AggregateSpecification:
    """Compute the minimum of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Min, (x,))


def max(x: Callable[[AbstractRow], Optional[Comparable]]) -> AggregateSpecification:
    """Compute the maximum of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(Max, (x,))


def cov_samp(
    x: Callable[[AbstractRow], R1], y: Callable[[AbstractRow], R2]
) -> AggregateSpecification:
    """Compute the sample covariance of two columns.

    Parameters
    ----------
    x
        A column selector.
    y
        A column selector.

    """
    return AggregateSpecification(SampleCovariance, (x, y))


def var_samp(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the sample variance of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(SampleVariance, (x,))


def stdev_samp(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the sample standard deviation of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(SampleStandardDeviation, (x,))


def cov_pop(
    x: Callable[[AbstractRow], R1], y: Callable[[AbstractRow], R2]
) -> AggregateSpecification:
    """Compute the population covariance of two columns.

    Parameters
    ----------
    x
        A column selector.
    y
        A column selector.

    """
    return AggregateSpecification(PopulationCovariance, (x, y))


def var_pop(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the population variance of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(PopulationVariance, (x,))


def stdev_pop(x: Callable[[AbstractRow], R]) -> AggregateSpecification:
    """Compute the population standard deviation of a column.

    Parameters
    ----------
    x
        A column selector.

    """
    return AggregateSpecification(PopulationStandardDeviation, (x,))
