from typing import Any, Callable, Iterable, Mapping, TypeVar

from toolz import curry

from stupidb.aggregation import (
    AggregateSpecification,
    Count,
    First,
    FrameClause,
    Last,
    Max,
    Mean,
    Min,
    Nth,
    PopulationCovariance,
    SampleCovariance,
    Sum,
    Total,
    WindowAggregateSpecification,
)
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
from stupidb.protocols import Comparable
from stupidb.typehints import OrderBy, RealGetter


class shiftable(curry):
    def __rrshift__(self, other):
        return self(other)


@shiftable
def table(rows: Iterable[Mapping[str, Any]]) -> Relation:
    """Construct a relation from an iterable of mappings."""
    return Relation.from_iterable(rows)


@shiftable
def cross_join(right: Relation, left: Relation) -> Relation:
    """Return the Cartesian product of tuples from `left` and `right`."""
    return Join(left, right, lambda row: True)


@shiftable
def inner_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.

    """
    return Join(left, right, predicate)


@shiftable
def left_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `left`.

    """
    return LeftJoin(left, right, predicate)


@shiftable
def right_join(
    right: Relation, predicate: Predicate, left: Relation
) -> Relation:
    """Join `left` and `right` relations using `predicate`.

    Drop rows if `predicate` returns ``False``.  Returns at least one of every
    row from `right`.

    """
    return RightJoin(left, right, predicate)


@shiftable
def _order_by(order_by: Tuple[OrderBy, ...], child: Relation) -> Relation:
    return SortBy(child, order_by)


def order_by(*order_by: OrderBy) -> shiftable:
    """Order the rows of the child operator according to `order_by`."""
    return _order_by(order_by)


@shiftable
def _select(
    projectors: Mapping[str, FullProjector], child: Relation
) -> Relation:
    return Projection(child, projectors)


def select(**projectors: FullProjector) -> shiftable:
    """Subset or compute columns from `projectors`."""
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


def mutate(**mutators: FullProjector) -> shiftable:
    """Add new columns specified by `mutators`."""
    return _mutate(mutators)


@shiftable
def sift(predicate: Predicate, child: Relation) -> Relation:
    """Filter rows in `child` according to `predicate`."""
    return Selection(child, predicate)


def exists(relation: Relation) -> bool:
    """Compute whether any of the rows in `relation` are truthy.

    Useful for specifying semi-joins

    Returns
    -------
    bool

    """
    return any(relation)


@shiftable
def _aggregate(aggregations: Aggregations, child: Relation) -> Relation:
    return Aggregation(child, aggregations)


def aggregate(**aggregations: AggregateSpecification) -> shiftable:
    """Aggregate values from the child operator using `aggregations`."""
    return _aggregate(aggregations)


@shiftable
def over(
    window: FrameClause, child: WindowAggregateSpecification
) -> WindowAggregateSpecification:
    return WindowAggregateSpecification(child.aggregate, child.getters, window)


@shiftable
def _group_by(
    group_by: Mapping[str, PartitionBy], child: Relation
) -> Relation:
    return GroupBy(child, group_by)


def group_by(**group_by: PartitionBy) -> shiftable:
    """Group the rows of the child operator according to `group_by`."""
    return _group_by(group_by)


# Set operations
@shiftable
def union(right: Relation, left: Relation) -> Relation:
    """Compute the set union of `left` and `right`."""
    return Union(left, right)


@shiftable
def intersection(right: Relation, left: Relation) -> Relation:
    """Compute the set intersection of `left` and `right`."""
    return Intersection(left, right)


@shiftable
def difference(right: Relation, left: Relation) -> Relation:
    """Compute the set difference of `left` and `right`."""
    return Difference(left, right)


V = TypeVar("V")


# Aggregations
def count(getter: Callable[[AbstractRow], V]) -> AggregateSpecification:
    return AggregateSpecification(Count, (getter,))


def sum(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Sum, (getter,))


def total(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Total, (getter,))


def first(getter: Callable[[AbstractRow], V]) -> AggregateSpecification:
    return AggregateSpecification(First, (getter,))


def last(getter: Callable[[AbstractRow], V]) -> AggregateSpecification:
    return AggregateSpecification(Last, (getter,))


def nth(
    getter: Callable[[AbstractRow], V], index: Callable[[AbstractRow], int]
) -> AggregateSpecification:
    return AggregateSpecification(Nth, (getter, index))


def mean(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Mean, (getter,))


def min(getter: Callable[[AbstractRow], Comparable]) -> AggregateSpecification:
    return AggregateSpecification(Min, (getter,))


def max(getter: Callable[[AbstractRow], Comparable]) -> AggregateSpecification:
    return AggregateSpecification(Max, (getter,))


def samp_cov(arg1: RealGetter, arg2: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(SampleCovariance, (arg1, arg2))


def pop_cov(arg1: RealGetter, arg2: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(PopulationCovariance, (arg1, arg2))
