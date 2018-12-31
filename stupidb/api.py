from typing import Any, Callable, Iterable, Iterator, Mapping, TypeVar

import toolz
from toolz import curry

from stupidb.comparable import Comparable
from stupidb.stupidb import (
    AggregateSpecification,
    Aggregation,
    Aggregations,
    Count,
    CrossJoin,
    Difference,
    FrameClause,
    FullProjector,
    GroupBy,
    InnerJoin,
    Intersection,
    JoinPredicate,
    Mean,
    Mutate,
    PartitionableIterable,
    PartitionBy,
    PopulationCovariance,
    Projection,
    Relation,
    Row,
    SampleCovariance,
    Selection,
    SortBy,
    Sum,
    Total,
    Tuple,
    UnaryRelation,
    Union,
    WindowAggregateSpecification,
)
from stupidb.typehints import OrderBy, Predicate, RealGetter


class shiftable(curry):
    def __rshift__(self, other: "shiftable") -> Relation:
        return other(self)

    def __rrshift__(self, other: Relation) -> Relation:
        return self(other)


@shiftable
def table(rows: Iterable[Mapping[str, Any]]) -> Relation:
    """Construct a relation from an iterable of mappings."""
    first, rows = toolz.peek(rows)
    return UnaryRelation(
        PartitionableIterable(
            (Row.from_mapping(row, _id=id),) for id, row in enumerate(rows)
        )
    )


@shiftable
def cross_join(right: UnaryRelation, left: UnaryRelation) -> Relation:
    """Return the Cartesian product of tuples from `left` and `right`."""
    return CrossJoin(left, right)


@shiftable
def inner_join(
    right: UnaryRelation, predicate: JoinPredicate, left: UnaryRelation
) -> Relation:
    """Join `left` and `right` relations using `predicate`."""
    return InnerJoin(left, right, predicate)


@shiftable
def _order_by(order_by: Tuple[OrderBy, ...], child: Relation) -> Relation:
    return SortBy(child, order_by)


def order_by(*order_by: Comparable) -> shiftable:
    """Group the child operator according to `order_by`."""
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
def sift(predicate: Predicate, child: UnaryRelation) -> Relation:
    """Filter rows in `child` according to `predicate`."""
    return Selection(child, predicate)


def exists(relation: UnaryRelation) -> bool:
    """Compute whether any of the rows in `relation` are truthy.

    Returns
    -------
    bool

    """
    return any(map(toolz.first, relation))


@shiftable
def _aggregate(aggregations: Aggregations, child: Relation) -> Relation:
    return Aggregation(child, aggregations)


def aggregate(**aggregations: AggregateSpecification) -> shiftable:
    """Aggregate child operator based on `aggregations`."""
    return _aggregate(aggregations)


@shiftable
def over(
    window: FrameClause, child: AggregateSpecification
) -> WindowAggregateSpecification:
    return WindowAggregateSpecification(
        window, child.aggregate, *child.getters
    )


@shiftable
def _group_by(
    group_by: Mapping[str, PartitionBy], child: UnaryRelation
) -> Relation:
    return GroupBy(child, group_by)


def group_by(**group_by: PartitionBy) -> shiftable:
    """Group the child operator according to `group_by`."""
    return _group_by(group_by)


@shiftable
def union(right: UnaryRelation, left: UnaryRelation) -> Relation:
    """Compute the set union of `left` and `right`."""
    return Union(left, right)


@shiftable
def intersection(right: UnaryRelation, left: UnaryRelation) -> Relation:
    """Compute the set intersection of `left` and `right`."""
    return Intersection(left, right)


@shiftable
def difference(right: UnaryRelation, left: UnaryRelation) -> Relation:
    """Compute the set difference of `left` and `right`."""
    return Difference(left, right)


@shiftable
def do(child: UnaryRelation) -> Iterator[Row]:
    """Pull the :class:`~stupidb.row.Row` instances out of `child`.

    Notes
    -----
    All operations should call this to materialize rows. Call the builtin
    ``list`` function on the result of ``do()`` to produce a list of rows.

    """
    return map(toolz.first, child)


V = TypeVar("V")


# Aggregations
def count(getter: Callable[[Row], V]) -> AggregateSpecification:
    return AggregateSpecification(Count, getter)


def sum(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Sum, getter)


def total(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Total, getter)


def mean(getter: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(Mean, getter)


def samp_cov(arg1: RealGetter, arg2: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(SampleCovariance, arg1, arg2)


def pop_cov(arg1: RealGetter, arg2: RealGetter) -> AggregateSpecification:
    return AggregateSpecification(PopulationCovariance, arg1, arg2)
