from typing import Callable, Iterable, Iterator, Mapping, Optional

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from stupidb.row import V
from stupidb.stupidb import (
    AggregateSpecification,
    Aggregation,
    Count,
    CrossJoin,
    Difference,
    GroupBy,
    GroupingKeyFunction,
    InnerJoin,
    Intersection,
    JoinPredicate,
    Mean,
    PopulationCovariance,
    Projection,
    Projector,
    Relation,
    Row,
    SampleCovariance,
    Selection,
    Sum,
    Total,
    UnaryRelation,
    Union,
)
from stupidb.typehints import Predicate, RealGetter

try:
    import cytoolz as toolz
    from cytoolz import curry
except ImportError:
    import toolz
    from toolz import curry


class shiftable(curry):
    def __rshift__(self, other: "shiftable") -> Relation:
        return other(self)

    def __rrshift__(self, other: Relation) -> Relation:
        return self(other)


@shiftable
def table(
    rows: Iterable[Mapping[str, V]], schema: Optional[sch.Schema] = None
) -> Relation:
    """Construct a relation from an iterable of mappings."""
    first, rows = toolz.peek(rows)
    child = ((Row.from_mapping(row, _id=id),) for id, row in enumerate(rows))
    return UnaryRelation(
        child,
        (
            sch.Schema.from_dict(toolz.valmap(dt.infer, first))
            if schema is None
            else schema
        ),
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
def _select(
    projectors: Mapping[str, Projector], child: Relation
) -> Projection:
    return Projection(child, projectors)


def select(**projectors: Projector) -> shiftable:
    """Compute columns from `projectors`."""
    return _select(projectors)


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
def _aggregate(
    aggregations: Mapping[str, AggregateSpecification], child: Relation
) -> Relation:
    return Aggregation(child, aggregations)


def aggregate(**aggregations: AggregateSpecification) -> shiftable:
    """Aggregate child operator based on `aggregations`."""
    return _aggregate(aggregations)


@shiftable
def _group_by(
    group_by: Mapping[str, GroupingKeyFunction], child: Relation
) -> Relation:
    return GroupBy(child, group_by)


def group_by(**group_by: GroupingKeyFunction) -> shiftable:
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
