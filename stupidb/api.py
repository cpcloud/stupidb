import functools
from typing import Any, Callable, Iterable, Iterator, Tuple
from typing import Union as Union_

from .stupidb import (
    AggregateSpecification,
    CrossJoin,
    Difference,
    GroupBy,
    GroupingKeySpecification,
    InnerJoin,
    Intersection,
    Join,
    JoinPredicate,
    JoinProjection,
    JoinProjector,
    Mean,
    PopulationCovariance,
    Projection,
    Projector,
    Relation,
    Row,
    Rows,
    SampleCovariance,
    Selection,
    Sum,
    Table,
    UnaryRelation,
    Union,
)


class RightShiftablePartial(
    functools.partial, Iterable[Union_[Tuple[Row], Tuple[Row, Row]]]
):
    def __rshift__(self, other: "RightShiftablePartial") -> Relation:
        return other(self)

    def __rrshift__(self, other: Relation) -> Relation:
        return self(other)

    def __iter__(self) -> Iterator[Union_[Tuple[Row], Tuple[Row, Row]]]:
        # XXX: Assumes all arguments have been bound
        # TODO: This seems a bit hacky. Refactor shifting.
        return iter(self())


def table(rows: Iterable[Rows]) -> RightShiftablePartial:
    return RightShiftablePartial(Table, rows=rows)


def cross_join(right: UnaryRelation) -> RightShiftablePartial:
    return RightShiftablePartial(CrossJoin, right=right)


def inner_join(
    right: UnaryRelation, predicate: JoinPredicate
) -> RightShiftablePartial:
    return RightShiftablePartial(InnerJoin, right=right, predicate=predicate)


def select(columns: Union_[Projector, JoinProjector]) -> RightShiftablePartial:
    return RightShiftablePartial(
        lambda child, columns: (
            JoinProjection(child, columns)
            if isinstance(child, Join)
            else Projection(child, columns)
        ),
        columns=columns,
    )


def sift(predicate: Callable[[Row], bool]) -> RightShiftablePartial:
    return RightShiftablePartial(Selection, predicate=predicate)


def group_by(
    group_by: GroupingKeySpecification, aggregates: AggregateSpecification
) -> RightShiftablePartial:
    return RightShiftablePartial(
        GroupBy, group_by=group_by, aggregates=aggregates
    )


def union(left: Relation, right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Union, left)


def intersection(left: Relation, right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Intersection, left)


def difference(left: Relation, right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Difference, left)


def do() -> RightShiftablePartial:
    return RightShiftablePartial(lambda child: (row for row, in child))


def sum(getter: Callable[[Row], Any]) -> AggregateSpecification:
    return AggregateSpecification(Sum, getter)


def mean(getter: Callable[[Row], Any]) -> AggregateSpecification:
    return AggregateSpecification(Mean, getter)


def samp_cov(
    arg1: Callable[[Row], Any], arg2: Callable[[Row], Any]
) -> AggregateSpecification:
    return AggregateSpecification(SampleCovariance, arg1, arg2)


def pop_cov(
    arg1: Callable[[Row], Any], arg2: Callable[[Row], Any]
) -> AggregateSpecification:
    return AggregateSpecification(PopulationCovariance, arg1, arg2)
