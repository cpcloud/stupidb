import functools
from numbers import Real
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
    Mean,
    PopulationCovariance,
    Projection,
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


Projector = Callable[[Row], Row]
JoinProjector = Callable[[Row, Row], Row]


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
    return RightShiftablePartial(Projection, projector=columns)


def sift(predicate: Callable[[Row], bool]) -> RightShiftablePartial:
    return RightShiftablePartial(Selection, predicate=predicate)


def group_by(
    group_by: GroupingKeySpecification, aggregates: AggregateSpecification
) -> RightShiftablePartial:
    return RightShiftablePartial(
        GroupBy, group_by=group_by, aggregates=aggregates
    )


def union(right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Union, right=right)


def intersection(right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Intersection, right=right)


def difference(right: Relation) -> RightShiftablePartial:
    return RightShiftablePartial(Difference, right=right)


def do() -> RightShiftablePartial:
    return RightShiftablePartial(lambda child: (row for row, in child))


def sum(getter: Callable[[Row], Real]) -> AggregateSpecification:
    return AggregateSpecification(Sum, getter)


def mean(getter: Callable[[Row], Real]) -> AggregateSpecification:
    return AggregateSpecification(Mean, getter)


def samp_cov(
    arg1: Callable[[Row], Real], arg2: Callable[[Row], Real]
) -> AggregateSpecification:
    return AggregateSpecification(SampleCovariance, arg1, arg2)


def pop_cov(
    arg1: Callable[[Row], Real], arg2: Callable[[Row], Real]
) -> AggregateSpecification:
    return AggregateSpecification(PopulationCovariance, arg1, arg2)
