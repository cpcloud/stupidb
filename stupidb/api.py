import functools
from numbers import Real
from typing import (
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    TypeVar,
)
from typing import Union as Union_

import toolz

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from stupidb.stupidb import (
    AbstractAggregateSpecification,
    AggregateSpecification,
    CrossJoin,
    Difference,
    GroupBy,
    GroupingKeySpecification,
    InnerJoin,
    Intersection,
    JoinPredicate,
    Mean,
    OutputType,
    PopulationCovariance,
    Projection,
    Relation,
    Row,
    SampleCovariance,
    Selection,
    Sum,
    UnaryRelation,
    Union,
)
from stupidb.typehints import JoinProjector, Predicate, Projector


class RightShiftablePartial(functools.partial, Generic[OutputType]):
    def __rshift__(self, other: "RightShiftablePartial") -> Relation:
        return other(self)

    def __rrshift__(self, other: Relation) -> Relation:
        return self(other)

    def __iter__(self) -> Iterator[OutputType]:
        # XXX: Assumes all arguments have been bound
        # TODO: This seems a bit hacky. Refactor shifting.
        return iter(self())

    @property
    def columns(self) -> List[str]:
        return self().columns

    @property
    def schema(self) -> sch.Schema:
        return self().schema


V = TypeVar("V")


def table(
    rows: Iterable[Mapping[str, V]], schema: Optional[sch.Schema] = None
) -> RightShiftablePartial:
    first, rows = toolz.peek(rows)
    return RightShiftablePartial(
        UnaryRelation,
        child=((Row.from_mapping(row, id=id),) for id, row in enumerate(rows)),
        schema=(
            sch.Schema.from_dict(toolz.valmap(dt.infer, first))
            if schema is None
            else schema
        ),
    )


def cross_join(right: UnaryRelation) -> RightShiftablePartial:
    return RightShiftablePartial(CrossJoin, right=right)


def inner_join(
    right: UnaryRelation, predicate: JoinPredicate
) -> RightShiftablePartial:
    return RightShiftablePartial(InnerJoin, right=right, predicate=predicate)


def select(
    **projectors: Union_[
        Projector, JoinProjector, AbstractAggregateSpecification
    ]
) -> RightShiftablePartial:
    return RightShiftablePartial(Projection, projectors=projectors)


def sift(predicate: Predicate) -> RightShiftablePartial:
    return RightShiftablePartial(Selection, predicate=predicate)


def exists(relation: Relation) -> bool:
    return any(row for (row,) in relation)


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


# Pull out the first element: all operations should ultimately call this


def do() -> RightShiftablePartial:
    return RightShiftablePartial(functools.partial(map, toolz.first))


# Aggregations


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
