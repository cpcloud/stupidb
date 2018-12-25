import functools
from typing import (
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
)

import toolz

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from stupidb.row import V
from stupidb.stupidb import (
    AbstractAggregateSpecification,
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
    OutputType,
    PopulationCovariance,
    Projection,
    R,
    Relation,
    Row,
    SampleCovariance,
    Selection,
    Sum,
    Total,
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

    def partition_key(
        self, row: Tuple[Row, ...]
    ) -> Tuple[Tuple[str, Hashable], ...]:
        return ()


def table(
    rows: Iterable[Mapping[str, V]], schema: Optional[sch.Schema] = None
) -> RightShiftablePartial:
    """Construct a relation from an iterable of mappings."""
    first, rows = toolz.peek(rows)
    child = ((Row.from_mapping(row, _id=id),) for id, row in enumerate(rows))
    return RightShiftablePartial(
        UnaryRelation,
        child=child,
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


ProjectorType = TypeVar(
    "ProjectorType", Projector, JoinProjector, AbstractAggregateSpecification
)


def select(**projectors: ProjectorType) -> RightShiftablePartial:
    """Compute columns from `projectors`."""
    return RightShiftablePartial(Projection, projectors=projectors)


def sift(predicate: Predicate) -> RightShiftablePartial:
    """Filter in rows according to `predicate`."""
    return RightShiftablePartial(Selection, predicate=predicate)


def exists(relation: Relation) -> bool:
    """Compute whether any of the rows in `relation` are truthy.

    Returns
    -------
    bool

    """
    return any(row for (row,) in relation)


def aggregate(**aggregations: AggregateSpecification) -> RightShiftablePartial:
    return RightShiftablePartial(Aggregation, aggregations=aggregations)


def group_by(**group_by: GroupingKeyFunction) -> RightShiftablePartial:
    return RightShiftablePartial(GroupBy, group_by=group_by)


def union(right: Relation) -> RightShiftablePartial:
    """Compute the set union of the piped input and `right`."""
    return RightShiftablePartial(Union, right=right)


def intersection(right: Relation) -> RightShiftablePartial:
    """Compute the set intersection of the piped input and `right`."""
    return RightShiftablePartial(Intersection, right=right)


def difference(right: Relation) -> RightShiftablePartial:
    """Compute the set difference of the piped input and `right`."""
    return RightShiftablePartial(Difference, right=right)


def do() -> RightShiftablePartial:
    """Pull the :class:`~stupidb.row.Row` instances out of the child.

    Notes
    -----
    All operations should ultimately call this. Call the builtin ``list``
    function to produce a list of rows.

    """
    return RightShiftablePartial(functools.partial(map, toolz.first))


# Aggregations
RealGetter = Callable[[Row], R]


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
