# -*- coding: utf-8 -*-

"""The stupidest database.

This is project designed to illustate the concepts that underly a typical
relational database implementation, starting at naive execution of table-stakes
features up to rule-based query optimization.

Please do not use this for any other reason than learning. There are no
guarantees here except that there will be bugs.

Plan of Attack
--------------
* Projection
* Selection
* Group By
* Joins

Requirements
------------
* The database must be able to operate on datasets that do not fit into main
  memory.
* The database must use as little memory as possible (within the bounds of
  standard Python code) and use only O(1) memory where possible.
* The database must produce rows using generators.
* The database must accept any iterable of mapping as its input(s).
* The database must attempt to use only built-in Python data structures
  including those in standard library modules.

"""

import abc
import collections
import functools
import itertools
import operator
import typing
from operator import methodcaller
from typing import (
    Any,
    Callable,
    FrozenSet,
    Generic,
    Hashable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)
from typing import Union as Union_

from typing_extensions import NoReturn

import ibis.expr.schema as sch
from stupidb.row import Row
from stupidb.typehints import (
    BinaryProjector,
    Following,
    GroupingKeyFunction,
    Input1,
    Input2,
    InputType,
    OrderBy,
    Output,
    OutputType,
    PartitionBy,
    Preceding,
    Predicate,
    R,
    UnaryProjector,
    PartitionKey,
)

try:
    import cytoolz as toolz
except ImportError:
    import toolz as toolz


class Relation(Generic[InputType, OutputType], metaclass=abc.ABCMeta):
    """A relation."""

    def __init__(self, child: "Relation", schema: sch.Schema) -> None:
        self.child: "Relation" = child
        self.schema = schema

    @property
    def columns(self) -> List[str]:
        return list(self.schema.names)

    def operate(self, args: InputType) -> OutputType:
        return typing.cast(OutputType, args)

    def __iter__(self) -> Iterator[OutputType]:
        for id, row in enumerate(filter(all, map(self.operate, self.child))):
            yield tuple(
                typing.cast(
                    OutputType,
                    (Row.from_mapping(element, _id=id) for element in row),
                )
            )

    def partition_key(
        self, row: InputType
    ) -> Tuple[Tuple[str, Hashable], ...]:
        return ()


class UnaryRelation(Relation[Tuple[Row], Tuple[Row]]):
    pass


Projector = TypeVar(
    "Projector",
    UnaryProjector,
    BinaryProjector,
    "AbstractAggregateSpecification",
)


class Projection(
    Generic[InputType, Projector], Relation[InputType, Tuple[Row]]
):
    def __init__(
        self, child: Relation, projectors: Mapping[str, Projector]
    ) -> None:
        self.child = child
        self.projectors: Mapping[str, Projector] = projectors

    def operate(self, args: InputType) -> Tuple[Row]:
        mapping = {
            name: projector(*args)
            for name, projector in self.projectors.items()
        }
        row = Row(mapping)
        return (row,)


class Aggregation(Relation[InputType, Tuple[Row]]):
    def __init__(
        self,
        child: Relation,
        aggregations: Mapping[str, "AggregateSpecification"],
    ) -> None:
        self.child = child
        self.aggregations = aggregations

    def __iter__(self) -> Iterator[Tuple[Row]]:
        aggregations = self.aggregations
        grouped_aggs: Mapping[
            Tuple[Tuple[str, Hashable], ...], Mapping[str, Aggregate]
        ] = collections.defaultdict(
            lambda: {
                name: aggspec.aggregate()
                for name, aggspec in aggregations.items()
            }
        )
        child = self.child
        for row in child:
            key = child.partition_key(row)
            aggs = grouped_aggs[key]
            for name, agg in aggs.items():
                inputs = [
                    getter(*row) for getter in aggregations[name].getters
                ]
                agg.step(*inputs)

        for id, (grouping_key, aggs) in enumerate(grouped_aggs.items()):
            finalized_aggregations = {
                name: agg.finalize() for name, agg in aggs.items()
            }
            data = toolz.merge(dict(grouping_key), finalized_aggregations)
            yield (Row(data, _id=id),)


class Selection(UnaryRelation):
    def __init__(self, child: UnaryRelation, predicate: Predicate) -> None:
        self.child = child
        self.predicate = predicate

    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        result = self.predicate(*row)
        return row if result else (Row({}, _id=row[0]._id),)


class UnaryAggregate(Generic[Input1, Output], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


class UnaryWindowAggregate(UnaryAggregate[Input1, Output]):
    @abc.abstractmethod
    def inverse(self, input1: Optional[Input1]) -> None:
        ...

    def value(self) -> Optional[Output]:
        return self.finalize()


class BinaryAggregate(Generic[Input1, Input2, Output]):
    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


Aggregate = Union_[UnaryAggregate, BinaryAggregate]


class FrameClause:
    def __init__(
        self,
        order_by: Sequence[OrderBy],
        partition_by: Sequence[PartitionBy],
        preceding: Optional[Preceding],
        following: Optional[Following],
    ) -> None:
        self._order_by = order_by
        self._partition_by = partition_by
        self._preceding = preceding
        self._following = following
        ...


class RowsMode(FrameClause):
    pass


class RangeMode(FrameClause):
    pass


class Window:
    @staticmethod
    def rows(
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RowsMode(order_by, partition_by, preceding, following)

    @staticmethod
    def range(
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RangeMode(order_by, partition_by, preceding, following)


Getter = Callable[[Row], Any]


class AbstractAggregateSpecification:
    def __init__(self, aggregate: Type[Aggregate], *getters: Getter) -> None:
        self.aggregate = aggregate
        self.getters = getters


class WindowAggregateSpecification(AbstractAggregateSpecification):
    def __init__(
        self,
        frame_clause: FrameClause,
        aggregate: Type[Aggregate],
        *getters: Getter,
    ) -> None:
        super().__init__(aggregate, *getters)
        self.frame_clause = frame_clause


class AggregateSpecification(AbstractAggregateSpecification):
    def over(self, window: FrameClause) -> WindowAggregateSpecification:
        return WindowAggregateSpecification(
            window, self.aggregate, *self.getters
        )


class Count(UnaryWindowAggregate[Input1, int]):
    def __init__(self) -> None:
        self.count = 0

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def inverse(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count -= 1

    def finalize(self) -> Optional[int]:
        return self.count


class Sum(UnaryWindowAggregate[R, R]):
    def __init__(self) -> None:
        self.total = typing.cast(R, 0)
        self.count = 0

    def step(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def inverse(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[R]:
        return self.total if self.count else None


class Total(Sum[R]):
    def finalize(self) -> Optional[R]:
        return self.total if self.count else typing.cast(R, 0)


class Mean(UnaryWindowAggregate[R, float]):
    def __init__(self) -> None:
        self.total: float = 0.0
        self.count: int = 0

    def step(self, value: Optional[R]) -> None:
        if value is not None:
            self.total += typing.cast(float, value)
            self.count += 1

    def inverse(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[float]:
        count = self.count
        return self.total / count if count > 0 else None


class Covariance(BinaryAggregate[R, R, float]):
    def __init__(self, *, ddof: int) -> None:
        self.meanx: float = 0.0
        self.meany: float = 0.0
        self.count: int = 0
        self.cov: float = 0.0
        self.ddof = ddof

    def step(self, x: Optional[R], y: Optional[R]) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.meanx
            self.meanx += delta_x + count
            self.meany += (y - self.meany) / count
            self.cov += delta_x * (y - self.meany)

    def finalize(self) -> Optional[float]:
        denom = self.count - self.ddof
        return self.cov / denom if denom > 0 else None


class SampleCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(ddof=0)


class GroupBy(Relation[InputType, OutputType]):
    def __init__(
        self,
        child: Relation[InputType, OutputType],
        group_by: Mapping[str, GroupingKeyFunction],
    ) -> None:
        self.child = child
        self.group_by: Mapping[str, GroupingKeyFunction] = group_by

    def operate(self, row: InputType) -> NoReturn:
        raise TypeError("Should never reach this")

    def __iter__(self) -> Iterator[OutputType]:
        return iter(self.child)

    def partition_key(
        self, row: InputType
    ) -> Tuple[Tuple[str, Hashable], ...]:
        group_by = self.group_by
        return tuple(
            (name, keyfunc(*row)) for name, keyfunc in group_by.items()
        )


JoinPredicate = Callable[[Row, Row], bool]


class Join(Relation[Tuple[Row, Row], Tuple[Row, Row]], metaclass=abc.ABCMeta):
    def __init__(
        self,
        left: UnaryRelation,
        right: UnaryRelation,
        predicate: JoinPredicate,
    ) -> None:
        self.child = itertools.starmap(
            operator.add, itertools.product(left, right)
        )
        self.predicate = predicate

    def operate(self, pair: Tuple[Row, Row]) -> Tuple[Row, Row]:
        left, right = pair
        if self.predicate(left, right):
            return left, right
        return self.failed_match_action(left, right)

    @abc.abstractmethod
    def failed_match_action(self, left: Row, right: Row) -> Tuple[Row, Row]:
        ...


class CrossJoin(Join):
    def __init__(self, left: UnaryRelation, right: UnaryRelation) -> None:
        super().__init__(left, right, lambda left, right: True)

    def failed_match_action(self, left: Row, right: Row) -> NoReturn:
        raise ValueError("CrossJoin should always match")


class InnerJoin(Join):
    def failed_match_action(self, left: Row, right: Row) -> Tuple[Row, Row]:
        return Row({}, _id=left._id), Row({}, _id=right._id)


items = methodcaller("items")


class SetOperation(Relation[Tuple[Row, Row], Tuple[Row]]):
    def __init__(self, left: UnaryRelation, right: UnaryRelation) -> None:
        self.left = left
        self.right = right


class Union(SetOperation):
    def __iter__(self) -> Iterator[Tuple[Row]]:
        return (
            (row,)
            for row in toolz.unique(
                toolz.concatv(self.left, self.right),
                key=toolz.compose(frozenset, items),
            )
        )


SetOperand = FrozenSet[Tuple[Tuple[str, Any], ...]]


class InefficientSetOperation(SetOperation, metaclass=abc.ABCMeta):
    def __iter__(self) -> Iterator[Tuple[Row]]:
        itemize = toolz.compose(frozenset, functools.partial(map, items))
        return (
            (Row.from_mapping(dict(row), _id=id),)
            for id, row in enumerate(
                self.binary_operation(itemize(self.left), itemize(self.right))
            )
        )

    @abc.abstractmethod
    def binary_operation(
        self, left: SetOperand, right: SetOperand
    ) -> SetOperand:
        ...


class Intersection(InefficientSetOperation):
    def binary_operation(
        self, left: SetOperand, right: SetOperand
    ) -> SetOperand:
        return left & right


class Difference(InefficientSetOperation):
    def binary_operation(
        self, left: SetOperand, right: SetOperand
    ) -> SetOperand:
        return left - right
