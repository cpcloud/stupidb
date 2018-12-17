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
from numbers import Real
from operator import methodcaller
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generic,
    Hashable,
    Iterable,
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

from typing_extensions import DefaultDict, NoReturn

import ibis.expr.schema as sch
from stupidb.row import Row
from stupidb.typehints import (
    Following,
    OrderBy,
    PartitionBy,
    Preceding,
    Predicate,
)

try:
    import cytoolz as toolz
except ImportError:
    import toolz as toolz


Rows = Iterable[Row]  # Rows are an Iterable of Row
InputType = TypeVar("InputType", Tuple[Row], Tuple[Row, Row])
OutputType = TypeVar("OutputType", Tuple[Row], Tuple[Row, Row])


class Relation(Generic[InputType, OutputType], metaclass=abc.ABCMeta):
    """A relation."""

    def __init__(self, child: Iterable[InputType], schema: sch.Schema) -> None:
        self.child: Iterable[InputType] = child
        self.schema = schema

    @property
    def columns(self) -> List[str]:
        return list(self.schema.names)

    @abc.abstractmethod
    def operate(self, args: InputType) -> OutputType:
        ...

    def __iter__(self) -> Iterator[OutputType]:
        for id, row in enumerate(filter(all, map(self.operate, self.child))):
            yield tuple(Row.from_mapping(element, id=id) for element in row)


class UnaryRelation(Relation[Tuple[Row], Tuple[Row]]):
    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        return row


class Projection(Relation[InputType, Tuple[Row]]):
    def __init__(
        self, child: Relation, projectors: Mapping[str, Callable[..., Row]]
    ) -> None:
        self.child = child
        self.projectors = projectors

    def operate(self, args: InputType) -> Tuple[Row]:
        mapping = {
            name: projector(*args)
            for name, projector in self.projectors.items()
        }
        row = Row(mapping)
        return (row,)


class Selection(UnaryRelation):
    def __init__(self, child: UnaryRelation, predicate: Predicate) -> None:
        self.child = child
        self.predicate = predicate

    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        result = self.predicate(*row)
        return row if result else (Row({}, id=row[0].id),)


GroupingKeySpecification = Mapping[str, Callable[[Row], Hashable]]
GroupingKey = Tuple[str, Hashable]
GroupingKeys = Tuple[GroupingKey, ...]

Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")


class UnaryAggregate(Generic[Input1, Output], metaclass=abc.ABCMeta):
    def step(self, input1: Optional[Input1]) -> None:
        ...

    def finalize(self) -> Optional[Output]:
        ...


class UnaryWindowAggregate(UnaryAggregate[Input1, Output]):
    def inverse(self, input1: Optional[Input1]) -> None:
        ...

    def value(self) -> Optional[Output]:
        ...


class BinaryAggregate(Generic[Input1, Input2, Output]):
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

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
    @classmethod
    def rows(
        self,
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RowsMode(order_by, partition_by, preceding, following)

    @classmethod
    def range(
        self,
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
        *getters: Getter
    ) -> None:
        super().__init__(aggregate, *getters)
        self.frame_clause = frame_clause


class AggregateSpecification(AbstractAggregateSpecification):
    def over(self, window: FrameClause) -> WindowAggregateSpecification:
        return WindowAggregateSpecification(
            window, self.aggregate, *self.getters
        )


class Sum(UnaryWindowAggregate[Real, Real]):
    def __init__(self) -> None:
        self.total = typing.cast(Real, 0)
        self.count = 0

    def step(self, input1: Optional[Real]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def inverse(self, input1: Optional[Real]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[Real]:
        return self.total if self.count else None

    def value(self) -> Optional[Real]:
        return self.finalize()


class Mean(UnaryWindowAggregate[Real, float]):
    def __init__(self) -> None:
        self.total: float = 0.0
        self.count: int = 0

    def step(self, value: Optional[Real]) -> None:
        if value is not None:
            self.total += typing.cast(float, value)
            self.count += 1

    def inverse(self, input1: Optional[Real]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[float]:
        count = self.count
        return self.total / count if count > 0 else None

    def value(self) -> Optional[float]:
        return self.finalize()


class Covariance(BinaryAggregate[Real, Real, float]):
    def __init__(self, denom: int) -> None:
        self.meanx: float = 0.0
        self.meany: float = 0.0
        self.count: int = 0
        self.cov: float = 0.0
        self.denom = denom

    def step(self, x: Optional[Real], y: Optional[Real]) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.meanx
            self.meanx += delta_x + count
            self.meany += (y - self.meany) / count
            self.cov += delta_x * (y - self.meany)

    def finalize(self) -> Optional[float]:
        denom = self.count - self.denom
        return self.cov / denom if denom > 0 else None


class SampleCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(1)


class PopulationCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(0)


class GroupBy(UnaryRelation):
    def __init__(
        self,
        child: UnaryRelation,
        group_by: GroupingKeySpecification,
        aggregates: Mapping[str, AggregateSpecification],
    ) -> None:
        self.child = child
        self.group_by = group_by
        self.aggregates = aggregates

    def __iter__(self) -> Iterator[Tuple[Row]]:
        aggregates = self.aggregates
        aggs: DefaultDict[
            GroupingKeys, Dict[str, Tuple[Aggregate, AggregateSpecification]]
        ] = collections.defaultdict(
            lambda: {
                name: (spec.aggregate(), spec)
                for name, spec in aggregates.items()
            }
        )

        for (row,) in self.child:
            keys: GroupingKeys = tuple(
                (name, keyfunc(row)) for name, keyfunc in self.group_by.items()
            )
            keyed_agg = aggs[keys]
            for name in aggregates.keys():
                agg, aggspec = keyed_agg[name]
                agg.step(*(getter(row) for getter in aggspec.getters))

        for key, topagg in aggs.items():
            agg_values = {
                agg_name: subagg.finalize()
                for agg_name, (subagg, _) in topagg.items()
            }
            res = toolz.merge(dict(key), agg_values)
            yield (res,)


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
        return Row({}, id=left.id), Row({}, id=right.id)


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
            (Row.from_mapping(dict(row), id=id),)
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
