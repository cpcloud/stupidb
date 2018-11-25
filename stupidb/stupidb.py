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
import ast
import collections
import inspect
import itertools
import operator
import typing
from numbers import Real
from operator import methodcaller
from typing import (
    Any,
    Callable,
    FrozenSet,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
from typing import Union as Union_

try:
    import cytoolz as toolz
    import cytoolz.curried
except ImportError:
    import toolz as toolz
    import toolz.curried


Row = Mapping[str, Any]  # A mapping from column name to anything
Rows = Iterable[Row]  # Rows are an Iterable of Row
InputType = TypeVar("InputType", Tuple[Row], Tuple[Row, Row])
OutputType = TypeVar("OutputType", Tuple[Row], Tuple[Row, Row])


class Relation(Generic[InputType, OutputType], metaclass=abc.ABCMeta):
    """A relation."""

    child: Iterable[InputType]

    @abc.abstractmethod
    def operate(self, args: InputType) -> OutputType:
        ...

    def __iter__(self) -> Iterator[OutputType]:
        return filter(all, map(self.operate, self.child))


class UnaryRelation(Relation[Tuple[Row], Tuple[Row]]):
    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        return row


class Table(UnaryRelation):
    def __init__(self, rows: Iterable[Row]) -> None:
        self.rows = rows

    def __iter__(self) -> Iterator[Tuple[Row]]:
        return ((row,) for row in self.rows)


Projector = Callable[[Row], Row]


class Projection(UnaryRelation):
    def __init__(self, child: UnaryRelation, projector: Projector) -> None:
        self.child = child
        self.projector = projector

    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        return (self.projector(*row),)


JoinProjector = Callable[[Row, Row], Row]


class JoinProjection(Relation[Tuple[Row, Row], Tuple[Row]]):
    def __init__(self, child: "Join", projector: JoinProjector) -> None:
        self.child = child
        self.projector = projector

    def operate(self, pair: Tuple[Row, Row]) -> Tuple[Row]:
        return (self.projector(*pair),)


class Selection(UnaryRelation):
    def __init__(
        self, child: UnaryRelation, predicate: Callable[[Row], bool]
    ) -> None:
        self.child = child
        self.predicate = predicate

    def operate(self, row: Tuple[Row]) -> Tuple[Row]:
        return row if self.predicate(*row) else ({},)


GroupingKeySpecification = Mapping[str, Callable[[Row], Hashable]]
GroupingKey = Tuple[str, Hashable]
GroupingKeys = Tuple[GroupingKey, ...]

T = TypeVar("T")


def make_tuple(item: Union_[T, Tuple[T, ...]]) -> Tuple[T, ...]:
    """Make item into a single element tuple if it is not already a tuple."""
    return (item,) if not isinstance(item, tuple) else item


Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")


class UnaryAggregate(Generic[Input1, Output], metaclass=abc.ABCMeta):
    def step(self, input1: Optional[Input1]) -> None:
        ...

    def finalize(self) -> Optional[Output]:
        ...


class BinaryAggregate(Generic[Input1, Input2, Output]):
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    def finalize(self) -> Optional[Output]:
        ...


Aggregate = Union_[UnaryAggregate, BinaryAggregate]


class AggregateSpecification:
    def __init__(
        self, aggregate: Type[Aggregate], *getters: Callable[[Row], Any]
    ) -> None:
        self.aggregate = aggregate
        self.getters = getters


class Sum(UnaryAggregate[Real, Real]):
    def __init__(self) -> None:
        self.total = typing.cast(Real, 0)
        self.count = 0

    def step(self, input1: Optional[Real]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> Optional[Real]:
        return self.total if self.count else None


class Mean(UnaryAggregate[Real, float]):
    def __init__(self) -> None:
        self.total: float = 0.0
        self.count: int = 0

    def step(self, value: Optional[Real]) -> None:
        if value is not None:
            self.total += typing.cast(float, value)
            self.count += 1

    def finalize(self) -> Optional[float]:
        count = self.count
        return self.total / count if count > 0 else None


class Covariance(BinaryAggregate[Real, Real, float]):
    def __init__(self, denom: int):
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
        aggs: Mapping[
            GroupingKeys,
            Mapping[str, Tuple[Aggregate, AggregateSpecification]],
        ] = collections.defaultdict(
            lambda: {
                name: (spec.aggregate(), spec)
                for name, spec in aggregates.items()
            }
        )

        for row in self.child:
            keys: GroupingKeys = tuple(
                (name, keyfunc(*row))
                for name, keyfunc in self.group_by.items()
            )
            keyed_agg = aggs[keys]
            for name in aggregates.keys():
                agg, aggspec = keyed_agg[name]
                agg.step(*(getter(*row) for getter in aggspec.getters))

        for key, topagg in aggs.items():
            agg_values = {
                agg_name: subagg.finalize()
                for agg_name, (subagg, _) in topagg.items()
            }
            res = toolz.merge(dict(key), agg_values)
            yield (res,)


JoinPredicate = Callable[[Row, Row], bool]


class Join(Relation[Tuple[Row, Row], Tuple[Row, Row]]):
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

    def failed_match_action(self, left: Row, right: Row) -> Tuple[Row, Row]:
        raise RuntimeError("CrossJoin should always match")


class InnerJoin(Join):
    def failed_match_action(self, left: Row, right: Row) -> Tuple[Row, Row]:
        return {}, {}


items = methodcaller("items")
itemize = toolz.compose(frozenset, toolz.curried.map(items))


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


class InefficientSetOperation(SetOperation):
    def __iter__(self) -> Iterator[Tuple[Row]]:
        return (
            (dict(row),)
            for row in self.binary_operation(
                itemize(self.left), itemize(self.right)
            )
        )

    @abc.abstractmethod
    def binary_operation(
        self,
        left: FrozenSet[Tuple[Tuple[str, Any], ...]],
        right: FrozenSet[Tuple[Tuple[str, Any], ...]],
    ) -> FrozenSet[Tuple[Tuple[str, Any], ...]]:
        ...


class Intersection(InefficientSetOperation):
    def binary_operation(
        self,
        left: FrozenSet[Tuple[Tuple[str, Any], ...]],
        right: FrozenSet[Tuple[Tuple[str, Any], ...]],
    ) -> FrozenSet[Tuple[Tuple[str, Any], ...]]:
        return left & right


class Difference(InefficientSetOperation):
    def binary_operation(
        self,
        left: FrozenSet[Tuple[Tuple[str, Any], ...]],
        right: FrozenSet[Tuple[Tuple[str, Any], ...]],
    ) -> FrozenSet[Tuple[Tuple[str, Any], ...]]:
        return left - right
