# -*- coding: utf-8 -*-

"""The stupidest database.

This is project designed to illustate the concepts that underly a typical
relational database implementation, starting at naive execution of the
table-stakes features up to rule-based query optimization.

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
import typing

from numbers import Real
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import toolz


Row = Mapping[str, Any]
Rows = Iterable[Row]


class Relation(abc.ABC):
    @abc.abstractmethod
    def operate(self, row: Row) -> Row:
        ...

    @abc.abstractmethod
    def produce(self, rows: Rows) -> Iterator[Row]:
        ...


class SimpleRelation(Relation):
    """A relation with a single child relation.

    Child classes must implement either
    :meth:`~stupidb.stupidb.Relation.operate` or
    :meth:`~stupidb.stupidb.Relation.produce`.

    If a Relation's operation can be described in terms of an operation on a
    single row, such projection, then implement
    :meth:`~stupidb.stupidb.Relation.operate`. Otherwise, implement
    :meth:`~stupidb.stupidb.Relation.produce`.

    See :class:`~stupidb.stupidb.GroupBy` for an example of a relation that
    cannot be defined in terms of single row operations and therefore must
    :meth:`~stupidb.stupidb.Relation.produce`.

    """

    child: Relation

    def operate(self, row: Row) -> Row:
        # empty rows (empty dicts) are ignored
        return toolz.identity(row)

    def produce(self, rows: Rows) -> Iterator[Row]:
        # get an iterator of rows from the child
        rowterator = self.child.produce(rows)
        # apply the current relation's operation
        operated_rows = map(self.operate, rowterator)
        # filter out empty rows
        non_empty_rows = filter(None, operated_rows)
        return non_empty_rows


class Table(SimpleRelation):
    def produce(self, rows: Rows) -> Iterator[Row]:
        """Produce the input."""
        return iter(rows)


class Projection(SimpleRelation):
    def __init__(
        self,
        child: SimpleRelation,
        columns: Mapping[str, Callable[[Row], Any]],
    ) -> None:
        self.child = child
        self.columns = columns

    def operate(self, row: Row) -> Row:
        return {column: func(row) for column, func in self.columns.items()}


class Selection(SimpleRelation):
    def __init__(
        self, child: SimpleRelation, predicate: Callable[[Row], bool]
    ) -> None:
        self.child = child
        self.predicate = predicate

    def operate(self, row: Row) -> Row:
        return row if self.predicate(row) else {}


GroupingKeySpecification = Mapping[str, Callable[[Row], Hashable]]
GroupingKey = Tuple[str, Hashable]
GroupingKeys = Tuple[GroupingKey, ...]

T = TypeVar("T")


def make_tuple(item: Union[T, Tuple[T, ...]]) -> Tuple[T, ...]:
    """Make item into a single element tuple if it is not already a tuple."""
    return (item,) if not isinstance(item, tuple) else item


Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")

Output = TypeVar("Output")


class UnaryAggregate(Generic[Input1, Output]):
    def step(self, input1: Optional[Input1]) -> None:
        ...

    def finalize(self) -> Optional[Output]:
        ...


class BinaryAggregate(Generic[Input1, Input2, Output]):
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    def finalize(self) -> Optional[Output]:
        ...


Aggregate = Union[UnaryAggregate, BinaryAggregate]
AggFuncPair = Tuple[Aggregate, Callable[[Row], Any]]


AggregateSpecification = Mapping[
    str, Tuple[Type[Aggregate], Callable[[Row], Any]]
]


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


class GroupBy(SimpleRelation):
    def __init__(
        self,
        child: SimpleRelation,
        group_by: GroupingKeySpecification,
        aggregates: AggregateSpecification,
    ) -> None:
        self.child = child
        self.group_by = group_by
        self.aggregates = aggregates

    def produce(self, rows: Rows) -> Iterator[Row]:
        aggs: Mapping[
            GroupingKeys, Mapping[str, Tuple[Aggregate, Callable[[Row], Any]]]
        ] = collections.defaultdict(
            lambda: {
                name: (agg(), func)
                for name, (agg, func) in self.aggregates.items()
            }
        )
        aggregates = self.aggregates
        for row in self.child.produce(rows):
            keys: GroupingKeys = tuple(
                (name, func(row)) for name, func in self.group_by.items()
            )
            keyed_agg = aggs[keys]
            for name in aggregates.keys():
                agg, func = keyed_agg[name]
                agg.step(*make_tuple(func(row)))

        for key, aggspec in aggs.items():
            agg_values = {
                agg_name: subagg.finalize()
                for agg_name, (subagg, _) in aggspec.items()
            }
            res = toolz.merge(dict(key), agg_values)
            yield res


class Join(Relation):
    left: Relation
    right: Relation

    def __init__(
        self,
        left: Relation,
        right: Relation,
        predicate: Callable[[Row, Row], bool],
    ) -> None:
        self.left = left
        self.right = right
        self.predicate = predicate

    def operate(self, left: Row, right: Row) -> Row:
        if self.predicate(left, right):
            yield toolz.merge(left, right)
        else:
            yield from self.failed_match_action(left, right)

    def produce(self, left: Rows, right: Rows) -> Iterator[Row]:
        return filter(
            None,
            toolz.concat(
                itertools.starmap(self.operate, itertools.product(left, right))
            ),
        )

    @abc.abstractmethod
    def failed_match_action(self, left: Row, right: Row) -> Row:
        ...


class CrossJoin(Join):
    def __init__(self, left: Relation, right: Relation) -> None:
        super().__init__(left, right, lambda left, right: True)

    def failed_match_action(self, left: Row, right: Row) -> Row:
        raise RuntimeError("CrossJoin should always match")


class InnerJoin(Join):
    def failed_match_action(self, left: Row, right: Row) -> Row:
        yield {}


class LeftJoin(Join):
    def failed_match_action(self, left: Row, right: Row) -> Row:
        yield left


class RightJoin(Join):
    def failed_match_action(self, left: Row, right: Row) -> Row:
        yield right


class RightShiftablePartial(functools.partial):
    def __rshift__(self, other):
        return other(self)

    def __rrshift__(self, other):
        return self(other)

    def produce(self, rows):
        # TODO: This seems a bit hacky. Refactor shifting.
        return self.func().produce(rows)


def table():
    return RightShiftablePartial(Table)


def select(**columns):
    return RightShiftablePartial(Projection, columns=columns)


def sift(predicate):
    return RightShiftablePartial(Selection, predicate=predicate)


def group_by(group_by, aggregates):
    return RightShiftablePartial(
        GroupBy, group_by=group_by, aggregates=aggregates
    )
