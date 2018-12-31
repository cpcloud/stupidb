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
from operator import methodcaller
from typing import (
    Any,
    Callable,
    FrozenSet,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
)
from typing import Union as Union_

import toolz as toolz

from stupidb.aggregation import (
    Aggregate,
    AggregateSpecification,
    WindowAggregateSpecification,
)
from stupidb.row import JoinedRow, Row
from stupidb.typehints import (
    OrderBy,
    PartitionBy,
    PartitionKey,
    Predicate,
    Projector,
)


class Partitionable(abc.ABC):
    def partition_key(self, row: Row) -> PartitionKey:
        return ()

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Row]:
        ...


class PartitionableIterable(Partitionable):
    def __init__(self, rows: Iterable[Row]):
        self.rows = rows

    def __iter__(self) -> Iterator[Row]:
        return iter(self.rows)


class Relation(Partitionable):
    """A relation."""

    def __init__(self, child: Partitionable) -> None:
        self.child = child

    def operate(self, row: Row) -> Optional[Row]:
        return row

    def __iter__(self) -> Iterator[Row]:
        for id, row in enumerate(filter(None, map(self.operate, self.child))):
            yield row.renew_id(id)


FullProjector = Union_[Projector, WindowAggregateSpecification]


class Projection(Relation):
    def __init__(
        self, child: Relation, projections: Mapping[str, FullProjector]
    ) -> None:
        super().__init__(child)
        self.projections = projections

    def __iter__(self) -> Iterator[Row]:
        # we need a row iterator for every aggregation to be fully generic
        # since they potentially share no structure
        from stupidb.window import window_agg

        aggregations = {
            aggname: aggspec
            for aggname, aggspec in self.projections.items()
            if isinstance(aggspec, WindowAggregateSpecification)
        }

        # one child iter for *all* projections
        # one child iter for *each* window aggregation
        child, *rowterators = itertools.tee(self.child, len(aggregations) + 1)
        aggnames = aggregations.keys()
        aggvalues = aggregations.values()
        aggrows = (
            dict(zip(aggnames, aggvalue))
            for aggvalue in zip(*map(window_agg, rowterators, aggvalues))
        )

        projections = {
            name: projector
            for name, projector in self.projections.items()
            if callable(projector)
        }

        projnames = projections.keys()
        projrows = (
            dict(
                zip(
                    projnames,
                    (projector(row) for projector in projections.values()),
                )
            )
            for row in child
        )

        for i, (aggrow, projrow) in enumerate(
            itertools.zip_longest(aggrows, projrows, fillvalue={})
        ):
            yield Row(toolz.merge(projrow, aggrow), _id=i)


class Mutate(Projection):
    def __iter__(self) -> Iterator[Row]:
        child, self.child = itertools.tee(self.child)
        for i, row in enumerate(map(toolz.merge, child, super().__iter__())):
            yield Row.from_mapping(row, _id=i)


Aggregations = Mapping[str, AggregateSpecification]
WindowAggregations = Mapping[str, WindowAggregateSpecification]


class Aggregation(Relation):
    def __init__(self, child: Relation, aggregations: Aggregations) -> None:
        super().__init__(child)
        self.aggregations = aggregations

    def __iter__(self) -> Iterator[Row]:
        aggregations = self.aggregations

        # initialize aggregates
        grouped_aggs: Mapping[
            PartitionKey, Mapping[str, Aggregate]
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
                inputs = [getter(row) for getter in aggregations[name].getters]
                agg.step(*inputs)

        for id, (grouping_key, aggs) in enumerate(grouped_aggs.items()):
            finalized_aggregations = {
                name: agg.finalize() for name, agg in aggs.items()
            }
            data = toolz.merge(dict(grouping_key), finalized_aggregations)
            yield Row(data, _id=id)


class Selection(Relation):
    def __init__(self, child: Relation, predicate: Predicate) -> None:
        super().__init__(child)
        self.predicate = predicate

    def operate(self, row: Row) -> Optional[Row]:
        return row if self.predicate(row) else None


class GroupBy(Relation):
    def __init__(
        self, child: Relation, group_by: Mapping[str, PartitionBy]
    ) -> None:
        super().__init__(child)
        self.group_by = group_by

    def __iter__(self) -> Iterator[Row]:
        return iter(self.child)

    def partition_key(self, row: Row) -> PartitionKey:
        return tuple(
            (name, keyfunc(row)) for name, keyfunc in self.group_by.items()
        )


class SortBy(Relation):
    def __init__(self, child: Relation, order_by: Tuple[OrderBy, ...]) -> None:
        super().__init__(child)
        self.order_by = order_by

    def __iter__(self) -> Iterator[Row]:
        yield from sorted(
            (row for row in self.child),
            key=lambda row: tuple(
                order_func(row) for order_func in self.order_by
            ),
        )


JoinPredicate = Callable[[Row], bool]


class Join(Relation):
    def __init__(
        self, left: Relation, right: Relation, predicate: JoinPredicate
    ) -> None:
        self.left, left_ = itertools.tee(left)
        self.right, right_ = itertools.tee(right)
        super().__init__(
            PartitionableIterable(
                (
                    JoinedRow(l, r, _id=i)
                    for i, (l, r) in enumerate(
                        itertools.product(left_, right_)
                    )
                )
            )
        )
        self.predicate = predicate

    def __iter__(self) -> Iterator[Row]:
        predicate = self.predicate
        return (row for row in self.child if predicate(row))


class CrossJoin(Join):
    def __init__(self, left: Relation, right: Relation) -> None:
        super().__init__(left, right, lambda row: True)


class InnerJoin(Join):
    pass


class AsymmetricJoin(Join):
    @property
    @abc.abstractmethod
    def match_provider(self):
        ...

    @abc.abstractmethod
    def mismatch_keys(self, row: Row) -> Set[str]:
        ...

    def __iter__(self) -> Iterator[Row]:
        matches: Set[Row] = set()
        k = 0
        for row in self.child:
            if self.predicate(row):
                matches.add(self.match_provider(row))
                yield row
            k += 1
        else:
            keys = self.mismatch_keys(row)

        for i, row in enumerate(self.match_provider(self), start=k):
            if row not in matches:
                yield JoinedRow(row, dict.fromkeys(keys), _id=i)


class LeftJoin(AsymmetricJoin):
    @property
    def match_provider(self):
        return operator.attrgetter("left")

    def mismatch_keys(self, row: Row) -> Set[str]:
        return row.right.keys()


class RightJoin(AsymmetricJoin):
    @property
    def match_provider(self):
        return operator.attrgetter("right")

    def mismatch_keys(self, row: Row) -> Set[str]:
        return row.left.keys()


items = methodcaller("items")


class SetOperation(Relation):
    def __init__(self, left: Relation, right: Relation) -> None:
        self.left = left
        self.right = right


class Union(SetOperation):
    def __iter__(self) -> Iterator[Row]:
        return toolz.unique(
            toolz.concatv(self.left, self.right),
            key=toolz.compose(frozenset, items),
        )


SetOperand = FrozenSet[Tuple[Tuple[str, Any], ...]]


class InefficientSetOperation(SetOperation):
    def __iter__(self) -> Iterator[Row]:
        itemize = toolz.compose(frozenset, functools.partial(map, items))
        return (
            Row.from_mapping(dict(row), _id=id)
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
