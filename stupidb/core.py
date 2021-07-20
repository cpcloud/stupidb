# -*- coding: utf-8 -*-

"""StupiDB. The stupidest database you'll ever come across.

This is project designed to illustate the concepts that underly a typical
relational database implementation, starting at naive execution of table-stakes
features up to rule-based query optimization.

.. warning::
   Please do not use this for any other reason than learning. There are no
   guarantees here except that there will be bugs.

"""

import abc
import collections
import functools
import itertools
import operator
import typing
from typing import (
    Any,
    Callable,
    FrozenSet,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    NoReturn,
    Set,
    Tuple,
)
from typing import Union as Union_

import cytoolz as toolz

from stupidb.aggregation import (
    AggregateSpecification,
    Nulls,
    WindowAggregateSpecification,
    row_key_compare,
)
from stupidb.associative import AssociativeAggregate
from stupidb.row import AbstractRow, JoinedRow, Row
from stupidb.typehints import (
    JoinPredicate,
    OrderBy,
    PartitionBy,
    PartitionKey,
    Predicate,
    Projector,
)


class Partitionable(abc.ABC):
    """An abstraction for a set of rows that can be partitioned by a key."""

    __slots__ = ("rows",)

    def __init__(self, rows: Iterable[AbstractRow]) -> None:
        self.rows = rows

    def partition_key(self, row: AbstractRow) -> PartitionKey:
        return ()

    def __iter__(self) -> Iterator[AbstractRow]:
        return iter(self.rows)


class Relation(Partitionable):
    """A relation."""

    __slots__ = ("child",)

    def __init__(self, child: Partitionable) -> None:
        self.child = child

    def __iter__(self) -> Iterator[AbstractRow]:
        """Iterate over the rows of a :class:`~stupidb.stupidb.Relation`."""
        for id, row in enumerate(filter(None, self.child)):
            yield row.renew_id(id)

    @classmethod
    def from_iterable(cls, iterable: Iterable[Mapping[str, Any]]) -> "Relation":
        return cls(
            Partitionable(
                Row.from_mapping(row, _id=i) for i, row in enumerate(iterable)
            )
        )


FullProjector = Union_[Projector, WindowAggregateSpecification]


class Projection(Relation):
    """A relation representing column selection.

    Attributes
    ----------
    aggregations
    projections

    """

    __slots__ = "aggregations", "projections"

    def __init__(
        self, child: Relation, projections: Mapping[str, FullProjector]
    ) -> None:
        super().__init__(child)
        self.aggregations: Mapping[str, WindowAggregateSpecification] = {
            aggname: aggspec
            for aggname, aggspec in projections.items()
            if isinstance(aggspec, WindowAggregateSpecification)
        }
        self.projections: Mapping[str, Projector] = {
            name: projector
            for name, projector in projections.items()
            if callable(projector)
        }

    def __iter__(self) -> Iterator[AbstractRow]:
        aggregations = self.aggregations
        # we need a row iterator for every aggregation to be fully generic
        # since they potentially share no structure
        #
        # one child iter for *all* projections
        # one child iter for *each* window aggregation
        child, *rowterators = itertools.tee(self.child, len(aggregations) + 1)
        aggnames = aggregations.keys()
        aggvalues = aggregations.values()

        # The .compute method returns an iterator of aggregation results
        # Each element of the iterator is the result of a single column in a
        # single row of the corresponding window function
        aggrows = (
            dict(zip(aggnames, aggrow))
            for aggrow in zip(
                *map(
                    WindowAggregateSpecification.compute,
                    aggvalues,
                    rowterators,
                )
            )
        )

        projections = self.projections
        projnames = projections.keys()
        projvalues = projections.values()
        projrows = (
            dict(zip(projnames, (proj(row) for proj in projvalues))) for row in child
        )

        # Use zip_longest here, because either of aggrows or projrows can be
        # empty
        for i, (aggrow, projrow) in enumerate(
            itertools.zip_longest(aggrows, projrows, fillvalue={})
        ):
            yield Row(toolz.merge(projrow, aggrow), _id=i)


class Mutate(Projection):
    """A relation representing appending columns to an existing relation."""

    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        # reasign self.child here to avoid clobbering its iteration
        # we need to use it twice: once for the computed columns (self.child)
        # used during the iteration of super().__iter__() and once for the
        # original relation (child)
        child, self.child = typing.cast(
            Tuple[Partitionable, Partitionable], itertools.tee(self.child)
        )
        for i, row in enumerate(map(toolz.merge, child, super().__iter__())):
            yield Row.from_mapping(row, _id=i)


class Aggregation(Generic[AssociativeAggregate], Relation):
    """A relation representing aggregation of columns.

    Attributes
    ----------
    aggregations

    """

    __slots__ = ("aggregations",)

    def __init__(
        self,
        child: Relation,
        aggregations: Mapping[str, AggregateSpecification[AssociativeAggregate]],
    ) -> None:
        super().__init__(child)
        self.aggregations: Mapping[
            str, AggregateSpecification[AssociativeAggregate]
        ] = aggregations

    def __iter__(self) -> Iterator[AbstractRow]:
        aggregations = self.aggregations

        # initialize aggregates
        grouped_aggs: Mapping[
            PartitionKey, Mapping[str, AssociativeAggregate]
        ] = collections.defaultdict(
            lambda: {
                name: aggspec.aggregate_type() for name, aggspec in aggregations.items()
            }
        )
        child = self.child
        for row in child:
            key = child.partition_key(row)
            aggs: Mapping[str, AssociativeAggregate] = grouped_aggs[key]
            for name, agg in aggs.items():
                inputs = [getter(row) for getter in aggregations[name].getters]
                agg.step(*inputs)

        for id, (grouping_key, aggs) in enumerate(grouped_aggs.items()):
            data = dict(grouping_key)
            finalized_aggregations = {
                name: agg.finalize() for name, agg in aggs.items()
            }
            data.update(finalized_aggregations)
            yield Row(data, _id=id)


class Selection(Relation):
    """A relation representing filtering rows based on a predicate.

    Attributes
    ----------
    predicate
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        a :class:`bool`.

    """

    __slots__ = ("predicate",)

    def __init__(self, child: Relation, predicate: Predicate) -> None:
        super().__init__(child)
        self.predicate = predicate

    def __iter__(self) -> Iterator[AbstractRow]:
        for id, row in enumerate(filter(self.predicate, self.child)):
            yield row.renew_id(id)


class GroupBy(Relation):
    """A relation representing a partitioning of rows by a key.

    Attributes
    ----------
    group_by
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        an instance of :class:`typing.Hashable`.

    """

    __slots__ = ("group_by",)

    def __init__(self, child: Relation, group_by: Mapping[str, PartitionBy]) -> None:
        super().__init__(child)
        self.group_by = group_by

    def partition_key(self, row: AbstractRow) -> PartitionKey:
        return tuple((name, keyfunc(row)) for name, keyfunc in self.group_by.items())


class SortBy(Relation):
    """A relation representing rows of its child sorted by one or more keys.

    Attributes
    ----------
    order_by
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        an instance of :class:`~stupidb.protocols.Comparable`.
    nulls
        Whether to place the nulls of a column first or last.

    """

    __slots__ = "order_by", "nulls"

    def __init__(
        self, child: Relation, order_by: Tuple[OrderBy, ...], nulls: Nulls
    ) -> None:
        super().__init__(child)
        self.order_by = order_by
        self.nulls = nulls

    def __iter__(self) -> Iterator[AbstractRow]:
        return iter(
            sorted(
                self.child,
                key=functools.cmp_to_key(
                    functools.partial(row_key_compare, self.order_by, self.nulls)
                ),
            )
        )


class Join(Relation):
    __slots__ = "left", "right", "predicate"

    def __init__(
        self, left: Relation, right: Relation, predicate: JoinPredicate
    ) -> None:
        self.left, left_ = itertools.tee(left)
        self.right, right_ = itertools.tee(right)
        self.predicate = predicate
        super().__init__(
            Partitionable(
                (
                    JoinedRow(left_row, right_row, _id=i)
                    for i, (left_row, right_row) in enumerate(
                        itertools.product(left_, right_)
                    )
                )
            )
        )

    def __iter__(self) -> Iterator[AbstractRow]:
        return (row for row in self.child if self.predicate(row.left, row.right))

    @classmethod
    def from_iterable(cls, *args: Any, **kwargs: Any) -> NoReturn:
        raise TypeError(f"from_iterable not supported for {cls.__name__} type")


class CrossJoin(Join):
    __slots__ = ()

    def __init__(self, left: Relation, right: Relation) -> None:
        super().__init__(left, right, lambda left, right: True)


class InnerJoin(Join):
    __slots__ = ()


MatchProvider = Callable[[AbstractRow], AbstractRow]


class AsymmetricJoin(Join):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def match_provider(self) -> MatchProvider:
        """Return a :data:`MatchProvider`."""

    @property
    @abc.abstractmethod
    def matching_relation(self) -> Iterator[AbstractRow]:
        """Return the relation to use for matching."""

    @abc.abstractmethod
    def mismatch_keys(self, row: AbstractRow) -> Set[str]:
        """Return keys used for mismatched keys."""

    def __iter__(self) -> Iterator[AbstractRow]:
        matches: Set[AbstractRow] = set()
        k = 0
        filtered = (row for row in self.child if self.predicate(row.left, row.right))
        for row in filtered:
            matches.add(self.match_provider(row))
            yield row.renew_id(k)
            k += 1
        else:
            keys = self.mismatch_keys(row)

        non_matching_rows = (
            row for row in self.matching_relation if row not in matches
        )
        for i, row in enumerate(non_matching_rows, start=k):
            yield JoinedRow(row, dict.fromkeys(keys), _id=i)


class LeftJoin(AsymmetricJoin):
    __slots__ = ()

    @property
    def match_provider(self) -> MatchProvider:
        return operator.attrgetter("left")

    @property
    def matching_relation(self) -> Iterator[AbstractRow]:
        return self.left

    def mismatch_keys(self, row: AbstractRow) -> Set[str]:
        return row.right.keys()


class RightJoin(AsymmetricJoin):
    __slots__ = ()

    @property
    def match_provider(self) -> MatchProvider:
        return operator.attrgetter("right")

    @property
    def matching_relation(self) -> Iterator[AbstractRow]:
        return self.right

    def mismatch_keys(self, row: AbstractRow) -> Set[str]:
        return row.left.keys()


SetOperand = FrozenSet[Tuple[Tuple[str, Any], ...]]


class SetOperation(Relation):
    """A generic set operation."""

    __slots__ = "left", "right"

    def __init__(self, left: Relation, right: Relation) -> None:
        self.left = left
        self.right = right

    @staticmethod
    def itemize(mappings: Iterable[AbstractRow]) -> SetOperand:
        return frozenset(tuple(mapping.items()) for mapping in mappings)


class Union(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        return (
            Row.from_mapping(row, _id=id)
            for id, row in enumerate(
                toolz.unique(
                    itertools.chain(self.left, self.right),
                    key=lambda row: frozenset(row.items()),
                )
            )
        )


class UnionAll(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        for id, row in enumerate(itertools.chain(self.left, self.right)):
            yield Row.from_mapping(row, _id=id)


class IntersectAll(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        left_rows = list(self.left)
        left_set = self.itemize(left_rows)
        right_rows = list(self.right)
        right_set = self.itemize(right_rows)
        left_filtered = (
            dict(row_items)
            for row_items in (tuple(row.items()) for row in left_rows)
            if row_items in right_set
        )
        right_filtered = (
            dict(row_items)
            for row_items in (tuple(row.items()) for row in right_rows)
            if row_items in left_set
        )
        for id, row in enumerate(itertools.chain(left_filtered, right_filtered)):
            yield Row.from_mapping(row, _id=id)


class Difference(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        right_set = self.itemize(self.right)
        filtered = (
            dict(row_items)
            for row_items in (tuple(row.items()) for row in self.left)
            if row_items not in right_set
        )
        rows = (Row.from_mapping(row, _id=id) for id, row in enumerate(filtered))
        return toolz.unique(rows)


class DifferenceAll(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        right_set = self.itemize(self.right)
        filtered = (
            dict(row_items)
            for row_items in (tuple(row.items()) for row in self.left)
            if row_items not in right_set
        )
        for id, row in enumerate(filtered):
            yield Row.from_mapping(row, _id=id)


class Intersect(SetOperation):
    __slots__ = ()

    def __iter__(self) -> Iterator[AbstractRow]:
        return (
            Row.from_mapping(dict(row), _id=id)
            for id, row in enumerate(self.itemize(self.left) & self.itemize(self.right))
        )
