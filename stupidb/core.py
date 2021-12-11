"""StupiDB. The stupidest database you'll ever come across.

This is project designed to illustate the concepts that underly a typical
relational database implementation, starting at naive execution of table-stakes
features up to rule-based query optimization.

.. warning::
   Please do not use this for any other reason than learning. There are no
   guarantees here except that there will be bugs.

"""

from __future__ import annotations

import abc
import collections
import functools
import itertools
import typing
from typing import Any, Generic, Iterable, Iterator, Mapping

import toolz

from .aggregation import (
    AggregateSpecification,
    Nulls,
    WindowAggregateSpecification,
    row_key_compare,
)
from .functions.associative.core import AssociativeAggregate
from .row import AbstractRow, JoinedRow, Row
from .typehints import (
    JoinPredicate,
    OrderBy,
    PartitionBy,
    PartitionKey,
    Predicate,
    Projector,
)


class Relation(abc.ABC):
    """An abstract relation."""

    __slots__ = ("partitioners",)

    def __init__(self) -> None:
        self.partitioners: Mapping[str, PartitionBy] = {}

    def __iter__(self) -> Iterator[AbstractRow]:
        """Iterate over the rows of a :class:`~stupidb.stupidb.Relation`.

        This method will reify rows with a new row identifier equal to the row number.

        """
        return (
            Row.from_mapping(row, _id=id)
            for id, row in enumerate(filter(None, self._produce()))
        )

    @abc.abstractmethod
    def _produce(self) -> Iterator[AbstractRow]:
        """Iterate over the rows of a :class:`~stupidb.stupidb.Relation`.

        Specific relation should implement this without reifying the row with
        the row identifier if possible. Reification is handled in the
        :meth:`~stupidb.stupidb.Relation.__iter__` method.

        """

    def __repr__(self) -> str:
        from stupidb.api import pretty

        return pretty(self)


class Table(Relation):
    __slots__ = ("rows",)

    def __init__(self, rows: Iterable[AbstractRow]) -> None:
        super().__init__()
        self.rows = rows

    @classmethod
    def from_iterable(cls, iterable: Iterable[Mapping[str, Any]]) -> Table:
        return cls(
            Row.from_mapping(mapping, _id=i) for i, mapping in enumerate(iterable)
        )

    def _produce(self) -> Iterator[AbstractRow]:
        return iter(self.rows)


class Projection(Relation):
    """A relation representing column selection.

    Attributes
    ----------
    child
    aggregations
    projections

    """

    __slots__ = "child", "aggregations", "projections"

    def __init__(
        self,
        child: Relation,
        projections: Mapping[str, Projector | WindowAggregateSpecification],
    ) -> None:
        super().__init__()
        self.child = child
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

    def _produce(self) -> Iterator[AbstractRow]:
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
        return (
            Row(toolz.merge(projrow, aggrow), _id=-1)
            for aggrow, projrow in itertools.zip_longest(
                aggrows, projrows, fillvalue={}
            )
        )


class Mutate(Projection):
    """A relation representing appending columns to an existing relation."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        # reasign self.child here to avoid clobbering its iteration
        # we need to use it twice: once for the computed columns (self.child)
        # used during the iteration of super().__iter__() and once for the
        # original relation (child)
        child, self.child = itertools.tee(self.child)
        return (
            Row.from_mapping(row, _id=-1)
            for row in map(toolz.merge, child, super()._produce())
        )


class Aggregation(Generic[AssociativeAggregate], Relation):
    """A relation representing aggregation of columns."""

    __slots__ = "child", "metrics"

    def __init__(
        self,
        child: Relation,
        metrics: Mapping[str, AggregateSpecification[AssociativeAggregate]],
    ) -> None:
        super().__init__()
        self.child = child
        self.metrics: Mapping[
            str, AggregateSpecification[AssociativeAggregate]
        ] = metrics

    def _produce(self) -> Iterator[AbstractRow]:
        aggregations = self.metrics

        # initialize aggregates
        grouped_aggs: Mapping[
            PartitionKey, Mapping[str, AssociativeAggregate]
        ] = collections.defaultdict(
            lambda: {
                name: aggspec.aggregate_type() for name, aggspec in aggregations.items()
            }
        )

        child = typing.cast(Relation, self.child)
        for row in child:
            key = tuple(
                (name, keyfunc(row)) for name, keyfunc in child.partitioners.items()
            )
            for name, agg in grouped_aggs[key].items():
                inputs = (getter(row) for getter in aggregations[name].getters)
                agg.step(*inputs)

        for grouping_key, aggs in grouped_aggs.items():
            data = dict(grouping_key)
            data.update((name, agg.finalize()) for name, agg in aggs.items())
            yield Row.from_mapping(data)


class Selection(Relation):
    """A relation of rows selected based on a predicate.

    Attributes
    ----------
    predicate
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        a :class:`bool`.

    """

    __slots__ = "child", "predicate"

    def __init__(self, child: Relation, predicate: Predicate) -> None:
        super().__init__()
        self.child = child
        self.predicate = predicate

    def _produce(self) -> Iterator[AbstractRow]:
        return filter(self.predicate, self.child)


class GroupBy(Relation):
    """A relation representing a partitioning of rows by a key.

    Attributes
    ----------
    group_by
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        an instance of :class:`typing.Hashable`.

    """

    __slots__ = "child", "group_by", "partitioners"

    def __init__(self, child: Relation, group_by: Mapping[str, PartitionBy]) -> None:
        super().__init__()
        self.child = child
        self.partitioners = group_by

    def _produce(self) -> Iterator[AbstractRow]:
        return iter(self.child)


class SortBy(Relation):
    """A relation representing rows of its child sorted by one or more keys.

    Attributes
    ----------
    order_by
        A callable that takes an :class:`~stupidb.row.AbstractRow` and returns
        an instance of :class:`~stupidb.protocols.Comparable`.
    null_ordering
        Whether to place the nulls of a column first or last.

    """

    __slots__ = "child", "order_by", "null_ordering"

    def __init__(
        self, child: Relation, order_by: tuple[OrderBy, ...], null_ordering: Nulls
    ) -> None:
        super().__init__()
        self.child = child
        self.order_by = order_by
        self.null_ordering = null_ordering

    def _produce(self) -> Iterator[AbstractRow]:
        return iter(
            sorted(
                self.child,
                key=functools.cmp_to_key(
                    functools.partial(
                        row_key_compare,
                        toolz.juxt(*self.order_by),
                        self.null_ordering,
                    )
                ),
            )
        )


class Limit(Relation):
    __slots__ = "child", "offset", "limit"

    def __init__(self, child: Relation, *, offset: int, limit: int | None) -> None:
        super().__init__()
        self.child = child
        self.offset = offset
        self.limit = limit

    def _produce(self) -> Iterator[AbstractRow]:
        limit = self.limit
        offset = self.offset
        return itertools.islice(
            self.child,
            offset,
            None if limit is None else offset + limit,
        )


class Join(Relation):
    __slots__ = "grouped", "rows"

    def __init__(self, left: Relation, right: Relation) -> None:
        super().__init__()
        self.grouped = itertools.groupby(
            (
                JoinedRow(left_row, right_row, _id=-1)
                for left_row, right_row in itertools.product(left, right)
            ),
            key=lambda row: row.left,
        )
        self.rows = itertools.chain.from_iterable(rows for _, rows in self.grouped)


class CrossJoin(Join):
    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        return iter(self.rows)


class InnerJoin(Join):
    __slots__ = ("predicate",)

    def __init__(
        self, left: Relation, right: Relation, predicate: JoinPredicate
    ) -> None:
        super().__init__(left, right)
        self.predicate = predicate

    def _produce(self) -> Iterator[AbstractRow]:
        return (row for row in self.rows if self.predicate(row.left, row.right))


class LeftJoin(Join):
    __slots__ = ("predicate",)

    def __init__(
        self, left: Relation, right: Relation, predicate: JoinPredicate
    ) -> None:
        super().__init__(left, right)
        self.predicate = predicate

    def _produce(self) -> Iterator[AbstractRow]:
        for left_row, joined_rows in self.grouped:
            matched = False

            for joined_row in joined_rows:
                right_row = joined_row.right
                if self.predicate(left_row, right_row):
                    matched = True
                    yield JoinedRow(left_row, right_row, _id=-1)
            if not matched:
                yield JoinedRow(left_row, dict.fromkeys(right_row), _id=-1)


class RightJoin(LeftJoin):
    __slots__ = ()

    def __init__(
        self, left: Relation, right: Relation, predicate: JoinPredicate
    ) -> None:
        super().__init__(right, left, predicate)

    def _produce(self) -> Iterator[AbstractRow]:
        for row in super()._produce():
            yield JoinedRow(row.right, row.left)


class SetOperation(Relation):
    """An abstract set operation."""

    __slots__ = "left", "right"

    def __init__(self, left: Relation, right: Relation) -> None:
        super().__init__()
        self.left = left
        self.right = right

    @staticmethod
    def itemize(
        mappings: Iterable[AbstractRow],
    ) -> frozenset[tuple[tuple[str, Any], ...]]:
        """Return a hashable version of `mappings`."""
        return frozenset(tuple(mapping.items()) for mapping in mappings)


class Union(SetOperation):
    """Union between two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        return toolz.unique(
            itertools.chain(self.left, self.right),
            key=lambda row: frozenset(row.items()),
        )


class UnionAll(SetOperation):
    """Non-unique union between two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        return itertools.chain(self.left, self.right)


class IntersectAll(SetOperation):
    """Non-unique intersection between two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        left_set = self.itemize(self.left)
        right_set = self.itemize(self.right)
        left_filtered = (row_items for row_items in left_set if row_items in right_set)
        right_filtered = (row_items for row_items in right_set if row_items in left_set)
        return (
            Row.from_mapping(dict(row))
            for row in itertools.chain(left_filtered, right_filtered)
        )


class Intersect(SetOperation):
    """Intersection of two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        return (
            Row.from_mapping(dict(row))
            for row in self.itemize(self.left) & self.itemize(self.right)
        )


class Difference(SetOperation):
    """Unique difference between two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        right_set = self.itemize(self.right)
        return toolz.unique(
            Row.from_mapping(dict(row_items))
            for row_items in (tuple(row.items()) for row in self.left)
            if row_items not in right_set
        )


class DifferenceAll(SetOperation):
    """Non-unique difference between two relations."""

    __slots__ = ()

    def _produce(self) -> Iterator[AbstractRow]:
        right_set = self.itemize(self.right)
        return (
            Row.from_mapping(dict(row_items))
            for row_items in (tuple(row.items()) for row in self.left)
            if row_items not in right_set
        )
