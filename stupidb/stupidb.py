# -*- coding: utf-8 -*-

"""The stupidest database."""

import abc
import collections
import typing

from numbers import Number
from pprint import pprint
from typing import (
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import toolz


Row = Mapping[str, Any]
Rows = Iterable[Row]


class Relation(abc.ABC):
    child: "Relation"

    def operate(self, row: Row) -> Row:
        # empty rows (empty dicts) are ignored
        return toolz.identity(row)

    def produce(self, rows: Rows) -> Iterator[Row]:
        return filter(None, map(self.operate, self.child.produce(rows)))


class Table(Relation):
    def produce(self, rows: Rows) -> Iterator[Row]:
        return iter(rows)


class Projection(Relation):
    def __init__(self, child: Relation, columns: Sequence[str]) -> None:
        self.child = child
        self.columns = columns

    def operate(self, row: Row) -> Row:
        return {column: row[column] for column in self.columns}


class Rename(Relation):
    def __init__(self, child: Relation, columns: Mapping[str, str]) -> None:
        self.child = child
        self.columns = columns

    def operate(self, row: Row) -> Row:
        columns = self.columns
        return {
            columns.get(column, column): row[column] for column in row.keys()
        }


class Selection(Relation):
    def __init__(
        self, child: Relation, predicate: Callable[[Row], bool]
    ) -> None:
        self.child = child
        self.predicate = predicate

    def operate(self, row: Row) -> Row:
        return row if self.predicate(row) else {}


class Aggregate(abc.ABC):
    @abc.abstractmethod
    def step(self, *values) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Any:
        ...


GroupingKey = Mapping[str, Callable[[Row], Hashable]]
AggregateSpecification = Mapping[
    str, Tuple[Type[Aggregate], Callable[[Row], Any]]
]


class GroupBy(Relation):
    def __init__(
        self,
        child: Relation,
        group_by: GroupingKey,
        aggregates: AggregateSpecification,
    ) -> None:
        self.child = child
        self.group_by = group_by
        self.aggregates = aggregates

    def produce(self, rows: Rows) -> Iterator[Row]:
        aggs: Mapping[Tuple[Tuple[str, Hashable]], Mapping[str, Tuple[Aggregate, Callable[[Row], Any]]]] = collections.defaultdict(
            lambda: {
                name: (agg(), func)
                for name, (agg, func) in self.aggregates.items()
            }
        )
        aggregates = self.aggregates
        for row in self.child.produce(rows):
            keys = tuple(
                (name, func(row)) for name, func in self.group_by.items()
            )
            keyed_agg = aggs[keys]
            for name in aggregates.keys():
                agg, func = keyed_agg[name]
                agg.step(func(row))
        for key, agg in dict(aggs).items():
            grouping_keys = dict(key)
            agg_values = {
                key: subagg.finalize() for key, (subagg, _) in agg.items()
            }
            res = toolz.merge(grouping_keys, agg_values)
            yield res


class Sum(Aggregate):
    def __init__(self) -> None:
        self.total: Number = 0
        self.count: int = 0

    def step(self, value: Optional[Number]) -> None:
        if value is not None:
            self.total += value
            self.count += 1

    def finalize(self) -> Optional[Number]:
        return self.total if self.count else None


class Mean(Aggregate):
    def __init__(self) -> None:
        self.total: float = 0.0
        self.count: int = 0

    def step(self, value: Optional[Number]) -> None:
        if value is not None:
            self.total += typing.cast(float, value)
            self.count += 1

    def finalize(self) -> Optional[float]:
        count = self.count
        return self.total / count if count > 0 else None


t = Table()
proj = Projection(t, ["a", "b", "z"])
ren = Rename(proj, dict(a="c", b="d"))
filt = Selection(ren, lambda row: True)  # type: ignore
gb = GroupBy(
    filt,
    {
        "c": lambda row: row['c'],
        "z": lambda row: row['z'],
    },
    {
        "total": (Sum, lambda row: row['d']),  # type: ignore
        "mean": (Mean, lambda row: row['d']),  # type: ignore
    },
)

data = [
    dict(z='a', a=1, b=2),
    dict(z='b', a=2, b=-1),
    dict(z='a', a=3, b=4),
    dict(z='a', a=4, b=-3),
    dict(z='a', a=1, b=-3),
    dict(z='b', a=2, b=-3),
    dict(z='b', a=3, b=-3),
]

rowz = gb.produce(data)
pprint(list(rowz))
