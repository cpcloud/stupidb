from __future__ import annotations

import abc
from typing import Generic, Sequence

from ...aggregator import Aggregate, Aggregator
from ...typehints import Input1, Input2, Input3, Output, Result, T


class NavigationAggregator(Aggregator["NavigationAggregate", Result]):
    """Custom aggregator for navigation functions.

    This aggregator is useful for a subset of window functions whose underlying
    binary combine operator (if it even exists) is not associative or easy to
    express without special knowledge of the underlying aggregator
    representation.

    See Also
    --------
    stupidb.ranking.RankingAggregator

    """

    __slots__ = ("aggregate",)

    def __init__(
        self,
        inputs: Sequence[tuple[T, ...]],
        aggregate_type: type[NavigationAggregate],
    ) -> None:
        self.aggregate = aggregate_type(*zip(*inputs))

    def query(self, begin: int, end: int) -> Result | None:
        return self.aggregate.execute(begin, end)


class NavigationAggregate(Aggregate[Output]):
    """Base class for navigation aggregate functions."""

    __slots__ = ()

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Output | None:
        """Execute the aggregation over the range from `begin` to `end`."""

    @classmethod
    def aggregator_class(cls, inputs: Sequence[T]) -> NavigationAggregator:
        return NavigationAggregator(inputs, cls)


class UnaryNavigationAggregate(Generic[Input1, Output], NavigationAggregate[Output]):
    """Navigation function taking one argument."""

    __slots__ = ("inputs1",)

    def __init__(self, inputs1: Sequence[Input1 | None]) -> None:
        self.inputs1 = inputs1


class BinaryNavigationAggregate(
    Generic[Input1, Input2, Output], NavigationAggregate[Output]
):
    """Navigation function taking two arguments."""

    __slots__ = "inputs1", "inputs2"

    def __init__(
        self,
        inputs1: Sequence[Input1 | None],
        inputs2: Sequence[Input2 | None],
    ) -> None:
        self.inputs1 = inputs1
        self.inputs2 = inputs2


class TernaryNavigationAggregate(
    Generic[Input1, Input2, Input3, Output], NavigationAggregate[Output]
):
    """Navigation function taking three arguments."""

    __slots__ = "inputs1", "inputs2", "inputs3"

    def __init__(
        self,
        inputs1: Sequence[Input1 | None],
        inputs2: Sequence[Input2 | None],
        inputs3: Sequence[Input3 | None],
    ) -> None:
        self.inputs1 = inputs1
        self.inputs2 = inputs2
        self.inputs3 = inputs3
