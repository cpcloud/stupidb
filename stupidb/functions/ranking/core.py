from __future__ import annotations

import abc
from typing import Sequence

from ...aggregator import Aggregate, Aggregator
from ...protocols import Comparable
from ...row import AbstractRow
from ...typehints import Getter, Output, Result, T


class RankingAggregator(Aggregator["RankingAggregate", Result]):
    """Custom aggregator for ranking window functions.

    This aggregator is required for ranking functions because ranking functions
    take no arguments, but need access to the rows produced by the ordering
    key.

    See Also
    --------
    stupidb.navigation.NavigationAggregator
    stupidb.associative.SegmentTree

    """

    __slots__ = ("aggregate",)

    def __init__(
        self,
        order_by_values: Sequence[tuple[Comparable | None, ...]],
        aggregate_type: type[RankingAggregate],
    ) -> None:
        self.aggregate = aggregate_type(order_by_values)

    def query(self, begin: int, end: int) -> Result | None:
        """Compute the aggregation over the range of rows between `begin` and `end`."""
        return self.aggregate.execute(begin, end)


class RankingAggregate(Aggregate[Output]):
    """Base ranking aggregation class."""

    __slots__ = ("order_by_values",)

    def __init__(
        self, order_by_values: Sequence[tuple[Comparable | None, ...]]
    ) -> None:
        super().__init__()
        self.order_by_values = order_by_values

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Output | None:
        """Compute an abstract row rank value for rows between `begin` and `end`."""

    @classmethod
    def prepare(
        cls,
        possible_peers: Sequence[AbstractRow],
        getters: tuple[Getter, ...],
        order_by_columns: Sequence[str],
    ) -> RankingAggregator[Output]:
        """Construct the aggregator for ranking."""
        order_by_values = [
            tuple(peer[column] for column in order_by_columns)
            for peer in possible_peers
        ]
        return cls.aggregator_class(order_by_values)

    @classmethod
    def aggregator_class(
        cls, values: Sequence[tuple[T | None, ...]]
    ) -> RankingAggregator[Output]:
        return RankingAggregator(values, cls)
