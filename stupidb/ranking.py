"""Navigation and simple window function interface and implementation."""

import abc
from typing import ClassVar, Optional, Sequence, Set, Tuple, Type

from stupidb.aggregatetypes import Aggregate
from stupidb.aggregator import Aggregator
from stupidb.protocols import Comparable
from stupidb.row import AbstractRow
from stupidb.typehints import Getter, Output, Result

OrderByValues = Sequence[Tuple[Comparable, ...]]


class RankingAggregator(Aggregator["RankingAggregate", Result]):
    """Custom aggregator for ranking window functions.

    This aggregator is required for ranking functions because ranking functions
    take no arguments, but need access to the rows produced by the ordering
    key.

    See Also
    --------
    stupidb.simple.SimpleAggregator

    """

    __slots__ = ("aggregate",)

    def __init__(
        self,
        order_by_values: Sequence[Tuple[Comparable, ...]],
        aggregate_type: Type["RankingAggregate"],
    ) -> None:
        self.aggregate: "RankingAggregate" = aggregate_type(order_by_values)

    def query(self, begin: int, end: int) -> Optional[Result]:
        return self.aggregate.execute(begin, end)


class RankingAggregate(Aggregate[Output]):
    __slots__ = "order_by_values", "seen"
    aggregator_class: ClassVar[Type[RankingAggregator]] = RankingAggregator

    def __init__(self, order_by_values: OrderByValues) -> None:
        super().__init__()
        self.order_by_values = order_by_values
        self.seen: Set[Comparable] = set()

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        """Executing the ranking function from `begin` to `end`."""

    @classmethod
    def prepare(
        cls,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        getters: Tuple[Getter, ...],
        order_by_columns: Sequence[str],
    ) -> RankingAggregator[Output]:
        order_by_values = [
            tuple(peer[column] for column in order_by_columns)
            for _, peer in possible_peers
        ]
        return cls.aggregator_class(order_by_values, cls)


class RowNumber(RankingAggregate[int]):
    __slots__ = ("row_number",)

    def __init__(self, order_by_values: OrderByValues) -> None:
        super().__init__(order_by_values)
        self.row_number = 0

    def execute(self, begin: int, end: int) -> Optional[int]:
        row_number = self.row_number
        self.row_number += 1
        return row_number


class Rank(RowNumber):
    __slots__ = ("rank",)

    def __init__(self, order_by_values: OrderByValues) -> None:
        super().__init__(order_by_values)
        self.rank = -1

    def execute(self, begin: int, end: int) -> Optional[int]:
        row_number = super().execute(begin, end)
        assert row_number is not None
        current_order_by_value = self.order_by_values[row_number]
        self.rank += current_order_by_value not in self.seen
        self.seen.add(current_order_by_value)
        assert self.rank >= 0
        return self.rank


class DenseRank(RankingAggregate[int]):
    __slots__ = ()


class PercentRank(RankingAggregate[float]):
    __slots__ = ()


class CumeDist(RankingAggregate[float]):
    __slots__ = ()
