"""Navigation and simple window function interface and implementation."""

import abc
from typing import Any, Callable, ClassVar, Optional, Sequence, Tuple, Type, Union

from stupidb.aggregatetypes import Aggregate
from stupidb.aggregator import Aggregator
from stupidb.protocols import Comparable
from stupidb.row import AbstractRow
from stupidb.typehints import Getter, Output, Result, T


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
        order_by_values: Sequence[Tuple[Optional[Comparable], ...]],
        aggregate_type: Type["RankingAggregate"],
    ) -> None:
        self.aggregate: "RankingAggregate" = aggregate_type(order_by_values)

    def query(self, begin: int, end: int) -> Optional[Result]:
        """Compute the aggregation over the range of rows between `begin` and `end`."""
        return self.aggregate.execute(begin, end)


class RankingAggregate(Aggregate[Output]):
    """Base ranking aggregation class."""

    __slots__ = ("order_by_values",)
    aggregator_class: ClassVar[Callable[..., RankingAggregator]] = RankingAggregator

    def __init__(
        self, order_by_values: Sequence[Tuple[Optional[Comparable], ...]]
    ) -> None:
        super().__init__()
        self.order_by_values = order_by_values

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        """Compute an abstract row rank value for rows between `begin` and `end`."""

    @classmethod
    def prepare(
        cls,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        getters: Tuple[Getter, ...],
        order_by_columns: Sequence[str],
    ) -> RankingAggregator[Output]:
        """Construct the aggregator for ranking."""
        order_by_values = [
            tuple(peer[column] for column in order_by_columns)
            for _, peer in possible_peers
        ]
        return cls.aggregator_class(order_by_values, cls)


class RowNumber(RankingAggregate[int]):
    """Row number analytic function."""

    __slots__ = ("row_number",)

    def __init__(
        self, order_by_values: Sequence[Tuple[Optional[Comparable], ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.row_number = 0

    def execute(self, begin: int, end: int) -> int:
        """Compute an abstract row rank value for rows between `begin` and `end`."""
        row_number = self.row_number
        self.row_number += 1
        return row_number


class Sentinel:
    """A class that is not equal to anything except instances of itself.

    This class is used as the starting value for :class:`stupidb.ranking.Rank`
    and :class:`stupidb.ranking.DenseRank` because their algorithms compare the
    previous ``ORDER BY`` value in the sequence to determine whether to
    increase the rank.

    """

    __slots__ = ()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self))


Either = Union[Sentinel, T]


class AbstractRank(RowNumber):
    """A class represnting a numerical ordering of rows."""

    __slots__ = ("previous_value",)

    def __init__(
        self, order_by_values: Sequence[Tuple[Optional[Comparable], ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.previous_value: Optional[Either] = Sentinel()

    @abc.abstractmethod
    def rank(self, current_order_by_value: Comparable, current_row_number: int) -> int:
        """Compute the rank of the current row."""

    def execute(self, begin: int, end: int) -> int:
        """Compute an abstract row rank value for rows between `begin` and `end`."""
        current_row_number = super().execute(begin, end)
        current_order_by_value = self.order_by_values[current_row_number]
        rank = self.rank(current_order_by_value, current_row_number)
        assert rank >= 0, f"rank == {rank:d}"
        self.previous_value = current_order_by_value
        assert not isinstance(
            self.previous_value, Sentinel
        ), f"{current_order_by_value}"
        return rank


class Rank(AbstractRank):
    """Non-dense ranking computation."""

    __slots__ = ("previous_rank",)

    def __init__(
        self, order_by_values: Sequence[Tuple[Optional[Comparable], ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.previous_rank = -1

    def rank(self, current_order_by_value: Comparable, current_row_number: int) -> int:
        """Rank the current row according to `current_order_by_value`."""
        if current_order_by_value != self.previous_value:
            rank = current_row_number
        else:
            rank = self.previous_rank
        self.previous_rank = rank
        return rank


class DenseRank(AbstractRank):
    """Dense ranking computation."""

    __slots__ = ("current_rank",)

    def __init__(
        self, order_by_values: Sequence[Tuple[Optional[Comparable], ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.current_rank = -1

    def rank(self, current_order_by_value: Comparable, current_row_number: int) -> int:
        """Compute the current rank, densely."""
        self.current_rank += current_order_by_value != self.previous_value
        return self.current_rank
