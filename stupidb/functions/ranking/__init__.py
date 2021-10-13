"""Navigation and simple window function interface and implementation."""

from __future__ import annotations

import abc
from typing import Any, Sequence, Union

from ...protocols import Comparable
from ...typehints import T
from .core import RankingAggregate


class RowNumber(RankingAggregate[int]):
    """Row number analytic function."""

    __slots__ = ("row_number",)

    def __init__(
        self, order_by_values: Sequence[tuple[Comparable | None, ...]]
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
        self, order_by_values: Sequence[tuple[Comparable | None, ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.previous_value: Either | None = Sentinel()

    @abc.abstractmethod
    def rank(self, current_order_by_value: Comparable, current_row_number: int) -> int:
        """Compute the rank of the current row."""

    def execute(self, begin: int, end: int) -> int:
        """Compute an abstract row rank value for rows between `begin` and `end`."""
        current_row_number = super().execute(begin, end)
        current_order_by_value = self.order_by_values[current_row_number]
        rank = self.rank(current_order_by_value, current_row_number)
        assert (
            rank >= 0
        ), f"rank should be greater than or equal to 0, got rank == {rank:d}"
        self.previous_value = current_order_by_value
        assert not isinstance(
            self.previous_value, Sentinel
        ), "expected non-Sentinel order by value, got Sentinel"
        return rank


class Rank(AbstractRank):
    """Non-dense ranking computation."""

    __slots__ = ("previous_rank",)

    def __init__(
        self, order_by_values: Sequence[tuple[Comparable | None, ...]]
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
        self, order_by_values: Sequence[tuple[Comparable | None, ...]]
    ) -> None:
        super().__init__(order_by_values)
        self.current_rank = -1

    def rank(self, current_order_by_value: Comparable, current_row_number: int) -> int:
        """Compute the current rank, densely."""
        self.current_rank += current_order_by_value != self.previous_value
        return self.current_rank
