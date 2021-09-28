"""Base aggregator interface."""

from __future__ import annotations

import abc
from typing import Callable, ClassVar, Generic, Sequence, TypeVar

from .row import AbstractRow
from .typehints import Getter, Output, Result, T

AggregateClass = TypeVar("AggregateClass", covariant=True)


class Aggregator(Generic[AggregateClass, Result], abc.ABC):
    """Interface for aggregators.

    Aggregators must implement the :meth:`~stupidb.aggregator.Aggregator.query`
    method. Aggregators are tied to a specific kind of aggregation. See the
    :meth:`~stupidb.aggregator.Aggregate.prepare` method for how to
    provide a custom aggregator.

    See Also
    --------
    stupidb.associative.SegmentTree
    stupidb.navigation.NavigationAggregator

    """

    @abc.abstractmethod
    def __init__(self, arguments: Sequence[T], cls: AggregateClass) -> None:
        """Initialize an aggregator from `arguments` and `cls`."""

    @abc.abstractmethod
    def query(self, begin: int, end: int) -> Result | None:
        """Query the aggregator over the range from `begin` to `end`."""


class Aggregate(Generic[Output], abc.ABC):
    """An aggregate or window function."""

    __slots__ = ()
    aggregator_class: ClassVar[Callable[..., Aggregator]]

    @classmethod
    def prepare(
        cls,
        possible_peers: Sequence[AbstractRow],
        getters: tuple[Getter, ...],
        order_by_columns: Sequence[str],
    ) -> Aggregator[Aggregate[Output], Output]:
        """Prepare an aggregation of this type for computation."""
        arguments = [
            tuple(getter(peer) for getter in getters) for peer in possible_peers
        ]
        return cls.aggregator_class(arguments, cls)
