"""Base aggregator interface."""

import abc
from typing import Generic, Optional, Sequence, TypeVar

from .typehints import Result, T

AggregateClass = TypeVar("AggregateClass", covariant=True)


class Aggregator(Generic[AggregateClass, Result], abc.ABC):
    """Interface for aggregators.

    Aggregators must implement the :meth:`~stupidb.aggregator.Aggregator.query`
    method. Aggregators are tied to a specific kind of aggregation. See the
    :meth:`~stupidb.aggregatetypes.UnaryAggregate.prepare` method for how to
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
    def query(self, begin: int, end: int) -> Optional[Result]:
        """Query the aggregator over the range from `begin` to `end`."""
