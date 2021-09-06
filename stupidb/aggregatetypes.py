"""Abstract and concrete aggregation types."""

from __future__ import annotations

import abc
from typing import Callable, ClassVar, Generic, Sequence, Tuple

from .aggregator import Aggregator
from .row import AbstractRow
from .typehints import Getter, Output


class Aggregate(Generic[Output], abc.ABC):
    """An aggregate or window function."""

    __slots__ = ()
    aggregator_class: ClassVar[Callable[..., Aggregator]]

    @classmethod
    def prepare(
        cls,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        getters: Tuple[Getter, ...],
        order_by_columns: Sequence[str],
    ) -> Aggregator[Aggregate[Output], Output]:
        """Prepare an aggregation of this type for computation."""
        arguments = [
            tuple(getter(peer) for getter in getters) for _, peer in possible_peers
        ]
        return cls.aggregator_class(arguments, cls)
