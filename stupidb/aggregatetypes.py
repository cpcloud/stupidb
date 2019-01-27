"""Abstract and concrete aggregation types."""

import abc
from typing import Generic, Optional, Sequence, Tuple, TypeVar

from stupidb.aggregator import Aggregator
from stupidb.typehints import Input1, Input2, Input3, Output


class NullaryAggregate(Generic[Output], abc.ABC):
    """An aggregate or window function with zero arguments."""

    __slots__ = ()

    @classmethod
    @abc.abstractmethod
    def prepare(
        cls, inputs: Sequence[Tuple[()]]
    ) -> Aggregator["NullaryAggregate", Output]:
        ...


NA = TypeVar("NA", bound=NullaryAggregate)


class UnaryAggregate(Generic[Input1, Output], abc.ABC):
    """An aggregate or window function with one argument."""

    __slots__ = ()

    @classmethod
    @abc.abstractmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1]]]
    ) -> Aggregator["UnaryAggregate", Output]:
        ...


UA = TypeVar("UA", bound=UnaryAggregate)


class BinaryAggregate(Generic[Input1, Input2, Output], abc.ABC):
    """An aggregate or window function with two arguments."""

    __slots__ = ()

    @classmethod
    @abc.abstractmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1], Optional[Input2]]]
    ) -> Aggregator["BinaryAggregate", Output]:
        ...


BA = TypeVar("BA", bound=BinaryAggregate)


class TernaryAggregate(Generic[Input1, Input2, Input3, Output], abc.ABC):
    """An aggregate or window function with three arguments."""

    __slots__ = ()

    @classmethod
    @abc.abstractmethod
    def prepare(
        cls,
        inputs: Sequence[
            Tuple[Optional[Input1], Optional[Input2], Optional[Input3]]
        ],
    ) -> Aggregator["TernaryAggregate", Output]:
        ...


TA = TypeVar("TA", bound=TernaryAggregate)