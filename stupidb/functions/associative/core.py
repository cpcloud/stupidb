from __future__ import annotations

import abc
from typing import Generic, Sequence, TypeVar

from ...aggregator import Aggregate, Aggregator
from ...typehints import Input1, Input2, Output, T

UA = TypeVar("UA", bound="UnaryAssociativeAggregate")
BA = TypeVar("BA", bound="BinaryAssociativeAggregate")


class AbstractAssociativeAggregate(Aggregate[Output]):
    """Base class for aggregations with an associative binary operation."""

    __slots__ = ("count",)

    def __init__(self) -> None:
        """Construct an abstract associative aggregate."""
        self.count = 0

    @abc.abstractmethod
    def finalize(self) -> Output | None:
        """Compute the value of the aggregation from its current state."""

    @classmethod
    def aggregator_class(
        cls, values: Sequence[tuple[T | None, ...]]
    ) -> Aggregator[Aggregate[Output], Output]:
        from ...associative.segmenttree import SegmentTree

        return SegmentTree(values, cls, fanout=4)


class UnaryAssociativeAggregate(
    AbstractAssociativeAggregate[Output],
    Generic[Input1, Output],
):
    """A an abstract associative aggregate that takes one argument."""

    __slots__ = ()

    @abc.abstractmethod
    def step(self, input1: Input1 | None) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def combine(self: UA, other: UA) -> None:
        """Combine two UnaryAssociativeAggregate instances."""

    @abc.abstractmethod
    def finalize(self) -> Output | None:
        """Compute the value of the aggregation from its current state."""


class BinaryAssociativeAggregate(
    AbstractAssociativeAggregate[Output],
    Generic[Input1, Input2, Output],
):
    """A an abstract associative aggregate that takes two arguments."""

    __slots__ = ()

    @abc.abstractmethod
    def step(self, input1: Input1 | None, input2: Input2 | None) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def combine(self: BA, other: BA) -> None:
        """Combine two BinaryAssociativeAggregate instances."""

    @abc.abstractmethod
    def finalize(self) -> Output | None:
        """Compute the value of the aggregation from its current state."""


AssociativeAggregate = TypeVar(
    "AssociativeAggregate",
    UnaryAssociativeAggregate,
    BinaryAssociativeAggregate,
)
