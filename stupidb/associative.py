"""Abstract and concrete aggregation types."""

import abc
import math
import typing
from typing import Callable, Optional, Sequence, Tuple, TypeVar

from stupidb.aggregatetypes import BA, UA, BinaryAggregate, UnaryAggregate
from stupidb.aggregator import Aggregator
from stupidb.protocols import Comparable
from stupidb.typehints import R1, R2, Input1, Input2, Output, R


class UnaryAssociativeAggregate(UnaryAggregate[Input1, Output]):
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        """Compute the value of the aggregation from its current state."""

    @abc.abstractmethod
    def update(self: UA, other: UA) -> None:
        """Update this aggregation based on another of the same type."""

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1]]]
    ) -> Aggregator["UnaryAssociativeAggregate", Output]:
        from stupidb.segmenttree import SegmentTree

        return SegmentTree(inputs, cls)


class BinaryAssociativeAggregate(BinaryAggregate[Input1, Input2, Output]):
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        """Perform a single step of the aggregation."""

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        """Compute the value of the aggregation from its current state."""

    @abc.abstractmethod
    def update(self: BA, other: BA) -> None:
        """Update this aggregation based on another of the same type."""

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1], Optional[Input2]]]
    ) -> Aggregator["BinaryAssociativeAggregate", Output]:
        from stupidb.segmenttree import SegmentTree

        return SegmentTree(inputs, cls)


AssociativeAggregate = TypeVar(
    "AssociativeAggregate",
    UnaryAssociativeAggregate,
    BinaryAssociativeAggregate,
)


class Count(UnaryAssociativeAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def __repr__(self) -> str:
        return f"{type(self).__name__}(count={self.count!r})"

    def finalize(self) -> Optional[int]:
        return self.count

    def update(self, other: "Count[Input1]") -> None:
        self.count += other.count


class Sum(UnaryAssociativeAggregate[R1, R2]):
    __slots__ = ("total",)

    def __init__(self) -> None:
        super().__init__()
        self.total = typing.cast(R2, 0)

    def __repr__(self) -> str:
        total = self.total
        count = self.count
        name = type(self).__name__
        return f"{name}(total={total!r}, count={count!r})"

    def step(self, input1: Optional[R1]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else None

    def update(self, other: "Sum[R1, R2]") -> None:
        self.total += other.total
        self.count += other.count


class Total(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else typing.cast(R2, 0)


class Mean(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        count = self.count
        return self.total / count if count > 0 else None

    def __repr__(self) -> str:
        name = type(self).__name__
        total = self.total
        count = self.count
        mean = self.finalize()
        return f"{name}(total={total!r}, count={count!r}, mean={mean!r})"


class MinMax(UnaryAssociativeAggregate[Comparable, Comparable]):
    __slots__ = "current_value", "comparator"

    def __init__(
        self, *, comparator: Callable[[Comparable, Comparable], Comparable]
    ) -> None:
        super().__init__()
        self.current_value: Optional[Comparable] = None
        self.comparator = comparator

    def step(self, input1: Optional[Comparable]) -> None:
        if input1 is not None:
            if self.current_value is None:
                self.current_value = input1
            else:
                self.current_value = self.comparator(
                    self.current_value, input1
                )

    def finalize(self) -> Optional[Comparable]:
        return self.current_value

    def update(self, other: "MinMax") -> None:
        assert self.comparator == other.comparator, (
            f"self.comparator == {self.comparator!r}, "
            f"other.comparator == {other.comparator!r}"
        )
        if other.current_value is not None:
            self.current_value = (
                other.current_value
                if self.current_value is None
                else self.comparator(self.current_value, other.current_value)
            )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(current_value={self.current_value!r})"


class Min(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=min)


class Max(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(comparator=max)


class Covariance(BinaryAssociativeAggregate[R1, R2, float]):
    __slots__ = "meanx", "meany", "cov", "ddof"

    def __init__(self, *, ddof: int) -> None:
        super().__init__()
        self.meanx = 0.0
        self.meany = 0.0
        self.cov = 0.0
        self.ddof = ddof

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f"{name}(meanx={self.meanx!r}, meany={self.meany!r}, "
            f"cov={self.cov!r}, count={self.count!r})"
        )

    def step(self, x: Optional[R1], y: Optional[R2]) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.meanx
            self.meanx += delta_x + count
            self.meany += (y - self.meany) / count
            self.cov += delta_x * (y - self.meany)

    def finalize(self) -> Optional[float]:
        denom = self.count - self.ddof
        return self.cov / denom if denom > 0 else None

    def update(self, other: "Covariance[R1, R2]") -> None:
        new_count = self.count + other.count
        self.cov += (
            other.cov
            + (self.meanx - other.meanx)
            * (self.meany - other.meany)
            * (self.count * other.count)
            / new_count
        )
        self.meanx = (
            self.count * self.meanx + other.count * other.meanx
        ) / new_count
        self.meany = (
            self.count * self.meany + other.count * other.meany
        ) / new_count
        self.count = new_count


class SampleCovariance(Covariance[R1, R2]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance[R1, R2]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class Variance(UnaryAssociativeAggregate[R, float]):
    __slots__ = ("aggregator",)

    def __init__(self, ddof: int) -> None:
        self.aggregator: Covariance[R, R] = Covariance(ddof=ddof)

    def step(self, x: Optional[R]) -> None:
        self.aggregator.step(x, x)

    def finalize(self) -> Optional[float]:
        return self.aggregator.finalize()

    def update(self, other: "Variance[R]") -> None:
        self.aggregator.update(other.aggregator)


class SampleVariance(Variance[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationVariance(Variance[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class StandardDeviation(Variance[R]):
    __slots__ = ("aggregator",)

    def finalize(self) -> Optional[float]:
        variance = super().finalize()
        return math.sqrt(variance) if variance is not None else None


class SampleStandardDeviation(StandardDeviation[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationStandardDeviation(StandardDeviation[R]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)
