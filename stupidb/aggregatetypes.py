"""Abstract and concrete aggregation types."""

import abc
import operator
import typing
from typing import Callable, Generic, Optional, TypeVar

from stupidb.protocols import Comparable
from stupidb.typehints import R1, R2, Input1, Input2, Input3, Output, R

UA = TypeVar("UA", bound="UnaryAggregate")


class UnaryAggregate(Generic[Input1, Output], abc.ABC):
    __slots__ = "count", "node_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        self.count = 0
        self.node_index = node_index

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: UA, other: UA) -> None:
        ...


BA = TypeVar("BA", bound="BinaryAggregate")


class BinaryAggregate(Generic[Input1, Input2, Output], abc.ABC):
    __slots__ = "count", "node_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        self.count = 0
        self.node_index = node_index

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: BA, other: BA) -> None:
        ...


TA = TypeVar("TA", bound="TernaryAggregate")


class TernaryAggregate(Generic[Input1, Input2, Input3, Output], abc.ABC):
    __slots__ = "count", "node_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        self.count = 0
        self.node_index = node_index

    @abc.abstractmethod
    def step(
        self,
        input1: Optional[Input1],
        input2: Optional[Input2],
        input3: Optional[Input3],
    ) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...

    @abc.abstractmethod
    def update(self: TA, other: TA) -> None:
        ...


Aggregate = TypeVar(
    "Aggregate", UnaryAggregate, BinaryAggregate, TernaryAggregate
)


class Count(UnaryAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def finalize(self) -> Optional[int]:
        return self.count

    def update(self, other: "Count[Input1]") -> None:
        self.count += other.count


class Sum(UnaryAggregate[R1, R2]):
    __slots__ = ("total",)

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(node_index=node_index)
        self.total = typing.cast(R2, 0)

    def __repr__(self) -> str:
        total = self.total
        count = self.count
        name = type(self).__name__
        return f"{name}(total={total}, count={count})"

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
        return f"{name}(total={total}, count={count}, mean={total / count})"


class MinMax(UnaryAggregate[Comparable, Comparable]):
    __slots__ = "current_value", "comparator"

    def __init__(
        self,
        *,
        comparator: Callable[[Comparable, Comparable], Comparable],
        node_index: Optional[int] = None,
    ) -> None:
        super().__init__(node_index=node_index)
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
            f"self.comparator == {self.comparator}, "
            f"other.comparator == {other.comparator}"
        )
        if other.current_value is not None:
            self.current_value = (
                other.current_value
                if self.current_value is None
                else self.comparator(self.current_value, other.current_value)
            )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(current_value={self.current_value})"


class Min(MinMax):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(comparator=min, node_index=node_index)


class Max(MinMax):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(comparator=max, node_index=node_index)


class Covariance(BinaryAggregate[R, R, float]):
    __slots__ = "meanx", "meany", "cov", "ddof"

    def __init__(self, *, ddof: int, node_index: Optional[int] = None) -> None:
        super().__init__(node_index=node_index)
        self.meanx = 0.0
        self.meany = 0.0
        self.cov = 0.0
        self.ddof = ddof

    def step(self, x: Optional[R], y: Optional[R]) -> None:
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

    def update(self, other: "Covariance[R]") -> None:
        raise NotImplementedError(
            "Covariance not yet implemented for segment tree"
        )


class SampleCovariance(Covariance):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(ddof=1, node_index=node_index)


class PopulationCovariance(Covariance):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(ddof=0, node_index=node_index)


class CurrentValueAggregate(UnaryAggregate[Input1, Input1]):
    __slots__ = ("current_value",)

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(node_index=node_index)
        self.current_value: Optional[Input1] = None

    def finalize(self) -> Optional[Input1]:
        return self.current_value

    def __repr__(self) -> str:
        return f"{type(self).__name__}(current_value={self.current_value})"


class FirstLast(CurrentValueAggregate[Input1]):
    __slots__ = ("comparator",)

    def __init__(
        self,
        *,
        comparator: Callable[[Comparable, Comparable], bool],
        node_index: Optional[int] = None,
    ) -> None:
        super().__init__(node_index=node_index)
        self.comparator = comparator

    def step(self, input1: Optional[Input1]) -> None:
        if self.current_value is None:
            self.current_value = input1

    def update(self, other: "FirstLast[Input1]") -> None:
        if self.current_value is None:
            self.current_value = other.current_value
            self.node_index = other.node_index
        else:
            other_index = other.node_index
            self_index = self.node_index
            assert other_index is not None
            assert self_index is not None
            assert self_index != other_index, f"{self_index} == {other_index}"
            if self.comparator(other_index, self_index):
                self.current_value = other.current_value
                self.node_index = other_index


class First(FirstLast[Input1]):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        # XXX: node indices should never be equal, see assertion in
        # FirstLast.update
        super().__init__(comparator=operator.lt, node_index=node_index)

    def step(self, input1: Optional[Input1]) -> None:
        if self.current_value is None:
            self.current_value = input1


class Last(FirstLast[Input1]):
    __slots__ = ()

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(comparator=operator.gt, node_index=node_index)

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.current_value = input1


class BinaryCurrentValueAggregate(BinaryAggregate[Input1, Input2, Input1]):
    __slots__ = ("current_value",)

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(node_index=node_index)
        self.current_value: Optional[Input1] = None

    def finalize(self) -> Optional[Input1]:
        return self.current_value


class Nth(BinaryAggregate[Input1, int, Input1]):
    __slots__ = "current_index", "target_index"

    def __init__(self, *, node_index: Optional[int] = None) -> None:
        super().__init__(node_index=node_index)
        self.current_index = 0
        self.target_index: Optional[int] = None

    def __repr__(self) -> str:
        name = type(self).__name__
        current_value = self.current_value
        current_index = self.current_index
        target_index = self.target_index
        return (
            f"{name}(current_value={current_value!r}, "
            f"current_index={current_index!r}, "
            f"target_index={target_index!r})"
        )

    def step(self, input1: Optional[Input1], index: Optional[int]) -> None:
        if index is not None and index == self.current_index:
            self.current_value = input1
        self.current_index += 1
        self.target_index = index

    def update(self, other: "Nth[Input1]") -> None:
        raise NotImplementedError(
            f"Segment tree method update not yet implemented for {type(self)}"
        )

    def finalize(self) -> Optional[Input1]:
        raise NotImplementedError(
            "Segment tree method finalize not yet implemented for "
            f"{type(self)}"
        )


class Lead(TernaryAggregate[Input1, int, Input1, Input1]):
    def step(
        self,
        input1: Optional[Input1],
        index: Optional[int],
        default: Optional[Input1],
    ) -> None:
        raise NotImplementedError(
            f"Segment tree method step not yet implemented for {type(self)}"
        )

    def update(self, other: "Lead[Input1]") -> None:
        raise NotImplementedError(
            f"Segment tree method update not yet implemented for {type(self)}"
        )

    def finalize(self) -> Optional[Input1]:
        raise NotImplementedError(
            "Segment tree method finalize not yet implemented for "
            f"{type(self)}"
        )


class Lag(TernaryAggregate[Input1, int, Input1, Input1]):
    def step(
        self,
        input1: Optional[Input1],
        index: Optional[int],
        default: Optional[Input1],
    ) -> None:
        raise NotImplementedError(
            f"Segment tree method step not yet implemented for {type(self)}"
        )

    def update(self, other: "Lag[Input1]") -> None:
        raise NotImplementedError(
            f"Segment tree method update not yet implemented for {type(self)}"
        )

    def finalize(self) -> Optional[Input1]:
        raise NotImplementedError(
            "Segment tree method finalize not yet implemented for "
            f"{type(self)}"
        )
