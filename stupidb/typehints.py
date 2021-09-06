"""Various type definitions used throughout stupidb."""

from numbers import Real
from typing import Any, Callable, Hashable, Optional, Tuple, TypeVar

from .protocols import AdditiveWithInverse, Comparable
from .row import AbstractRow

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Input3 = TypeVar("Input3")
Output = TypeVar("Output")

T = TypeVar("T")
R = TypeVar("R", bound=Real)
R1 = TypeVar("R1", bound=Real)
R2 = TypeVar("R2", bound=Real)

OrderingKey = Tuple[Comparable[T], ...]
PartitionKey = Tuple[Tuple[str, Hashable], ...]

PartitionBy = Callable[[AbstractRow], Hashable]
OrderBy = Callable[[AbstractRow], Comparable[T]]
Preceding = Callable[[AbstractRow], AdditiveWithInverse[T]]
Following = Callable[[AbstractRow], AdditiveWithInverse[T]]

JoinPredicate = Callable[[AbstractRow, AbstractRow], Optional[bool]]
Predicate = Callable[[AbstractRow], Optional[bool]]

Projector = Callable[[AbstractRow], AbstractRow]
Result = TypeVar("Result")
Getter = Callable[[AbstractRow], Any]
