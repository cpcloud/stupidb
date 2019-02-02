"""Various type definitions used throughout stupidb."""

from numbers import Real
from typing import Callable, Hashable, Optional, Tuple, TypeVar

from stupidb.protocols import AdditiveWithInverse
from stupidb.row import AbstractRow

OrderingKey = Tuple[AdditiveWithInverse, ...]
PartitionKey = Tuple[Tuple[str, Hashable], ...]

PartitionBy = Callable[[AbstractRow], Hashable]
OrderBy = Callable[[AbstractRow], AdditiveWithInverse]
Preceding = Callable[[AbstractRow], AdditiveWithInverse]
Following = Callable[[AbstractRow], AdditiveWithInverse]

Predicate = Callable[[AbstractRow], Optional[bool]]

Projector = Callable[[AbstractRow], AbstractRow]

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Input3 = TypeVar("Input3")
Output = TypeVar("Output")

R = TypeVar("R", bound=Real)
R1 = TypeVar("R1", bound=Real)
R2 = TypeVar("R2", bound=Real)
Result = TypeVar("Result")
RealGetter = Callable[[AbstractRow], Optional[R]]
