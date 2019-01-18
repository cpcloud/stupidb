from numbers import Real
from typing import Callable, Hashable, Tuple, TypeVar

from stupidb.protocols import AdditiveWithInverse
from stupidb.row import AbstractRow

OrderingKey = Tuple[AdditiveWithInverse, ...]
PartitionKey = Tuple[Tuple[str, Hashable], ...]

PartitionBy = Callable[[AbstractRow], Hashable]
OrderBy = Callable[[AbstractRow], AdditiveWithInverse]
Preceding = Callable[[AbstractRow], AdditiveWithInverse]
Following = Callable[[AbstractRow], AdditiveWithInverse]

Predicate = Callable[[AbstractRow], bool]

Projector = Callable[[AbstractRow], AbstractRow]

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")

R = TypeVar("R", bound=Real)
R1 = TypeVar("R1", bound=Real)
R2 = TypeVar("R2", bound=Real)
RealGetter = Callable[[AbstractRow], R]
