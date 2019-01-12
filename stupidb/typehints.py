from numbers import Real
from typing import Callable, Hashable, Tuple, TypeVar

from stupidb.protocols import AdditiveWithInverse
from stupidb.row import Row

PartitionKey = Tuple[Tuple[str, Hashable], ...]

PartitionBy = Callable[[Row], Hashable]
OrderBy = Callable[[Row], AdditiveWithInverse]
Preceding = Callable[[Row], AdditiveWithInverse]
Following = Callable[[Row], AdditiveWithInverse]

Predicate = Callable[[Row], bool]

Projector = Callable[[Row], Row]

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")

R = TypeVar("R", bound=Real)
RealGetter = Callable[[Row], R]
