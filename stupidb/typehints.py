from numbers import Real
from typing import Callable, Hashable, Optional, Tuple, TypeVar

from stupidb.comparable import Comparable
from stupidb.row import Row

PartitionKey = Tuple[Tuple[str, Hashable], ...]

PartitionBy = Callable[[Row], Hashable]
OrderBy = Callable[[Row], Comparable]
Preceding = Optional[Callable[[Row], int]]
Following = Preceding

Predicate = Callable[[Row], bool]

Projector = Callable[[Row], Row]

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")

R = TypeVar("R", bound=Real)
RealGetter = Callable[[Row], R]
