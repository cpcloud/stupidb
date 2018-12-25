from numbers import Real
from typing import Callable, Hashable, Iterable, Tuple, TypeVar

from stupidb.comparable import Comparable
from stupidb.row import Row

PartitionBy = Callable[[Row], Hashable]
OrderBy = Callable[[Row], Comparable]
Preceding = Callable[[Row], int]
Following = Preceding

Predicate = Callable[[Row], bool]

Projector = Callable[[Row], Row]
JoinProjector = Callable[[Row, Row], Row]

Rows = Iterable[Row]  # Rows are an Iterable of Row
InputType = TypeVar("InputType", Tuple[Row], Tuple[Row, Row])
OutputType = TypeVar("OutputType", Tuple[Row], Tuple[Row, Row])

GroupingKeyFunction = Callable[..., Hashable]

Input = TypeVar("Input")
Input1 = TypeVar("Input1")
Input2 = TypeVar("Input2")
Output = TypeVar("Output")

R = TypeVar("R", bound=Real)
RealGetter = Callable[[Row], R]
