from typing import Callable, Hashable

from stupidb.comparable import Comparable
from stupidb.row import Row

PartitionBy = Callable[[Row], Hashable]
OrderBy = Callable[[Row], Comparable]
Preceding = Callable[[Row], int]
Following = Preceding

Predicate = Callable[[Row], bool]

Projector = Callable[[Row], Row]
JoinProjector = Callable[[Row, Row], Row]
