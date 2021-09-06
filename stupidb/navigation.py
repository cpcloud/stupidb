"""Navigation and simple window function interface and implementation."""

import abc
import operator
from typing import (
    Callable,
    ClassVar,
    Generic,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from .aggregatetypes import Aggregate
from .aggregator import Aggregator
from .reversed import Reversed
from .typehints import Input1, Input2, Input3, Output, Result, T


class NavigationAggregator(Aggregator["NavigationAggregate", Result]):
    """Custom aggregator for navigation functions.

    This aggregator is useful for a subset of window functions whose underlying
    binary combine operator (if it even exists) is not associative or easy to
    express without special knowledge of the underlying aggregator
    representation.

    See Also
    --------
    stupidb.ranking.RankingAggregator

    """

    __slots__ = ("aggregate",)

    def __init__(
        self,
        inputs: Sequence[Tuple[T, ...]],
        aggregate_type: Type["NavigationAggregate"],
    ) -> None:
        self.aggregate: "NavigationAggregate" = aggregate_type(  # type: ignore
            *zip(*inputs)
        )

    def query(self, begin: int, end: int) -> Optional[Result]:
        return self.aggregate.execute(begin, end)


class NavigationAggregate(Aggregate[Output]):
    """Base class for navigation aggregate functions."""

    __slots__ = ()
    aggregator_class: ClassVar[
        Callable[..., NavigationAggregator]
    ] = NavigationAggregator

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        """Execute the aggregation over the range from `begin` to `end`."""


class UnaryNavigationAggregate(Generic[Input1, Output], NavigationAggregate[Output]):
    """Navigation function taking one argument."""

    __slots__ = ("inputs1",)

    def __init__(self, inputs1: Sequence[Optional[Input1]]) -> None:
        self.inputs1 = inputs1


class BinaryNavigationAggregate(
    Generic[Input1, Input2, Output], NavigationAggregate[Output]
):
    """Navigation function taking two arguments."""

    __slots__ = "inputs1", "inputs2"

    def __init__(
        self,
        inputs1: Sequence[Optional[Input1]],
        inputs2: Sequence[Optional[Input2]],
    ) -> None:
        self.inputs1 = inputs1
        self.inputs2 = inputs2


class TernaryNavigationAggregate(
    Generic[Input1, Input2, Input3, Output], NavigationAggregate[Output]
):
    """Navigation function taking three arguments."""

    __slots__ = "inputs1", "inputs2", "inputs3"

    def __init__(
        self,
        inputs1: Sequence[Optional[Input1]],
        inputs2: Sequence[Optional[Input2]],
        inputs3: Sequence[Optional[Input3]],
    ) -> None:
        self.inputs1 = inputs1
        self.inputs2 = inputs2
        self.inputs3 = inputs3


class LeadLag(TernaryNavigationAggregate[Input1, int, Input1, Input1]):
    """Base class for shifting operations.

    This class tracks the index of the current row that is being computed.

    """

    __slots__ = "index", "ninputs"
    offset_operation: ClassVar[Callable[[int, int], int]]

    @classmethod
    def offset(cls, index: int, offset: Optional[int]) -> int:
        return -1 if offset is None else cls.offset_operation(index, offset)

    def __init__(
        self,
        inputs: Sequence[Optional[Input1]],
        offsets: Sequence[Optional[int]],
        defaults: Sequence[Optional[Input1]],
    ) -> None:
        super().__init__(inputs, offsets, defaults)
        self.index = 0
        self.ninputs = len(inputs)

    def execute(self, begin: int, end: int) -> Optional[Input1]:
        """Compute the value of the navigation function `lead` or `lag`.

        Notes
        -----
        `begin` and `end` are ignored in lead/lag, by definition.

        """
        index = self.index
        offset = self.offset(index, self.inputs2[index])
        default = self.inputs3[index]

        # if we asked for a null offset or we're out of bounds then return a
        # null
        if offset < 0 or offset >= self.ninputs:
            result: Optional[Input1] = default if default is not None else None
        else:
            result = self.inputs1[offset]

        self.index += 1
        return result


class Lead(LeadLag[Input1]):
    __slots__ = ()
    offset_operation = operator.add


class Lag(LeadLag[Input1]):
    __slots__ = ()
    offset_operation = operator.sub


class FirstLast(UnaryNavigationAggregate[Input1, Input1]):
    """Base class for first and last navigation functions.

    The difference between first and last is where the search for non NULL
    values starts.

    This aggregation keeps a cache of computed aggregations keyed by the begin
    and end of the range it's been queried over.

    """

    __slots__ = ("cache",)

    def __init__(self, inputs1: Sequence[Optional[Input1]]) -> None:
        super().__init__(inputs1)
        self.cache: MutableMapping[Tuple[int, int], Optional[Input1]] = {}

    def execute(self, begin: int, end: int) -> Optional[Input1]:
        try:
            return self.cache[begin, end]
        except KeyError:
            inputs = (self.inputs1[i] for i in range(begin, end))
            try:
                value: Optional[Input1] = next(arg for arg in inputs if arg is not None)
            except StopIteration:
                value = None
            self.cache[begin, end] = value
            return value


class First(FirstLast[Input1]):
    __slots__ = ()


class Last(FirstLast[Input1]):
    __slots__ = ()

    def __init__(self, inputs1: Sequence[Optional[Input1]]) -> None:
        super().__init__(Reversed(inputs1))


class Nth(BinaryNavigationAggregate[Input1, int, Input1]):
    """Compute the nth row in a window frame."""

    __slots__ = "index", "cache"

    def __init__(
        self,
        inputs1: Sequence[Optional[Input1]],
        inputs2: Sequence[Optional[int]],
    ) -> None:
        super().__init__(inputs1, inputs2)
        self.index = 0
        self.cache: MutableMapping[Tuple[int, int], Optional[Input1]] = {}

    def execute(self, begin: int, end: int) -> Optional[Input1]:
        # Assert invariants:
        # 1. The start of the range must be less than or equal to the end,
        #    which must be less than or equal to the number of input rows
        # 2. The current index must be between the begin and end of the queried
        #    range.
        assert 0 <= begin <= end <= len(self.inputs1)
        assert begin <= self.index <= end
        try:
            return self.cache[begin, end]
        except KeyError:
            # the current position in the frame
            frame_position = begin + self.index

            assert (
                frame_position <= end
            ), f"frame_position == {frame_position} :: end == {end}"

            # compute the offset relative to the current row
            offsets = self.inputs2
            target_index = offsets[frame_position]
            ninputs = end - begin

            data = self.inputs1
            if target_index is not None and -ninputs <= target_index < ninputs:
                result = data[target_index]
            else:
                # if the user asked for a row outside the frame, return
                # None
                result = None
            self.cache[begin, end] = result
        self.index += 1
        return result
