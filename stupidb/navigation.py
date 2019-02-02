"""Navigation and simple window function interface and implementation."""

import abc
import operator
from typing import (
    Callable,
    ClassVar,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import toolz

from stupidb.aggregatetypes import (
    BinaryAggregate,
    NullaryAggregate,
    TernaryAggregate,
    UnaryAggregate,
)
from stupidb.aggregator import Aggregator
from stupidb.reversed import Reversed
from stupidb.typehints import Input1, Input2, Input3, Output, Result


class UnaryNavigationAggregate(UnaryAggregate[Input1, Output]):
    """Navigation function taking one argument."""

    __slots__ = ("inputs1",)

    def __init__(self, inputs1: Sequence[Optional[Input1]]) -> None:
        self.inputs1 = inputs1

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1]]]
    ) -> Aggregator["UnaryNavigationAggregate", Output]:
        return NavigationAggregator(inputs, cls)

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        """Execute the aggregation over the range from `begin` to `end`."""


class BinaryNavigationAggregate(BinaryAggregate[Input1, Input2, Output]):
    """Navigation function taking two arguments."""

    __slots__ = "inputs1", "inputs2"

    def __init__(
        self,
        inputs1: Sequence[Optional[Input1]],
        inputs2: Sequence[Optional[Input2]],
    ) -> None:
        self.inputs1 = inputs1
        self.inputs2 = inputs2

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1], Optional[Input2]]]
    ) -> Aggregator["BinaryNavigationAggregate", Output]:
        return NavigationAggregator(inputs, cls)

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        ...


class TernaryNavigationAggregate(
    TernaryAggregate[Input1, Input2, Input3, Output]
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

    @classmethod
    def prepare(
        cls,
        inputs: Sequence[
            Tuple[Optional[Input1], Optional[Input2], Optional[Input3]]
        ],
    ) -> Aggregator["TernaryNavigationAggregate", Output]:
        return NavigationAggregator(inputs, cls)

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        ...


class NullaryRankingAggregate(NullaryAggregate[Output]):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[()]]
    ) -> Aggregator["NullaryRankingAggregate", Output]:
        return NavigationAggregator(inputs, cls)

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        ...


class UnaryRankingAggregate(UnaryAggregate[Input1, Output]):
    __slots__ = ()

    def __init__(self, inputs1: Sequence[Optional[Input1]]) -> None:
        self.inputs1 = inputs1

    @classmethod
    def prepare(
        cls, inputs: Sequence[Tuple[Optional[Input1]]]
    ) -> Aggregator["UnaryRankingAggregate", Output]:
        return NavigationAggregator(inputs, cls)

    @abc.abstractmethod
    def execute(self, begin: int, end: int) -> Optional[Output]:
        ...


class RowNumber(NullaryRankingAggregate[int]):
    __slots__ = ("row_number",)

    def __init__(self) -> None:
        super().__init__()
        self.row_number = 0

    def execute(self, begin: int, end: int) -> Optional[int]:
        row_number = self.row_number
        self.row_number += 1
        return row_number


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
        offset = self.__class__.offset(index, self.inputs2[index])
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


T = TypeVar("T")


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

            try:
                value = toolz.first(
                    filter(
                        None, map(self.inputs1.__getitem__, range(begin, end))
                    )
                )
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
        try:
            return self.cache[begin, end]
        except KeyError:
            current_index = self.index

            # the current position in the frame
            frame_position = begin + current_index

            if frame_position >= end:
                # if the current position is past the end of the window, return
                # None
                result = None
            else:
                # compute the offset relative to the current row
                target_index = self.inputs2[frame_position]
                ninputs = end - begin

                if (
                    target_index is not None
                    and -ninputs <= target_index < ninputs
                ):
                    result = self.inputs1[target_index]
                else:
                    # if the user asked for a row outside the frame, return
                    # None
                    result = None
            self.cache[begin, end] = result
        self.index += 1
        return result


SimpleAggregate = TypeVar(
    "SimpleAggregate",
    UnaryNavigationAggregate,
    BinaryNavigationAggregate,
    TernaryNavigationAggregate,
    NullaryRankingAggregate,
    UnaryRankingAggregate,
)


class NavigationAggregator(Aggregator[SimpleAggregate, Result]):
    """Custom aggregator for simple window functions.

    "Navigation" is slightly too specific here, this should almost certainly be
    renamed.

    This aggregator is useful for a subset of window functions whose underlying
    binary combine operator is not associative or easy to express without
    special knowledge of the underlying aggregator representation.

    See Also
    --------
    stupidb.segmenttree.associative.AssociativeAggregate

    """

    __slots__ = ("aggregate",)

    def __init__(
        self,
        inputs: Sequence[Tuple[T, ...]],
        aggregate_type: Type[SimpleAggregate],
    ) -> None:
        self.aggregate: SimpleAggregate = aggregate_type(  # type: ignore
            *zip(*inputs)
        )

    def query(self, begin: int, end: int) -> Optional[Result]:
        return self.aggregate.execute(begin, end)
