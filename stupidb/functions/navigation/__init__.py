"""Navigation and simple window function interface and implementation."""

from __future__ import annotations

import operator
from typing import Callable, ClassVar, MutableMapping, Sequence

from ...typehints import Input
from .core import (
    BinaryNavigationAggregate,
    TernaryNavigationAggregate,
    UnaryNavigationAggregate,
)


class LeadLag(TernaryNavigationAggregate[Input, int, Input, Input]):
    """Base class for shifting operations.

    This class tracks the index of the current row that is being computed.

    """

    __slots__ = "index", "ninputs"
    offset_operation: ClassVar[Callable[[int, int], int]]

    @classmethod
    def offset(cls, index: int, offset: int | None) -> int:
        return -1 if offset is None else cls.offset_operation(index, offset)

    def __init__(
        self,
        inputs: Sequence[Input | None],
        offsets: Sequence[int | None],
        defaults: Sequence[Input | None],
    ) -> None:
        super().__init__(inputs, offsets, defaults)
        self.index = 0
        self.ninputs = len(inputs)

    def execute(self, begin: int, end: int) -> Input | None:
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
            result = default if default is not None else None
        else:
            result = self.inputs1[offset]

        self.index += 1
        return result


class Lead(LeadLag[Input]):
    __slots__ = ()
    offset_operation = operator.add


class Lag(LeadLag[Input]):
    __slots__ = ()
    offset_operation = operator.sub


class FirstLast(UnaryNavigationAggregate[Input, Input]):
    """Base class for first and last navigation functions.

    The difference between first and last is where the search for non NULL
    values starts.

    This aggregation keeps a cache of computed aggregations keyed by the begin
    and end of the range it's been queried over.

    """

    __slots__ = ("cache",)

    def __init__(self, inputs1: Sequence[Input | None]) -> None:
        super().__init__(inputs1)
        self.cache: MutableMapping[tuple[int, int], Input | None] = {}

    def execute(self, begin: int, end: int) -> Input | None:
        try:
            return self.cache[begin, end]
        except KeyError:
            inputs = (self.inputs1[i] for i in range(begin, end))
            value = self.cache[begin, end] = next(
                (arg for arg in inputs if arg is not None),
                None,
            )
            return value


class First(FirstLast[Input]):
    __slots__ = ()


class Last(FirstLast[Input]):
    __slots__ = ()

    def __init__(self, inputs1: Sequence[Input | None]) -> None:
        super().__init__(inputs1[::-1])


class Nth(BinaryNavigationAggregate[Input, int, Input]):
    """Compute the nth row in a window frame."""

    __slots__ = "index", "cache"

    def __init__(
        self,
        inputs1: Sequence[Input | None],
        inputs2: Sequence[int | None],
    ) -> None:
        super().__init__(inputs1, inputs2)
        self.index = 0
        self.cache: MutableMapping[tuple[int, int], Input | None] = {}

    def execute(self, begin: int, end: int) -> Input | None:
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
