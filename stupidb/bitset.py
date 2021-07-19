"""An efficiently stored set of unsigned integers."""

import math
from typing import AbstractSet, Any, Iterable, Iterator, MutableSet


class BitSet(MutableSet[int]):
    """A efficiently stored set of unsigned integers."""

    __slots__ = ("bitset",)

    def __init__(self, elements: Iterable[int] = ()) -> None:
        """Construct a bitset."""
        self.bitset = 0
        for element in elements:
            self.add(element)

    def intersection(self, other: Iterable[int]) -> AbstractSet[int]:
        """Return the intersection of `self` and `other`."""
        return self & BitSet(other)

    def intersection_update(self: AbstractSet[int], other: Iterable[int]) -> None:
        """Update `self` to be its intersection with `other`."""
        self &= BitSet(other)

    def union(self, other: Iterable[int]) -> AbstractSet[int]:
        """Return the union of `self` and `other`."""
        return self | BitSet(other)

    def update(self: AbstractSet[int], other: Iterable[int]) -> None:
        """Add the elements of `other` to `self`."""
        self |= BitSet(other)

    def difference(self, other: Iterable[int]) -> AbstractSet[int]:
        """Return the set difference of `self` and `other`."""
        return self - BitSet(other)

    def difference_update(self: AbstractSet[int], other: Iterable[int]) -> None:
        """Update `self` to be its set difference with `other`."""
        self -= BitSet(other)

    def symmetric_difference(self, other: Iterable[int]) -> AbstractSet[int]:
        """Return the symmetric difference of `self` and `other`."""
        return self ^ BitSet(other)

    def symmetric_difference_update(
        self: AbstractSet[int], other: Iterable[int]
    ) -> None:
        """Update `self` to be its symmetric difference with `other`."""
        self ^= BitSet(other)

    def __iter__(self) -> Iterator[int]:
        """Iterate over the elements of a bitset."""
        bitset = self.bitset
        nbits = int(math.ceil(math.log2(bitset + 1)))
        return (i for i in range(nbits) if bitset & (1 << i))

    def __len__(self) -> int:
        """Compute the length of the set."""
        bitcount = 0
        value = self.bitset
        while value:
            chunk = value & 0xFFFFFFFF
            chunk -= (chunk >> 1) & 0x55555555
            chunk = (chunk & 0x33333333) + ((chunk >> 2) & 0x33333333)
            bitcount += (
                ((chunk + (chunk >> 4) & 0xF0F0F0F) * 0x1010101) & 0xFFFFFFFF
            ) >> 24
            value >>= 32
        return bitcount

    def __repr__(self) -> str:
        """Return the string representation of a bitset."""
        return f"{self.__class__.__name__}({set(self)})"

    def __contains__(self, element: Any) -> bool:
        """Check whether `element` is in the set."""
        return self.bitset & (1 << element) != 0

    def add(self, element: int) -> None:
        """Add `element` to the set.

        Parameters
        ----------
        element
            An unsigned integer

        Raises
        ------
        ValueError
            If `element` is negative

        """
        if element < 0:
            raise ValueError(
                f"element not greater than or equal to 0, element == {element}"
            )
        self.bitset |= 1 << element

    def discard(self, element: int) -> None:
        """Remove `element` from the set.

        Parameters
        ----------
        element
            An unsigned integer

        Raises
        ------
        ValueError
            If `element` is negative

        """
        if element < 0:
            raise ValueError(
                f"element not greater than or equal to 0, element == {element}"
            )
        self.bitset ^= 1 << element
