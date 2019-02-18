"""An efficiently stored set of unsigned integers."""

import math
from typing import Any, Iterable, Iterator, MutableSet


class BitSet(MutableSet[int]):
    """A efficiently stored set of unsigned integers."""

    __slots__ = ("bitset",)

    def __init__(self) -> None:
        """Construct an empty bitset."""
        self.bitset = 0

    @classmethod
    def from_iterable(cls, elements: Iterable[int]) -> "BitSet":
        """Construct a :class:`BitSet` from an iterable of integers.

        Parameters
        ----------
        elements
            An iterable of unsigned integers.

        Raises
        ------
        ValueError
            If any element of `elements` is negative.

        """
        bitset = cls()
        for element in elements:
            bitset.add(element)
        return bitset

    def __iter__(self) -> Iterator[int]:
        """Iterate over the elements of a bitset."""
        bitset = self.bitset
        nbits = int(math.ceil(math.log2(bitset + 1)))
        return (i for i in range(nbits) if bitset & (1 << i))

    def __len__(self) -> int:
        """Compute the length of the set."""
        return bin(self.bitset).count("1")

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
