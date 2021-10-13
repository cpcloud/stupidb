"""An efficiently stored set of unsigned integers."""

from typing import Any, Iterable, Iterator, MutableSet


class BitSet(MutableSet[int]):
    """A efficiently stored set of unsigned integers."""

    __slots__ = "bits", "len"

    def __init__(self, bits: Iterable[int] = ()) -> None:
        """Construct a bitset."""
        self.bits = 0
        self.len = 0

        for bit in bits:
            self.add(bit)

    def __contains__(self, bit: Any) -> bool:
        """Check whether `bit` is in the set."""
        return (self.bits & (1 << bit)) != 0

    def __iter__(self) -> Iterator[int]:
        """Iterate over the elements of the set."""
        return filter(self.__contains__, range(self.bits.bit_length()))

    def __len__(self) -> int:
        """Return the number of set bits."""
        return self.len

    def __repr__(self) -> str:
        """Return the string representation of a bitset."""
        values = str(set(self)) if self else ""
        return f"{self.__class__.__name__}({values})"

    def add(self, bit: int) -> None:
        """Add `bit` to the set.

        Raises
        ------
        ValueError
            If `bit` is negative

        """
        if bit < 0:
            raise ValueError(f"bit not greater than or equal to 0, bit == {bit}")
        self.len += bit not in self
        self.bits |= 1 << bit

    def discard(self, bit: int) -> None:
        """Remove `bit` from the set.

        Raises
        ------
        ValueError
            If `bit` is negative

        """
        if bit < 0:
            raise ValueError(f"bit not greater than or equal to 0, bit == {bit}")
        self.len -= bit in self
        self.bits &= ~(1 << bit)
