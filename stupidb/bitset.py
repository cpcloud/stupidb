"""Abstraction for efficiently testing set membership of unsigned integers."""


class BitSet:
    __slots__ = ("bitset",)

    def __init__(self) -> None:
        self.bitset = 0

    def __contains__(self, element: int) -> int:
        return self.bitset & (1 << element) != 0

    def add(self, element: int) -> None:
        assert (
            element >= 0
        ), f"element not greater than or equal to 0, element == {element}"
        self.bitset |= 1 << element
