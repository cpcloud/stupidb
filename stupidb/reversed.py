"""Implementation of an indexable :class:`stupidb.reversed.Reversed` object."""

from typing import Any, Sequence, TypeVar

T = TypeVar("T", covariant=True)


class Reversed(Sequence[T]):
    """A sequence that iterates backwards over its input."""

    __slots__ = ("values",)

    def __init__(self, values: Sequence[T] = ()) -> None:
        self.values = values

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self)!r})"

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, index: Any) -> Any:
        # Why must this be Any -> Any?
        nvalues = len(self)
        if -nvalues <= index < nvalues:
            offset = nvalues * (index >= 0) - index - 1
            return self.values[offset]
        raise IndexError(index)
