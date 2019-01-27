from typing import Sequence, TypeVar

T = TypeVar("T", covariant=True)


class Reversed(Sequence[T]):
    def __init__(self, values: Sequence[T] = ()) -> None:
        self.values = values

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self)!r})"

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, index):
        nvalues = len(self)
        if -nvalues <= index < nvalues:
            offset = nvalues * (index >= 0) - index - 1
            return self.values[offset]
        raise IndexError()
