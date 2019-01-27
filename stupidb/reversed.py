from typing import Sequence, TypeVar

T = TypeVar("T", covariant=True)


class Reversed(Sequence[T]):
    def __init__(self, values: Sequence[T] = ()) -> None:
        self.values = values
        self.nvalues = len(values)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self)!r})"

    def __len__(self) -> int:
        return self.nvalues

    def __getitem__(self, index):
        nvalues = self.nvalues
        if -nvalues <= index < nvalues:
            offset = nvalues * (index >= 0) - index - 1
            return self.values[offset]
        raise IndexError()
