from typing import Any, Iterator, Mapping, TypeVar

V = TypeVar("V")


class Row(Mapping[str, V]):
    def __init__(self, data: Mapping[str, V], id: int) -> None:
        assert not isinstance(data, type(self))
        self.data = data
        self.id = id

    def __getitem__(self, column: str) -> Any:
        return self.data[column]

    def __hash__(self) -> int:
        return hash(tuple(tuple(item) for item in self.data.items()))

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, V], id: int) -> "Row":
        return cls(getattr(mapping, "data", mapping), id)

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return f"Row({self.data}, id={self.id:d})"
