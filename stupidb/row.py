from typing import Any, Iterator, Mapping, TypeVar

V = TypeVar("V")


class Row(Mapping[str, V]):
    def __init__(self, data: Mapping[str, V], id: int = -1) -> None:
        # an id of -1 is never used since rows are always reconstructed with
        # their ids in the core loop. See the Relation class
        #
        # this needs to be tested
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
