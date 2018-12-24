from typing import Iterator, Mapping, TypeVar

V = TypeVar("V")


class Row(Mapping[str, V]):
    def __init__(self, data: Mapping[str, V], _id: int = -1) -> None:
        # an id of -1 is never used since rows are always reconstructed with
        # their ids in the core loop. See the Relation class
        #
        # this needs to be tested
        assert not isinstance(data, type(self)), f"data is {type(self)}"
        self._data = data
        self._id = _id

    def __getitem__(self, column: str) -> V:
        return self.data[column]

    @property
    def id(self) -> int:
        return self._id

    @property
    def data(self) -> Mapping[str, V]:
        return self._data

    def __hash__(self) -> int:
        return hash(tuple(tuple(item) for item in self.data.items()))

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, V], _id: int) -> "Row":
        return cls(getattr(mapping, "data", mapping), _id)

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data}, _id={self.id:d})"
