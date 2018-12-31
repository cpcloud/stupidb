from typing import Any, Iterator, List, Mapping

import toolz


class Row(Mapping[str, Any]):
    def __init__(self, data: Mapping[str, Any], _id: int) -> None:
        # an id of -1 is never used since rows are always reconstructed with
        # their ids in the core loop. See the Relation class
        #
        # this needs to be tested
        assert not isinstance(data, type(self)), f"data is {type(self)}"
        self._data = data
        self._id = _id

    def renew_id(self, _id: int) -> "Row":
        return type(self)(self.data, _id=_id)

    def __getitem__(self, column: str) -> Any:
        return self._data[column]

    def __getattr__(self, attr: str) -> Any:
        try:
            return self._data[attr]
        except KeyError as e:
            raise AttributeError(attr) from e

    @property
    def columns(self) -> List[str]:
        return list(self.keys())

    @property
    def data(self) -> Mapping[str, Any]:
        return self._data

    def __hash__(self) -> int:
        return hash(tuple(tuple(item) for item in self._data.items()))

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any], _id: int) -> "Row":
        return cls(getattr(mapping, "data", mapping), _id)

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data}, _id={self._id:d})"


class JoinedRow(Row):
    def __init__(
        self, left: Mapping[str, Any], right: Mapping[str, Any], _id: int
    ) -> None:
        self.left = Row.from_mapping(left, _id=_id)
        self.right = Row.from_mapping(right, _id=_id)
        self._overlapping_keys = left.keys() & right.keys()
        super().__init__(toolz.merge(left, right), _id)

    def __getitem__(self, key: str) -> Any:
        if self._overlapping_keys:
            raise ValueError(
                "Joined rows have overlapping. Use .left or .right to choose "
                "the appropriate key"
            )
        return super().__getitem__(key)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.left.data}, "
            f"{self.right.data}, _id={self._id:d})"
        )

    def renew_id(self, _id: int) -> "JoinedRow":
        return type(self)(self.left, self.right, _id=_id)
