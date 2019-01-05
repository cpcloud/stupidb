import abc
from typing import Any, Iterator, Mapping, NoReturn

import toolz


class AbstractRow(Mapping[str, Any], metaclass=abc.ABCMeta):
    def __init__(self, *pieces: Mapping[str, Any], _id: int) -> None:
        self.pieces = pieces
        self._id = _id

    @abc.abstractmethod
    def __hash__(self) -> int:
        return hash(tuple(tuple(item) for item in self.data.items()))

    @property
    @abc.abstractmethod
    def data(self) -> Mapping[str, Any]:
        ...

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError as e:
            raise AttributeError(attr) from e

    def __getitem__(self, column: str) -> Any:
        return self.data[column]

    def renew_id(self, _id: int) -> "AbstractRow":
        return type(self)(*self.pieces, _id=_id)


class Row(AbstractRow):
    def merge(self, other: Mapping[str, Any]) -> "Row":
        return type(self)(
            toolz.merge(self.data, getattr(other, "data", other)), _id=self._id
        )

    def __hash__(self) -> int:
        return super().__hash__()

    @property
    def data(self) -> Mapping[str, Any]:
        return self.pieces[0]

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any], *, _id: int) -> "Row":
        return cls(getattr(mapping, "data", mapping), _id=_id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data}, _id={self._id:d})"


class JoinedRow(AbstractRow):
    def __init__(
        self, left: Mapping[str, Any], right: Mapping[str, Any], *, _id: int
    ) -> None:
        self.left = Row.from_mapping(left, _id=_id)
        self.right = Row.from_mapping(right, _id=_id)
        self._overlapping_keys = left.keys() & right.keys()
        self._data = toolz.merge(left, right)
        super().__init__(left, right, _id=_id)

    def __hash__(self) -> int:
        return hash((self.left, self.right))

    @property
    def data(self) -> Mapping[str, Any]:
        return self._data

    @classmethod
    def from_mapping(cls, *args, **kwargs) -> NoReturn:
        raise TypeError(f"from_mapping not supported for {cls.__name__!r}")

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
