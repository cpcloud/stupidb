"""Schema and data type classes."""

import functools
from typing import AbstractSet, Any, Mapping, Sequence, Tuple, ValuesView

import attr
from multipledispatch import Dispatcher

_datatype = attr.s(frozen=True, slots=True)
_schema = attr.s(frozen=True, slots=True, auto_attribs=True)


@_datatype
class DataType:
    """Base data type class."""


@_datatype
class Numeric(DataType):
    """Data type representing an abstract numeric value."""


@_datatype
class Int64(Numeric):
    """Data type representing a sixty-four bit integer."""


@_datatype
class Float64(Numeric):
    """Data type representing a double precision floating point value."""


@_datatype
class Bool(DataType):
    """Data type representing a bool."""


@_datatype
class String(DataType):
    """Data type representing a string."""


@_schema
class Schema:
    """A relation's schema."""

    names: Sequence[str]
    types: Sequence[DataType]
    _mapping: Mapping[str, DataType] = attr.ib(
        init=False, repr=False, hash=False
    )

    @classmethod
    def from_pairs(cls, pairs: Sequence[Tuple[str, DataType]]) -> "Schema":
        """Construct a schema from pairs."""
        return cls(*zip(*pairs))

    def __attrs_post_init__(self) -> None:
        object.__setattr__(self, "_mapping", dict(zip(self.names, self.types)))

    def __getitem__(self, key: str) -> DataType:
        return self._mapping[key]

    def keys(self) -> AbstractSet[str]:
        """Return the names of the fields in the schema."""
        return self._mapping.keys()

    def values(self) -> ValuesView[DataType]:
        """Return the types of the field in the schema."""
        return self._mapping.values()

    def items(self) -> AbstractSet[Tuple[str, DataType]]:
        """Return an iterable of pairs of name and type."""
        return self._mapping.items()


typeof = Dispatcher("typeof")


@functools.singledispatch
def typeof(value: Any) -> DataType:
    """Raise an exception because we don't know the type of `value`."""
    raise NotImplementedError(f"typeof({value}) is undefined")


@typeof.register
def typeof_int(_: int) -> Int64:
    """Return the type of an integer literal."""
    return Int64()


@typeof.register
def typeof_float(_: float) -> Float64:
    """Return the type of a float literal."""
    return Float64()


@typeof.register
def typeof_str(_: str) -> String:
    """Return the type of a string literal."""
    return String()
