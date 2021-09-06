"""Module containing classes for representing rows."""

from __future__ import annotations

import abc
from typing import Any, Hashable, Iterator, Mapping, Optional

import cytoolz as toolz


class AbstractRow(Mapping[str, Any], Hashable, abc.ABC):
    """The base row type of StupidDB.

    This is the base type of the objects that are received in most user facing
    APIs. They behave nearly identically to a standard :class:`typing.Mapping`,
    with the exception that they are instances of :class:`typing.Hashable` and
    values can be accessed with square-bracket syntax as well as dot notation.

    Attributes
    ----------
    pieces
        One or more mappings that make up row. One for most relations, and two
        for joins.
    _id
        The index of this row in a table. This a private field whose details
        are subject to change without notice.

    """

    __slots__ = "pieces", "_id", "_hash"

    def __init__(
        self,
        piece: Mapping[str, Any],
        *pieces: Mapping[str, Any],
        _id: int = -1,
        _hash: Optional[int] = None,
    ) -> None:
        """Construct an :class:`AbstractRow`.

        Parameters
        ----------
        piece
            A mapping from :class:`str` to :class:`typing.Any`.
        pieces
            A tuple of mappings from :class:`str` to :class:`typing.Any`.

        """
        self.pieces = piece, *pieces
        self._id = _id
        self._hash = _hash

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(
                tuple(tuple(item) for piece in self.pieces for item in piece.items())
            )
        return self._hash

    def __eq__(self, other: Any) -> bool:
        return self.data == getattr(other, "data", other)

    def __ne__(self, other: Any) -> bool:
        return not (self == other)

    @property
    @abc.abstractmethod
    def data(self) -> Mapping[str, Any]:
        """Return the underlying data for this row."""

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        """Return the number of columns in this row."""
        return len(self.data)

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError as e:
            raise AttributeError(attr) from e

    def __getitem__(self, column: str) -> Any:
        return self.data[column]

    def _renew_id(self, id: int) -> AbstractRow:
        """Reify this row with a new id `_id`.

        Parameters
        ----------
        id
            The return value's new `_id`.

        """
        return type(self)(*self.pieces, _id=id, _hash=self._hash)


class Row(AbstractRow):
    """A concrete :class:`AbstractRow` subclass for single child relations."""

    def merge(self, other: Mapping[str, Any]) -> Row:
        """Combine the :class:`typing.Mapping` `other` with this one.

        Parameters
        ----------
        other
            Any Mapping whose keys are instances of :class:`str`.

        """
        return type(self)(
            toolz.merge(self.data, getattr(other, "data", other)), _id=self._id
        )

    @property
    def data(self) -> Mapping[str, Any]:
        """Return the underlying mapping of this :class:`Row`."""
        return self.pieces[0]

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any], *, _id: int = -1) -> AbstractRow:
        """Construct a Row instance from any mapping with string keys.

        Parameters
        ----------
        mapping
            Any mapping with :class:`str` keys.
        _id
            A new row id for the returned :class:`Row` instance.

        """
        return (
            mapping._renew_id(_id)
            if isinstance(mapping, AbstractRow)
            else cls(getattr(mapping, "data", mapping), _id=_id)
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data})"


class JoinedRow(AbstractRow):
    """A concrete :class:`AbstractRow` subclass for two-child relations.

    This row type is used to represent rows of a relation with two children.
    Currently this is only used for :class:`~stupidb.stupidb.Join` relations.

    .. note::

       :class:`JoinedRow` is the row type yielded when iterating over an
       instance of :class:`~stupidb.stupidb.Join`.  If you want to consume the
       rows of a join and there are overlapping column names in the left and
       right relations you must select from the :attr:`left` and :attr:`right`
       attributes of instances of this class to disambiguate.

    Attributes
    ----------
    left
        A row from the left relation
    right
        A row from the right relation

    """

    def __init__(
        self,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
        *,
        _id: int = -1,
        _hash: Optional[None] = None,
    ) -> None:
        """Construct a :class:`JoinedRow` instance.

        Parameters
        ----------
        left
            A mapping of :class:`str` to :class:`typing.Any`.
        right
            A mapping of :class:`str` to :class:`typing.Any`.
        _id
            The row id of this row.

        """
        self.left = Row.from_mapping(left, _id=_id)
        self.right = Row.from_mapping(right, _id=_id)
        self._overlapping_keys = left.keys() & right.keys()
        self._data = toolz.merge(left, right)
        super().__init__(left, right, _id=_id, _hash=_hash)

    @property
    def data(self) -> Mapping[str, Any]:
        """Return the underlying data of the row."""
        return self._data

    @classmethod
    def from_mapping(cls, *args: Any, **kwargs: Any) -> JoinedRow:
        """Raise an error.

        A :class:`JoinedRow` cannot be constructed from an arbitrary
        :class:`typing.Mapping`.

        """
        raise TypeError(f"`from_mapping` not supported for {cls.__name__!r}")

    def __getitem__(self, key: str) -> Any:
        overlap = self._overlapping_keys
        if overlap:
            raise ValueError(
                f"Joined rows have overlapping columns: {overlap!r}. "
                "Use `row.left` or `row.right` to choose the appropriate key."
            )
        return super().__getitem__(key)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(left={self.left.data}, "
            f"right={self.right.data})"
        )
