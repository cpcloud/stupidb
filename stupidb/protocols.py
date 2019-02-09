import abc
from typing import Any, TypeVar

from typing_extensions import Protocol

C = TypeVar("C", bound="Comparable")


class Comparable(Protocol):
    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Return whether `self` equals `other`."""

    @abc.abstractmethod
    def __lt__(self: C, other: C) -> bool:
        """Return whether `self` is less than `other`."""

    def __gt__(self: C, other: C) -> bool:
        """Return whether `self` is greater than `other`."""
        return (not self < other) and self != other

    def __le__(self: C, other: C) -> bool:
        """Return whether `self` is less than or equal to `other`."""
        return self < other or self == other

    def __ge__(self: C, other: C) -> bool:
        """Return whether `self` is greater than or equal to `other`."""
        return not self < other


A = TypeVar("A", bound="AdditiveWithInverse")


class AdditiveWithInverse(Comparable):
    @abc.abstractmethod
    def __add__(self: A, other: A) -> A:
        """Add `other` to `self`."""

    @abc.abstractmethod
    def __radd__(self: A, other: A) -> A:
        """Add `self` to `other`."""

    @abc.abstractmethod
    def __sub__(self: A, other: A) -> A:
        """Subtract `other` from `self`."""

    @abc.abstractmethod
    def __rsub__(self: A, other: A) -> A:
        """Subtract `self` from `other`."""

    @abc.abstractmethod
    def __neg__(self: A) -> A:
        """Negate `self`."""

    @abc.abstractmethod
    def __pos__(self: A) -> A:
        """Return positive `self`."""
