"""Various stupidb related protocol classes."""

import abc
from typing import Any, TypeVar

from typing_extensions import Protocol

B = TypeVar("B", contravariant=True)


class Comparable(Protocol[B]):
    """A protocol for comparable objects."""

    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Return whether `self` equals `other`."""

    def __ne__(self, other: Any) -> bool:
        """Return whether `self` does not equal `other`."""
        return not (self == other)

    @abc.abstractmethod
    def __lt__(self, other: B) -> bool:
        """Return whether `self` is less than `other`."""

    def __gt__(self, other: B) -> bool:
        """Return whether `self` is greater than `other`."""
        return (not self < other) and self != other

    def __le__(self, other: B) -> bool:
        """Return whether `self` is less than or equal to `other`."""
        return self < other or self == other

    def __ge__(self, other: B) -> bool:
        """Return whether `self` is greater than or equal to `other`."""
        return not self < other


A = TypeVar("A")


class AdditiveWithInverse(Protocol[A]):
    """A protocol for objects that are additive with an inverse."""

    @abc.abstractmethod
    def __add__(self, other: A) -> A:
        """Add `other` to `self`."""

    @abc.abstractmethod
    def __radd__(self, other: A) -> A:
        """Add `self` to `other`."""

    @abc.abstractmethod
    def __sub__(self, other: A) -> A:
        """Subtract `other` from `self`."""

    @abc.abstractmethod
    def __rsub__(self, other: A) -> A:
        """Subtract `self` from `other`."""

    @abc.abstractmethod
    def __neg__(self) -> A:
        """Negate `self`."""

    @abc.abstractmethod
    def __pos__(self) -> A:
        """Return positive `self`."""
