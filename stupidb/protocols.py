import abc
from typing import Any, TypeVar

from typing_extensions import Protocol

C = TypeVar("C", bound="Comparable")


class Comparable(Protocol):
    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abc.abstractmethod
    def __lt__(self: C, other: C) -> bool:
        ...

    def __gt__(self: C, other: C) -> bool:
        return (not self < other) and self != other

    def __le__(self: C, other: C) -> bool:
        return self < other or self == other

    def __ge__(self: C, other: C) -> bool:
        return not self < other


A = TypeVar("A", bound="AdditiveWithInverse")


class AdditiveWithInverse(Comparable):
    @abc.abstractmethod
    def __add__(self: A, other: A) -> A:
        ...

    @abc.abstractmethod
    def __radd__(self: A, other: A) -> A:
        ...

    @abc.abstractmethod
    def __sub__(self: A, other: A) -> A:
        ...

    @abc.abstractmethod
    def __rsub__(self: A, other: A) -> A:
        ...

    @abc.abstractmethod
    def __neg__(self: A) -> A:
        ...

    @abc.abstractmethod
    def __pos__(self: A) -> A:
        ...
