from __future__ import annotations

from stupidb.protocols import Comparable


class CustomComparable(Comparable):
    def __init__(self, value: float) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CustomComparable):
            return NotImplemented
        return self.value == other.value

    def __lt__(self, other: CustomComparable) -> bool:
        return self.value < other.value


def test_comparable_implements_methods_eq() -> None:
    comp = CustomComparable(1.0)
    comp2 = CustomComparable(1.0)
    assert comp == comp2
    assert not (comp != comp2)
    assert not (comp < comp2)
    assert not (comp > comp2)
    assert comp <= comp2
    assert comp >= comp2


def test_comparable_implements_methods_ne() -> None:
    comp = CustomComparable(1.0)
    comp2 = CustomComparable(2.0)
    assert comp != comp2
    assert comp < comp2
    assert comp <= comp2
    assert not (comp == comp2)
    assert not (comp > comp2)
    assert not (comp >= comp2)
