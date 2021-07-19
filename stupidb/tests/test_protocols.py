from stupidb.protocols import Comparable


class CustomComparable(Comparable):
    def __init__(self, value: float) -> None:
        self.value = value

    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __lt__(self, other) -> bool:
        return self.value < other.value


def test_comparable_implements_methods_eq():
    comp = CustomComparable(1.0)
    comp2 = CustomComparable(1.0)
    assert comp == comp2
    assert not (comp != comp2)
    assert not (comp < comp2)
    assert not (comp > comp2)
    assert comp <= comp2
    assert comp >= comp2


def test_comparable_implements_methods_ne():
    comp = CustomComparable(1.0)
    comp2 = CustomComparable(2.0)
    assert comp != comp2
    assert comp < comp2
    assert comp <= comp2
    assert not (comp == comp2)
    assert not (comp > comp2)
    assert not (comp >= comp2)
