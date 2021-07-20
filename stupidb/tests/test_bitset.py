import pytest

from stupidb.bitset import BitSet


def test_construction():
    bs = BitSet()
    assert not bs
    assert len(bs) == 0

    bs = BitSet({1, 2})
    assert len(bs) == 2
    assert list(bs) == [1, 2]

    with pytest.raises(ValueError):
        BitSet({2, -1})


def test_repr():
    bs = BitSet()
    assert repr(bs) == "BitSet()"

    bs = BitSet([1, 2])
    assert repr(bs) == "BitSet({1, 2})"


def test_add():
    bs = BitSet()
    assert 0 not in bs

    bs.add(1)
    assert 1 in bs
    assert 2 not in bs

    bs.add(2)
    assert 1 in bs
    assert 2 in bs
    assert 0 not in bs

    with pytest.raises(ValueError):
        bs.add(-42)


def test_remove():
    bs = BitSet()
    assert 0 not in bs

    bs.add(1)
    assert 1 in bs
    assert 2 not in bs

    bs.add(2)
    assert 1 in bs
    assert 2 in bs
    assert 0 not in bs

    bs.remove(2)
    assert 1 in bs
    assert 2 not in bs

    with pytest.raises(KeyError):
        bs.remove(40)

    with pytest.raises(ValueError):
        bs.remove(-1)


def test_discard():
    bs = BitSet()
    assert 0 not in bs

    bs.add(1)
    assert 1 in bs
    assert 2 not in bs

    bs.add(2)
    assert 1 in bs
    assert 2 in bs
    assert 0 not in bs

    bs.discard(2)
    assert 1 in bs
    assert 2 not in bs

    bs.discard(40)

    with pytest.raises(ValueError):
        bs.discard(-1)


def test_iter():
    bs = BitSet()
    assert list(bs) == []

    bs.add(1)
    assert list(bs) == [1]

    bs.add(2)
    assert list(bs) == [1, 2]

    bs.add(3)
    assert list(bs) == [1, 2, 3]

    bs.remove(1)
    assert list(bs) == [2, 3]

    bs.remove(2)
    assert list(bs) == [3]


def test_len():
    bs = BitSet()
    assert len(bs) == 0

    bs.add(1)
    assert len(bs) == 1

    bs.add(2)
    assert len(bs) == 2

    bs.add(3)
    assert len(bs) == 3

    bs.remove(3)
    assert len(bs) == 2

    bs.remove(2)
    assert len(bs) == 1

    bs.remove(1)
    assert len(bs) == 0
