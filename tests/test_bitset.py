from stupidb.bitset import BitSet


def test_construction():
    bs = BitSet()
    assert bs.bitset == 0


def test_add_contains():
    bs = BitSet()
    assert 0 not in bs

    bs.add(1)
    assert 1 in bs
    assert 2 not in bs

    bs.add(2)
    assert 1 in bs
    assert 2 in bs
    assert 0 not in bs
