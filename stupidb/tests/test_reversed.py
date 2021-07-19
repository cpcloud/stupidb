import pytest

from stupidb.reversed import Reversed


@pytest.fixture
def rev():
    return Reversed([1, 2, 3, 4])


def test_iter(rev):
    assert list(rev) == list(reversed(rev.values))


def test_invertible(rev):
    assert list(reversed(rev)) == rev.values


@pytest.mark.parametrize(("index", "expected"), [(0, 4), (1, 3), (2, 2), (3, 1)])
def test_indexing(rev, index, expected):
    assert rev[index] == expected


def test_invalid_indexing(rev):
    with pytest.raises(IndexError):
        rev[5]


@pytest.mark.parametrize(("index", "expected"), [(-1, 1), (-2, 2), (-3, 3), (-4, 4)])
def test_negative_indexing(rev, index, expected):
    assert rev[index] == expected


def test_invalid_negative_indexing(rev):
    with pytest.raises(IndexError):
        rev[-5]


def test_repr(rev):
    assert repr(rev) == "Reversed([4, 3, 2, 1])"
