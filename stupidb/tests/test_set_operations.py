from stupidb.api import (
    difference,
    difference_all,
    intersect,
    intersect_all,
    order_by,
    union,
    union_all,
)

from .conftest import assert_rowset_equal


def test_union_distinct() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> union(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b"), dict(name="c")]
    assert result == expected


def test_union_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> union(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b")]
    assert result == expected


def test_union_all_distinct() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> union_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b"), dict(name="c")]
    assert result == expected


def test_union_all_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> union_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a"), dict(name="b")]
    assert result == expected


def test_intersect_distinct() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> intersect(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected: list[dict[str, str]] = []
    assert result == expected


def test_intersect_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> intersect(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a")]
    assert result == expected


def test_intersect_all_distinct() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> intersect_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected: list[dict[str, str]] = []
    assert result == expected


def test_intersect_all_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> intersect_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a")]
    assert result == expected


def test_difference_distinct() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> difference(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = rows[:]
    assert result == expected


def test_difference_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> difference(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="b")]
    assert result == expected
    assert_rowset_equal(result, expected)


def test_difference_all_distinct() -> None:
    rows = [dict(name="a"), dict(name="b"), dict(name="a")]
    other_rows = [dict(name="c"), dict(name="b")]
    query = rows >> difference_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a")]
    assert result == expected


def test_difference_all_duplicates() -> None:
    rows = [dict(name="a"), dict(name="b"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> difference_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="b"), dict(name="b")]
    assert result == expected
