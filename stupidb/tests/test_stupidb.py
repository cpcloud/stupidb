# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import builtins
import itertools
import operator
import statistics
from datetime import date, timedelta
from typing import (
    Any,
    Callable,
    FrozenSet,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Type,
    TypeVar,
)

import cytoolz as toolz
import pytest

from stupidb.aggregation import Window
from stupidb.api import (
    aggregate,
    count,
    cov_pop,
    cov_samp,
    cross_join,
    exists,
    group_by,
    inner_join,
    left_join,
    limit,
    max,
    mean,
    min,
    mutate,
    order_by,
    over,
    right_join,
    select,
    sift,
    stdev_pop,
    stdev_samp,
    sum,
    table,
    total,
    var_pop,
    var_samp,
)
from stupidb.core import Relation, Table
from stupidb.row import Row

from .conftest import assert_rowset_equal


def test_projection(rows):
    expected = [
        dict(z="a", c=1, d=2),
        dict(z="b", c=2, d=-1),
        dict(z="a", c=3, d=4),
        dict(z="a", c=4, d=-3),
        dict(z="a", c=1, d=-3),
        dict(z="b", c=2, d=-3),
        dict(z="b", c=3, d=-3),
    ]
    pipeline = table(rows) >> select(
        c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"]
    )
    result = list(pipeline)
    assert_rowset_equal(result, expected)


def test_selection(rows):
    expected = [
        dict(z="a", c=1, d=2),
        dict(z="a", c=1, d=-3),
        dict(z="a", c=4, d=-3),
        dict(z="a", c=3, d=4),
        dict(z="b", c=2, d=-1),
        dict(z="b", c=2, d=-3),
        dict(z="b", c=3, d=-3),
    ]
    selection = (
        table(rows)
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda r: True)
    )
    assert_rowset_equal(selection, expected)


def test_group_by(rows):
    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    gb = (
        table(rows)
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda _: True)
        >> group_by(c=lambda r: r["c"], z=lambda r: r["z"])
        >> aggregate(total=sum(lambda r: r["d"]), mean=mean(lambda r: r["d"]))
    )
    result = list(gb)
    assert_rowset_equal(result, expected)


def subclasses(cls: Type) -> FrozenSet[Type]:
    classes = cls.__subclasses__()
    return (
        frozenset({cls})
        | frozenset(classes)
        | frozenset(itertools.chain.from_iterable(map(subclasses, classes)))
    )


@pytest.mark.parametrize(
    "cls",
    (cls for cls in subclasses(Relation) if cls is not Relation and cls is not Table),
)
def test_from_iterable_is_invalid(cls):
    with pytest.raises(AttributeError):
        cls.from_iterable([dict(a=1)])


def test_cross_join(left, right):
    join = (
        table(left)
        >> cross_join(table(right))
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    assert len(result) == len(left) * len(right)
    expected = [
        {"left_z": left["z"], "right_z": right["z"]}
        for left, right in list(itertools.product(left, right))
    ]
    assert len(expected) == len(result)
    assert_rowset_equal(result, expected)


def test_inner_join(left, right, con):
    join = (
        table(left)
        >> inner_join(
            table(right),
            lambda left, right: (
                left["z"] == "a" and right["z"] == "a" and left["a"] == right["a"]
            ),
        )
        >> select(
            left_a=lambda r: r.left["a"],
            right_a=lambda r: r.right["a"],
            right_z=lambda r: r.right["z"],
            left_z=lambda r: r.left["z"],
        )
    )
    result = list(join)
    query = """
SELECT
    t.a AS left_a,
    s.a AS right_a,
    s.z AS right_z,
    t.z AS left_z
FROM left t
INNER JOIN right s
ON t.z = 'a'
  AND s.z = 'a'
  AND t.a = s.a"""
    expected = [
        dict(left_a=left_a, right_a=right_a, right_z=right_z, left_z=left_z)
        for left_a, right_a, right_z, left_z in con.execute(query).fetchall()
    ]
    assert_rowset_equal(result, expected)


def test_left_join(left, right, con):
    join = (
        table(left)
        >> left_join(table(right), lambda left, right: left["z"] == right["z"])
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    query = """
SELECT t.z AS left_z, s.z AS right_z
FROM left t
LEFT OUTER JOIN right s
ON t.z = s.z"""
    expected = [
        dict(left_z=left_z, right_z=right_z)
        for left_z, right_z in con.execute(query).fetchall()
    ]
    assert_rowset_equal(result, expected)


def test_right_join(left, right, con):
    join = (
        table(left)
        >> right_join(table(right), lambda left, right: left["z"] == right["z"])
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    query = """
SELECT t.z AS left_z, s.z AS right_z
FROM right s
LEFT OUTER JOIN left t
ON t.z = s.z"""
    expected = [
        dict(left_z=left_z, right_z=right_z)
        for left_z, right_z in con.execute(query).fetchall()
    ]
    assert_rowset_equal(result, expected)


def test_semi_join():
    rows = [
        dict(z="a", a=1, b=2),
        dict(z="b", a=2, b=-1),
        dict(z="a", a=3, b=4),
        dict(z="a", a=4, b=-3),
        dict(z="a", a=1, b=-3),
        dict(z="b", a=2, b=-3),
        dict(z="b", a=3, b=-3),
    ]
    other_rows = [
        dict(z="a", a=4, b=-3),
        dict(z="a", a=1, b=-3),
        dict(z="b", a=2, b=-3),
        dict(z="b", a=3, b=-3),
    ]

    pipeline = table(rows) >> sift(
        lambda r: exists(table(other_rows) >> sift(lambda o: r["z"] == o["z"]))
    )
    result = list(pipeline)
    assert result == rows


def test_semi_join_not_all_rows_match():
    rows = [
        dict(z="a", a=1, b=2),
        dict(z="b", a=2, b=-1),
        dict(z="a", a=3, b=4),
        dict(z="a", a=4, b=-3),
        dict(z="a", a=1, b=-3),
        dict(z="b", a=2, b=-3),
        dict(z="b", a=3, b=-3),
    ]
    other_rows = [dict(z="b", a=2, b=-3), dict(z="b", a=3, b=-3)]

    pipeline = table(rows) >> sift(
        lambda r: exists(table(other_rows) >> sift(lambda o: r["z"] == o["z"]))
    )
    result = list(pipeline)
    expected = [row for row in rows if row["z"] == "b"]
    assert result == expected


@pytest.mark.parametrize("offset", range(4))  # type: ignore[misc]
@pytest.mark.parametrize("lim", range(4))  # type: ignore[misc]
def test_valid_limit(rows: Sequence[Mapping[str, Any]], offset: int, lim: int) -> None:
    pipeline = table(rows) >> limit(lim, offset=offset)
    assert list(pipeline) == rows[offset : offset + lim]


@pytest.mark.parametrize(  # type: ignore[misc]
    ("offset", "lim"),
    ((offset, lim) for lim in range(-2, 1) for offset in range(-2, 1) if offset or lim),
)
def test_invalid_limit(
    rows: Sequence[Mapping[str, Any]],
    offset: int,
    lim: int,
) -> None:
    with pytest.raises(ValueError):
        table(rows) >> limit(lim, offset=offset)


def test_right_shiftable(rows):
    pipeline = (
        table(rows)
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda r: True)
        >> group_by(c=lambda r: r["c"], z=lambda r: r["z"])
        >> aggregate(
            total=sum(lambda r: r["d"]),
            mean=mean(lambda r: r["d"]),
            my_samp_cov=cov_samp(lambda r: r["d"], lambda r: r["d"]),
            my_pop_cov=cov_pop(lambda r: r["d"], lambda r: r["d"]),
        )
    )

    expected = [
        {
            "c": 1,
            "mean": -0.5,
            "my_samp_cov": 15.0,
            "my_pop_cov": 7.5,
            "total": -1,
            "z": "a",
        },
        {
            "c": 2,
            "mean": -2.0,
            "my_samp_cov": 3.0,
            "my_pop_cov": 1.5,
            "total": -4,
            "z": "b",
        },
        {
            "c": 3,
            "mean": 4.0,
            "my_samp_cov": None,
            "my_pop_cov": 0.0,
            "total": 4,
            "z": "a",
        },
        {
            "c": 4,
            "mean": -3.0,
            "my_samp_cov": None,
            "my_pop_cov": 0.0,
            "total": -3,
            "z": "a",
        },
        {
            "c": 3,
            "mean": -3.0,
            "my_samp_cov": None,
            "my_pop_cov": 0.0,
            "total": -3,
            "z": "b",
        },
    ]
    result = list(pipeline)
    assert_rowset_equal(result, expected)


def test_rows_window(rows):
    pipeline = (
        table(rows)
        >> mutate(
            my_agg=sum(lambda r: r["a"])
            >> over(
                Window.rows(
                    order_by=[lambda r: r["e"]],
                    partition_by=[lambda r: r["z"]],
                    preceding=lambda r: 2,
                    following=lambda r: 0,
                )
            )
        )
        >> order_by(lambda r: r["z"], lambda r: r["e"])
    )
    result = list(pipeline)
    expected_aggrows = [
        {"my_agg": 1},
        {"my_agg": 2},
        {"my_agg": 4},
        {"my_agg": 8},
        {"my_agg": 8},
        {"my_agg": 4},
        {"my_agg": 7},
    ]
    expected = sorted(
        map(toolz.merge, rows, expected_aggrows),
        key=lambda r: (r["z"], r["e"]),
    )
    assert len(result) == len(expected)
    assert_rowset_equal(result, expected)


def test_rows_window_partition(rows):
    pipeline = (
        table(rows)
        >> mutate(
            my_agg=sum(lambda r: r.a) >> over(Window.rows(partition_by=[lambda r: r.z]))
        )
        >> order_by(lambda r: r.z, lambda r: r.e)
    )
    result = list(pipeline)
    expected_aggrows = [
        {"my_agg": 9},
        {"my_agg": 7},
        {"my_agg": 9},
        {"my_agg": 9},
        {"my_agg": 9},
        {"my_agg": 7},
        {"my_agg": 7},
    ]
    assert len(result) == len(rows)
    assert len(result) == len(expected_aggrows)
    expected = sorted(
        map(toolz.merge, rows, expected_aggrows),
        key=lambda r: (r["z"], r["e"]),
    )
    assert len(result) == len(expected)
    assert_rowset_equal(result, expected)


def test_range_window_with_multiple_ordering_keys_fails(rows):
    with pytest.raises(ValueError):
        table(rows) >> mutate(
            my_agg=sum(lambda r: r["a"])
            >> over(
                Window.range(
                    order_by=[lambda r: r["e"], lambda r: r["a"]],
                    partition_by=[lambda r: r["z"]],
                    preceding=lambda r: 2,
                    following=lambda r: 0,
                )
            )
        )


def test_range_window(rows):
    pipeline = (
        table(rows)
        >> mutate(
            my_agg=sum(lambda r: r["a"])
            >> over(
                Window.range(
                    order_by=[lambda r: r["e"]],
                    partition_by=[lambda r: r["z"]],
                    preceding=lambda r: 2,
                    following=lambda r: 0,
                )
            )
        )
        >> order_by(lambda r: r["z"], lambda r: r["e"])
        >> select(
            a=lambda r: r.a,
            e=lambda r: r.e,
            my_agg=lambda r: r.my_agg,
            z=lambda r: r.z,
        )
    )
    result = list(pipeline)
    expected_a = [
        {"a": 1, "e": 1, "my_agg": 1, "z": "a"},
        {"a": 3, "e": 3, "my_agg": 4, "z": "a"},
        {"a": 4, "e": 4, "my_agg": 7, "z": "a"},
        {"a": 1, "e": 5, "my_agg": 8, "z": "a"},
    ]
    expected_b = [
        {"a": 2, "e": 2, "my_agg": 2, "z": "b"},
        {"a": 2, "e": 6, "my_agg": 2, "z": "b"},
        {"a": 3, "e": 7, "my_agg": 5, "z": "b"},
    ]

    expected = expected_a + expected_b

    assert len(result) == len(expected)
    assert_rowset_equal(result, expected)


def test_temporal_range_window(t_table, t_rows):
    query = t_table >> mutate(
        avg_balance=mean(lambda r: r["balance"])
        >> over(
            Window.range(
                order_by=[lambda r: r["date"]],
                partition_by=[lambda r: r["name"]],
                preceding=lambda r: timedelta(days=3),
                following=lambda r: timedelta(days=0),
            )
        )
    )
    result = list(query)
    expected = [
        {
            "avg_balance": 2.0,
            "balance": 2,
            "date": date(2018, 1, 1),
            "name": "alice",
        },
        {
            "avg_balance": 3.0,
            "balance": 4,
            "date": date(2018, 1, 4),
            "name": "alice",
        },
        {
            "avg_balance": 0.5,
            "balance": -3,
            "date": date(2018, 1, 6),
            "name": "alice",
        },
        {
            "avg_balance": -0.666_666_666_666_666_6,
            "balance": -3,
            "date": date(2018, 1, 7),
            "name": "alice",
        },
        {
            "avg_balance": -1.0,
            "balance": -1,
            "date": date(2018, 1, 2),
            "name": "bob",
        },
        {
            "avg_balance": -2.0,
            "balance": -3,
            "date": date(2018, 1, 3),
            "name": "bob",
        },
        {
            "avg_balance": -2.333_333_333_333_333_5,
            "balance": -3,
            "date": date(2018, 1, 4),
            "name": "bob",
        },
    ]
    assert_rowset_equal(result, expected)


def test_agg(rows):
    pipeline = table(rows) >> aggregate(
        sum=sum(lambda r: r["e"]),
        mean=mean(lambda r: r["e"]),
        count=count(lambda r: r["e"]),
    )
    (result,) = list(pipeline)
    assert result["sum"] == 28
    assert result["mean"] == result["sum"] / result["count"]


def test_invalid_agg(rows):
    with pytest.raises(TypeError, match="Invalid projection"):
        select(
            not_an_agg=lambda r: r["e"],
            my_agg=sum(lambda r: r["e"]),
            my_agg2=mean(lambda r: r["e"]),
            my_count=count(lambda r: r["e"]),
        )


T = TypeVar("T")
U = TypeVar("U")


def cumagg(seq: Iterable[T], combine: Callable[[T, T], T]) -> Iterator[T]:
    """Cumulative aggregation."""
    it = iter(seq)
    result = next(it)
    yield result
    for value in it:
        result = combine(value, result)
        yield result


def test_cumagg(rows):
    window = Window.rows(order_by=[lambda r: r.e])
    query = table(rows) >> select(
        cumsum=sum(lambda r: r.e) >> over(window),
        cumcount=count(lambda r: r.e) >> over(window),
    )
    result = list(query)
    expected = [
        dict(cumsum=cs, cumcount=cc)
        for cs, cc in zip(cumagg(range(1, 8), operator.add), range(1, 8))
    ]
    assert_rowset_equal(result, expected)


def test_total_vs_sum():
    rows = [dict(value=None), dict(value=None)]
    query = table(rows) >> aggregate(
        sum=sum(lambda r: r.value), total=total(lambda r: r.value)
    )
    (result_row,) = list(query)
    assert result_row.sum is None
    assert result_row.total == 0


def test_minmax(rows):
    query = table(rows) >> aggregate(min=min(lambda r: r.e), max=max(lambda r: r.e))
    result = list(query)
    e = [r["e"] for r in rows]
    expected = [dict(min=builtins.min(e), max=builtins.max(e))]
    assert_rowset_equal(result, expected)


def test_min_max_window(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        min_date=min(lambda r: r.date) >> over(window),
        max_date=max(lambda r: r.date) >> over(window),
    )
    result = list(query)
    expected = [
        dict(min_date=date(2018, 1, 1), max_date=date(2018, 1, 7)),
        dict(min_date=date(2018, 1, 1), max_date=date(2018, 1, 7)),
        dict(min_date=date(2018, 1, 1), max_date=date(2018, 1, 7)),
        dict(min_date=date(2018, 1, 1), max_date=date(2018, 1, 7)),
        dict(min_date=date(2018, 1, 2), max_date=date(2018, 1, 4)),
        dict(min_date=date(2018, 1, 2), max_date=date(2018, 1, 4)),
        dict(min_date=date(2018, 1, 2), max_date=date(2018, 1, 4)),
    ]
    assert_rowset_equal(result, expected)


def test_variance_window(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        name=lambda r: r.name,
        var=var_samp(lambda r: r.balance) >> over(window),
        std=stdev_samp(lambda r: r.balance) >> over(window),
        popvar=var_pop(lambda r: r.balance) >> over(window),
        popstd=stdev_pop(lambda r: r.balance) >> over(window),
    )
    result = list(query)
    alice = [row["balance"] for row in t_rows if row["name"] == "alice"]
    bob = [row["balance"] for row in t_rows if row["name"] == "bob"]
    expected = {
        Row(
            dict(
                name="alice",
                var=statistics.variance(alice),
                std=statistics.stdev(alice),
                popvar=statistics.pvariance(alice),
                popstd=statistics.pstdev(alice),
            ),
            _id=1,
        ),
        Row(
            dict(
                name="bob",
                var=statistics.variance(bob),
                std=statistics.stdev(bob),
                popvar=statistics.pvariance(bob),
                popstd=statistics.pstdev(bob),
            ),
            _id=2,
        ),
    }
    assert set(result) == expected
