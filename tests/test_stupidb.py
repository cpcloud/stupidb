#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import builtins
import itertools
import operator
from datetime import date, timedelta
from typing import Callable, Iterable, Iterator, TypeVar

import pytest
import toolz

from stupidb.aggregation import Window
from stupidb.api import (
    aggregate,
    count,
    cross_join,
    exists,
    first,
    group_by,
    inner_join,
    last,
    left_join,
    max,
    mean,
    min,
    mutate,
    nth,
    order_by,
    over,
    pop_cov,
    right_join,
    samp_cov,
    select,
    sift,
    sum,
)
from stupidb.api import table as table_


@pytest.fixture
def rows():
    return [
        dict(z="a", a=1, b=2, e=1),
        dict(z="b", a=2, b=-1, e=2),
        dict(z="a", a=3, b=4, e=3),
        dict(z="a", a=4, b=-3, e=4),
        dict(z="a", a=1, b=-3, e=5),
        dict(z="b", a=2, b=-3, e=6),
        dict(z="b", a=3, b=-3, e=7),
    ]


@pytest.fixture
def left(rows):
    return rows


@pytest.fixture
def right(rows):
    return [
        dict(z="a", a=1, b=2, e=1),
        dict(z="c", a=2, b=-1, e=2),
        dict(z="a", a=3, b=4, e=3),
        dict(z="c", a=4, b=-3, e=4),
        dict(z="a", a=1, b=-3, e=5),
        dict(z="c", a=2, b=-3, e=6),
        dict(z="c", a=3, b=-3, e=7),
    ]


def assert_rowset_equal(left, right):
    tupleize = toolz.compose(frozenset, operator.methodcaller("items"))
    assert set(map(tupleize, left)) == set(map(tupleize, right))


@pytest.fixture
def table(rows):
    return table_(rows)


@pytest.fixture
def left_table(table):
    return table


@pytest.fixture
def right_table(right):
    return table_(right)


@pytest.fixture
def test_table(table, rows):
    expected = rows[:]
    op = table_(rows)
    result = list(op)
    assert_rowset_equal(result, expected)


def test_projection(table, rows):
    expected = [
        dict(z="a", c=1, d=2),
        dict(z="b", c=2, d=-1),
        dict(z="a", c=3, d=4),
        dict(z="a", c=4, d=-3),
        dict(z="a", c=1, d=-3),
        dict(z="b", c=2, d=-3),
        dict(z="b", c=3, d=-3),
    ]
    pipeline = table >> select(
        c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"]
    )
    result = list(pipeline)
    assert_rowset_equal(result, expected)


def test_selection(table, rows):
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
        table
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda r: True)
    )
    assert_rowset_equal(selection, expected)


def test_group_by(table, rows):
    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    gb = (
        table
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda r: True)
        >> group_by(c=lambda r: r["c"], z=lambda r: r["z"])
        >> aggregate(total=sum(lambda r: r["d"]), mean=mean(lambda r: r["d"]))
    )
    result = list(gb)
    assert_rowset_equal(result, expected)


def test_cross_join(left_table, right_table, left, right):
    join = (
        left_table
        >> cross_join(right_table)
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    assert len(result) == len(left) * len(right)
    expected = [
        {"left_z": l["z"], "right_z": r["z"]}
        for l, r in list(itertools.product(left, right))
    ]
    assert len(expected) == len(result)
    assert_rowset_equal(result, expected)


def test_inner_join(left_table, right_table, left):
    join = (
        left_table
        >> inner_join(
            right_table,
            lambda r: (
                r.left["z"] == "a"
                and r.right["z"] == "a"
                and r.left["a"] == r.right["a"]
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
    expected = [
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
        {"left_a": 3, "left_z": "a", "right_a": 3, "right_z": "a"},
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
    ]
    assert_rowset_equal(result, expected)


def test_left_join(left_table, right_table, left):
    join = (
        left_table
        >> left_join(right_table, lambda r: r.left["z"] == r.right["z"])
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    expected = [
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "b", "right_z": None},
        {"left_z": "b", "right_z": None},
        {"left_z": "b", "right_z": None},
    ]
    assert_rowset_equal(result, expected)


def test_right_join(left_table, right_table, left):
    join = (
        left_table
        >> right_join(right_table, lambda r: r.left["z"] == r.right["z"])
        >> select(left_z=lambda r: r.left["z"], right_z=lambda r: r.right["z"])
    )
    result = list(join)
    expected = [
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "a", "right_z": "a"},
        {"left_z": "c", "right_z": None},
        {"left_z": "c", "right_z": None},
        {"left_z": "c", "right_z": None},
        {"left_z": "c", "right_z": None},
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

    pipeline = table_(rows) >> sift(
        lambda r: exists(
            table_(other_rows) >> sift(lambda o: r["z"] == o["z"])
        )
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

    pipeline = table_(rows) >> sift(
        lambda r: exists(
            table_(other_rows) >> sift(lambda o: r["z"] == o["z"])
        )
    )
    result = list(pipeline)
    expected = [row for row in rows if row["z"] == "b"]
    assert result == expected


def test_right_shiftable(table, right_table):
    pipeline = (
        table
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> sift(lambda r: True)
        >> group_by(c=lambda r: r["c"], z=lambda r: r["z"])
        >> aggregate(
            total=sum(lambda r: r["d"]),
            mean=mean(lambda r: r["d"]),
            my_samp_cov=samp_cov(lambda r: r["d"], lambda r: r["d"]),
            my_pop_cov=pop_cov(lambda r: r["d"], lambda r: r["d"]),
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


def test_rows_window(table, rows):
    pipeline = (
        table
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
    assert len(result) == len(rows)
    assert len(result) == len(expected_aggrows)
    expected = sorted(
        map(toolz.merge, rows, expected_aggrows),
        key=lambda r: (r["z"], r["e"]),
    )
    assert len(result) == len(expected)
    assert_rowset_equal(result, expected)


def test_range_window(table, rows):
    pipeline = (
        table
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


@pytest.fixture
def t_rows():
    return [
        dict(name="alice", date=date(2018, 1, 1), balance=2),
        dict(name="alice", date=date(2018, 1, 4), balance=4),
        dict(name="alice", date=date(2018, 1, 6), balance=-3),
        dict(name="alice", date=date(2018, 1, 7), balance=-3),
        dict(name="bob", date=date(2018, 1, 2), balance=-1),
        dict(name="bob", date=date(2018, 1, 3), balance=-3),
        dict(name="bob", date=date(2018, 1, 4), balance=-3),
    ]


@pytest.fixture
def t_table(t_rows):
    return table_(t_rows)


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


def test_agg(table, rows):
    pipeline = table >> aggregate(
        sum=sum(lambda r: r["e"]),
        mean=mean(lambda r: r["e"]),
        count=count(lambda r: r["e"]),
    )
    result, = list(pipeline)
    assert result["sum"] == 28
    assert result["mean"] == result["sum"] / result["count"]


def test_invalid_agg(table, rows):
    with pytest.raises(TypeError, match="Invalid projection"):
        select(
            not_an_agg=lambda r: r["e"],
            my_agg=sum(lambda r: r["e"]),
            my_agg2=mean(lambda r: r["e"]),
            my_count=count(lambda r: r["e"]),
        )


T = TypeVar("T")
U = TypeVar("U")


def cumagg(seq: Iterable[T], combine: Callable[[T, U], U]) -> Iterator[U]:
    """Cumulative aggregation."""
    result = toolz.first(seq)
    yield result
    for value in itertools.islice(seq, 1, None):
        result = combine(value, result)
        yield result


def test_cumsum(table):
    query = table >> select(
        my_cumsum=sum(lambda r: r.e)
        >> over(Window.rows(order_by=[lambda r: r.e]))
    )
    result = [r.my_cumsum for r in query]
    expected = list(cumagg(range(1, 8), operator.add))
    assert result == expected


def test_minmax(table, rows):
    query = table >> aggregate(min=min(lambda r: r.e), max=max(lambda r: r.e))
    result = list(query)
    e = [r["e"] for r in rows]
    expected = [dict(min=builtins.min(e), max=builtins.max(e))]
    assert_rowset_equal(result, expected)


def test_first_last_nth_min_max(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table_(t_rows) >> select(
        first_date=first(lambda r: r.date) >> over(window),
        last_date=last(lambda r: r.date) >> over(window),
        nth_date=nth(lambda r: r.date, lambda r: 2) >> over(window),
        min_date=min(lambda r: r.date) >> over(window),
        max_date=max(lambda r: r.date) >> over(window)
    )
    result = list(query)
    expected = [
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            nth_date=date(2018, 1, 6),
            min_date=date(2018, 1, 1),
            max_date=date(2018, 1, 7),
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            nth_date=date(2018, 1, 6),
            min_date=date(2018, 1, 1),
            max_date=date(2018, 1, 7),
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            nth_date=date(2018, 1, 6),
            min_date=date(2018, 1, 1),
            max_date=date(2018, 1, 7),
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            nth_date=date(2018, 1, 6),
            min_date=date(2018, 1, 1),
            max_date=date(2018, 1, 7),
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            nth_date=date(2018, 1, 4),
            min_date=date(2018, 1, 2),
            max_date=date(2018, 1, 4),
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            nth_date=date(2018, 1, 4),
            min_date=date(2018, 1, 2),
            max_date=date(2018, 1, 4),
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            nth_date=date(2018, 1, 4),
            min_date=date(2018, 1, 2),
            max_date=date(2018, 1, 4),
        ),
    ]
    assert_rowset_equal(result, expected)
