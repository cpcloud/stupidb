#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import itertools
import operator

import pytest
import toolz

from stupidb.api import (
    aggregate,
    count,
    cross_join,
    do,
    exists,
    group_by,
    inner_join,
    mean,
    pop_cov,
    samp_cov,
    select,
    sift,
    sum,
)
from stupidb.api import table as table_
from stupidb.stupidb import Window


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
    return rows[3:]


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
    op = table_(rows) >> do()
    result = list(op)
    assert_rowset_equal(result, expected)
    assert set(table_(rows).columns) == set(rows[0].keys())


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
    pipeline = (
        table
        >> select(c=lambda r: r["a"], d=lambda r: r["b"], z=lambda r: r["z"])
        >> do()
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
        >> do()
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
    result = list(gb >> do())
    assert_rowset_equal(result, expected)


def test_cross_join(left_table, right_table, left, right):
    join = left_table >> cross_join(right_table)
    result = list(join >> do())
    assert len(result) == len(left) * len(right)
    expected = list(map(toolz.first, itertools.product(left, right)))
    assert len(expected) == len(result)
    assert_rowset_equal(result, expected)


def test_inner_join(left_table, right_table, left):
    join = (
        left_table
        >> inner_join(
            right_table, lambda l, r: l["z"] == "a" and l["a"] == r["a"]
        )
        >> select(
            left_a=lambda l, r: l["a"],
            right_a=lambda l, r: r["a"],
            right_z=lambda l, r: r["z"],
            left_z=lambda l, r: l["z"],
        )
    )
    pipeline = join >> do()
    result = list(pipeline)
    expected = [
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
        {"left_a": 3, "left_z": "a", "right_a": 3, "right_z": "b"},
        {"left_a": 4, "left_z": "a", "right_a": 4, "right_z": "a"},
        {"left_a": 1, "left_z": "a", "right_a": 1, "right_z": "a"},
    ]
    assert_rowset_equal(result, expected)


@pytest.mark.xfail(raises=AssertionError, reason="Not yet implemented")
def test_left_join():
    assert False


@pytest.mark.xfail(raises=AssertionError, reason="Not yet implemented")
def test_right_join():
    assert False


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
    result = list(pipeline >> do())
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
    result = list(pipeline >> do())
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
    result = list(pipeline >> do())
    assert_rowset_equal(result, expected)


@pytest.mark.xfail(raises=KeyError, reason="Not yet implemented")
def test_window(table, rows):
    preceding = lambda r: 2
    following = lambda r: 0
    partition_by = []
    order_by = [lambda r: r["e"]]
    pipeline = (
        table
        >> select(a=lambda r: r["a"])
        >> aggregate(
            my_agg=sum(lambda r: r["e"]).over(
                Window.rows(
                    order_by=order_by,
                    partition_by=partition_by,
                    preceding=preceding,
                    following=following,
                )
            )
        )
    )
    result = list(pipeline >> do())
    assert result is not None


def test_agg(table, rows):
    pipeline = table >> aggregate(
        sum=sum(lambda r: r["e"]),
        mean=mean(lambda r: r["e"]),
        count=count(lambda r: r["e"]),
    )
    result, = list(pipeline >> do())
    assert result["sum"] == 28
    assert result["mean"] == result["sum"] / result["count"]


@pytest.mark.xfail(reason="Not yet validating")
def test_invalid_agg(table, rows):
    with pytest.raises(TypeError, match="Invalid projection"):
        select(
            not_an_agg=lambda r: r["e"],
            my_agg=sum(lambda r: r["e"]),
            my_agg2=mean(lambda r: r["e"]),
            my_count=count(lambda r: r["e"]),
        )
