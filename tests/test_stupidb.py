#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import itertools
import operator

import pytest
import toolz

from stupidb.api import (
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
from stupidb.stupidb import GroupBy, Projection, Selection, Window


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
def projection(table):
    return Projection(
        table, lambda row: dict(c=row["a"], d=row["b"], z=row["z"])
    )


@pytest.fixture
def selection(projection):
    return Selection(projection, lambda row: True)


@pytest.fixture
def group_by_(selection):
    return GroupBy(
        selection,
        {"c": lambda row: row["c"], "z": lambda row: row["z"]},
        {
            "total": sum(lambda row: row["d"]),
            "mean": mean(lambda row: row["d"]),
        },
    )


def test_table(table, rows):
    expected = rows[:]
    op = table_(rows) >> do()
    result = list(op)
    assert_rowset_equal(result, expected)
    assert set(table_(rows).columns) == set(rows[0].keys())


def test_projection(projection, rows):
    expected = [
        dict(z="a", c=1, d=2),
        dict(z="b", c=2, d=-1),
        dict(z="a", c=3, d=4),
        dict(z="a", c=4, d=-3),
        dict(z="a", c=1, d=-3),
        dict(z="b", c=2, d=-3),
        dict(z="b", c=3, d=-3),
    ]
    pipeline = projection >> do()
    result = list(pipeline)
    assert_rowset_equal(result, expected)


def test_selection(selection, rows):
    expected = [
        dict(z="a", c=1, d=2),
        dict(z="a", c=1, d=-3),
        dict(z="a", c=4, d=-3),
        dict(z="a", c=3, d=4),
        dict(z="b", c=2, d=-1),
        dict(z="b", c=2, d=-3),
        dict(z="b", c=3, d=-3),
    ]
    result = selection >> do()
    assert_rowset_equal(result, expected)


def test_group_by(group_by_, rows):
    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    result = list(group_by_ >> do())
    assert_rowset_equal(result, expected)


def test_cross_join(left_table, right_table, left, right):
    join = (
        left_table
        >> cross_join(right_table)
        >> select(lambda left, right: left)
    )
    result = list(join >> do())
    assert len(result) == len(left) * len(right)
    expected = list(map(toolz.first, itertools.product(left, right)))
    assert len(expected) == len(result)
    assert_rowset_equal(result, expected)


def test_inner_join(left_table, right_table, left):
    join = (
        left_table
        >> inner_join(
            right_table,
            lambda left, right: left["z"] == "a" and left["a"] == right["a"],
        )
        >> select(
            left_a=lambda l, r: l["a"],
            right_a=lambda l, r: r["a"],
            right_z=lambda l, r: r["z"],
            left_z=lambda l, r: left["z"],
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
        lambda row: exists(
            table_(other_rows) >> sift(lambda other: row["z"] == other["z"])
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
        lambda row: exists(
            table_(other_rows) >> sift(lambda other: row["z"] == other["z"])
        )
    )
    result = list(pipeline >> do())
    expected = [row for row in rows if row["z"] == "b"]
    assert result == expected


def test_right_shiftable(table, right_table):
    pipeline = (
        table
        >> select(lambda r: dict(c=r["a"], d=r["b"], z=r["z"]))
        >> sift(lambda r: True)
        >> group_by(
            {"c": lambda r: r["c"], "z": lambda r: r["z"]},
            {
                "total": sum(lambda r: r["d"]),
                "mean": mean(lambda r: r["d"]),
                "my_samp_cov": samp_cov(lambda r: r["d"], lambda r: r["d"]),
                "my_pop_cov": pop_cov(lambda r: r["d"], lambda r: r["d"]),
            },
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


def test_window(table, rows):
    preceding = lambda r: 2
    following = lambda r: 0
    partition_by = []
    order_by = [lambda r: r["e"]]
    pipeline = table >> select(
        a=lambda r: r["a"],
        my_agg=sum(lambda r: r["e"]).over(
            Window.rows(
                order_by=order_by,
                partition_by=partition_by,
                preceding=preceding,
                following=following,
            )
        ),
    )
    result = list(pipeline >> do())
    # import pdb; pdb.set_trace()  # noqa
    assert result is not None
