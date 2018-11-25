#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import itertools
import operator

import pytest
import toolz

from stupidb.api import cross_join, do
from stupidb.api import group_by as group_by_
from stupidb.api import inner_join, mean, pop_cov, samp_cov, select, sift, sum
from stupidb.api import table as table_
from stupidb.stupidb import (
    GroupBy,
    Mean,
    Projection,
    SampleCovariance,
    Selection,
    Sum,
    Table,
)


@pytest.fixture
def rows():
    return [
        dict(z="a", a=1, b=2),
        dict(z="b", a=2, b=-1),
        dict(z="a", a=3, b=4),
        dict(z="a", a=4, b=-3),
        dict(z="a", a=1, b=-3),
        dict(z="b", a=2, b=-3),
        dict(z="b", a=3, b=-3),
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
    return Table(rows)


@pytest.fixture
def left_table(table):
    return table


@pytest.fixture
def right_table(right):
    return Table(right)


@pytest.fixture
def projection(table):
    return Projection(
        table, lambda row: dict(c=row["a"], d=row["b"], z=row["z"])
    )


@pytest.fixture
def selection(projection):
    return Selection(projection, lambda row: True)


@pytest.fixture
def group_by(selection):
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
    op = Table(rows) >> do()
    result = list(op)
    assert_rowset_equal(result, expected)


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


def test_group_by(group_by, rows):
    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    result = list(group_by >> do())
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
        >> inner_join(right_table, lambda left, right: left["z"] == right["z"])
        >> select(lambda left, right: dict(z=left["z"]))
    )
    pipeline = join >> do()
    result = list(pipeline)
    assert result
    # assert len(result) == len(left)
    # assert len(result) == len(right)
    # assert_rowset_equal(result, left)
    # assert_rowset_equal(result, right)


@pytest.mark.xfail
def test_left_join():
    assert False


@pytest.mark.xfail
def test_right_join():
    assert False


def test_right_shiftable(group_by, rows, right_table):
    pipeline = (
        table_(rows)
        >> select(lambda r: dict(c=r["a"], d=r["b"], z=r["z"]))
        >> sift(lambda r: True)
        >> group_by_(
            {"c": lambda r: r["c"], "z": lambda r: r["z"]},
            {
                "total": sum(lambda r: r["d"]),
                "mean": mean(lambda r: r["d"]),
                "my_cov": samp_cov(lambda r: r["d"], lambda r: r["d"]),
            },
        )
        # >> inner_join(right_table, lambda left, right: left["z"] == right["z"])
        # >> select(
        # lambda left, right: dict(
        # z=right["z"], c=left["c"], total=left["total"]
        # )
        # )
        # >> select(lambda row: row)
    )

    expected = [
        {"c": 1, "mean": -0.5, "my_cov": 15.0, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "my_cov": 3.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "my_cov": None, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "my_cov": None, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "my_cov": None, "total": -3, "z": "b"},
    ]
    result = list(pipeline >> do())
    assert_rowset_equal(result, expected)
