#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import operator

import pytest

import toolz

from stupidb.stupidb import (
    Table,
    Projection,
    Selection,
    GroupBy,
    CrossJoin,
    InnerJoin,
    Sum,
    Mean,
    table as table_,
    select,
    sift,
    group_by as group_by_,
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
def right():
    return [
        dict(z="a", a=1, b=2),
        dict(z="b", a=2, b=-1),
        dict(z="a", a=3, b=4),
        dict(z="a", a=4, b=-3),
        dict(z="a", a=1, b=-3),
        dict(z="b", a=2, b=-3),
        dict(z="b", a=3, b=-3),
    ]


def assert_rowset_equal(left, right):
    tupleize = toolz.compose(frozenset, operator.methodcaller("items"))
    assert set(map(tupleize, left)) == set(map(tupleize, right))


@pytest.fixture
def table():
    return Table()


@pytest.fixture
def left_table():
    return Table()


@pytest.fixture
def right_table():
    return Table()


@pytest.fixture
def projection(table):
    return Projection(
        table,
        dict(
            c=lambda row: row["a"],
            d=lambda row: row["b"],
            z=lambda row: row["z"],
        ),
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
            "total": (Sum, lambda row: row["d"]),
            "mean": (Mean, lambda row: row["d"]),
        },
    )


def test_table(table, rows):
    expected = rows[:]
    op = Table()
    result = list(op.produce(rows))
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
    result = list(projection.produce(rows))
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
    result = selection.produce(rows)
    assert_rowset_equal(result, expected)


def test_group_by(group_by, rows):
    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    result = list(group_by.produce(rows))
    assert_rowset_equal(result, expected)


def test_cross_join(left_table, right_table, left, right):
    join = CrossJoin(left_table, right_table)
    result = list(join.produce(left, right))
    assert len(result) == len(left) * len(right)
    assert_rowset_equal(result, left)
    assert_rowset_equal(result, right)


def test_inner_join(left_table, right_table, left):
    right = [{"z": "a"}]
    join = InnerJoin(
        left_table, right_table, lambda left, right: left["z"] == right["z"]
    )
    result = list(join.produce(left, right))
    assert len(result) == len(left)
    assert len(result) == len(right)
    assert_rowset_equal(result, left)
    assert_rowset_equal(result, right)


@pytest.mark.xfail
def test_left_join():
    assert False


@pytest.mark.xfail
def test_right_join():
    assert False


def test_right_shiftable(group_by, rows):
    pipeline = (
        table_()
        >> select(
            c=lambda row: row["a"],
            d=lambda row: row["b"],
            z=lambda row: row["z"],
        )
        >> sift(lambda row: True)
        >> group_by_(
            {"c": lambda row: row["c"], "z": lambda row: row["z"]},
            {
                "total": (Sum, lambda row: row["d"]),
                "mean": (Mean, lambda row: row["d"]),
            },
        )
    )

    expected = [
        {"c": 1, "mean": -0.5, "total": -1, "z": "a"},
        {"c": 2, "mean": -2.0, "total": -4, "z": "b"},
        {"c": 3, "mean": 4.0, "total": 4, "z": "a"},
        {"c": 4, "mean": -3.0, "total": -3, "z": "a"},
        {"c": 3, "mean": -3.0, "total": -3, "z": "b"},
    ]
    result = list(pipeline.produce(rows))
    assert_rowset_equal(result, expected)
