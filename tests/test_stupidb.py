#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `stupidb` package."""

import builtins
import itertools
import operator
import statistics
from datetime import date, timedelta
from typing import Callable, Iterable, Iterator, TypeVar

import pytest
import toolz

from stupidb.aggregation import Window
from stupidb.api import (
    aggregate,
    count,
    cov_pop,
    cov_samp,
    cross_join,
    dense_rank,
    difference,
    difference_all,
    exists,
    first,
    group_by,
    inner_join,
    intersect,
    intersect_all,
    lag,
    last,
    lead,
    left_join,
    max,
    mean,
    min,
    mutate,
    nth,
    order_by,
    over,
    rank,
    right_join,
    row_number,
    select,
    sift,
    stdev_samp,
    sum,
    table,
    total,
    union,
    union_all,
    var_samp,
)
from stupidb.associative import SegmentTree, Sum, next_power_of_2
from stupidb.row import JoinedRow, Row


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


def tupleize(row):
    return frozenset(row.items())


def assert_rowset_equal(left, right):
    assert set(map(tupleize, left)) == set(map(tupleize, right))


def test_row_invalid_attribute():
    row = Row({"a": 1}, _id=0)
    with pytest.raises(AttributeError):
        row.b


def test_row_equality():
    row1 = Row({"a": 1}, _id=0)
    row2 = Row({"a": 1}, _id=1)
    assert row1 == row2


def test_row_inequality():
    row1 = Row({"a": 0}, _id=0)
    row2 = Row({"a": 1}, _id=1)
    assert row1 != row2


def test_row_repr():
    row = Row({"a": 1}, _id=0)
    assert repr(row) == "Row({'a': 1})"


def test_joined_row_hash():
    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    assert hash(row)


def test_joined_row_data():
    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    assert row.data == {"a": 1, "b": 2}


def test_joined_row_from_mapping():
    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    with pytest.raises(TypeError):
        row.from_mapping({"c": 1})


def test_joined_row_getitem():
    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    assert row.a == 1
    assert row.b == 2


def test_joined_row_overlapping():
    row = JoinedRow({"a": 1}, {"a": 2}, _id=0)
    with pytest.raises(ValueError):
        row.a
    assert row.left.a == 1
    assert row.right.a == 2


def test_joined_row_repr():
    row = JoinedRow({"a": 1}, {"a": 2}, _id=0)
    assert repr(row) == "JoinedRow({'a': 1}, {'a': 2})"

    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    assert repr(row) == "JoinedRow({'a': 1}, {'b': 2})"


def test_next_power_of_2():
    result = next_power_of_2(0)
    assert result == 0
    with pytest.raises(ValueError):
        next_power_of_2(-1)
    assert next_power_of_2(1) == 1
    assert next_power_of_2(3) == 4
    assert next_power_of_2(5) == 8


def test_repr_segment_tree():
    tree = SegmentTree([(1,), (2,), (3,)], aggregate_type=Sum)
    result = repr(tree)
    expected = """\
|-- Sum(total=6, count=3)
    |-- Sum(total=3, count=2)
        |-- Sum(total=1, count=1)
        |-- Sum(total=2, count=1)
    |-- Sum(total=3, count=1)
        |-- Sum(total=3, count=1)"""
    assert result == expected


@pytest.fixture
def test_table(rows):
    expected = rows[:]
    op = table(rows)
    result = list(op)
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
    return table(t_rows)


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
        >> sift(lambda r: True)
        >> group_by(c=lambda r: r["c"], z=lambda r: r["z"])
        >> aggregate(total=sum(lambda r: r["d"]), mean=mean(lambda r: r["d"]))
    )
    result = list(gb)
    assert_rowset_equal(result, expected)


def test_cross_join(left, right):
    join = (
        table(left)
        >> cross_join(table(right))
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


def test_inner_join(left, right):
    join = (
        table(left)
        >> inner_join(
            table(right),
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


def test_left_join(left, right):
    join = (
        table(left)
        >> left_join(table(right), lambda r: r.left["z"] == r.right["z"])
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


def test_right_join(left, right):
    join = (
        table(left)
        >> right_join(table(right), lambda r: r.left["z"] == r.right["z"])
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
            my_agg=sum(lambda r: r.a)
            >> over(Window.rows(partition_by=[lambda r: r.z]))
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
    result, = list(pipeline)
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


def cumagg(seq: Iterable[T], combine: Callable[[T, U], U]) -> Iterator[U]:
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
    result_row, = list(query)
    assert result_row.sum is None
    assert result_row.total == 0


def test_minmax(rows):
    query = table(rows) >> aggregate(
        min=min(lambda r: r.e), max=max(lambda r: r.e)
    )
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


def test_first_last(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        first_date=first(lambda r: r.date) >> over(window),
        last_date=last(lambda r: r.date) >> over(window),
    )
    result = list(query)
    expected = [
        dict(first_date=date(2018, 1, 1), last_date=date(2018, 1, 7)),
        dict(first_date=date(2018, 1, 1), last_date=date(2018, 1, 7)),
        dict(first_date=date(2018, 1, 1), last_date=date(2018, 1, 7)),
        dict(first_date=date(2018, 1, 1), last_date=date(2018, 1, 7)),
        dict(first_date=date(2018, 1, 2), last_date=date(2018, 1, 4)),
        dict(first_date=date(2018, 1, 2), last_date=date(2018, 1, 4)),
        dict(first_date=date(2018, 1, 2), last_date=date(2018, 1, 4)),
    ]
    assert_rowset_equal(result, expected)


def test_nth(t_rows):
    query = table(t_rows) >> select(
        nth_date=nth(lambda r: r.date, lambda r: 1)
        >> over(Window.range(partition_by=[lambda r: r.name]))
    )
    result = list(query)
    expected = [
        dict(nth_date=date(2018, 1, 4)),
        dict(nth_date=date(2018, 1, 4)),
        dict(nth_date=date(2018, 1, 4)),
        dict(nth_date=date(2018, 1, 4)),
        dict(nth_date=date(2018, 1, 3)),
        dict(nth_date=date(2018, 1, 3)),
        dict(nth_date=date(2018, 1, 3)),
    ]
    assert_rowset_equal(result, expected)


def test_lead_lag(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        lead_date=lead(lambda r: r.date, lambda r: 1) >> over(window),
        lag_date=lag(lambda r: r.date, lambda r: 1) >> over(window),
    )
    result = list(query)
    expected = [
        dict(lead_date=date(2018, 1, 4), lag_date=None),
        dict(lead_date=date(2018, 1, 6), lag_date=date(2018, 1, 1)),
        dict(lead_date=date(2018, 1, 7), lag_date=date(2018, 1, 4)),
        dict(lead_date=None, lag_date=date(2018, 1, 6)),
        dict(lead_date=date(2018, 1, 3), lag_date=None),
        dict(lead_date=date(2018, 1, 4), lag_date=date(2018, 1, 2)),
        dict(lead_date=None, lag_date=date(2018, 1, 3)),
    ]
    assert_rowset_equal(result, expected)


def test_row_number(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(row_id=row_number() >> over(window))
    result = list(query)
    expected = [
        dict(row_id=0),
        dict(row_id=1),
        dict(row_id=2),
        dict(row_id=3),
        dict(row_id=0),
        dict(row_id=1),
        dict(row_id=2),
    ]
    assert_rowset_equal(result, expected)


def test_variance_window(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        name=lambda r: r.name,
        var=var_samp(lambda r: r.balance) >> over(window),
        std=stdev_samp(lambda r: r.balance) >> over(window),
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
            ),
            _id=1,
        ),
        Row(
            dict(
                name="bob",
                var=statistics.variance(bob),
                std=statistics.stdev(bob),
            ),
            _id=2,
        ),
    }
    assert set(result) == expected


def test_rank():
    rows = [
        dict(name="apple"),
        dict(name="apple"),
        dict(name="grapes"),
        dict(name="grapes"),
        dict(name="orange"),
        dict(name="watermelon"),
    ]
    window = Window.rows(order_by=[lambda r: r.name])
    query = table(rows) >> select(
        name=lambda r: r.name, ranked=rank() >> over(window)
    )
    result = [row.ranked for row in query]
    expected = [0, 0, 2, 2, 4, 5]
    assert result == expected


def test_rank_with_nulls():
    rows = [dict(name="a"), dict(name=None), dict(name=None), dict(name="b")]
    window = Window.rows(order_by=[lambda r: r.name])
    query = (
        table(rows)
        >> select(name=lambda r: r.name, ranked=rank() >> over(window))
        >> order_by(lambda r: r.ranked)
    )
    result = [row.ranked for row in query]
    expected = [0, 0, 2, 3]
    assert result == expected


def test_dense_rank():
    rows = [
        dict(name="apple"),
        dict(name="apple"),
        dict(name="grapes"),
        dict(name="grapes"),
        dict(name="orange"),
        dict(name="watermelon"),
    ]
    window = Window.rows(order_by=[lambda r: r.name])
    query = table(rows) >> select(
        name=lambda r: r.name, ranked=dense_rank() >> over(window)
    )
    result = [row.ranked for row in query]
    expected = [0, 0, 1, 1, 2, 3]
    assert result == expected


def test_dense_rank_with_nulls():
    rows = [dict(name="a"), dict(name=None), dict(name=None), dict(name="b")]
    window = Window.rows(order_by=[lambda r: r.name])
    query = (
        table(rows)
        >> select(name=lambda r: r.name, ranked=dense_rank() >> over(window))
        >> order_by(lambda r: r.ranked)
    )
    result = [row.ranked for row in query]
    expected = [0, 0, 1, 2]
    assert result == expected


def test_union_distinct():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> union(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b"), dict(name="c")]
    assert result == expected


def test_union_duplicates():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> union(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b")]
    assert result == expected


def test_union_all_distinct():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> union_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="b"), dict(name="c")]
    assert result == expected


def test_union_all_duplicates():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> union_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a"), dict(name="b")]
    assert result == expected


def test_intersect_distinct():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> intersect(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = []
    assert result == expected


def test_intersect_duplicates():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> intersect(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a")]
    assert result == expected


def test_intersect_all_distinct():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> intersect_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = []
    assert result == expected


def test_intersect_all_duplicates():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> intersect_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a")]
    assert result == expected


def test_difference_distinct():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="c")]
    query = rows >> difference(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = rows[:]
    assert result == expected


def test_difference_duplicates():
    rows = [dict(name="a"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> difference(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="b")]
    assert result == expected
    assert_rowset_equal(result, expected)


def test_difference_all_distinct():
    rows = [dict(name="a"), dict(name="b"), dict(name="a")]
    other_rows = [dict(name="c"), dict(name="b")]
    query = rows >> difference_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="a"), dict(name="a")]
    assert result == expected


def test_difference_all_duplicates():
    rows = [dict(name="a"), dict(name="b"), dict(name="b")]
    other_rows = [dict(name="a")]
    query = rows >> difference_all(other_rows) >> order_by(lambda r: r.name)
    result = list(query)
    expected = [dict(name="b"), dict(name="b")]
    assert result == expected
