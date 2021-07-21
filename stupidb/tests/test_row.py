# -*- coding: utf-8 -*-

import pytest

from stupidb.row import JoinedRow, Row


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


def test_hash_after_renew_id():
    row1 = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    row2 = row1._renew_id(id=1)
    assert row1._id != row2._id
    assert hash(row1) == hash(row2)


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
    assert repr(row) == "JoinedRow(left={'a': 1}, right={'a': 2})"

    row = JoinedRow({"a": 1}, {"b": 2}, _id=0)
    assert repr(row) == "JoinedRow(left={'a': 1}, right={'b': 2})"
