import sqlite3
from datetime import date

import pytest

from stupidb.api import table


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def left(rows):
    return rows


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def t_table(t_rows):
    return table(t_rows)


@pytest.fixture(scope="session")
def con(rows, left, right):
    connection = sqlite3.connect(":memory:")

    connection.execute("CREATE TABLE rows (z text, a integer, b integer, e integer)")
    connection.executemany(
        "INSERT INTO rows VALUES (?, ?, ?, ?)", (tuple(row.values()) for row in rows)
    )

    connection.execute("CREATE TABLE left (z text, a integer, b integer, e integer)")
    connection.executemany(
        "INSERT INTO left VALUES (?, ?, ?, ?)", (tuple(row.values()) for row in left)
    )

    connection.execute("CREATE TABLE right (z text, a integer, b integer, e integer)")
    connection.executemany(
        "INSERT INTO right VALUES (?, ?, ?, ?)", (tuple(row.values()) for row in right)
    )

    return connection
