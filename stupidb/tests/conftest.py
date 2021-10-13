from __future__ import annotations

import sqlite3
from datetime import date
from typing import Mapping, Sequence, Union

import pytest

from stupidb.api import table
from stupidb.core import Table

Element = Union[date, float, int, str, None]


@pytest.fixture(scope="session")  # type: ignore[misc]
def rows() -> list[dict[str, Element]]:
    return [
        dict(z="a", a=1, b=2, e=1),
        dict(z="b", a=2, b=-1, e=2),
        dict(z="a", a=3, b=4, e=3),
        dict(z="a", a=4, b=-3, e=4),
        dict(z="a", a=1, b=-3, e=5),
        dict(z="b", a=2, b=-3, e=6),
        dict(z="b", a=3, b=-3, e=7),
    ]


@pytest.fixture(scope="session")  # type: ignore[misc]
def left(rows: list[dict[str, Element]]) -> list[dict[str, Element]]:
    return rows


@pytest.fixture(scope="session")  # type: ignore[misc]
def right(rows: list[dict[str, Element]]) -> list[dict[str, Element]]:
    return [
        dict(z="a", a=1, b=2, e=1),
        dict(z="c", a=2, b=-1, e=2),
        dict(z="a", a=3, b=4, e=3),
        dict(z="c", a=4, b=-3, e=4),
        dict(z="a", a=1, b=-3, e=5),
        dict(z="c", a=2, b=-3, e=6),
        dict(z="c", a=3, b=-3, e=7),
    ]


def tupleize(row: Mapping[str, Element]) -> frozenset[tuple[str, Element]]:
    return frozenset(row.items())


def assert_rowset_equal(
    left: Sequence[Mapping[str, Element]],
    right: Sequence[Mapping[str, Element]],
) -> None:
    assert set(map(tupleize, left)) == set(map(tupleize, right))


@pytest.fixture(scope="session")  # type: ignore[misc]
def t_rows() -> list[dict[str, Element]]:
    return [
        dict(name="alice", date=date(2018, 1, 1), balance=2),
        dict(name="alice", date=date(2018, 1, 4), balance=4),
        dict(name="alice", date=date(2018, 1, 6), balance=-3),
        dict(name="alice", date=date(2018, 1, 7), balance=-3),
        dict(name="bob", date=date(2018, 1, 2), balance=-1),
        dict(name="bob", date=date(2018, 1, 3), balance=-3),
        dict(name="bob", date=date(2018, 1, 4), balance=-3),
    ]


@pytest.fixture(scope="session")  # type: ignore[misc]
def t_table(t_rows: list[dict[str, Element]]) -> Table:
    return table(t_rows)


@pytest.fixture(scope="session")  # type: ignore[misc]
def employee() -> list[dict[str, Element]]:
    return [
        dict(last_name="Rafferty", department_id=1),
        dict(last_name="Jones", department_id=2),
        dict(last_name="Heisenberg", department_id=2),
        dict(last_name="Robinson", department_id=3),
        dict(last_name="Smith", department_id=3),
        dict(last_name="Williams", department_id=None),
    ]


@pytest.fixture(scope="session")  # type: ignore[misc]
def department() -> list[dict[str, Element]]:
    return [
        dict(department_id=1, department_name="Sales"),
        dict(department_id=2, department_name="Engineering"),
        dict(department_id=3, department_name="Clerical"),
        dict(department_id=4, department_name="Marketing"),
    ]


@pytest.fixture(scope="session")  # type: ignore[misc]
def con(
    rows: list[dict[str, Element]],
    left: list[dict[str, Element]],
    right: list[dict[str, Element]],
    employee: list[dict[str, Element]],
    department: list[dict[str, Element]],
) -> sqlite3.Connection:
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

    connection.execute(
        """
        CREATE TABLE department (
            department_id INTEGER PRIMARY KEY,
            department_name TEXT NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO department VALUES (?, ?)",
        (tuple(row.values()) for row in department),
    )

    connection.execute(
        """
        CREATE TABLE employee (
            employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_name TEXT NOT NULL,
            department_id INTEGER,
            FOREIGN KEY (department_id) REFERENCES department (department_id)
        )
        """
    )
    connection.executemany(
        "INSERT INTO employee (last_name, department_id) VALUES (?, ?)",
        (tuple(row.values()) for row in employee),
    )

    return connection
