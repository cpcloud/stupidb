from __future__ import annotations

from datetime import date
from typing import Mapping

from stupidb import Window, const, first, get, lag, last, lead, nth, over, select, table

from .conftest import Element, assert_rowset_equal


def test_first_last(t_rows: list[dict[str, Element]]) -> None:
    window = Window.range(partition_by=[get("name")])
    query = table(t_rows) >> select(
        first_date=first(get("date")) >> over(window),
        last_date=last(get("date")) >> over(window),
        first_date_nulls=first(const(None)) >> over(window),
    )
    result = list(query)
    expected = [
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 1),
            last_date=date(2018, 1, 7),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            first_date_nulls=None,
        ),
        dict(
            first_date=date(2018, 1, 2),
            last_date=date(2018, 1, 4),
            first_date_nulls=None,
        ),
    ]
    assert_rowset_equal(result, expected)


def test_nth(t_rows: list[dict[str, Element]]) -> None:
    query = table(t_rows) >> select(
        nth_date=nth(get("date"), const(1))
        >> over(Window.range(partition_by=[get("name")]))
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


def test_nth_past_frame(t_rows: list[dict[str, Element]]) -> None:
    query = table(t_rows) >> select(
        nth_date=nth(get("date"), const(4000))
        >> over(Window.range(partition_by=[get("name")]))
    )
    result = list(query)
    expected = [
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
    ]
    assert_rowset_equal(result, expected)


def test_nth_past_frame_preceding_following(t_rows: list[dict[str, Element]]) -> None:
    query = table(t_rows) >> select(
        nth_date=nth(get("date"), const(4000))
        >> over(
            Window.range(
                partition_by=[get("name")],
                preceding=const(200),
                following=const(1000),
            )
        )
    )
    result = list(query)
    expected = [
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
        dict(nth_date=None),
    ]
    assert_rowset_equal(result, expected)


def test_lead_lag(t_rows: list[dict[str, Element]]) -> None:
    window = Window.range(partition_by=[get("name")])
    query = table(t_rows) >> select(
        lead_date=lead(get("date"), const(1)) >> over(window),
        lag_date=lag(get("date"), const(1)) >> over(window),
    )
    result = list(query)
    expected: list[Mapping[str, Element]] = [
        dict(lead_date=date(2018, 1, 4), lag_date=None),
        dict(lead_date=date(2018, 1, 6), lag_date=date(2018, 1, 1)),
        dict(lead_date=date(2018, 1, 7), lag_date=date(2018, 1, 4)),
        dict(lead_date=None, lag_date=date(2018, 1, 6)),
        dict(lead_date=date(2018, 1, 3), lag_date=None),
        dict(lead_date=date(2018, 1, 4), lag_date=date(2018, 1, 2)),
        dict(lead_date=None, lag_date=date(2018, 1, 3)),
    ]
    assert_rowset_equal(result, expected)
