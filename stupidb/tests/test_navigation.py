from datetime import date

from stupidb.api import Window, first, lag, last, lead, nth, over, select, table

from .conftest import assert_rowset_equal


def test_first_last(t_rows):
    window = Window.range(partition_by=[lambda r: r.name])
    query = table(t_rows) >> select(
        first_date=first(lambda r: r.date) >> over(window),
        last_date=last(lambda r: r.date) >> over(window),
        first_date_nulls=first(lambda r: None) >> over(window),
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


def test_nth_past_frame(t_rows):
    query = table(t_rows) >> select(
        nth_date=nth(lambda r: r.date, lambda r: 4000)
        >> over(Window.range(partition_by=[lambda r: r.name]))
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


def test_nth_past_frame_preceding_following(t_rows):
    query = table(t_rows) >> select(
        nth_date=nth(lambda r: r.date, lambda r: 4000)
        >> over(
            Window.range(
                partition_by=[lambda r: r.name],
                preceding=lambda r: 200,
                following=lambda r: 1000,
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
