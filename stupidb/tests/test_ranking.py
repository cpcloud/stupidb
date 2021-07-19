from stupidb.api import (
    Window,
    dense_rank,
    order_by,
    over,
    rank,
    row_number,
    select,
    table,
)
from stupidb.ranking import Sentinel

from .conftest import assert_rowset_equal


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
    query = table(rows) >> select(name=lambda r: r.name, ranked=rank() >> over(window))
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


def test_sentinel():
    sen = Sentinel()
    assert repr(sen) == "Sentinel()"
    assert sen == sen
    assert not (sen != sen)
