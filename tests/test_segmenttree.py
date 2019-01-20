import pytest

from stupidb.aggregation import First, Last
from stupidb.segmenttree import SegmentTree


@pytest.mark.parametrize(
    ('start', 'stop', 'expected'),
    [
        (0, 3, 1),
        (1, 3, 2),
        (2, 3, 3),
    ]
)
def test_segment_tree_first(start, stop, expected):
    tree = SegmentTree([(1,), (2,), (3,)], First)
    result = tree.query(start, stop)
    assert result == expected


@pytest.mark.parametrize(
    ('start', 'stop', 'expected'),
    [
        (0, 3, 3),
        (0, 2, 2),
        (0, 1, 1),
        (1, 3, 3),
        (1, 2, 2),
        (2, 3, 3),
    ]
)
def test_segment_tree_last(start, stop, expected):
    tree = SegmentTree([(1,), (2,), (3,)], Last)
    result = tree.query(start, stop)
    assert result == expected