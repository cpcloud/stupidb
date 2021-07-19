# -*- coding: utf-8 -*-

from stupidb.associative import (
    Count,
    Mean,
    Min,
    SampleCovariance,
    SegmentTree,
    Sum,
    Total,
)


def test_repr_segment_tree_fanout_2():
    tree = SegmentTree([(1,), (2,), (3,)], aggregate_type=Sum, fanout=2)
    result = repr(tree)
    expected = """\
|-- Sum(total=6, count=3)
    |-- Sum(total=3, count=2)
        |-- Sum(total=1, count=1)
        |-- Sum(total=2, count=1)
    |-- Sum(total=3, count=1)
        |-- Sum(total=3, count=1)
        |-- Sum(total=None, count=0)"""
    assert result == expected


def test_repr_segment_tree_fanout_4():
    tree = SegmentTree([(1,), (2,), (3,)], aggregate_type=Sum, fanout=4)
    result = repr(tree)
    expected = """\
|-- Sum(total=6, count=3)
    |-- Sum(total=1, count=1)
    |-- Sum(total=2, count=1)
    |-- Sum(total=3, count=1)
    |-- Sum(total=None, count=0)"""
    assert result == expected


def test_count_repr():
    count = Count()
    assert repr(count) == "Count(count=0)"
    count.step("a")
    assert repr(count) == "Count(count=1)"


def test_sum_repr():
    sum = Sum()
    assert repr(sum) == "Sum(total=None, count=0)"
    sum.step(1)
    assert repr(sum) == "Sum(total=1, count=1)"


def test_total_repr():
    total = Total()
    assert repr(total) == "Total(total=0, count=0)"
    total.step(1)
    assert repr(total) == "Total(total=1, count=1)"


def test_mean_repr():
    mean = Mean()
    assert repr(mean) == "Mean(total=None, count=0, mean=None)"
    mean.step(3)
    assert repr(mean) == "Mean(total=3, count=1, mean=3.0)"
    mean.step(4)
    assert repr(mean) == "Mean(total=7, count=2, mean=3.5)"


def test_min_repr():
    min = Min()
    assert repr(min) == "Min(current_value=None)"
    min.step(2)
    assert repr(min) == "Min(current_value=2)"
    min.step(1)
    assert repr(min) == "Min(current_value=1)"
    min.step(3)
    assert repr(min) == "Min(current_value=1)"


def test_sample_covariance_repr():
    cov = SampleCovariance()
    assert repr(cov) == "SampleCovariance(meanx=0.0, meany=0.0, cov=0.0, count=0)"
    cov.step(1.0, 2.0)
    assert repr(cov) == "SampleCovariance(meanx=2.0, meany=2.0, cov=0.0, count=1)"
    cov.step(3.0, 4.5)
    assert repr(cov) == "SampleCovariance(meanx=5.0, meany=3.25, cov=1.25, count=2)"
