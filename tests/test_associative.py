from stupidb.associative.segmenttree import SegmentTree
from stupidb.functions.associative import Count, Mean, Min, SampleCovariance, Sum, Total


def test_repr_segment_tree_fanout_2() -> None:
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


def test_repr_segment_tree_fanout_4() -> None:
    tree = SegmentTree([(1,), (2,), (3,)], aggregate_type=Sum, fanout=4)
    result = repr(tree)
    expected = """\
|-- Sum(total=6, count=3)
    |-- Sum(total=1, count=1)
    |-- Sum(total=2, count=1)
    |-- Sum(total=3, count=1)
    |-- Sum(total=None, count=0)"""
    assert result == expected


def test_count_repr() -> None:
    count = Count[str]()
    assert repr(count) == "Count(count=0)"
    count.step("a")
    assert repr(count) == "Count(count=1)"


def test_sum_repr() -> None:
    sum = Sum[int, int]()
    assert repr(sum) == "Sum(total=None, count=0)"
    sum.step(1)
    assert repr(sum) == "Sum(total=1, count=1)"


def test_total_repr() -> None:
    total = Total[int, int]()
    assert repr(total) == "Total(total=0, count=0)"
    total.step(1)
    assert repr(total) == "Total(total=1, count=1)"


def test_mean_repr() -> None:
    mean = Mean[int, float]()
    assert repr(mean) == "Mean(total=None, count=0, mean=None)"
    mean.step(3)
    assert repr(mean) == "Mean(total=3, count=1, mean=3.0)"
    mean.step(4)
    assert repr(mean) == "Mean(total=7, count=2, mean=3.5)"


def test_min_repr() -> None:
    min = Min()
    assert repr(min) == "Min(current_value=None)"
    min.step(2)
    assert repr(min) == "Min(current_value=2)"
    min.step(1)
    assert repr(min) == "Min(current_value=1)"
    min.step(3)
    assert repr(min) == "Min(current_value=1)"


def test_sample_covariance_repr() -> None:
    cov = SampleCovariance[float, float]()
    assert repr(cov) == "SampleCovariance(mean_x=0.0, mean_y=0.0, cov=0.0, count=0)"
    cov.step(1.0, 2.0)
    assert repr(cov) == "SampleCovariance(mean_x=2.0, mean_y=2.0, cov=0.0, count=1)"
    cov.step(3.0, 4.5)
    assert repr(cov) == "SampleCovariance(mean_x=5.0, mean_y=3.25, cov=1.25, count=2)"
