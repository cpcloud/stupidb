import abc
import collections
import itertools
import typing
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
)

from stupidb.comparable import Comparable
from stupidb.row import Row
from stupidb.typehints import (
    Following,
    Input1,
    Input2,
    OrderBy,
    Output,
    PartitionBy,
    Preceding,
    R,
)


class UnaryAggregate(Generic[Input1, Output], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


class UnaryWindowAggregate(UnaryAggregate[Input1, Output]):
    @abc.abstractmethod
    def inverse(self, input1: Optional[Input1]) -> None:
        ...

    def value(self) -> Optional[Output]:
        return self.finalize()


class BinaryAggregate(Generic[Input1, Input2, Output]):
    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


class BinaryWindowAggregate(BinaryAggregate[Input1, Input2, Output]):
    @abc.abstractmethod
    def inverse(
        self, input1: Optional[Input1], input2: Optional[Input2]
    ) -> None:
        ...

    def value(self) -> Optional[Output]:
        return self.finalize()


Aggregate = Union[UnaryAggregate, BinaryAggregate]


class FrameClause(abc.ABC):
    def __init__(
        self,
        order_by: Iterable[OrderBy],
        partition_by: Iterable[PartitionBy],
        preceding: Optional[Preceding],
        following: Optional[Following],
    ) -> None:
        self._order_by = list(order_by)
        self._partition_by = list(partition_by)
        self._preceding = preceding
        self._following = following
        ...

    @abc.abstractmethod
    def compute_window_frame(
        self, current_row: Row, partition_id: int, possible_peers: List[Row]
    ) -> List[Row]:
        ...


class RowsMode(FrameClause):
    def compute_window_frame(
        self, current_row: Row, partition_id: int, possible_peers: List[Row]
    ) -> List[Row]:
        npeers = len(possible_peers)
        preceding = self._preceding
        if preceding is not None:
            start = max(partition_id - preceding(current_row), 0)
        else:
            start = 0

        following = self._following
        if following is not None:
            # because of zero-based indexing we must add one to `stop` to make
            # sure the current row is included
            stop = min(partition_id + following(current_row) + 1, npeers)
        else:
            stop = npeers
        return possible_peers[start:stop]


class RangeMode(FrameClause):
    pass


class Window:
    @staticmethod
    def rows(
        order_by: Iterable[OrderBy] = (),
        partition_by: Iterable[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RowsMode(order_by, partition_by, preceding, following)

    @staticmethod
    def range(
        order_by: Iterable[OrderBy] = (),
        partition_by: Iterable[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RangeMode(order_by, partition_by, preceding, following)


Getter = Callable[[Row], Any]


class AbstractAggregateSpecification:
    def __init__(self, aggregate: Type[Aggregate], *getters: Getter) -> None:
        self.aggregate = aggregate
        self.getters = getters


class AggregateSpecification(AbstractAggregateSpecification):
    pass


def compute_partition_key(
    row: Row, partition_by: Iterable[PartitionBy]
) -> Tuple[Hashable, ...]:
    return tuple(partition_func(row) for partition_func in partition_by)


def make_key_func(order_by: Iterable[OrderBy]) -> Callable[[Row], Comparable]:
    def key(row: Row) -> Tuple[Comparable, ...]:
        return tuple(order_func(row) for order_func in order_by)

    return key


class WindowAggregateSpecification(AbstractAggregateSpecification):
    def __init__(
        self,
        frame_clause: FrameClause,
        aggregate: Type[Aggregate],
        *getters: Getter,
    ) -> None:
        super().__init__(aggregate, *getters)
        self.frame_clause = frame_clause

    def compute(self, rows: Iterable[Row]) -> Iterator[Any]:
        """Aggregate `rows` over a window specified by `aggspec`."""
        frame_clause = self.frame_clause
        partition_by = frame_clause._partition_by
        order_by = frame_clause._order_by

        # A mapping from each row's partition key to a list of rows.
        raw_partitions: Mapping[
            Tuple[Hashable, ...], List[Row]
        ] = collections.defaultdict(list)

        # We use one iterator for partitioning and one for computing the frame
        # given a partition and a row.
        #
        # Alternatively we could reuse the rows that are in memory in the
        # partitions, but they aren't going to be in the order of the original
        # relation's rows.
        #
        # This leads to incorrect results when an aggregation is used
        # downstream.
        #
        # We could sort them to address this, but it's less code this way and I
        # suspect more efficient to dup the iterator.
        rows_for_partition, rows_for_frame_computation = itertools.tee(rows)

        # partition
        for row in rows_for_partition:
            partition_key = compute_partition_key(row, partition_by)
            raw_partitions[partition_key].append(row)

        partitions = dict(raw_partitions)

        # sort
        key_func = make_key_func(order_by)
        for partition_key in partitions.keys():
            partitions[partition_key].sort(key=key_func)

        for row in rows_for_frame_computation:
            # compute the partition the row is in
            partition_key = compute_partition_key(row, partition_by)

            # the maximal set of rows that could be in the current row's peer
            # set
            possible_peers = partitions[partition_key]

            # Compute the index of the row in the partition. This value is the
            # value that all peers (using preceding and following if they are
            # not None) are computed relative to.
            #
            # Stupidly, this is a linear search for a matching row and assumes
            # that there are no duplicate rows in `possible_peers`.
            partition_id = possible_peers.index(row)

            # Compute the window frame, which is a subset of `possible_peers`.
            peers = self.frame_clause.compute_window_frame(
                row, partition_id, possible_peers
            )

            # Aggregate over the rows in the frame.
            agg = self.aggregate()
            for peer in peers:
                args = [getter(peer) for getter in self.getters]
                agg.step(*args)
            result = agg.finalize()
            yield result


class Count(UnaryWindowAggregate[Input1, int]):
    def __init__(self) -> None:
        self.count = 0

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def inverse(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count -= 1

    def finalize(self) -> Optional[int]:
        return self.count


class Sum(UnaryWindowAggregate[R, R]):
    def __init__(self) -> None:
        self.total = typing.cast(R, 0)
        self.count = 0

    def step(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def inverse(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[R]:
        return self.total if self.count else None


class Total(Sum[R]):
    def finalize(self) -> Optional[R]:
        return self.total if self.count else typing.cast(R, 0)


class Mean(UnaryWindowAggregate[R, float]):
    def __init__(self) -> None:
        self.total = 0.0
        self.count = 0

    def step(self, value: Optional[R]) -> None:
        if value is not None:
            self.total += typing.cast(float, value)
            self.count += 1

    def inverse(self, input1: Optional[R]) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1

    def finalize(self) -> Optional[float]:
        count = self.count
        return self.total / count if count > 0 else None


class Covariance(BinaryAggregate[R, R, float]):
    def __init__(self, *, ddof: int) -> None:
        self.meanx = 0.0
        self.meany = 0.0
        self.count = 0
        self.cov = 0.0
        self.ddof = ddof

    def step(self, x: Optional[R], y: Optional[R]) -> None:
        if x is not None and y is not None:
            self.count += 1
            count = self.count
            delta_x = x - self.meanx
            self.meanx += delta_x + count
            self.meany += (y - self.meany) / count
            self.cov += delta_x * (y - self.meany)

    def finalize(self) -> Optional[float]:
        denom = self.count - self.ddof
        return self.cov / denom if denom > 0 else None


class SampleCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance):
    def __init__(self) -> None:
        super().__init__(ddof=0)
