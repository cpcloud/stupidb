import abc
import bisect
import itertools
import typing
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import attr

from stupidb.protocols import AdditiveWithInverse
from stupidb.row import Row
from stupidb.typehints import (
    R1,
    R2,
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
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


class BinaryAggregate(Generic[Input1, Input2, Output], metaclass=abc.ABCMeta):
    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


Aggregate = Union[UnaryAggregate, BinaryAggregate]
BeginEnd = Tuple[int, int]


@attr.s(frozen=True, slots=True)
class FrameClause(abc.ABC):
    order_by = attr.ib(converter=list, type=Collection[OrderBy])
    partition_by = attr.ib(converter=list, type=Iterable[PartitionBy])
    preceding = attr.ib(type=Preceding)
    following = attr.ib(type=Following)

    @abc.abstractmethod
    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> BeginEnd:
        ...


@attr.s(frozen=True, slots=True)
class RowsMode(FrameClause):
    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> BeginEnd:
        current_row = possible_peers[partition_id]
        npeers = len(possible_peers)
        preceding = self.preceding
        if preceding is not None:
            start = max(
                partition_id - typing.cast(int, preceding(current_row)), 0
            )
        else:
            start = 0

        following = self.following
        if following is not None:
            # because of zero-based indexing we must add one to `stop` to make
            # sure the current row is included
            stop = min(
                partition_id + typing.cast(int, following(current_row)) + 1,
                npeers,
            )
        else:
            stop = npeers
        return start, stop


@attr.s(frozen=True, slots=True)
class RangeMode(FrameClause):
    def find_partition_begin(
        self,
        current_row: Row,
        current_row_order_by_value: AdditiveWithInverse,
        order_by_values: List[AdditiveWithInverse],
    ) -> int:
        preceding = self.preceding
        assert preceding is not None
        delta_preceding = preceding(current_row)
        value_to_find = current_row_order_by_value - delta_preceding
        bisected_index = bisect.bisect_left(order_by_values, value_to_find)
        return bisected_index

    def find_partition_end(
        self,
        current_row: Row,
        current_row_order_by_value: AdditiveWithInverse,
        order_by_values: List[AdditiveWithInverse],
    ) -> int:
        following = self.following
        assert following is not None
        delta_following = following(current_row)
        value_to_find = current_row_order_by_value + delta_following
        bisected_index = bisect.bisect_right(order_by_values, value_to_find)
        return bisected_index

    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> BeginEnd:
        ncolumns = len(order_by_columns)
        if ncolumns != 1:
            raise ValueError(
                "Must have exactly one order by column to use range mode. "
                f"Got {ncolumns:d}."
            )
        npeers = len(possible_peers)
        preceding = self.preceding
        order_by_column, = order_by_columns
        order_by_values = [peer[order_by_column] for peer in possible_peers]
        current_row = possible_peers[partition_id]
        current_row_order_by_value = current_row[order_by_column]

        if preceding is not None:
            start = max(
                0,
                self.find_partition_begin(
                    current_row, current_row_order_by_value, order_by_values
                ),
            )
        else:
            start = 0

        following = self.following
        if following is not None:
            stop = min(
                npeers,
                self.find_partition_end(
                    current_row, current_row_order_by_value, order_by_values
                ),
            )
        else:
            stop = npeers
        return start, stop


@attr.s(frozen=True, slots=True)
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


class AggregateSpecification:
    aggregate = attr.ib(type=Type[Aggregate])
    getters = attr.ib(type=Tuple[Getter, ...])


def compute_partition_key(
    row: Row, partition_by: Iterable[PartitionBy]
) -> Tuple[Hashable, ...]:
    return tuple(partition_func(row) for partition_func in partition_by)


@attr.s(frozen=True, slots=True)
class WindowAggregateSpecification(AggregateSpecification):
    frame_clause = attr.ib(type=FrameClause)

    def compute(self, rows: Iterable[Row]) -> Iterator[Any]:
        """Aggregate `rows` over a window."""
        frame_clause = self.frame_clause
        partition_by = frame_clause.partition_by
        order_by = frame_clause.order_by

        # A mapping from each row's partition key to a list of rows.
        partitions: Dict[Tuple[Hashable, ...], List[Row]] = {}

        order_by_columns = [f"_order_by_{i:d}" for i in range(len(order_by))]

        # Add computed order by columns that are used when evaluating window
        # functions in range mode
        # TODO: check that if in range mode we only have single order by
        rows_with_computed_order_by_columns = (
            row.merge(
                dict(
                    zip(
                        order_by_columns,
                        (order_func(row) for order_func in order_by),
                    )
                )
            )
            for row in rows
        )
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
        # We could unsort them (by storing their original index) to address
        # this, but it's less code this way and therefore maximally stupider.
        rows_for_partition, rows_for_frame_computation = itertools.tee(
            rows_with_computed_order_by_columns
        )

        # partition
        for i, row in enumerate(rows_for_partition):
            partition_key = compute_partition_key(row, partition_by)
            partitions.setdefault(partition_key, []).append(row)

        # sort
        for partition_key in partitions.keys():
            partitions[partition_key].sort(
                key=lambda row: tuple(map(row.__getitem__, order_by_columns))
            )

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
            start, stop = frame_clause.compute_window_frame(
                possible_peers, partition_id, order_by_columns
            )

            # Aggregate over the rows in the frame.
            agg = self.aggregate()
            for i in range(start, stop):
                peer = possible_peers[i]
                args = [getter(peer) for getter in self.getters]
                agg.step(*args)
            result = agg.finalize()
            yield result


class Count(UnaryAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def finalize(self) -> Optional[int]:
        return self.count


class Sum(UnaryAggregate[R1, R2]):
    __slots__ = ("total",)

    def __init__(self) -> None:
        super().__init__()
        self.total = typing.cast(R2, 0)

    def step(self, input1: Optional[R1]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else None


class Total(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else typing.cast(R2, 0)


class Mean(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        count = self.count
        return self.total / count if count > 0 else None


class Covariance(BinaryAggregate[R, R, float]):
    __slots__ = "meanx", "meany", "cov", "ddof"

    def __init__(self, *, ddof: int) -> None:
        super().__init__()
        self.meanx = 0.0
        self.meany = 0.0
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
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)
