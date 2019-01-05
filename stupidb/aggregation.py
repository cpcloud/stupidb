import abc
import collections
import itertools
import typing
from operator import itemgetter
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
    Sequence,
    Tuple,
    Type,
    Union,
)

import attr
import toolz

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
    R1,
    R2,
)


class UnaryAggregate(Generic[Input1, Output], metaclass=abc.ABCMeta):
    __slots__ = 'count',

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


class BinaryAggregate(Generic[Input1, Input2, Output], metaclass=abc.ABCMeta):
    __slots__ = 'count',

    def __init__(self) -> None:
        self.count = 0

    @abc.abstractmethod
    def step(self, input1: Optional[Input1], input2: Optional[Input2]) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> Optional[Output]:
        ...


Aggregate = Union[UnaryAggregate, BinaryAggregate]


@attr.s(frozen=True, slots=True)
class FrameClause(abc.ABC):
    order_by = attr.ib(converter=list, type=Iterable[OrderBy])
    partition_by = attr.ib(converter=list, type=Iterable[PartitionBy])
    preceding = attr.ib(type=Preceding)
    following = attr.ib(type=Following)

    @abc.abstractmethod
    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: Sequence[str],
    ) -> Tuple[int, int]:
        ...


@attr.s(frozen=True, slots=True)
class RowsMode(FrameClause):
    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: Sequence[str],
    ) -> Tuple[int, int]:
        current_row = possible_peers[partition_id]
        npeers = len(possible_peers)
        preceding = self.preceding
        if preceding is not None:
            start = max(partition_id - preceding(current_row), 0)
        else:
            start = 0

        following = self.following
        if following is not None:
            # because of zero-based indexing we must add one to `stop` to make
            # sure the current row is included
            stop = min(partition_id + following(current_row) + 1, npeers)
        else:
            stop = npeers
        return start, stop


@attr.s(frozen=True, slots=True)
class RangeMode(FrameClause):
    def find_partition_begin(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_column: str,
    ) -> int:
        preceding = self.preceding
        assert preceding is not None, "preceding is None"

        current_row = possible_peers[partition_id]
        last_peer = partition_id + 1  # include the current row
        peers = possible_peers[:last_peer]
        delta_preceding = preceding(current_row)
        current_row_order_by_value = current_row[order_by_column]
        return toolz.first(
            index
            for index, order_by_value in enumerate(
                map(itemgetter(order_by_column), peers)
            )
            if current_row_order_by_value - order_by_value <= delta_preceding
        )

    def find_partition_end(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_column: str,
    ) -> int:
        following = self.following
        assert following is not None, "following is None"

        current_row = possible_peers[partition_id]
        delta_following = following(current_row)
        current_row_order_by_value = current_row[order_by_column]
        npeers = len(possible_peers)
        indexes = range(npeers - 1, partition_id - 1, -1)
        return toolz.first(
            # add one to make sure we include the current row
            index + 1
            for index, peer in zip(
                indexes, map(possible_peers.__getitem__, indexes)
            )
            if peer[order_by_column] - current_row_order_by_value
            <= delta_following
        )

    def compute_window_frame(
        self,
        possible_peers: Sequence[Row],
        partition_id: int,
        order_by_columns: Sequence[str],
    ) -> Tuple[int, int]:
        ncolumns = len(order_by_columns)
        if ncolumns != 1:
            raise ValueError(
                "Must have exactly one order by column to use range mode. "
                f"Got {ncolumns:d}."
            )
        npeers = len(possible_peers)
        preceding = self.preceding
        order_by_column, = order_by_columns

        if preceding is not None:
            start = self.find_partition_begin(
                possible_peers, partition_id, order_by_column
            )
        else:
            start = 0

        following = self.following
        if following is not None:
            stop = self.find_partition_end(
                possible_peers, partition_id, order_by_column
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


@attr.s(frozen=True, slots=True)
class AbstractAggregateSpecification:
    aggregate = attr.ib(type=Type[Aggregate])
    getters = attr.ib(type=Tuple[Getter, ...])


@attr.s(frozen=True, slots=True)
class AggregateSpecification(AbstractAggregateSpecification):
    pass


def compute_partition_key(
    row: Row, partition_by: Iterable[PartitionBy]
) -> Tuple[Hashable, ...]:
    return tuple(partition_func(row) for partition_func in partition_by)


@attr.s(frozen=True, slots=True)
class WindowAggregateSpecification(AbstractAggregateSpecification):
    frame_clause = attr.ib(type=FrameClause)

    def compute(self, rows: Iterable[Row]) -> Iterator[Any]:
        """Aggregate `rows` over a window."""
        frame_clause = self.frame_clause
        partition_by = frame_clause.partition_by
        order_by = frame_clause.order_by

        # A mapping from each row's partition key to a list of rows.
        raw_partitions: Mapping[
            Tuple[Hashable, ...], List[Row]
        ] = collections.defaultdict(list)

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
            raw_partitions[partition_key].append(row)

        partitions = dict(raw_partitions)

        # sort
        for partition_key in partitions.keys():
            partitions[partition_key].sort(
                key=lambda row: tuple(
                    row[order_by_column]
                    for order_by_column in order_by_columns
                )
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

            peers = possible_peers[start:stop]

            # Aggregate over the rows in the frame.
            agg = self.aggregate()
            for peer in peers:
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
    __slots__ = 'total',

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
    __slots__ = 'meanx', 'meany', 'cov', 'ddof'

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
