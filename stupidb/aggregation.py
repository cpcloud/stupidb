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
        self.order_by = list(order_by)
        self.partition_by = list(partition_by)
        self.preceding = preceding
        self.following = following
        ...

    @abc.abstractmethod
    def compute_window_frame(
        self,
        possible_peers: List[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> Tuple[Optional[int], Optional[int]]:
        ...


class RowsMode(FrameClause):
    def compute_window_frame(
        self,
        possible_peers: List[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> Tuple[Optional[int], Optional[int]]:
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


class RangeMode(FrameClause):
    def find_partition_begin(
        self,
        possible_peers: List[Row],
        partition_id: int,
        order_by_column: str,
    ) -> int:
        preceding = self.preceding
        assert preceding is not None

        current_row = possible_peers[partition_id]
        peers = possible_peers[: partition_id + 1]
        delta_preceding = preceding(current_row)
        current_row_order_by_value = current_row[order_by_column]
        for index, peer in enumerate(peers):
            order_by_value = peer[order_by_column]
            if current_row_order_by_value - order_by_value <= delta_preceding:
                return index
        return -1

    def find_partition_end(
        self,
        possible_peers: List[Row],
        partition_id: int,
        order_by_column: str,
    ) -> int:
        following = self.following
        assert following is not None

        current_row = possible_peers[partition_id]
        npeers = len(possible_peers)
        indexes = range(npeers - 1, partition_id - 1, -1)
        delta_following = following(current_row)
        current_row_order_by_value = current_row[order_by_column]
        for index in indexes:
            peer = possible_peers[index]
            order_by_value = peer[order_by_column]
            if order_by_value - current_row_order_by_value <= delta_following:
                return index + 1
        return -1

    def compute_window_frame(
        self,
        possible_peers: List[Row],
        partition_id: int,
        order_by_columns: List[str],
    ) -> Tuple[Optional[int], Optional[int]]:
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
            start, stop = self.frame_clause.compute_window_frame(
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
