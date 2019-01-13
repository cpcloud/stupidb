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
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import attr

from stupidb.protocols import AdditiveWithInverse
from stupidb.row import AbstractRow
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
    order_by = attr.ib(type=Collection[OrderBy])
    partition_by = attr.ib(type=Collection[PartitionBy])
    preceding = attr.ib(type=Optional[Preceding])  # type: ignore
    following = attr.ib(type=Optional[Following])  # type: ignore

    @abc.abstractmethod
    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        ...

    @abc.abstractmethod
    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        ...

    @abc.abstractmethod
    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[Optional[AdditiveWithInverse], Sequence[AdditiveWithInverse]]:
        ...

    def compute_window_frame_bounds(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        row_id_in_partition: int,
        order_by_columns: Sequence[str],
    ) -> BeginEnd:
        """Compute the bounds of the window frame.

        Parameters
        ----------
        possible_peers
            The sequence of possible rows that the window could consist of.
        current_row
            The row relative to which we are computing the window.
        row_id_in_partition
            The zero-based index of `current_row` in possible_peers.
        order_by_columns
            The columns by which we have ordered our window.

        Returns
        -------
        BeginEnd
            A pair of integers, indicating the start and stop indicies of the
            window frame suitable to use as a slice.

        """
        current_row_order_by_value, order_by_values = self.setup_window(
            possible_peers, current_row, order_by_columns
        )

        preceding = self.preceding
        if preceding is not None:
            start = self.find_partition_begin(
                current_row,
                row_id_in_partition,
                current_row_order_by_value,
                order_by_values,
            )
        else:
            start = 0

        npeers = len(possible_peers)
        following = self.following
        if following is not None:
            stop = self.find_partition_end(
                current_row,
                row_id_in_partition,
                current_row_order_by_value,
                order_by_values,
            )
        else:
            stop = npeers
        return max(start, 0), min(stop, npeers)


@attr.s(frozen=True, slots=True)
class RowsMode(FrameClause):
    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        preceding = self.preceding
        assert preceding is not None, "preceding is None"
        return row_id_in_partition - typing.cast(int, preceding(current_row))

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        following = self.following
        assert following is not None, "following is None"
        return (
            row_id_in_partition + typing.cast(int, following(current_row)) + 1
        )

    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[Optional[AdditiveWithInverse], Sequence[AdditiveWithInverse]]:
        return None, ()


@attr.s(frozen=True, slots=True)
class RangeMode(FrameClause):
    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[Optional[AdditiveWithInverse], Sequence[AdditiveWithInverse]]:
        ncolumns = len(order_by_columns)
        if ncolumns != 1:
            raise ValueError(
                "Must have exactly one order by column to use range mode. "
                f"Got {ncolumns:d}."
            )
        order_by_column, = order_by_columns
        order_by_values = [peer[order_by_column] for _, peer in possible_peers]
        current_row_order_by_value = current_row[order_by_column]
        return current_row_order_by_value, order_by_values

    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        """Find the beginning of a window in a partition.

        Parameters
        ----------
        current_row
            The row relative to which we are computing the window.
        row_id_in_partition
            The zero-based index of `current_row` in possible_peers.
        current_row_order_by_value
            The value of the ORDER BY key in the current row.
        order_by_values
            The order by values for the current partition.

        Returns
        -------
        int
            The start point of the window in the current partition

        """
        assert (
            current_row_order_by_value is not None
        ), "current_row_order_by_value is None"
        preceding = self.preceding
        assert preceding is not None, "preceding is None"
        delta_preceding = preceding(current_row)
        value_to_find = current_row_order_by_value - delta_preceding
        bisected_index = bisect.bisect_left(order_by_values, value_to_find)
        return bisected_index

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[AdditiveWithInverse],
        order_by_values: Sequence[AdditiveWithInverse],
    ) -> int:
        """Find the end of a window in a partition.

        Parameters
        ----------
        current_row
            The row relative to which we are computing the window.
        row_id_in_partition
            The zero-based index of `current_row` in possible_peers.
        current_row_order_by_value
            The value of the ORDER BY key in the current row.
        order_by_values
            The order by values for the current partition.

        Returns
        -------
        int
            The end point of the window in the current partition

        """
        assert (
            current_row_order_by_value is not None
        ), "current_row_order_by_value"
        following = self.following
        assert following is not None, "following is None"
        delta_following = following(current_row)
        value_to_find = current_row_order_by_value + delta_following
        bisected_index = bisect.bisect_right(order_by_values, value_to_find)
        return bisected_index


@attr.s(frozen=True, slots=True)
class Window:
    @staticmethod
    def rows(
        order_by: Collection[OrderBy] = (),
        partition_by: Collection[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RowsMode(order_by, partition_by, preceding, following)

    @staticmethod
    def range(
        order_by: Collection[OrderBy] = (),
        partition_by: Collection[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
    ) -> FrameClause:
        return RangeMode(order_by, partition_by, preceding, following)


Getter = Callable[[AbstractRow], Any]


@attr.s(frozen=True, slots=True)
class AggregateSpecification:
    aggregate = attr.ib(type=Type[Aggregate])  # type: ignore
    getters = attr.ib(type=Tuple[Getter, ...])  # type: ignore


def compute_partition_key(
    row: AbstractRow, partition_by: Iterable[PartitionBy]
) -> Tuple[Hashable, ...]:
    return tuple(partition_func(row) for partition_func in partition_by)


def make_key_func(
    order_by_columns: Sequence[str],
) -> Callable[[Tuple[int, AbstractRow]], Tuple[AdditiveWithInverse, ...]]:
    def key(
        row_with_id: Tuple[int, AbstractRow]
    ) -> Tuple[AdditiveWithInverse, ...]:
        _, row = row_with_id
        return tuple(row[column] for column in order_by_columns)

    return key


@attr.s(frozen=True, slots=True)
class WindowAggregateSpecification(AggregateSpecification):
    frame_clause = attr.ib(type=FrameClause)

    def compute(self, rows: Iterable[AbstractRow]) -> Iterator[Any]:
        """Aggregate `rows` over a window."""
        frame_clause = self.frame_clause
        partition_by = frame_clause.partition_by
        order_by = frame_clause.order_by

        # A mapping from each row's partition key to a list of rows.
        partitions: Dict[
            Tuple[Hashable, ...], List[Tuple[int, AbstractRow]]
        ] = {}

        order_by_columns = [f"_order_by_{i:d}" for i in range(len(order_by))]

        # Add computed order by columns that are used when evaluating window
        # functions in range mode
        # TODO: check that if in range mode we only have single order by
        rows_with_computed_order_by_columns = (
            (
                i,
                row.merge(
                    dict(
                        zip(
                            order_by_columns,
                            (order_func(row) for order_func in order_by),
                        )
                    )
                ),
            )
            for i, row in enumerate(rows)
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

        id_mapping: MutableMapping[Tuple[int, Tuple[Hashable, ...]], int] = {}

        # partition
        for i, row in rows_for_partition:
            partition_key = compute_partition_key(row, partition_by)
            partitions.setdefault(partition_key, []).append((i, row))

        # sort and compute the location of each row in its partition
        key = make_key_func(order_by_columns)
        for partition_key in partitions.keys():
            rows_in_partition = partitions[partition_key]
            rows_in_partition.sort(key=key)
            for partition_id, (row_id, _) in enumerate(rows_in_partition):
                id_mapping[row_id, partition_key] = partition_id

        for i, row in rows_for_frame_computation:
            # compute the partition the row is in
            partition_key = compute_partition_key(row, partition_by)

            # the maximal set of rows that could be in the current row's peer
            # set
            possible_peers = partitions[partition_key]

            # Compute the index of the row in the partition. This value is the
            # value that all peers (using preceding and following if they are
            # not None) are computed relative to.
            row_id_in_partition = id_mapping[i, partition_key]

            # Compute the window frame bounds, which is a subset of
            # `possible_peers`.
            start, stop = frame_clause.compute_window_frame_bounds(
                possible_peers, row, row_id_in_partition, order_by_columns
            )
            assert (
                0 <= start <= stop <= len(possible_peers)
            ), f"start == {start}, stop == {stop}"

            # Aggregate over the rows in the frame.
            agg = self.aggregate()
            for i in range(start, stop):
                _, peer = possible_peers[i]
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
