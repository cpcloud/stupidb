import abc
import bisect
import operator
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
import toolz

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


class UnaryWindowAggregate(UnaryAggregate[Input1, Output]):
    __slots__ = ()

    @abc.abstractmethod
    def inverse(self, input1: Input1) -> None:
        ...

    def value(self) -> Optional[Output]:
        return self.finalize()


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


class BinaryWindowAggregate(BinaryAggregate[Input1, Input2, Output]):
    __slots__ = ()

    @abc.abstractmethod
    def inverse(
        self, input1: Optional[Input1], input2: Optional[Input2]
    ) -> None:
        ...

    def value(self) -> Optional[Output]:
        return self.finalize()


Aggregate = Union[UnaryAggregate, BinaryAggregate]
WindowAggregate = Union[UnaryWindowAggregate, BinaryWindowAggregate]

# TODO: give these meaningful names by using typing.NamedTuple
Ranges = Tuple[Tuple[int, int], Tuple[int, int], Tuple[int, int]]


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
        current_start_stop: Optional[Tuple[int, int]],
    ) -> Ranges:
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
        Ranges

        """
        current_row_order_by_value, order_by_values = self.setup_window(
            possible_peers, current_row, order_by_columns
        )
        if current_start_stop is not None:
            current_start, current_stop = current_start_stop
        else:
            current_start = current_stop = 0

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

        new_start = max(start, 0)
        new_stop = min(stop, npeers)

        # current_start can at least be equal to new_start. This will produce a
        # range whose start is larger than it's stop, e.g., seq[1:0]. This
        # results in removal of no rows, which is what we want when the window
        # has only increased in size and not shrunk, we would use max here if
        # for a language that doesn't have the same slice semantics as Python.
        #
        # we track the absolute range so that we can determine how the window
        # changes throughout the iteration over the partitions
        range_to_remove = (current_start, new_start - current_start)
        range_to_add = (current_stop, new_stop)
        absolute_range = (new_start, new_stop)
        return range_to_remove, range_to_add, absolute_range


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
        assert preceding is not None, "preceding function is None"
        value_to_find = current_row_order_by_value - preceding(current_row)
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
        assert following is not None, "following function is None"
        value_to_find = current_row_order_by_value + following(current_row)
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
    aggregate = attr.ib(type=Type[WindowAggregate])  # type: ignore
    getters = attr.ib(type=Tuple[Getter, ...])  # type: ignore
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
        rows_for_partition = (
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

        # partition
        for table_row_index, row in rows_for_partition:
            partition_key = compute_partition_key(row, partition_by)
            partitions.setdefault(partition_key, []).append(
                (table_row_index, row)
            )

        # sort and compute the location of each row in its partition
        key = make_key_func(order_by_columns)
        for partition_key in partitions.keys():
            partitions[partition_key].sort(key=key)

        results: List[Tuple[int, Any]] = []
        most_recent_start_stop: MutableMapping[int, Tuple[int, int]] = {}
        for partition_id, (partition_key, possible_peers) in enumerate(
            partitions.items()
        ):
            agg = self.aggregate()
            for row_id_in_partition, (table_row_index, row) in enumerate(
                possible_peers
            ):
                current_start_stop = most_recent_start_stop.get(partition_id)
                (range_to_remove_start, range_to_remove_stop), (
                    range_to_add_start,
                    range_to_add_stop,
                ), (
                    absolute_start,
                    absolute_stop,
                ) = frame_clause.compute_window_frame_bounds(
                    possible_peers,
                    row,
                    row_id_in_partition,
                    order_by_columns,
                    current_start_stop,
                )
                assert (
                    0 <= absolute_start <= absolute_stop <= len(possible_peers)
                ), f"start == {absolute_start}, stop == {absolute_stop}"

                most_recent_start_stop[partition_id] = (
                    absolute_start,
                    absolute_stop,
                )

                # Aggregate over the rows in the frame.
                for _, peer in possible_peers[
                    range_to_remove_start:range_to_remove_stop
                ]:
                    args = [getter(peer) for getter in self.getters]
                    agg.inverse(*args)

                for _, peer in possible_peers[
                    range_to_add_start:range_to_add_stop
                ]:
                    args = [getter(peer) for getter in self.getters]
                    agg.step(*args)

                result = agg.value()
                results.append((table_row_index, result))
        return map(toolz.second, sorted(results, key=operator.itemgetter(0)))


class Count(UnaryWindowAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def finalize(self) -> Optional[int]:
        return self.count

    def inverse(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count -= 1


class Sum(UnaryWindowAggregate[R1, R2]):
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

    def inverse(self, input1: Input1) -> None:
        if input1 is not None:
            self.total -= input1
            self.count -= 1


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
