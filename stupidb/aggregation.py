import abc
import bisect
import operator
import typing
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import attr
import toolz

from stupidb.protocols import Comparable
from stupidb.row import AbstractRow
from stupidb.segmenttree import (
    Aggregate,
    BinaryAggregate,
    SegmentTree,
    UnaryAggregate,
)
from stupidb.typehints import (
    R1,
    R2,
    Following,
    Input1,
    OrderBy,
    OrderingKey,
    PartitionBy,
    Preceding,
    R,
)

T = TypeVar("T")

StartStop = typing.NamedTuple("StartStop", [("start", int), ("stop", int)])
Ranges = Tuple[StartStop, StartStop, StartStop]


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
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:
        ...

    @abc.abstractmethod
    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:
        ...

    @abc.abstractmethod
    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:
        ...

    def compute_window_frame(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        row_id_in_partition: int,
        order_by_columns: Sequence[str],
    ) -> StartStop:
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
            if not all(order_by_values):
                # if we don't have an order by then all possible peers are the
                # actual peers of this row
                stop = npeers
            else:
                # default to the current row if following is not provided. This
                # is consistent with the defaults in at least PostgreSQL and
                # SQLite.
                stop = row_id_in_partition + 1

        new_start = max(start, 0)
        new_stop = min(stop, npeers)
        return StartStop(new_start, new_stop)


@attr.s(frozen=True, slots=True)
class RowsMode(FrameClause):
    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:
        preceding = self.preceding
        assert preceding is not None, "preceding is None"
        return row_id_in_partition - typing.cast(int, preceding(current_row))

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
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
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:
        cols = [
            tuple(map(peer.__getitem__, order_by_columns))
            for _, peer in possible_peers
        ]
        return tuple(map(current_row.__getitem__, order_by_columns)), cols


@attr.s(frozen=True, slots=True)
class RangeMode(FrameClause):
    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:
        # range mode allows no order by
        if not order_by_columns:
            return (), [()]

        ncolumns = len(order_by_columns)
        if ncolumns != 1:
            raise ValueError(
                "Must have exactly one order by column to use range mode. "
                f"Got {ncolumns:d}."
            )
        order_by_column, = order_by_columns
        order_by_values = [
            (peer[order_by_column],) for _, peer in possible_peers
        ]
        current_row_order_by_value = (current_row[order_by_column],)
        return current_row_order_by_value, order_by_values

    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_values: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
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
            current_row_order_by_values is not None
        ), "current_row_order_by_value is None"
        preceding = self.preceding
        assert preceding is not None, "preceding function is None"
        assert len(current_row_order_by_values) == 1
        current_row_order_by_value, = current_row_order_by_values
        value_to_find = current_row_order_by_value - preceding(current_row)
        bisected_index = bisect.bisect_left(
            [value for value, in order_by_values], value_to_find
        )
        return bisected_index

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_values: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
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
            current_row_order_by_values is not None
        ), "current_row_order_by_values is None"
        following = self.following
        assert following is not None, "following function is None"
        assert len(current_row_order_by_values) == 1
        current_row_order_by_value, = current_row_order_by_values
        value_to_find = current_row_order_by_value + following(current_row)
        bisected_index = bisect.bisect_right(
            [value for value, in order_by_values], value_to_find
        )
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
) -> Callable[[Tuple[int, AbstractRow]], OrderingKey]:
    def key(row_with_id: Tuple[int, AbstractRow]) -> OrderingKey:
        _, row = row_with_id
        return tuple(row[column] for column in order_by_columns)

    return key


@attr.s(frozen=True, slots=True)
class WindowAggregateSpecification(AggregateSpecification):
    aggregate = attr.ib(type=Type[Aggregate])  # type: ignore
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

        # sort
        key = make_key_func(order_by_columns)
        for partition_key in partitions.keys():
            partitions[partition_key].sort(key=key)

        results: List[Tuple[int, Any]] = []
        for partition_key, possible_peers in partitions.items():
            arguments = [
                tuple(getter(peer) for getter in self.getters)
                for _, peer in possible_peers
            ]
            tree = SegmentTree(arguments, self.aggregate)
            for row_id_in_partition, (table_row_index, row) in enumerate(
                possible_peers
            ):
                start, stop = frame_clause.compute_window_frame(
                    possible_peers, row, row_id_in_partition, order_by_columns
                )
                result = tree.query(start, stop)
                results.append((table_row_index, result))

        # Sort the results in order of the child relation, because we processed
        # them in partition order, which might not be the same
        return map(toolz.second, sorted(results, key=operator.itemgetter(0)))


class Count(UnaryAggregate[Input1, int]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if input1 is not None:
            self.count += 1

    def finalize(self) -> Optional[int]:
        return self.count

    def update(self, other: "Count[Input1]") -> None:
        self.count += other.count


class Sum(UnaryAggregate[R1, R2]):
    __slots__ = ("total",)

    def __init__(self) -> None:
        super().__init__()
        self.total = typing.cast(R2, 0)

    def __repr__(self) -> str:
        total = self.total
        count = self.count
        name = type(self).__name__
        return f"{name}(total={total}, count={count})"

    def step(self, input1: Optional[R1]) -> None:
        if input1 is not None:
            self.total += input1
            self.count += 1

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else None

    def update(self, other: "Sum[R1, R2]") -> None:
        self.total += other.total
        self.count += other.count


class Total(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        return self.total if self.count else typing.cast(R2, 0)


class Mean(Sum[R1, R2]):
    __slots__ = ()

    def finalize(self) -> Optional[R2]:
        count = self.count
        return self.total / count if count > 0 else None

    def __repr__(self) -> str:
        name = type(self).__name__
        total = self.total
        count = self.count
        return f"{name}(total={total}, count={count}, mean={total / count})"


class MinMax(UnaryAggregate[Comparable, Comparable]):
    __slots__ = "current_value", "comparator"

    def __init__(
        self, comparator: Callable[[Comparable, Comparable], Comparable]
    ) -> None:
        super().__init__()
        self.current_value: Optional[Comparable] = None
        self.comparator = comparator

    def step(self, input1: Optional[Comparable]) -> None:
        if input1 is not None:
            if self.current_value is None:
                self.current_value = input1
            else:
                self.current_value = self.comparator(
                    self.current_value, input1
                )

    def finalize(self) -> Optional[Comparable]:
        return self.current_value

    def update(self, other: "MinMax") -> None:
        assert self.comparator == other.comparator, (
            f"self.comparator == {self.comparator}, "
            f"other.comparator == {other.comparator}"
        )
        if self.current_value is not None and other.current_value is not None:
            self.current_value = self.comparator(
                self.current_value, other.current_value
            )

    def __repr__(self) -> str:
        name = type(self).__name__
        current_value = self.current_value
        comparator = self.comparator
        return (
            f"{name}(current_value={current_value}, "
            f"comparator={comparator})"
        )


class Min(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(min)


class Max(MinMax):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(max)


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

    def update(self, other: "Covariance[R]") -> None:
        raise NotImplementedError(
            "Covariance not yet implemented for segment tree"
        )


class SampleCovariance(Covariance):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=1)


class PopulationCovariance(Covariance):
    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(ddof=0)


class CurrentValueAggregate(UnaryAggregate[Input1, Input1]):
    __slots__ = ("current_value",)

    def __init__(self):
        self.current_value: Optional[Input1] = None

    def finalize(self) -> Optional[Input1]:
        return self.current_value


class First(CurrentValueAggregate[Input1]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        if self.current_value is None:
            self.current_value = input1

    def update(self, other: "First[Input1]") -> None:
        if self.current_value is None:
            self.current_value = other.current_value


class Last(CurrentValueAggregate[Input1]):
    __slots__ = ()

    def step(self, input1: Optional[Input1]) -> None:
        self.current_value = input1

    def update(self, other: "Last[Input1]") -> None:
        self.current_value = other.current_value


class Nth(BinaryAggregate[Input1, int, Input1]):
    __slots__ = "current_value", "current_index"

    def __init__(self):
        super().__init__()
        self.current_value: Optional[Input1] = None
        self.current_index = 0

    def step(self, input1: Optional[Input1], index: Optional[int]) -> None:
        if index is not None and index == self.current_index:
            self.current_value = input1
        self.current_index += 1

    def finalize(self) -> Optional[Input1]:
        return self.current_value

    def update(self, other: "Nth[Input1]") -> None:
        raise NotImplementedError("Nth not yet implemented for segment tree")
