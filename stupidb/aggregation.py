"""Algorithsm for aggregation."""

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
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import toolz

from stupidb.aggregatetypes import (
    BinaryAggregate,
    NullaryAggregate,
    TernaryAggregate,
    UnaryAggregate,
)
from stupidb.aggregator import Aggregator
from stupidb.row import AbstractRow
from stupidb.typehints import (
    Following,
    OrderBy,
    OrderingKey,
    PartitionBy,
    Preceding,
)

T = TypeVar("T")

ConcreteAggregate = TypeVar(
    "ConcreteAggregate",
    NullaryAggregate,
    UnaryAggregate,
    BinaryAggregate,
    TernaryAggregate,
)

StartStop = typing.NamedTuple("StartStop", [("start", int), ("stop", int)])
Ranges = Tuple[StartStop, StartStop, StartStop]
AggregationResultPair = Tuple[int, T]


class FrameClause(abc.ABC):
    def __init__(
        self,
        order_by: Collection[OrderBy],
        partition_by: Collection[PartitionBy],
        preceding: Optional[Preceding],
        following: Optional[Following],
    ) -> None:
        self.order_by = order_by
        self.partition_by = partition_by
        self.preceding = preceding
        self.following = following

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


class AggregateSpecification(Generic[ConcreteAggregate]):
    __slots__ = "aggregate", "getters"

    def __init__(
        self, aggregate: Type[ConcreteAggregate], getters: Tuple[Getter, ...]
    ) -> None:
        self.aggregate: Type[ConcreteAggregate] = aggregate
        self.getters = getters


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


class WindowAggregateSpecification(Generic[ConcreteAggregate]):
    __slots__ = "aggregate", "getters", "frame_clause"

    def __init__(
        self,
        aggregate: Type[ConcreteAggregate],
        getters: Tuple[Getter, ...],
        frame_clause: FrameClause,
    ) -> None:
        self.aggregate: Type[ConcreteAggregate] = aggregate
        self.getters = getters
        self.frame_clause = frame_clause

    def compute(self, rows: Iterable[AbstractRow]) -> Iterator[T]:
        """Aggregate `rows` over a window."""
        frame_clause = self.frame_clause
        partition_by = frame_clause.partition_by
        order_by = frame_clause.order_by

        # A mapping from each row's partition key to a list of rows in that
        # partition.
        partitions: Dict[
            Tuple[Hashable, ...], List[Tuple[int, AbstractRow]]
        ] = {}

        # Generate names for temporary order by columns, users never see these.
        #
        # TODO: If we had static schema information these wouldn't be necessary
        # in cases where the ordering keys are named columns (either physical
        # or computed)
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

        # (row_id, value) pairs containing the aggregation results
        results: List[AggregationResultPair] = []

        # Aggregate over each partition
        for partition_key, possible_peers in partitions.items():
            # Pull out the arguments using the user provided getter functions.
            # We only need to do this once per partition, because that's the
            # only time the arguments potentially change.
            arguments = [
                tuple(getter(peer) for getter in self.getters)
                for _, peer in possible_peers
            ]

            # Construct an aggregator for the function being computed
            #
            # For navigation functions like lead, lag, first, last and nth, we
            # construct a simple structure that computes the current value
            # of the navigation function given the inputs.
            #
            # For associative aggregations we construct a segment tree using
            # `arguments` as the leaves, with `aggregate` instances as the
            # interior nodes. Each node (both leaves and non-leaves) is a state
            # of the aggregation. The leaves are the initial states, the root
            # is the final state.
            aggregate = self.aggregate
            aggregator: Aggregator[ConcreteAggregate, T] = aggregate.prepare(
                arguments
            )

            # For every row in the set of possible peers of the current row
            # compute the window frame, and query the aggregator for the value
            # of the aggregation within that frame.
            for row_id_in_partition, (table_row_index, row) in enumerate(
                possible_peers
            ):
                start, stop = frame_clause.compute_window_frame(
                    possible_peers, row, row_id_in_partition, order_by_columns
                )
                result = aggregator.query(start, stop)
                results.append((table_row_index, result))

        # Sort the results in order of the child relation, because we processed
        # them in partition order, which might not be the same. Pull out the
        # second element of each AggregationResultPair in results.
        return map(toolz.second, sorted(results, key=operator.itemgetter(0)))
