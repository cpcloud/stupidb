"""Algorithms for aggregation."""

import abc
import bisect
import enum
import functools
import operator
import typing
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from .aggregatetypes import Aggregate
from .aggregator import Aggregator
from .associative import BinaryAssociativeAggregate, UnaryAssociativeAggregate
from .navigation import (
    BinaryNavigationAggregate,
    TernaryNavigationAggregate,
    UnaryNavigationAggregate,
)
from .ranking import RankingAggregate
from .row import AbstractRow
from .typehints import Following, OrderBy, OrderingKey, PartitionBy, Preceding, T


class StartStop(NamedTuple):
    """A class to hold start and stop values for a range."""

    start: int
    stop: int


Ranges = Tuple[StartStop, StartStop, StartStop]


@enum.unique
class Nulls(enum.Enum):
    """An enumeration indicating how to handle null values when sorting."""

    FIRST = -1
    LAST = 1


class FrameClause(abc.ABC):
    """Class for computing frame boundaries."""

    __slots__ = "order_by", "partition_by", "preceding", "following", "nulls"

    def __init__(
        self,
        order_by: Sequence[OrderBy],
        partition_by: Sequence[PartitionBy],
        preceding: Optional[Preceding],
        following: Optional[Following],
        nulls: Nulls,
    ) -> None:
        self.order_by = order_by
        self.partition_by = partition_by
        self.preceding = preceding
        self.following = following
        self.nulls = nulls

    @abc.abstractmethod
    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
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

    @abc.abstractmethod
    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
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

    @abc.abstractmethod
    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:
        """Compute the current row's ordering keys."""

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
            The sequence of possible rows of which the window could consist.
        current_row
            The row relative to which we are computing the window.
        row_id_in_partition
            The zero-based index of `current_row` in possible_peers.
        order_by_columns
            The columns by which we have ordered our window, if any.

        Returns
        -------
        Ranges
            The start and stop of the window frame.

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
    """A frame clause implementation for window function ``ROWS`` mode.

    ``ROWS`` mode computes the window frame relative to the difference between
    the row index of the current row and what is given by ``preceding`` and
    ``following``.

    See Also
    --------
    RangeMode

    """

    __slots__ = ()

    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:  # noqa: D102
        preceding = self.preceding
        assert preceding is not None, "preceding is None"
        return row_id_in_partition - typing.cast(int, preceding(current_row))

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:  # noqa: D102
        following = self.following
        assert following is not None, "following is None"
        return row_id_in_partition + typing.cast(int, following(current_row)) + 1

    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:  # noqa: D102
        cols = [
            tuple(map(peer.__getitem__, order_by_columns)) for _, peer in possible_peers
        ]
        return tuple(map(current_row.__getitem__, order_by_columns)), cols


class RangeMode(FrameClause):
    """A frame clause implementation for window function ``RANGE`` mode.

    ``RANGE`` mode computes the window frame relative to the difference between
    ``preceding`` and ``following`` and the current row's ordering key.

    See Also
    --------
    RowsMode

    """

    __slots__ = ()

    def __init__(
        self,
        order_by: Sequence[OrderBy],
        partition_by: Sequence[PartitionBy],
        preceding: Optional[Preceding],
        following: Optional[Following],
        nulls: Nulls,
    ) -> None:
        n_order_by = len(order_by)
        if n_order_by > 1:
            raise ValueError(
                "Must have exactly ONE order by to use range windows. "
                f"Got {n_order_by:d} functions."
            )
        super().__init__(order_by, partition_by, preceding, following, nulls)

    def setup_window(
        self,
        possible_peers: Sequence[Tuple[int, AbstractRow]],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> Tuple[OrderingKey, Sequence[OrderingKey]]:  # noqa: D102
        # range mode allows no order by
        if not order_by_columns:
            return (), [()]

        ncolumns = len(order_by_columns)
        assert ncolumns == 1, f"ncolumns == {ncolumns:d}"
        (order_by_column,) = order_by_columns
        order_by_values = [(peer[order_by_column],) for _, peer in possible_peers]
        current_row_order_by_value = (current_row[order_by_column],)
        return current_row_order_by_value, order_by_values

    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_values: Optional[OrderingKey],
        order_by_values: Sequence[OrderingKey],
    ) -> int:  # noqa: D102
        assert (
            current_row_order_by_values is not None
        ), "current_row_order_by_value is None"
        preceding = self.preceding
        assert preceding is not None, "preceding function is None"
        if not current_row_order_by_values:
            return 0
        assert len(current_row_order_by_values) == 1
        (current_row_order_by_value,) = current_row_order_by_values
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
    ) -> int:  # noqa: D102
        assert (
            current_row_order_by_values is not None
        ), "current_row_order_by_values is None"
        following = self.following
        assert following is not None, "following function is None"
        if not current_row_order_by_values:
            return len(order_by_values)
        assert len(current_row_order_by_values) == 1
        (current_row_order_by_value,) = current_row_order_by_values
        value_to_find = current_row_order_by_value + following(current_row)
        bisected_index = bisect.bisect_right(
            [value for value, in order_by_values], value_to_find
        )
        return bisected_index


class Window:
    """A namespace class providing the user-facing API for windowing modes."""

    __slots__ = ()

    @staticmethod
    def rows(
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
        nulls: Nulls = Nulls.FIRST,
    ) -> FrameClause:
        """Construct a ``ROWS`` mode frame clause.

        ``ROWS`` windows are useful for computing over windows that can be
        determined by relative row index alone.

        See Also
        --------
        Window.range

        """
        return RowsMode(order_by, partition_by, preceding, following, nulls)

    @staticmethod
    def range(
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Optional[Preceding] = None,
        following: Optional[Following] = None,
        nulls: Nulls = Nulls.FIRST,
    ) -> FrameClause:
        """Construct a ``RANGE`` mode frame clause.

        ``RANGE`` windows can be used to compute over windows whose bounds are
        not easily determined by row number such as time based windows.

        See Also
        --------
        Window.rows

        """
        return RangeMode(order_by, partition_by, preceding, following, nulls)


Getter = Callable[[AbstractRow], Any]
ConcreteAggregate = TypeVar(
    "ConcreteAggregate",
    UnaryAssociativeAggregate,
    BinaryAssociativeAggregate,
    UnaryNavigationAggregate,
    BinaryNavigationAggregate,
    TernaryNavigationAggregate,
    RankingAggregate,
)


class AggregateSpecification(Generic[ConcreteAggregate]):
    """Specification for computing a (non-windowed) aggregation.

    Attributes
    ----------
    aggregate_type
        The aggregate class to use for aggregation.
    getters
        A tuple of callables used to produce the arguments for the aggregation.

    See Also
    --------
    WindowAggregateSpecification

    """

    __slots__ = "aggregate_type", "getters"

    def __init__(
        self,
        aggregate_type: Type[ConcreteAggregate],
        getters: Tuple[Getter, ...],
    ) -> None:
        self.aggregate_type: Type[ConcreteAggregate] = aggregate_type
        self.getters = getters


def compute_partition_key(
    row: AbstractRow, partition_by: Iterable[PartitionBy]
) -> Tuple[Hashable, ...]:
    """Compute a partition key from `row` and `partition_by`.

    Parameters
    ----------
    row
        An :class:`~stupidb.row.AbstractRow` instance.
    partition_by
        An iterable of :class:`~typing.Callable` taking a single
        :class:`~stupidb.row.AbstractRow` argument and returning a
        :class:`Hashable` object.

    """
    return tuple(partition_func(row) for partition_func in partition_by)


def row_key_compare(
    order_by: Sequence[OrderBy],
    null_ordering: Nulls,
    left_row: AbstractRow,
    right_row: AbstractRow,
) -> int:
    """Compare `left_row` and `right_row` using `order_by`.

    Notes
    -----
    ``NULL`` ordering is handled using `null_ordering`.

    """
    left_keys = [order_func(left_row) for order_func in order_by]
    right_keys = [order_func(right_row) for order_func in order_by]

    for left_key, right_key in zip(left_keys, right_keys):
        if left_key is None and right_key is not None:
            return null_ordering.value
        if left_key is not None and right_key is None:
            return -null_ordering.value
        if left_key is None and right_key is None:
            return 0

        assert left_key is not None, "left_key is None"
        assert right_key is not None, "right_key is None"
        if left_key < right_key:
            return -1
        if left_key > right_key:
            return 1
    return 0


def make_key_func(
    order_by: Sequence[OrderBy], nulls: Nulls
) -> Callable[[Tuple[int, AbstractRow]], OrderingKey[T]]:
    """Make a function usable with the key argument to sorting functions.

    This return value of this function can be passed to
    :func:`sorted`/:meth:`list.sort`.

    Parameters
    ----------
    order_by_columns
        A sequence of :class:`str` instances referring to the keys of an
        :class:`~stupidb.row.AbstractRow`.

    """

    def cmp(lefts: Tuple[int, AbstractRow], rights: Tuple[int, AbstractRow]) -> int:
        _, left_row = lefts
        _, right_row = rights
        return row_key_compare(order_by, nulls, left_row, right_row)

    return functools.cmp_to_key(cmp)  # type: ignore


class WindowAggregateSpecification(Generic[ConcreteAggregate]):
    """A specification for a window aggregate.

    Attributes
    ----------
    aggregate_type
        The class of :data:`~stupidb.aggregation.ConcreteAggregate` to use for
        aggregation.
    getters
        A tuple of functions that produce single column values given an
        instance of :class:`~stupidb.row.AbstractRow`.
    frame_clause
        A thin struct encapsulating the details of the window such as ``ORDER
        BY`` (:attr:`stupidb.aggregation.FrameClause.order_by`), ``PARTITION
        BY`` (:attr:`stupidb.aggregation.FrameClause.partition_by`) and
        preceding and following.

    See Also
    --------
    stupidb.aggregation.FrameClause

    """

    __slots__ = "aggregate_type", "getters", "frame_clause"

    def __init__(
        self,
        aggregate_type: Type[ConcreteAggregate],
        getters: Tuple[Getter, ...],
        frame_clause: FrameClause,
    ) -> None:
        self.aggregate_type: Type[ConcreteAggregate] = aggregate_type
        self.getters = getters
        self.frame_clause = frame_clause

    def compute(self, rows: Iterable[AbstractRow]) -> Iterator[Optional[T]]:
        """Aggregate `rows` over a window, producing an iterator of results.

        Parameters
        ----------
        rows
            An :class:`~typing.Iterable` of rows.

        """
        frame_clause = self.frame_clause
        partition_by = frame_clause.partition_by
        order_by = frame_clause.order_by

        # A mapping from each row's partition key to a list of rows in that
        # partition.
        partitions: MutableMapping[
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
            partitions.setdefault(partition_key, []).append((table_row_index, row))

        # sort
        key = make_key_func(order_by, frame_clause.nulls)
        for partition_key in partitions.keys():
            partitions[partition_key].sort(key=key)

        # (row_id, value) pairs containing the aggregation results
        results: List[Tuple[int, Optional[T]]] = []

        # Aggregate over each partition
        aggregate_type = self.aggregate_type
        getters = self.getters
        for partition_key, possible_peers in partitions.items():
            # Construct an aggregator for the function being computed
            #
            # For navigation functions like lead, lag, first, last and nth, we
            # construct a simple structure that computes the current value
            # of the navigation function given the inputs. We use the same
            # approach for ranking functions such as row_number, rank, and
            # dense_rank.
            #
            # For associative aggregations we construct a segment tree
            # using `arguments` as the leaves, with `aggregate` instances as
            # the interior nodes. Each node (both leaves and non-leaves) is a
            # state of the aggregation. The leaves are the initial states, the
            # root is the final state.
            aggregator: Aggregator[Aggregate, T] = aggregate_type.prepare(
                possible_peers, getters, order_by_columns
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
        # second element of each element in results.
        return (value for _, value in sorted(results, key=operator.itemgetter(0)))
