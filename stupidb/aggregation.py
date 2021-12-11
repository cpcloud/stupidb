"""Algorithms for aggregation."""

from __future__ import annotations

import abc
import bisect
import enum
import functools
import typing
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    NamedTuple,
    Sequence,
    TypeVar,
)

import toolz

from .aggregator import Aggregate, Aggregator
from .functions.associative import BinaryAssociativeAggregate, UnaryAssociativeAggregate
from .functions.navigation import (
    BinaryNavigationAggregate,
    TernaryNavigationAggregate,
    UnaryNavigationAggregate,
)
from .functions.ranking import RankingAggregate
from .protocols import Comparable
from .row import AbstractRow
from .typehints import Following, OrderBy, OrderingKey, PartitionBy, Preceding, T


class StartStop(NamedTuple):
    """A class to hold start and stop values for a range of rows."""

    start: int
    stop: int


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
        preceding: Preceding | None,
        following: Following | None,
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
        current_row_order_by_value: OrderingKey | None,
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
        current_row_order_by_value: OrderingKey | None,
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
        possible_peers: Sequence[AbstractRow],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> tuple[OrderingKey, Sequence[OrderingKey]]:
        """Compute the current row's ordering keys."""

    def compute_window_frame(
        self,
        possible_peers: Sequence[AbstractRow],
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
        StartStop
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
        current_row_order_by_value: OrderingKey | None,
        order_by_values: Sequence[OrderingKey],
    ) -> int:  # noqa: D102
        preceding = self.preceding
        assert preceding is not None, "preceding is None"
        return row_id_in_partition - typing.cast(int, preceding(current_row))

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_value: OrderingKey | None,
        order_by_values: Sequence[OrderingKey],
    ) -> int:  # noqa: D102
        following = self.following
        assert following is not None, "following is None"
        return row_id_in_partition + typing.cast(int, following(current_row)) + 1

    def setup_window(
        self,
        possible_peers: Sequence[AbstractRow],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> tuple[OrderingKey, Sequence[OrderingKey]]:  # noqa: D102
        cols = [
            tuple(map(peer.__getitem__, order_by_columns)) for peer in possible_peers
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
        preceding: Preceding | None,
        following: Following | None,
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
        possible_peers: Sequence[AbstractRow],
        current_row: AbstractRow,
        order_by_columns: Sequence[str],
    ) -> tuple[OrderingKey, Sequence[OrderingKey]]:  # noqa: D102
        # range mode allows no order by
        if not order_by_columns:
            return (), [()]

        ncolumns = len(order_by_columns)
        assert ncolumns == 1, f"ncolumns == {ncolumns:d}"
        (order_by_column,) = order_by_columns
        order_by_values = [(peer[order_by_column],) for peer in possible_peers]
        current_row_order_by_value = (current_row[order_by_column],)
        return current_row_order_by_value, order_by_values

    def find_partition_begin(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_values: OrderingKey | None,
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
        return bisect.bisect_left(order_by_values, (value_to_find,))

    def find_partition_end(
        self,
        current_row: AbstractRow,
        row_id_in_partition: int,
        current_row_order_by_values: OrderingKey | None,
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
        return bisect.bisect_right(order_by_values, (value_to_find,))


class Window:
    """A namespace class providing the user-facing API for windowing modes."""

    __slots__ = ()

    @staticmethod
    def rows(
        order_by: Sequence[OrderBy] = (),
        partition_by: Sequence[PartitionBy] = (),
        preceding: Preceding | None = None,
        following: Following | None = None,
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
        preceding: Preceding | None = None,
        following: Following | None = None,
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
        aggregate_type: type[ConcreteAggregate],
        *getters: Getter,
    ) -> None:
        self.aggregate_type: type[ConcreteAggregate] = aggregate_type
        self.getters = getters


def row_key_compare(
    order_func: Callable[[AbstractRow], tuple[Comparable[T], ...]],
    null_ordering: Nulls,
    left_row: AbstractRow,
    right_row: AbstractRow,
) -> int:
    """Compare `left_row` and `right_row` using `order_by`.

    Notes
    -----
    ``NULL`` ordering is handled using `null_ordering`.

    """
    for left_key, right_key in zip(order_func(left_row), order_func(right_row)):
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
    order_func: Callable[[AbstractRow], tuple[Comparable[T], ...]],
    nulls: Nulls,
) -> Callable[[AbstractRow], OrderingKey[T]]:
    """Make a function usable with the key argument to sorting functions.

    This return value of this function can be passed to
    :func:`sorted`/:meth:`list.sort`.

    Parameters
    ----------
    order_by_columns
        A sequence of :class:`str` instances referring to the keys of an
        :class:`~stupidb.row.AbstractRow`.

    """
    return functools.cmp_to_key(functools.partial(row_key_compare, order_func, nulls))


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
        aggregate_type: type[ConcreteAggregate],
        getters: tuple[Getter, ...],
        frame_clause: FrameClause,
    ) -> None:
        self.aggregate_type: type[ConcreteAggregate] = aggregate_type
        self.getters = getters
        self.frame_clause = frame_clause

    def compute(self, rows: Iterable[AbstractRow]) -> Iterator[T | None]:
        """Aggregate `rows` over a window, producing an iterator of results.

        Parameters
        ----------
        rows
            An :class:`~typing.Iterable` of rows.

        """
        frame_clause = self.frame_clause
        order_by = frame_clause.order_by

        # Generate names for temporary order by columns, users never see these.
        #
        # TODO: If we had static schema information these wouldn't be necessary
        # in cases where the ordering keys are named columns (either physical
        # or computed)
        order_by_columns = [f"_order_by_{i:d}" for i in range(len(order_by))]

        # Add computed order by columns that are used when evaluating window
        # functions in range mode
        # TODO: check that if in range mode we only have single order by
        order_func = toolz.juxt(*order_by)
        rows_for_partition = (
            row.merge(dict(zip(order_by_columns, order_func(row)))) for row in rows
        )

        # divide the input rows into partitions
        #
        # we only need the values once we've grouped, so there's no need to
        # store a possibly expensive key like a tuple, so we store just the
        # integer result of hash in the dict
        #
        # we also only need the partition values once the rows have
        # been partitioned
        partitions = toolz.groupby(
            toolz.compose(hash, toolz.juxt(*frame_clause.partition_by)),
            rows_for_partition,
        ).values()

        # aggregation results, preallocated to avoid the need to sort
        # before returning: we later assign elements to this list using
        # the original row id
        results: list[T | None] = [None] * sum(map(len, partitions))

        # Aggregate over each partition
        aggregate_type = self.aggregate_type
        getters = self.getters
        key_func = make_key_func(order_func, frame_clause.nulls)
        for possible_peers in partitions:
            # sort the partition according to the ordering key
            possible_peers.sort(key=key_func)

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
            for row_id_in_partition, row in enumerate(possible_peers):
                start, stop = frame_clause.compute_window_frame(
                    possible_peers, row, row_id_in_partition, order_by_columns
                )
                # Assign the result to the position of the original row id
                # because we processed them in partition order, which might not
                # be the same as the input order.
                results[row._id] = aggregator.query(start, stop)

        return iter(results)
