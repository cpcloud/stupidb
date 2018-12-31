import collections
import itertools
from typing import (
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    Sequence,
    Tuple,
)

from stupidb.comparable import Comparable
from stupidb.row import Row
from stupidb.stupidb import WindowAggregateSpecification
from stupidb.typehints import Following, OrderBy, PartitionBy, Preceding


def compute_partition_key(
    row: Row, partition_by: Sequence[PartitionBy]
) -> Tuple[Hashable, ...]:
    partition_key = tuple(
        partition_func(row) for partition_func in partition_by
    )
    return partition_key


def compute_window_frame(
    current_row: Row,
    partition_id: int,
    possible_peers: List[Row],
    preceding: Preceding,
    following: Following,
) -> List[Row]:
    npeers = len(possible_peers)
    if preceding is not None:
        start = max(partition_id - preceding(current_row), 0)
    else:
        start = 0

    if following is not None:
        # because of zero-based indexing we must add one to `stop` to make sure
        # the current row is included
        stop = min(partition_id + following(current_row) + 1, npeers)
    else:
        stop = npeers
    return possible_peers[start:stop]


def make_key_func(order_by: Iterable[OrderBy]) -> Callable[[Row], Comparable]:
    def key(row: Row) -> Tuple[Comparable, ...]:
        return tuple(order_func(row) for order_func in order_by)

    return key


def window_agg(
    rows: Iterable[Row], aggspec: WindowAggregateSpecification
) -> Iterator[Any]:
    """Aggregate `rows` over a window specified by `aggspec`."""
    frame_clause = aggspec.frame_clause
    partition_by = frame_clause._partition_by
    order_by = frame_clause._order_by
    preceding = frame_clause._preceding
    following = frame_clause._following

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
    # This leads to incorrect results when an aggregation is used downstream.
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
    key = make_key_func(order_by)
    for partition_key in partitions.keys():
        partitions[partition_key].sort(key=key)

    for row in rows_for_frame_computation:
        # compute the partition the row is in
        partition_key = compute_partition_key(row, partition_by)

        # the maximal set of rows that could be in the current row's peer set
        possible_peers = partitions[partition_key]

        # Compute the index of the row in the partition. This value is the
        # value that all peers (using preceding and following if they are not
        # None) are computed relative to.
        #
        # Stupidly, this is a linear search for a matching row and assumes that
        # there are no duplicate rows in `possible_peers`.
        partition_id = possible_peers.index(row)

        # Compute the window frame, which is a subset of `possible_peers`.
        peers = compute_window_frame(
            row, partition_id, possible_peers, preceding, following
        )

        # Aggregate over the rows in the frame.
        agg = aggspec.aggregate()
        for peer in peers:
            args = [getter(peer) for getter in aggspec.getters]
            agg.step(*args)
        result = agg.finalize()
        yield result
