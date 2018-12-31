import collections
import itertools
from typing import Any, Hashable, Iterable, Iterator, List, Sequence, Tuple

from typing_extensions import DefaultDict

from stupidb.row import Row
from stupidb.stupidb import WindowAggregateSpecification
from stupidb.typehints import Following, PartitionBy, Preceding


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
        # because of slice semantics based on zero-based indexing we have to
        # add one here to make sure the current row is included
        stop = min(partition_id + following(current_row) + 1, npeers)
    else:
        stop = npeers
    return possible_peers[start:stop]


def window_agg(
    rows: Iterable[Tuple[Row]], aggspec: WindowAggregateSpecification
) -> Iterator[Any]:
    frame_clause = aggspec.frame_clause
    partition_by = frame_clause._partition_by
    order_by = frame_clause._order_by
    preceding = frame_clause._preceding
    following = frame_clause._following
    raw_partitions: DefaultDict[
        Tuple[Hashable, ...], List[Row]
    ] = collections.defaultdict(list)

    # partition
    rows1, rows2 = itertools.tee(rows)
    for (row,) in rows1:
        partition_key = compute_partition_key(row, partition_by)
        raw_partitions[partition_key].append(row)

    partitions = dict(raw_partitions)

    # sort
    for partition_key in partitions.keys():
        partitions[partition_key].sort(
            key=lambda row: tuple(order_func(row) for order_func in order_by)
        )

    for (row,) in rows2:
        # compute the partition the row is in
        partition_key = compute_partition_key(row, partition_by)
        possible_peers = partitions[partition_key]
        partition_id = possible_peers.index(row)

        # compute the window frame, ROWS mode only for now
        # compute the aggregation over the rows in the partition in the frame
        peers = compute_window_frame(
            row, partition_id, possible_peers, preceding, following
        )
        agg = aggspec.aggregate()
        for peer in peers:
            args = [getter(peer) for getter in aggspec.getters]
            agg.step(*args)
        result = agg.finalize()
        yield result
