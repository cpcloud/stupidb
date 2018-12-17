import collections
from typing import Hashable, Iterator, List, Optional, Sequence, Tuple

import toolz
from typing_extensions import DefaultDict

from stupidb.row import Row
from stupidb.stupidb import AggregateSpecification
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
    possible_peers: List[Row],
    preceding: Preceding,
    following: Following,
) -> List[Row]:
    npeers = len(possible_peers)
    if preceding is not None:
        start: Optional[int] = max(current_row.id - preceding(current_row), 0)
    else:
        start = 0

    if following is not None:
        stop = min(current_row.id + following(current_row), npeers)
    else:
        stop = npeers
    return possible_peers[start:stop]


def window_agg(
    rows: Sequence[Row],
    partition_by: Sequence[PartitionBy],
    order_by: Sequence[OrderBy],
    preceding: Preceding,
    following: Following,
    aggspec: AggregateSpecification,
) -> Iterator[Row]:
    partitions: DefaultDict[
        Tuple[Hashable, ...], List[Row]
    ] = collections.defaultdict(list)

    # partition
    for row in rows:
        partition_key = compute_partition_key(row, partition_by)
        partitions[partition_key].append(row)

    # sort
    for partition_key in partitions.keys():
        partitions[partition_key].sort(
            key=lambda row: tuple(order_func(row) for order_func in order_by)
        )

    for row in rows:
        # compute the partition the row is in
        partition_key = compute_partition_key(row, partition_by)
        possible_peers = partitions[partition_key]

        # compute the window frame, ROWS mode only for now
        # compute the aggregation over the rows in the partition in the frame
        peers = compute_window_frame(row, possible_peers, preceding, following)
        agg = aggspec.aggregate()
        for peer in peers:
            args = [getter(peer) for getter in aggspec.getters]
            agg.step(*args)
        result = agg.finalize()
        yield Row(toolz.merge(row, {"agg": result}), id=row.id)
