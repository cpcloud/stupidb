import abc
import collections

from typing import (
    Any,
    Callable,
    Hashable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)
from typing_extensions import Protocol, DefaultDict

import toolz

from stupidb.stupidb import AggregateSpecification

C = TypeVar("C", bound="Comparable")


class Comparable(Protocol):
    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abc.abstractmethod
    def __lt__(self: C, other: C) -> bool:
        ...

    def __gt__(self: C, other: C) -> bool:
        return (not self < other) and self != other

    def __le__(self: C, other: C) -> bool:
        return self < other or self == other

    def __ge__(self: C, other: C) -> bool:
        return not self < other


V = TypeVar("V")


class Row(Mapping[str, V]):
    def __init__(self, id: int, data: Mapping[str, V]) -> None:
        self.id = id
        self.data = data

    def __hash__(self) -> int:
        return hash(tuple(tuple(item) for item in self.data.items()))

    def __repr__(self) -> str:
        joined_items = ", ".join(f"{k}={v!r}" for k, v in self.data.items())
        return f"{self.__class__.__name__}({joined_items})"

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)


PartitionBy = Callable[[Row], Hashable]
OrderBy = Callable[[Row], Comparable]
Preceding = Optional[Callable[[Row], int]]
Following = Optional[Callable[[Row], int]]


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
    if preceding is None:
        start = None
    else:
        start = max(current_row.id - preceding(current_row), 0)

    if following is None:
        stop = None
    else:
        stop = min(current_row.id + following(current_row), npeers)
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
        yield toolz.merge(row, {"agg": result})
