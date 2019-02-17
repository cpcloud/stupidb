"""Animate construction of segment trees."""

import collections
from typing import IO, Iterator, MutableMapping

import imageio
import networkx as nx

from stupidb import indextree
from stupidb.associative import SegmentTree
from stupidb.bitset import BitSet


class SegmentTreeAnimator:
    """Animate construction of a segment tree."""

    __slots__ = ("segment_tree",)

    def __init__(self, segment_tree: SegmentTree) -> None:
        """Construct a :class:`~stupidb.animate.SegmentTreeAnimator`."""
        self.segment_tree = segment_tree

    @property
    def nx_graph(self) -> nx.DiGraph:
        """Return a NetworkX graph created from self.nodes."""
        segment_tree = self.segment_tree
        tree = indextree.IndexTree(
            height=segment_tree.height, fanout=segment_tree.fanout
        )
        seen = BitSet()
        queue = collections.deque(tree.leaves)
        graph = nx.DiGraph()
        graph.add_nodes_from(tree.nodes)

        while queue:
            node = queue.popleft()
            if node not in seen:
                seen.add(node)
                parent = tree.parent(node)
                if parent != node:
                    graph.add_edge(parent, node, dir="back")
                queue.append(parent)
                nx_node = graph.node[node]
                if node in tree.leaves:
                    nx_node["label"] = segment_tree.nodes[node].finalize()
                else:
                    nx_node["label"] = ""
                nx_node["fontcolor"] = "black"
                nx_node["fillcolor"] = "white"
                nx_node["style"] = "filled"
        return graph

    @property
    def iterframes(self) -> Iterator[imageio.core.Array]:
        """Produce the frames of an animated construction of the tree."""
        graph = self.nx_graph
        segment_tree = self.segment_tree
        queue = collections.deque(
            u for u, out_edge_count in graph.out_degree if not out_edge_count
        )
        parent_count: MutableMapping[int, int] = collections.Counter()
        nodes = [
            segment_tree.aggregate_type()
            for _ in range(len(segment_tree.nodes))
        ]
        for leaf in queue:
            nodes[leaf] = segment_tree.nodes[leaf]

        seen = BitSet()
        while queue:
            node = queue.popleft()
            if node not in seen:
                seen.add(node)
                parent, = graph.predecessors(node)
                node_agg = nodes[node]
                parent_agg = nodes[parent]
                parent_agg.combine(node_agg)
                parent_count[parent] += 1

                nx_node = graph.node[node]
                nx_node["fillcolor"] = "blue"
                nx_node["fontcolor"] = "white"

                nx_parent_node = graph.node[parent]
                nx_parent_node["label"] = parent_agg.finalize()

                pydot_graph = nx.nx_pydot.to_pydot(graph)
                png_bytes = pydot_graph.create_png()
                yield imageio.imread(png_bytes)

                # don't traverse the root, since it will already contain its
                # full aggregate value due to the way we traverse
                if parent:
                    queue.append(parent)

                if parent_count[parent] == segment_tree.fanout - 1:
                    nx_parent_node["fillcolor"] = "red"
                    nx_parent_node["fontcolor"] = "white"
                    nx_parent_node["label"] = parent_agg.finalize()

    def animate(self, io: IO, fps: float = 1.5) -> None:
        """Convert this segment tree's construction into an animated gif.

        Parameters
        ----------
        outfile
            A path to write the animated GIF to.
        fps
            Frames per second.

        """
        imageio.mimsave(io, list(self.iterframes), fps=fps, format="gif")


def main() -> None:
    """Animate the construction of a SegmentTree."""
    import argparse
    import sys

    from stupidb.associative import Sum

    parser = argparse.ArgumentParser(
        description=(
            "Animate the construction of a segment tree using the Sum "
            "aggregation."
        )
    )
    parser.add_argument(
        "-o",
        "--outfile",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
        help="Where to write the animated GIF. Defaults to stdout.",
    )
    parser.add_argument(
        "-f", "--fanout", type=int, default=2, help="The tree fanout."
    )
    parser.add_argument(
        "-l",
        "--leaf",
        type=int,
        default=None,
        action="append",
        help="The leaves of the tree.",
    )
    args = parser.parse_args()
    if args.leaf is None:
        leaves = [(1,), (2,), (3,), (4,)]
    else:
        leaves = [(leaf,) for leaf in args.leaf]
    segment_tree = SegmentTree(leaves, Sum, fanout=args.fanout)
    animator = SegmentTreeAnimator(segment_tree)
    animator.animate(args.outfile)


if __name__ == "__main__":
    main()
