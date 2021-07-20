"""Animate construction of segment trees."""

import argparse
import collections
import sys
from typing import BinaryIO, Iterator, MutableMapping, Sequence

import imageio
import pydot

from stupidb import indextree
from stupidb.associative import SegmentTree, Sum
from stupidb.bitset import BitSet


class SegmentTreeAnimator:
    """Animate construction of a segment tree."""

    __slots__ = ("segment_tree",)

    def __init__(self, segment_tree: SegmentTree) -> None:
        """Construct a :class:`~stupidb.animate.SegmentTreeAnimator`."""
        self.segment_tree = segment_tree

    def make_graph(self, *, font: str) -> pydot.Dot:
        """Return a graph created from ``self.nodes``."""
        segment_tree = self.segment_tree
        tree = indextree.IndexTree(
            height=segment_tree.height, fanout=segment_tree.fanout
        )
        seen = BitSet()
        queue = collections.deque(tree.leaves)

        graph = pydot.Dot(graph_type="digraph")
        for node in tree.nodes:
            graph.add_node(pydot.Node(node))

        while queue:
            node = queue.popleft()
            if node not in seen:
                seen.add(node)
                parent = tree.parent(node)
                if parent != node:
                    graph.add_edge(pydot.Edge(parent, node, dir="back"))
                queue.append(parent)
                if node in tree.leaves:
                    result = segment_tree.nodes[node].finalize()
                    label = "" if result is None else str(result)
                else:
                    label = ""
                (pydot_node,) = graph.get_node(str(node))
                pydot_node.set_label(label or "")
                pydot_node.set_fontcolor("black")
                pydot_node.set_fontname(f"{font} bold")
                pydot_node.set_fillcolor("white")
                pydot_node.set_style("filled")

        return graph

    def iterframes(self, *, font: str) -> Iterator[imageio.core.Array]:
        """Produce the frames of an animated construction of the tree."""
        graph = self.make_graph(font=font)
        segment_tree = self.segment_tree

        adj_list = {int(node.get_name()): BitSet() for node in graph.get_nodes()}

        for edge in graph.get_edges():
            adj_list[edge.get_source()].add(edge.get_destination())

        predecessors: MutableMapping[int, int] = {}

        for parent, children in adj_list.items():
            for child in children:
                predecessors[child] = parent

        queue = collections.deque(
            source for source, nodes in adj_list.items() if not nodes
        )
        parent_count: MutableMapping[int, int] = collections.Counter()
        nodes = [segment_tree.aggregate_type() for _ in range(len(segment_tree.nodes))]
        for leaf in queue:
            nodes[leaf] = segment_tree.nodes[leaf]

        seen = BitSet()
        while queue:
            node = queue.popleft()
            if node not in seen:
                seen.add(node)
                parent = predecessors[node]
                node_agg = nodes[node]
                parent_agg = nodes[parent]
                parent_agg.combine(node_agg)
                parent_count[parent] += 1

                (pydot_node,) = graph.get_node(str(node))
                pydot_node.set_fillcolor("blue")
                pydot_node.set_fontcolor("white")
                pydot_node.set_fontname(f"{font} bold")

                result = parent_agg.finalize()

                (pydot_parent_node,) = graph.get_node(str(parent))
                pydot_parent_node.set_label("" if result is None else str(result))

                png_bytes = graph.create_png()
                yield imageio.imread(png_bytes)

                # don't traverse the root, since it will already contain its
                # full aggregate value due to the way we traverse
                if parent:
                    queue.append(parent)

                if parent_count[parent] == segment_tree.fanout - 1:
                    pydot_parent_node.set_fillcolor("red")
                    pydot_parent_node.set_fontcolor("white")
                    pydot_parent_node.set_fontname(f"{font} bold")
                    result = parent_agg.finalize()
                    pydot_parent_node.set_label("" if result is None else str(result))

    def animate(self, io: BinaryIO, font: str = "Helvetica", fps: float = 1.5) -> None:
        """Convert this segment tree's construction into an animated gif.

        Parameters
        ----------
        io
            A writable binary input/output stream.
        fps
            Frames per second.

        Examples
        --------
        >>> import os
        >>> from stupidb.associative import SegmentTree, Sum
        >>> from stupidb.animate import SegmentTreeAnimator
        >>> segment_tree = SegmentTree([(1,), (2,), (3,), (4,)], Sum, fanout=2)
        >>> animator = SegmentTreeAnimator(segment_tree)
        >>> with open(os.devnull, "wb") as devnull:
        ...     animator.animate(devnull)

        """
        imageio.mimsave(io, ims=self.iterframes(font=font), fps=fps, format="gif")


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Animate the construction of a segment tree build "
            "with the Sum aggregation."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "-o",
        "--output-file",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
        help="Where to write the animated GIF. Defaults to stdout.",
    )
    p.add_argument(
        "-f",
        "--fanout",
        type=int,
        default=2,
        help="Segment tree fanout.",
    )
    p.add_argument(
        "-l",
        "--leaf",
        type=int,
        action="append",
        default=[],
        help="The leaves of the segment tree.",
    )
    p.add_argument(
        "-r",
        "--frame-rate",
        type=float,
        default=1.5,
        help="Frames per second of the resulting animation.",
    )
    p.add_argument(
        "-F",
        "--font",
        type=str,
        default="Helvetica",
        help="Node font.",
    )
    return p.parse_args()


def main(
    *,
    output_file: BinaryIO,
    fanout: int,
    leaf: Sequence[int],
    frame_rate: float,
    font: str,
) -> None:
    """Animate the construction of a SegmentTree."""
    if not leaf:
        leaves = [(lf,) for lf in range(1, 9)]
    else:
        leaves = [(lf,) for lf in leaf]
    segment_tree: SegmentTree = SegmentTree(leaves, Sum, fanout=fanout)
    animator = SegmentTreeAnimator(segment_tree)
    animator.animate(output_file, font=font, fps=frame_rate)


if __name__ == "__main__":  # pragma: no cover
    args = parse_args()
    main(
        output_file=args.output_file,
        fanout=args.fanout,
        leaf=args.leaf,
        frame_rate=args.frame_rate,
        font=args.font,
    )
