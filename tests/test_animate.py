import os

from stupidb.associative import SegmentTree, Sum
from stupidb.animate import SegmentTreeAnimator


def test_to_gif():
    seg = SegmentTree([(i,) for i in range(1, 5)], Sum, fanout=2)
    anim = SegmentTreeAnimator(seg)
    with open(os.devnull, "wb") as io:
        anim.animate(io)
