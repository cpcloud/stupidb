import os

from stupidb.associative import SegmentTree, Sum
from stupidb.animate import SegmentTreeAnimator, main


def test_to_gif():
    seg = SegmentTree([(i,) for i in range(1, 5)], Sum, fanout=2)
    anim = SegmentTreeAnimator(seg)
    with open(os.devnull, "wb") as io:
        anim.animate(io)


def test_main_default_args():
    main(["-o", os.devnull])


def test_main_custom_leaves():
    main(["-o", os.devnull, "-l", "1", "-l", "2", "-f", "2"])
