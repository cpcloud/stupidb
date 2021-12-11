import os
import subprocess
import sys

import pytest

from stupidb.associative.segmenttree import SegmentTree
from stupidb.functions.associative import Sum

pytestmark = pytest.mark.animate


def test_to_gif() -> None:
    from stupidb.associative.animate import SegmentTreeAnimator

    seg = SegmentTree([(i,) for i in range(1, 5)], Sum[int, int], fanout=2)
    anim = SegmentTreeAnimator(seg)
    with open(os.devnull, "wb") as io:
        anim.animate(io)


def test_main_default_args() -> None:
    try:
        subprocess.check_call(
            [f"{sys.executable}", "-m", "stupidb.associative.animate", "-o", os.devnull]
        )
    except subprocess.CalledProcessError as e:  # pragma: no cover
        print(e.stderr, file=sys.stderr)
        raise


def test_main_custom_leaves() -> None:
    try:
        subprocess.check_call(
            [
                f"{sys.executable}",
                "-m",
                "stupidb.associative.animate",
                "-o",
                os.devnull,
                "-l",
                "1",
                "-l",
                "2",
                "-f",
                "2",
            ]
        )
    except subprocess.CalledProcessError as e:  # pragma: no cover
        print(e.stderr, file=sys.stderr)
        raise
