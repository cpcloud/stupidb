import os
import subprocess
import sys

import pytest

from stupidb.associative import SegmentTree, Sum

pytestmark = pytest.mark.animate


def test_to_gif():
    from stupidb.animate import SegmentTreeAnimator

    seg = SegmentTree([(i,) for i in range(1, 5)], Sum, fanout=2)
    anim = SegmentTreeAnimator(seg)
    with open(os.devnull, "wb") as io:
        anim.animate(io)


def test_main_default_args():
    try:
        subprocess.check_call(
            [f"{sys.executable}", "-m", "stupidb.animate", "-o", os.devnull]
        )
    except subprocess.CalledProcessError as e:  # pragma: no cover
        print(e.stderr, file=sys.stderr)
        raise


def test_main_custom_leaves():
    try:
        subprocess.check_call(
            [
                f"{sys.executable}",
                "-m",
                "stupidb.animate",
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
