from stupidb.indextree import IndexTree


def test_tree_repr_fanout_2():
    tree = IndexTree(height=3, fanout=2)
    expected = """\
|-- 0
    |-- 1
        |-- 3
        |-- 4
    |-- 2
        |-- 5
        |-- 6"""
    assert repr(tree) == expected


def test_tree_repr_fanout_4():
    tree = IndexTree(height=2, fanout=4)
    expected = """\
|-- 0
    |-- 1
    |-- 2
    |-- 3
    |-- 4"""
    assert repr(tree) == expected


def test_parent_of_root():
    tree = IndexTree(height=2, fanout=2)
    assert tree.parent(0) == 0
