from stupidb.indextree import Tree


def test_tree_repr_fanout_2():
    tree = Tree(height=3, fanout=2)
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
    tree = Tree(height=2, fanout=4)
    expected = """\
|-- 0
    |-- 1
    |-- 2
    |-- 3
    |-- 4"""
    assert repr(tree) == expected
