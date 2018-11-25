import ast
import inspect
import typing
from typing import Iterator, Set, Tuple

import toolz

from .stupidb import JoinPredicate, Row, Rows


class AttributeFinder(ast.NodeVisitor):
    def __init__(self) -> None:
        self.keys: Set[str] = set()

    def visit_Subscript(self, node: ast.Subscript) -> None:
        index: ast.Index = typing.cast(ast.Index, node.slice)
        value: ast.Str = typing.cast(ast.Str, index.value)
        self.keys.add(value.s)


def find_attributes(predicate: JoinPredicate) -> Set[str]:
    finder = AttributeFinder()
    finder.visit(ast.parse(inspect.getsource(predicate)))
    return finder.keys


def merge(
    left: Rows, right: Rows, predicate: JoinPredicate
) -> Iterator[Tuple[Row, Row]]:
    attributes = find_attributes(predicate)
    sorter = toolz.flip(toolz.pluck)(attributes)
    left_sorted = sorted(left, key=sorter)
    right_sorted = sorted(right, key=sorter)
    left_iter = iter(left_sorted)
    right_iter = iter(right_sorted)
    left_row = next(left_iter)
    right_row = next(right_iter)
    while True:
        if predicate(left_row, right_row):
            yield left_row, right_row
            left_row = next(left_iter)
            right_row = next(right_iter)
        elif sorter(left) < sorter(right):
            left_row = next(left_iter)
        else:  # if left.Key > right.Key
            right_row = next(right_iter)
