"""Docstring."""

import contextlib
import textwrap
from typing import Callable, Generator

import stupidb.db.typedsyntax as syn
from stupidb.db.syntax import Node
from stupidb.db.visitor import Visitor

NodeVisitorMethod = Callable[["StringTreeVisitor", Node], str]


class StringTreeVisitor(Visitor):
    """Pretty printer of nodes."""

    def __init__(self, indent: int = 2, prefix: str = " ") -> None:
        """Construct a StringTreeVisitor."""
        self.level = 0
        self._indent = indent
        self.prefix = prefix

    def indent(self, lines: str) -> str:
        """Indent lines."""
        return textwrap.indent(lines, self.level * self._indent * self.prefix)

    @property
    @contextlib.contextmanager
    def nest(self) -> Generator:
        """Add one nesting level."""
        self.level += 1
        try:
            yield
        finally:
            self.level = max(self.level - 1, 0)

    def visit_Relation(self, node: syn.Relation) -> str:
        """Visit a `Relation`."""
        return node.name

    def visit_Literal(self, node: syn.Literal) -> str:
        """Visit a `Literal`."""
        return f"({node.value} {node.type})"

    def visit_Select(self, node: syn.Select) -> str:
        """Visit a `Select`."""
        pieces = ["select"]
        columns = "\n".join(map(self.visit, node.exprs))
        with self.nest:
            pieces.append(self.indent("(columns"))
            with self.nest:
                pieces.append(f"{self.indent(columns)})")

        relations = "\n".join(map(self.visit, node.relations))
        with self.nest:
            pieces.append(self.indent("(from"))
            with self.nest:
                pieces.append(f"{self.indent(relations)})")

        if node.where is not None:
            where = self.visit(node.where)
            with self.nest:
                pieces.append(self.indent("(where"))
                with self.nest:
                    pieces.append(f"{self.indent(where)})")

        if node.group_by:
            group_by = "\n".join(map(self.visit, node.group_by))
            with self.nest:
                pieces.append(self.indent("(groupby"))
                with self.nest:
                    pieces.append(f"{self.indent(group_by)}")
        return "({})".format("\n".join(pieces))

    def visit_Column(self, node: syn.Column) -> str:
        """Visit a `Column`."""
        return f"({node.name} {node.type})"

    def visit_Named(self, node: syn.Named) -> str:
        """Visit a `Named`."""
        visited = self.visit(node.expr)
        with self.nest:
            expr = self.indent(visited)
        return f"({node.name}\n{expr})"

    def visit_binop(self, node: Node, op: str) -> str:
        """Visit a binary operation."""
        left = self.visit(node.left)
        right = self.visit(node.right)
        with self.nest:
            return f"({op}\n{self.indent(left)}\n{self.indent(right)})"

    def visit_Add(self, node: syn.Add) -> str:
        """Visit an `Add`."""
        return self.visit_binop(node, "+")

    def visit_Sub(self, node: syn.Sub) -> str:
        """Visit a `Sub`."""
        return self.visit_binop(node, "-")

    def visit_Mul(self, node: syn.Mul) -> str:
        """Visit a `Mul`."""
        return self.visit_binop(node, "*")

    def visit_Div(self, node: syn.Div) -> str:
        """Visit a `Div`."""
        return self.visit_binop(node, "/")

    def visit_Eq(self, node: syn.Eq) -> str:
        """Visit an `Eq`."""
        return self.visit_binop(node, "=")

    def visit_Ne(self, node: syn.Ne) -> str:
        """Visit a `Ne`."""
        return self.visit_binop(node, "!=")

    def visit_Lt(self, node: syn.Lt) -> str:
        """Visit a `Lt`."""
        return self.visit_binop(node, "<")

    def visit_Le(self, node: syn.Le) -> str:
        """Visit a `Le`."""
        return self.visit_binop(node, "<=")

    def visit_Gt(self, node: syn.Gt) -> str:
        """Visit a `Gt`."""
        return self.visit_binop(node, ">")

    def visit_Ge(self, node: syn.Ge) -> str:
        """Visit a `Ge`."""
        return self.visit_binop(node, ">=")

    def visit_And(self, node: syn.And) -> str:
        """Visit an `And`."""
        return self.visit_binop(node, "and")

    def visit_Or(self, node: syn.Or) -> str:
        """Visit an `Or`."""
        return self.visit_binop(node, "or")

    def visit_T(self, node: syn.T) -> str:
        """Visit a `T`."""
        return f"(true {node.type})"

    def visit_F(self, node: syn.F) -> str:
        """Visit an `F`."""
        return f"(false {node.type})"
