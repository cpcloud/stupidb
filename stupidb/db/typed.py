"""Add typing to AST.

We've passed the parsing stage, so it's time to add types to AST nodes.

The approach we take here is to visit every node, seeding types at the leaves.
On the way back up the tree, we check that expressions do not produce type
errors, and if they don't we produce a new tree with typed nodes.

"""

from typing import List, Mapping, Sequence

import stupidb.db.syntax as syn
import stupidb.db.typedsyntax as t
from stupidb.db.schema import DataType, Float64, Int64, Schema, String, typeof


class TypeVisitor:
    """Visit untyped nodes."""

    def __init__(self, database: Mapping[str, DataType]) -> None:
        """Construct a type visitor."""
        self.database = database

    def visit(self, node: syn.Node, **kwargs) -> syn.Node:
        """Visit an untyped node."""
        node_typename = type(node).__name__
        method_name = f"visit_{node_typename}"
        method = getattr(self, method_name, None)
        if method is None:
            raise NotImplementedError(f"Undefined method {method_name!r}")
        return method(node, **kwargs)

    def visit_Relation(self, node: syn.Relation, **kwargs) -> t.Relation:
        """Visit a Relation node."""
        name = node.name
        return t.Relation(name, schema=self.database[name])

    def visit_Select(self, node: syn.Select, **kwargs) -> t.Select:
        relations = list(map(self.visit, node.relations))
        return t.Select(
            exprs=[
                self.visit(expr, relations=relations) for expr in node.exprs
            ],
            relations=[
                self.visit(expr, relations=relations)
                for expr in node.relations
            ],
            where=(
                self.visit(node.where, relations=relations)
                if node.where is not None
                else None
            ),
            group_by=[
                self.visit(group, relations=relations)
                for group in node.group_by
            ],
        )

    def visit_Column(
        self, node: syn.Column, relations: Sequence[t.Relation]
    ) -> t.Column:
        """Visit a `Column`."""
        name = node.name
        occurs_in = {
            relation
            for relation in relations
            if name in relation.schema.keys()
        }
        if not occurs_in:
            raise ValueError(
                f"Column {name!r} not found in any relations in SELECT "
                "statement."
            )
        if len(occurs_in) > 1:
            raise ValueError(
                f"Column {name!r} occurs in more than one relation in "
                "SELECT statement."
            )
        return t.Column(name, relation=occurs_in.pop())

    def visit_Named(
        self, node: syn.Named, relations: Sequence[t.Relation]
    ) -> List[t.Column]:
        name = node.name
        expr = self.visit(node.expr, relations=relations)
        return t.Named(expr, name, type=expr.type)

    def visit_Add(self, node: syn.Add, **kwargs) -> t.Add:
        return t.Add(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Sub(self, node: syn.Sub, **kwargs) -> t.Sub:
        return t.Sub(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Mul(self, node: syn.Mul, **kwargs) -> t.Mul:
        return t.Mul(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Div(self, node: syn.Div, **kwargs) -> t.Div:
        return t.Div(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Neg(self, node: syn.Neg, **kwargs) -> t.Neg:
        return t.Neg(self.visit(node.left, **kwargs))

    def visit_Eq(self, node: syn.Eq, **kwargs) -> t.Eq:
        return t.Eq(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Ne(self, node: syn.Ne, **kwargs) -> t.Ne:
        return t.Ne(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Lt(self, node: syn.Lt, **kwargs) -> t.Lt:
        return t.Lt(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Le(self, node: syn.Le, **kwargs) -> t.Le:
        return t.Le(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Gt(self, node: syn.Gt, **kwargs) -> t.Gt:
        return t.Gt(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Ge(self, node: syn.Ge, **kwargs) -> t.Ge:
        return t.Ge(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_And(self, node: syn.And, **kwargs) -> t.And:
        return t.And(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Or(self, node: syn.Or, **kwargs) -> t.Or:
        return t.Or(
            self.visit(node.left, **kwargs), self.visit(node.right, **kwargs)
        )

    def visit_Literal(self, node: syn.Literal, **kwargs) -> t.Literal:
        value = node.value
        return t.Literal(value, typeof(value))

    def visit_T(self, node: syn.T, **kwargs):
        return t.T()

    def visit_F(self, node: syn.F, **kwargs):
        return t.F()


if __name__ == "__main__":
    import argparse
    from stupidb.db.parser import parse
    from stupidb.db.stringtree import StringTreeVisitor

    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str)
    args = parser.parse_args()

    schema = Schema.from_pairs(
        [("a", Int64()), ("b", String()), ("c", Float64())]
    )
    database = {"t": schema}
    visitor = TypeVisitor(database)
    query = "select a + c, b from t"
    parsed = parse(args.query)
    result = visitor.visit(parsed)
    stringtree = StringTreeVisitor()
    print(stringtree.visit(result))
