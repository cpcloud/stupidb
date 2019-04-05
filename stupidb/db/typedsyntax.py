"""Typed syntax."""

from typing import List, Sequence

import attr

import stupidb.db.schema as schema
import stupidb.db.syntax as syn

from stupidb.db.schema import DataType, Schema


@syn.node
class Boolean(syn.Boolean):
    """Boolean expression."""

    @property
    def type(self) -> schema.Bool:
        """Return the type of the expression."""
        return schema.Bool()


@syn.node
class Relation(syn.Relation):
    """A relation."""

    name = attr.ib(validator=attr.validators.instance_of(str))
    schema = attr.ib(validator=attr.validators.instance_of(Schema))


@syn.node
class Select(syn.Node):
    """A select statement."""

    exprs = attr.ib(validator=attr.validators.instance_of(list))
    relations = attr.ib(
        validator=attr.validators.instance_of(list),
        default=[],
        type=List[Relation],
    )
    where = attr.ib(default=None)
    group_by = attr.ib(default=[], type=Sequence[syn.Expr])


@syn.node
class Literal(syn.Literal):
    """A literal value."""

    type = attr.ib(validator=attr.validators.instance_of(DataType))


@syn.node
class T(Boolean):
    """True."""


@syn.node
class F(Boolean):
    """False."""


@syn.node
class Column(syn.Column):
    """A column selection."""

    @property
    def type(self) -> DataType:
        return self.relation.schema[self.name]


@syn.node
class Named(syn.Named):
    """A named expression."""

    type = attr.ib(validator=attr.validators.instance_of(DataType))


@syn.node
class Add(syn.Add):
    """Add."""

    @property
    def type(self) -> DataType:
        return self.left.type


@syn.node
class Sub(syn.Sub):
    """Subtract."""

    @property
    def type(self) -> DataType:
        return self.left.type


@syn.node
class Mul(syn.Mul):
    """Multiply."""

    @property
    def type(self) -> DataType:
        return self.left.type


@syn.node
class Div(syn.Div):
    """Divide."""

    @property
    def type(self) -> DataType:
        return self.left.type


@syn.node
class Neg(syn.Expr):
    """Negate."""

    operand = attr.ib(validator=attr.validators.instance_of(syn.Expr))

    @property
    def type(self) -> DataType:
        return self.left.type


@syn.node
class Equality(syn.Equality, Boolean):
    """Equality."""


@syn.node
class Ordering(syn.Ordering, Boolean):
    """Ordering."""


@syn.node
class Eq(syn.Eq, Equality):
    """Equal."""


@syn.node
class Ne(syn.Ne, Equality):
    """Not equal."""


@syn.node
class Lt(syn.Lt, Ordering):
    """Less than."""


@syn.node
class Le(syn.Le, Ordering):
    """Less than or equal to."""


@syn.node
class Gt(syn.Gt, Ordering):
    """Greater than."""


@syn.node
class Ge(syn.Ge, Ordering):
    """Greater than or equal to."""

    left = attr.ib(validator=attr.validators.instance_of(syn.Expr))
    right = attr.ib(validator=attr.validators.instance_of(syn.Expr))


@syn.node
class And(syn.And, Boolean):
    """Conjunction."""

    left = attr.ib(validator=attr.validators.instance_of(Boolean))
    right = attr.ib(validator=attr.validators.instance_of(Boolean))


@syn.node
class Or(syn.Or, Boolean):
    """Disjunction."""

    left = attr.ib(validator=attr.validators.instance_of(Boolean))
    right = attr.ib(validator=attr.validators.instance_of(Boolean))


@syn.node
class Not(syn.Not, Boolean):
    """Logical negation."""

    operand = attr.ib(validator=attr.validators.instance_of(Boolean))
