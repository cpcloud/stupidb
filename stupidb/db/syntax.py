"""Abstract syntax tree elements."""

from typing import List, Optional, Union

import attr

node = attr.s(frozen=True, slots=True)


@node
class Node:
    """Base class for all syntax tree nodes."""


@node
class Expr(Node):
    """A node that yields a value."""


@node
class Boolean(Expr):
    """Boolean expression."""


@node
class Relation(Node):
    """A relation."""

    name = attr.ib(validator=attr.validators.instance_of(str))


@node
class Select(Node):
    """A select statement."""

    exprs = attr.ib(
        validator=attr.validators.instance_of(list),
        default=[],
        type=List[Expr],
    )
    relations = attr.ib(
        validator=attr.validators.instance_of(list),
        default=[],
        type=List[Relation],
    )
    where = attr.ib(
        validator=attr.validators.optional(attr.validators.instance_of(Expr)),
        default=None,
        type=Optional[Expr],
    )
    group_by = attr.ib(
        validator=attr.validators.instance_of(list),
        default=[],
        type=List[Expr],
    )


@node
class Star(Node):
    """Node representing SELECT * queries."""


@node
class Literal(Expr):
    """A literal value."""

    value: Optional[Union[int, float, bool, str, bytes]] = attr.ib(
        validator=attr.validators.instance_of((int, float, bool, type(None)))
    )


@node
class T(Boolean):
    """True node."""


@node
class F(Boolean):
    """False node."""


@node
class Named(Expr):
    """A named expression."""

    expr = attr.ib(validator=attr.validators.instance_of(Expr))
    name = attr.ib(validator=attr.validators.instance_of(str))


@node
class Column(Expr):
    """A column selection."""

    name = attr.ib(validator=attr.validators.instance_of(str))
    relation = attr.ib(
        validator=attr.validators.optional(
            attr.validators.instance_of(Relation)
        ),
        default=None,
    )


@node
class Add(Expr):
    """Add."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Sub(Expr):
    """Subtract."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Mul(Expr):
    """Multiply."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Div(Expr):
    """Divide."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Neg(Expr):
    """Negate."""

    operand = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Pos(Expr):
    """Unary add."""

    operand = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Equality(Boolean):
    """Equality."""


@node
class Ordering(Boolean):
    """Ordering."""


@node
class Eq(Equality):
    """Equal."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Ne(Equality):
    """Not equal."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Lt(Ordering):
    """Less than."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Le(Ordering):
    """Less than or equal to."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Gt(Ordering):
    """Greater than."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class Ge(Ordering):
    """Greater than or equal to."""

    left = attr.ib(validator=attr.validators.instance_of(Expr))
    right = attr.ib(validator=attr.validators.instance_of(Expr))


@node
class And(Boolean):
    """Conjunction."""

    left = attr.ib(validator=attr.validators.instance_of(Boolean))
    right = attr.ib(validator=attr.validators.instance_of(Boolean))


@node
class Or(Boolean):
    """Disjunction."""

    left = attr.ib(validator=attr.validators.instance_of(Boolean))
    right = attr.ib(validator=attr.validators.instance_of(Boolean))


@node
class Not(Boolean):
    """Logical negation."""

    operand = attr.ib(validator=attr.validators.instance_of(Boolean))
