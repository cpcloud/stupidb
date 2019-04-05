"""A parser for StupidSQL."""

import tokenize

from sly import Lexer, Parser

from stupidb.db.syntax import (
    Add,
    And,
    Column,
    Div,
    Eq,
    Expr,
    F,
    Ge,
    Gt,
    Le,
    Literal,
    Lt,
    Mul,
    Named,
    Ne,
    Neg,
    Node,
    Not,
    Or,
    Relation,
    Select,
    Star,
    Sub,
    T,
)


class SQLLexer(Lexer):
    """A sly lexer for SQL."""

    tokens = {
        AND,
        AS,
        BY,
        COMMA,
        DIVIDE,
        DOT,
        EQ,
        FALSE,
        FLOAT,
        FROM,
        GE,
        GROUP,
        GT,
        INTEGER,
        LE,
        LPAREN,
        LT,
        MINUS,
        NAME,
        NE,
        NOT,
        NULL,
        OR,
        PLUS,
        RPAREN,
        SELECT,
        STAR,
        TIMES,
        TRUE,
        WHERE,
    }
    ignore = " \t"

    # Tokens
    AND = "and"
    AS = "as"
    OR = "or"
    COMMA = ","
    NOT = "not"
    NULL = "null"
    TRUE = "true"
    FALSE = "false"
    SELECT = "select"
    FROM = "from"
    WHERE = "where"
    GROUP = "group"
    BY = "by"
    NE = "!="
    LE = "<="
    GE = ">="
    EQ = "="
    LT = "<"
    GT = ">"

    NAME = r"[a-zA-Z_][a-zA-Z0-9_]*"
    INTEGER = tokenize.Decnumber
    FLOAT = tokenize.Floatnumber

    # Special symbols
    PLUS = r"\+"
    MINUS = r"-"
    TIMES = r"\*"
    DIVIDE = r"/"
    LPAREN = r"\("
    RPAREN = r"\)"

    # Ignored pattern
    ignore_newline = r"\n+"

    # Extra action for newlines
    def ignore_newline(self, t) -> None:
        """Ignore newlines."""
        import pdb

        pdb.set_trace()  # noqa
        self.lineno += t.value.count("\n")

    def error(self, t) -> None:
        print(f"Illegal character {t.value[0]!r}")
        self.index += 1


class SQLParser(Parser):
    """A sly parser for SQL."""

    tokens = SQLLexer.tokens

    precedence = (
        ("left", OR),
        ("left", AND),
        ("nonassoc", LT, LE, GT, GE, EQ, NE),
        ("left", PLUS, MINUS),
        ("left", TIMES, DIVIDE),
        ("right", UPLUS, UMINUS, NOT),
    )

    @_("SELECT items FROM relations WHERE expr GROUP BY group_by")
    def query(self, p):
        return Select(
            p.items, relations=p.relations, where=p.expr, group_by=p.group_by
        )

    @_("SELECT items FROM relations WHERE expr")
    def query(self, p):
        return Select(p.items, relations=p.relations, where=p.expr)

    @_("SELECT items FROM relations")
    def query(self, p):
        return Select(p.items, relations=p.relations)

    @_("SELECT items")
    def query(self, p):
        return Select(p.items)

    @_("items COMMA item")
    def items(self, p):
        p.items.append(p.item)
        return p.items

    @_("item")
    def items(self, p):
        return [p.item]

    @_("expr AS NAME")
    def item(self, p):
        return Named(p.expr, name=p.NAME)

    @_("expr")
    def item(self, p):
        return p.expr

    @_("STAR")
    def item(self, p):
        return Star()

    @_("NAME DOT STAR")
    def item(self, p):
        return Star(relation=p.NAME)

    @_("group_by COMMA grouping_element")
    def group_by(self, p):
        p.group_by.append(p.grouping_element)
        return p.group_by

    @_("grouping_element")
    def group_by(self, p):
        return [p.grouping_element]

    @_("expr")
    def grouping_element(self, p):
        return p.expr

    @_("relations COMMA relation")
    def relations(self, p):
        p.relations.append(p.relation)
        return p.relations

    @_("relation")
    def relations(self, p):
        return [p.relation]

    @_("NAME")
    def relation(self, p):
        return Relation(p.NAME)

    @_("NAME")
    def expr(self, p) -> Column:
        return Column(p.NAME)

    @_("expr PLUS expr")
    def expr(self, p) -> Add:
        return Add(p.expr0, p.expr1)

    @_("expr MINUS expr")
    def expr(self, p) -> Sub:
        return Sub(p.expr0, p.expr1)

    @_("expr TIMES expr")
    def expr(self, p) -> Mul:
        return Mul(p.expr0, p.expr1)

    @_("expr DIVIDE expr")
    def expr(self, p) -> Div:
        return Div(p.expr0, p.expr1)

    @_("expr EQ expr")
    def expr(self, p) -> Eq:
        return Eq(p.expr0, p.expr1)

    @_("expr NE expr")
    def expr(self, p) -> Ne:
        return Ne(p.expr0, p.expr1)

    @_("expr LT expr")
    def expr(self, p) -> Lt:
        return Lt(p.expr0, p.expr1)

    @_("expr LE expr")
    def expr(self, p) -> Le:
        return Le(p.expr0, p.expr1)

    @_("expr GT expr")
    def expr(self, p) -> Gt:
        return Gt(p.expr0, p.expr1)

    @_("expr GE expr")
    def expr(self, p) -> Ge:
        return Ge(p.expr0, p.expr1)

    @_("MINUS expr %prec UMINUS")
    def expr(self, p) -> Neg:
        return Neg(p.expr)

    @_("PLUS expr %prec UPLUS")
    def expr(self, p) -> Neg:
        return Pos(p.expr)

    @_("expr AND expr")
    def expr(self, p) -> And:
        return And(p.expr0, p.expr1)

    @_("expr OR expr")
    def expr(self, p) -> Or:
        return Or(p.expr0, p.expr1)

    @_("NOT expr")
    def expr(self, p) -> Not:
        return Not(p.expr)

    @_("LPAREN expr RPAREN")
    def expr(self, p) -> Expr:
        return p.expr

    @_("TIMES")
    def expr(self, p) -> Star:
        return Star()

    @_("INTEGER")
    def expr(self, p) -> Literal:
        return Literal(int(p.INTEGER))

    @_("FLOAT")
    def expr(self, p) -> Literal:
        return Literal(float(p.FLOAT))

    @_("TRUE")
    def expr(self, p) -> T:
        return T()

    @_("FALSE")
    def expr(self, p) -> F:
        return F()

    @_("NULL")
    def expr(self, p) -> Literal:
        return Literal(None)


def parse(query: str) -> Node:
    """Parse a query."""
    lexer = SQLLexer()
    parser = SQLParser()
    tokens = list(lexer.tokenize(query))
    node = parser.parse(iter(tokens))
    return node


def main() -> None:  # noqa: D103
    while True:
        try:
            text = input("stupidb > ")
        except EOFError:
            break
        except KeyboardInterrupt:
            print()
            continue
        else:
            if text:
                node = parse(text)
                print(node)


if __name__ == "__main__":
    main()
