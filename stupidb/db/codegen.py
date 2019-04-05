"""Code generator implementation."""

from typing import Generator, List, Tuple

import stupidb.db.typedsyntax as syn
from stupidb.db.visitor import Visitor


class IRGenerator(Visitor):
    """A :class:`Visitor` that constructs the IR for the database."""

    def __init__(self) -> None:  # noqa: D107
        self.register_count = 0
        self.block_count = 0
        self.code: List[Tuple[str, ...]] = []

    instr = property()

    @instr.setter
    def instr(self, instruction: Tuple[str, ...]) -> None:
        """Add a new instruction to the list of program instructions."""
        self.code.append(instruction)

    @property
    def current_register(self) -> str:
        """Return the name of the current register."""
        return f"R{self.register_count}"

    def genreg(self) -> str:
        """Bump the register counter and return the newly created register."""
        self.register_count += 1
        return self.current_register

    def new_register(self) -> Generator:
        """Yield a new register."""
        target = self.genreg()
        yield target

    def new_block(self) -> str:
        """Create a new basic block."""
        self.block_count += 1
        return f"L{self.block_count}"

    def visit_Column(self, node: syn.Column) -> None:
        column_types = {Float64(): "F", Int64(): "I", String(): "S"}
        reg = self.genreg()
        kind = column_types[node.type]
        ireg = self.genreg()
        self.instr = "LOAD_I", "i", ireg
        self.instr = (
            f"GETITEM_{kind.upper()}",
            f"{node.relation.name}.{node.name}",
            ireg,
            reg,
        )

    def visit_Select(self, node: syn.Select) -> None:
        """Construct the IR for a ``SELECT`` statement."""
        # generate attribute reads
        # loop over the rows
        test_block = self.new_block()
        body_block = self.new_block()
        exit_block = self.new_block()

        self.instr = "JUMP", test_block
        self.instr = "BLOCK", test_block

        # generate code for the loop termination
        self.instr = "ALLOC_I", "i"

        ireg = self.genreg()
        self.instr = "MOV_I", 0, ireg
        self.instr = "STORE_I", ireg, "i"

        self.instr = "LOAD_I", "i", ireg

        self.instr = "ALLOC_I", "n"
        nreg = self.genreg()
        self.instr = "STORE_I", nreg, "n"

        condition = self.genreg()
        self.instr = "LT", ireg, nreg, condition

        self.instr = "BRANCH", condition, body_block, exit_block
        self.instr = "BLOCK", body_block

        for expr in node.exprs:
            self.visit(expr)
        # generate the loop body code:
        # * Allocate an array for every expression
        # * For each select item:
        # *   evaluate the expression
        # *   assign the result to the i-th element of the expression's array

        # generate the "next iteration" code
        onereg = self.genreg()
        self.instr = "MOV_I", 1, onereg
        self.instr = "ADD_I", ireg, onereg, ireg
        self.instr = "STORE_I", ireg, "i"
        self.instr = "JUMP", test_block
        self.instr = "BLOCK", exit_block


if __name__ == "__main__":
    import argparse
    from pprint import pprint
    from stupidb.db.parser import parse
    from stupidb.db.schema import Schema, Int64, String, Float64
    from stupidb.db.typed import TypeVisitor

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
    typed_ast = visitor.visit(parsed)
    ir_gen = IRGenerator()
    ir = ir_gen.visit(typed_ast)
    pprint(ir_gen.code)
