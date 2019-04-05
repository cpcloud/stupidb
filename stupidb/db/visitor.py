"""Visitor pattern implementation."""


class Visitor:
    def visit(self, node, **kwargs):
        """Visit a node."""
        node_typename = type(node).__name__
        method_name = f"visit_{node_typename}"
        method = getattr(self, method_name, None)
        if method is None:
            raise NotImplementedError(f"Undefined method {method_name!r}")
        return method(node, **kwargs)
