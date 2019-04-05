"""Apply algebraic optimizations to a query.

If we're calling code in this module, we've passed the following stages:

#. Parsing
#. Typing
#. Type checking

At this point, assuming we generate correct code, we can run a query though
probably somewhat inefficiently.

We'd like to apply some optimizations before generating code to speed things up
a bit.

* Constant folding
* Expression reassociation
* Projection pushdown
* Predicate pushdown
* Subquery flattening

"""
