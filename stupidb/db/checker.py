"""Check the types of the query.

If we're calling code in this module, we've passed the following stages:

#. Parsing
#. Typing (implies all relations are queryable)

Here we ensure that the query doesn't contain any type errors.

"""
