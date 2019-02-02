=====
Usage
=====

API
---
StupiDB's user facing API is heavily inspired by dpylr.

Constructing a Relation
-----------------------
You can construct a relation (a table) by calling the table function with a
list of mappings

.. code-block:: python

   >>> from stupidb import *
   >>> from datetime import date, timedelta
   >>> today = date.today()
   >>> days = timedelta(days=1)
   >>> rows = [
   ...     {"name": "Alice", "balance": 400, "date": today},
   ...     {"name": "Alice", "balance": 300, "date": today + 1 * days},
   ...     {"name": "Alice", "balance": 100, "date": today + 2 * days},
   ...     {"name": "Bob", "balance": -150, "date": today - 4 * days)},
   ...     {"name": "Bob", "balance": 200, "date": today - 3 * days},
   ... ]
   >>> t = table(rows)

.. note::

   The examples that follow assume you've executed the above code in a Python
   interpreter.

Since every relation in StupiDB implements the iterator protocol, you can
materialize the rows of a relation by calling ``list`` on the relation.

One design goal of StupiDB is that it allows you to use any mapping you want to
to represent a row, as long as it conforms to the ``typing.Mapping[str, T]``
interface (where ``T`` is a type variable).

Operations on Relations
-----------------------
StupiDB provides standard relational operations such as:

#. Projection (column selection, SELECT)
#. Selection (row filtering, WHERE)
#. Simple aggregation
#. Window functions (including standard aggregate functions, lead, lag, etc.)
#. Group by (aggregate by a key, GROUP BY)
#. Order by (sorting a relation by one or more columns, ORDER BY)
#. Join (match rows in one table to another, INNER JOIN, LEFT JOIN, etc.)
#. Set operations (UNION, INTERSECT, EXCEPT)

We'll briefly describe each of these in turn and and show how to use them in
the stupidest way.

Projection (``SELECT``)
-----------------------
.. code-block:: python

   >>> name_and_bal = t >> select(n=lambda r:.name, b=lambda r: r.balance)
   >>> bal_times_2 = name_and_bal >> mutate(bal2=lambda r: r.b * 2)

The :func:`~stupidb.api.mutate` function preserves the child table in the
result, while :func:`~stupidb.api.select` does not.

Selection (``WHERE``)
---------------------
Filtering rows is done with the :func:`~stupidb.api.sift` function.

.. code-block:: python

   >>> alices = t >> sift(lambda r: r.name == "Alice")

Simple Aggregation
------------------
.. code-block:: python

   >>> agg = t >> aggregate(
   ...     my_sum=sum(lambda r: r.balance),
   ...     my_avg=mean(lambda r: r.balance)
   ... )

``GROUP BY``
------------
.. code-block:: python

   >>> gb = (
   ...     t >> group_by(lambda r: r.name)
   ...       >> aggregate(bal_over_time=sum(lambda r: r.balance))
   ... )

``ORDER BY``
------------
To sort in ascending order of the specified columns:

.. code-block:: python

   >>> ob = t >> order_by(lambda r: r.name, lambda r: r.date)

Currently there is no convenient way to sort descending.

Joins
-----

``CROSS JOIN``
~~~~~~~~~~~~~~
For two relations :math:`L` and :math:`R`, the cross join, denoted
:math:`\times`, is defined as:

.. math::

   L\times{R} = \left\{l \cup r \mid l \in L\mbox{ and }r \in R\right\}

It's worth noting that all joins can be defined in terms of a cross join.

In stupidb this is:

.. code-block:: python

   >>> t >> cross_join(t)

``INNER JOIN``
~~~~~~~~~~~~~~
Given the definition of a cross join and two relations :math:`L` and :math:`R`
and a predicate :math:`p\left(l, r\right)\rightarrow\mbox{bool}`, which is a
function that takes a tuple :math:`l\in{L}` and a tuple :math:`r\in{R}` the
inner join is defined as:

.. math::

   \left\{l\cup{r}\mid l\in{L}\mbox{ and }r\in{R}\mbox{ if }p\left(l, r\right)\right\}

In stupidb this is:

.. code-block:: python

   >>> t >> inner_join(t, lambda left, right: left.name == right.name)

``LEFT JOIN``
~~~~~~~~~~~~~
The left join is the set of rows from an inner join of two relations, plus the
rows from the left relation that are not in the inner join, substituting NULL
values for those attributes that are missing in the inner join.

In stupidb this is:

.. code-block:: python

   >>> t >> left_join(t, lambda left, right: left.name == right.name)

``RIGHT JOIN``
~~~~~~~~~~~~~~
The right join follows the same logic as the left join, with the tables
reversed.

In stupidb this is:

.. code-block:: python

   >>> t >> right_join(t, lambda left, right: left.balance < right.balance)

Set Operations
--------------

``UNION``
~~~~~~~~~
The `union` of two relations :math:`L` and :math:`R` is defined as:

.. math::

   L\cup{R}

that is, tuples that are in either :math:`L` or :math:`R`.

In stupidb this is:

.. code-block:: python

   >>> t >> union(t)

``INTERSECT``
~~~~~~~~~~~~~
The `intersection` of two relations :math:`L` and :math:`R` is defined as:

.. math::

   L\cap{R}

that is, tuples that are in both :math:`L` and :math:`R`.

In stupidb this is:

.. code-block:: python

   >>> t >> intersect(t)

``DIFFERENCE``
~~~~~~~~~~~~~~
The `difference` of two relations :math:`L` and :math:`R` is defined as:

.. math::

   L - R

that is, tuples that are in :math:`L` and not in :math:`R`.

In stupidb this is:

.. code-block:: python

   >>> t >> difference(t)

Aggregations
------------
StupiDB is focused on creating the right abstractions. Aggregations are no
exception. To that end there is really one goal:

**Easy creation of custom aggregates, including window functions.**

The UD(A)F interface is heavily inspired by SQLite's aggregate function
interface, so there isn't anything new here with respect to the API.
