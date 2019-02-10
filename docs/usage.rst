=====
Usage
=====

API
---
StupiDB's user facing API is heavily inspired by `dpylr
<https://dplyr.tidyverse.org>`_.

Constructing a Relation
-----------------------
You can construct a relation (a table) by calling the table function with a
list of mappings

.. testsetup:: *

   from stupidb import *
   from pprint import pprint
   from datetime import date, timedelta
   today = date.today()
   days = timedelta(days=1)
   rows = [
       {"name": "Alice", "balance": 400, "date": today},
       {"name": "Alice", "balance": 300, "date": today + 1 * days},
       {"name": "Alice", "balance": 100, "date": today + 2 * days},
       {"name": "Bob", "balance": -150, "date": today - 4 * days},
       {"name": "Bob", "balance": 200, "date": today - 3 * days},
   ]
   L = table(rows)
   R = table(rows[2:])

.. doctest::

   >>> from stupidb import *
   >>> from pprint import pprint
   >>> from datetime import date, timedelta
   >>> today = date.today()
   >>> days = timedelta(days=1)
   >>> rows = [
   ...     {"name": "Alice", "balance": 400, "date": today},
   ...     {"name": "Alice", "balance": 300, "date": today + 1 * days},
   ...     {"name": "Alice", "balance": 100, "date": today + 2 * days},
   ...     {"name": "Bob", "balance": -150, "date": today - 4 * days},
   ...     {"name": "Bob", "balance": 200, "date": today - 3 * days},
   ... ]
   >>> t = table(rows)
   >>> pprint(list(t))
   [Row({'name': 'Alice', 'balance': 400, 'date': datetime.date(2019, 2, 9)}),
    Row({'name': 'Alice', 'balance': 300, 'date': datetime.date(2019, 2, 10)}),
    Row({'name': 'Alice', 'balance': 100, 'date': datetime.date(2019, 2, 11)}),
    Row({'name': 'Bob', 'balance': -150, 'date': datetime.date(2019, 2, 5)}),
    Row({'name': 'Bob', 'balance': 200, 'date': datetime.date(2019, 2, 6)})]

Since every :class:`~stupidb.stupidb.Relation` in StupiDB implements the
`iterator protocol
<https://docs.python.org/3/library/stdtypes.html#iterator-types>`_ (see
:meth:`stupidb.stupidb.Relation.__iter__`), you can

materialize the rows of a relation by calling :class:`list` on the relation.

.. note::

   The :class:`~stupidb.row.Row` objects that make up the elements of the
   :class:`list` above are a very thin layer on top of :class:`dict`, allowing
   two things:

     - Column access by attribute
     - User friendly handling of ambiguous column naming in
       :class:`~stupidb.stupidb.Join` relations.

One design goal of StupiDB is that it allows you to use any mapping you want to
to represent a row, as long as it conforms to the ``typing.Mapping[str, T]``
interface (where ``T`` is an instance of :class:`typing.TypeVar`).

Operations on Relations
-----------------------
StupiDB provides standard operations over relations:

#. Projection (column selection, SELECT, :func:`~stupidb.api.select`).
#. Selection (row filtering, WHERE, :func:`~stupidb.api.sift`).
#. Simple aggregation, using :func:`~stupidb.api.aggregate`.
#. Window functions (including standard aggregate functions, and
   :func:`~stupidb.api.lead`, :func:`~stupidb.api.lag`, etc.).
#. Group by (aggregate by a key, GROUP BY, :func:`~stupidb.api.group_by`)
#. Order by (sorting a relation by one or more columns, ORDER BY,
   :func:`~stupidb.api.order_by`)
#. Join (match rows in one table to another, INNER JOIN, LEFT JOIN, etc., e.g.,
   :func:`~stupidb.api.left_join`)
#. Set operations (UNION [ALL], INTERSECT [ALL], EXCEPT [ALL], using
   :func:`~stupidb.api.union`, :func:`~stupidb.api.union_all`,
   :func:`~stupidb.api.intersect`, :func:`~stupidb.api.intersect_all`,
   :func:`~stupidb.api.difference`, :func:`~stupidb.api.difference_all`)

We'll briefly describe each of these in turn and and show how to use them in
the stupidest way.

Projection (``SELECT``)
-----------------------
.. doctest::

   >>> name_and_bal = (
   ...     table(rows) >> select(n=lambda r: r.name, b=lambda r: r.balance)
   ... )
   >>> bal_times_2 = name_and_bal >> mutate(bal2=lambda r: r.b * 2)
   >>> pprint(list(bal_times_2))
   [Row({'n': 'Alice', 'b': 400, 'bal2': 800}),
    Row({'n': 'Alice', 'b': 300, 'bal2': 600}),
    Row({'n': 'Alice', 'b': 100, 'bal2': 200}),
    Row({'n': 'Bob', 'b': -150, 'bal2': -300}),
    Row({'n': 'Bob', 'b': 200, 'bal2': 400})]

The :func:`~stupidb.api.mutate` function preserves the child table in the
result, while :func:`~stupidb.api.select` does not.

Selection (``WHERE``)
---------------------
Filtering rows is done with the :func:`~stupidb.api.sift` function.

.. doctest::

   >>> alice = table(rows) >> sift(lambda r: r.name == "Alice")
   >>> pprint(list(alice))
   [Row({'name': 'Alice', 'balance': 400, 'date': datetime.date(2019, 2, 9)}),
    Row({'name': 'Alice', 'balance': 300, 'date': datetime.date(2019, 2, 10)}),
    Row({'name': 'Alice', 'balance': 100, 'date': datetime.date(2019, 2, 11)})]

Simple Aggregation
------------------
.. doctest::

   >>> agg = table(rows) >> aggregate(
   ...     my_sum=sum(lambda r: r.balance),
   ...     my_avg=mean(lambda r: r.balance)
   ... )
   >>> pprint(list(agg))
   [Row({'my_sum': 850, 'my_avg': 170.0})]

``GROUP BY``
------------
.. doctest::

   >>> gb = (
   ...     table(rows) >> group_by(name=lambda r: r.name)
   ...                 >> aggregate(bal_over_time=sum(lambda r: r.balance))
   ... )
   >>> pprint(list(gb))
   [Row({'name': 'Alice', 'bal_over_time': 800}),
    Row({'name': 'Bob', 'bal_over_time': 50})]

``ORDER BY``
------------
To sort in ascending order of the specified columns:

.. doctest::

   >>> ob = table(rows) >> order_by(lambda r: r.name, lambda r: r.date)
   >>> pprint(list(ob))
   [Row({'name': 'Alice', 'balance': 400, 'date': datetime.date(2019, 2, 9)}),
    Row({'name': 'Alice', 'balance': 300, 'date': datetime.date(2019, 2, 10)}),
    Row({'name': 'Alice', 'balance': 100, 'date': datetime.date(2019, 2, 11)}),
    Row({'name': 'Bob', 'balance': -150, 'date': datetime.date(2019, 2, 5)}),
    Row({'name': 'Bob', 'balance': 200, 'date': datetime.date(2019, 2, 6)})]

Currently there is no convenient way to sort descending if your order by values
are not numeric.

Joins
-----

``CROSS JOIN``
~~~~~~~~~~~~~~
For two relations :math:`L` and :math:`R`, the cross join, denoted
:math:`\times`, is defined as:

.. math::

   L\times{R} = \left\{l \cup r \mid l \in L\mbox{ and }r \in R\right\}

It's worth noting that all joins can be defined as variations and filters on a
cross join.

In stupidb this is:

.. code-block:: python

   >>> L >> cross_join(R)

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

   >>> L >> inner_join(R, lambda left, right: left.name == right.name)

``LEFT JOIN``
~~~~~~~~~~~~~
The left join is the set of rows from an inner join of two relations, plus the
rows from the left relation that are not in the inner join, substituting NULL
values for those attributes that are missing in the inner join.

In stupidb this is:

.. code-block:: python

   >>> L >> left_join(R, lambda left, right: left.name == right.name)

``RIGHT JOIN``
~~~~~~~~~~~~~~
The right join follows the same logic as the left join, with the tables
reversed.

In stupidb this is:

.. code-block:: python

   >>> L >> right_join(R, lambda left, right: left.name == right.name)

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

   >>> L >> union(R)

``INTERSECT``
~~~~~~~~~~~~~
The `intersection` of two relations :math:`L` and :math:`R` is defined as:

.. math::

   L\cap{R}

that is, tuples that are in both :math:`L` and :math:`R`.

In stupidb this is:

.. code-block:: python

   >>> L >> intersect(R)

``DIFFERENCE``
~~~~~~~~~~~~~~
The `difference` of two relations :math:`L` and :math:`R` is defined as:

.. math::

   L - R

that is, tuples that are in :math:`L` and not in :math:`R`.

In stupidb this is:

.. code-block:: python

   >>> L >> difference(R)

Aggregations
------------
StupiDB is focused on creating the right abstractions. Aggregations are no
exception. To that end there is really one goal:

**Easy creation of custom aggregates, including window functions.**

The UD(A)F interface is heavily inspired by SQLite's aggregate function
interface, so there isn't anything new here with respect to the API.
