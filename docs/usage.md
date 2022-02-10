# Usage

## API

StupiDB's API is heavily inspired by [dplyr](https://dplyr.tidyverse.org).

## Constructing a Relation

You can construct a relation (a table) by calling the table function with a
list of mappings:

```python
>>> from stupidb import *
>>> from datetime import date, timedelta
>>> today = date(2019, 2, 9)
>>> days = timedelta(days=1)
>>> rows = [
...     {"name": "Alice", "balance": 400, "date": today},
...     {"name": "Alice", "balance": 300, "date": today + 1 * days},
...     {"name": "Alice", "balance": 100, "date": today + 2 * days},
...     {"name": "Bob", "balance": -150, "date": today - 4 * days},
...     {"name": "Bob", "balance": 200, "date": today - 3 * days},
... ]
>>> t = table(rows)
>>> t
name      balance  date
------  ---------  ----------
Alice         400  2019-02-09
Alice         300  2019-02-10
Alice         100  2019-02-11
Bob          -150  2019-02-05
Bob           200  2019-02-06
```

<!-- prettier-ignore-start -->
Since every [`Relation`][stupidb.core.Relation] in StupiDB implements the
[iterator protocol](https://docs.python.org/3/library/stdtypes.html#iterator-types), you can
materialize the rows of a relation by calling [`list`][list] on the relation.
<!-- prettier-ignore-end -->

!!! note "Rows are mappings!"

    The [`Row`][stupidb.row.Row] objects that make up the elements of the
    [`list`][list] above are a thin wrapper around [`dict`][dict], allowing
    two things:

      - Column access by attribute
      - User friendly handling of ambiguous column naming in
        [`Join`][stupidb.core.Join] relations.

<!-- prettier-ignore-start -->
One design goal of StupiDB is that it allows you to use any mapping you want to
to represent a row, as long as it conforms to the
[`typing.Mapping`][typing.Mapping] interface.
<!-- prettier-ignore-end -->

## Operations on Relations

StupiDB provides standard operations over relations:

| Operation        | SQL                   | StupiDB                              | Examples                             |
| ---------------- | --------------------- | ------------------------------------ | ------------------------------------ |
| Projection       | `SELECT`              | [`select`][stupidb.api.select]       | -                                    |
| Selection        | `WHERE`               | [`sift`][stupidb.api.sift]           | -                                    |
| Aggregation      | Ex: `SUM`             | [`aggregate`][stupidb.api.aggregate] | [`sum`][stupidb.api.sum]             |
| Window Functions | `OVER`                | [`over`][stupidb.api.over]           | [`lag`][stupidb.api.lag]             |
| Group By         | `GROUP BY`            | [`group_by`][stupidb.api.group_by]   | -                                    |
| Order By         | `ORDER BY`            | [`order_by`][stupidb.api.order_by]   | -                                    |
| Join             | Ex: `LEFT OUTER JOIN` | -                                    | [`left_join`][stupidb.api.left_join] |
| Set Operations   | Ex: `UNION`           | -                                    | [`union`][stupidb.api.union]         |

Let's look at each of these concepts

<!--  -->
<!-- Projection (``SELECT``) -->
<!-- ----------------------- -->
<!-- .. doctest:: -->
<!--  -->
<!--    >>> name_and_bal = ( -->
<!--    ...     table(rows) >> select(n=lambda r: r.name, b=lambda r: r.balance) -->
<!--    ... ) -->
<!--    >>> bal_times_2 = name_and_bal >> mutate(bal2=lambda r: r.b * 2) -->
<!--    >>> bal_times_2 -->
<!--    n         b    bal2 -->
<!--    -----  ----  ------ -->
<!--    Alice   400     800 -->
<!--    Alice   300     600 -->
<!--    Alice   100     200 -->
<!--    Bob    -150    -300 -->
<!--    Bob     200     400 -->
<!--  -->
<!-- The :func:`~stupidb.api.mutate` function preserves the child table in the -->
<!-- result, while :func:`~stupidb.api.select` does not. -->
<!--  -->
<!-- Selection (``WHERE``) -->
<!-- --------------------- -->
<!-- Filtering rows is done with the :func:`~stupidb.api.sift` function. -->
<!--  -->
<!-- .. doctest:: -->
<!--  -->
<!--    >>> alice = table(rows) >> sift(lambda r: r.name == "Alice") -->
<!--    >>> alice -->
<!--    name      balance  date -->
<!--    ------  ---------  ---------- -->
<!--    Alice         400  2019-02-09 -->
<!--    Alice         300  2019-02-10 -->
<!--    Alice         100  2019-02-11 -->
<!--  -->
<!-- Simple Aggregation -->
<!-- ------------------ -->
<!-- .. doctest:: -->
<!--  -->
<!--    >>> agg = table(rows) >> aggregate( -->
<!--    ...     my_sum=sum(lambda r: r.balance), -->
<!--    ...     my_avg=mean(lambda r: r.balance) -->
<!--    ... ) -->
<!--    >>> agg -->
<!--      my_sum    my_avg -->
<!--    --------  -------- -->
<!--         850       170 -->
<!--  -->
<!-- ``GROUP BY`` -->
<!-- ------------ -->
<!-- .. doctest:: -->
<!--  -->
<!--    >>> gb = ( -->
<!--    ...     table(rows) >> group_by(name=lambda r: r.name) -->
<!--    ...                 >> aggregate(bal_over_time=sum(lambda r: r.balance)) -->
<!--    ... ) -->
<!--    >>> gb -->
<!--    name      bal_over_time -->
<!--    ------  --------------- -->
<!--    Alice               800 -->
<!--    Bob                  50 -->
<!--  -->
<!-- ``ORDER BY`` -->
<!-- ------------ -->
<!-- To sort in ascending order of the specified columns: -->
<!--  -->
<!-- .. doctest:: -->
<!--  -->
<!--    >>> ob = table(rows) >> order_by(lambda r: r.name, lambda r: r.date) -->
<!--    >>> ob -->
<!--    name      balance  date -->
<!--    ------  ---------  ---------- -->
<!--    Alice         400  2019-02-09 -->
<!--    Alice         300  2019-02-10 -->
<!--    Alice         100  2019-02-11 -->
<!--    Bob          -150  2019-02-05 -->
<!--    Bob           200  2019-02-06 -->
<!--  -->
<!-- Currently there is no convenient way to sort descending if your order by values -->
<!-- are not numeric. -->
<!--  -->
<!-- Joins -->
<!-- ----- -->
<!--  -->
<!-- ``CROSS JOIN`` -->
<!-- ~~~~~~~~~~~~~~ -->
<!-- For two relations :math:`L` and :math:`R`, the cross join, denoted -->
<!-- :math:`\times`, is defined as: -->
<!--  -->
<!-- .. math:: -->
<!--  -->
<!--    L\times{R} = \left\{l \cup r \mid l \in L\mbox{ and }r \in R\right\} -->
<!--  -->
<!-- It's worth noting that all joins can be defined as variations and filters on a -->
<!-- cross join. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> cross_join(R) -->
<!--  -->
<!-- ``INNER JOIN`` -->
<!-- ~~~~~~~~~~~~~~ -->
<!-- Given the definition of a cross join and two relations :math:`L` and :math:`R` -->
<!-- and a predicate :math:`p\left(l, r\right)\rightarrow\mbox{bool}`, which is a -->
<!-- function that takes a tuple :math:`l\in{L}` and a tuple :math:`r\in{R}` the -->
<!-- inner join is defined as: -->
<!--  -->
<!-- .. math:: -->
<!--  -->
<!--    \left\{l\cup{r}\mid l\in{L}\mbox{ and }r\in{R}\mbox{ if }p\left(l, r\right)\right\} -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> inner_join(R, lambda left, right: left.name == right.name) -->
<!--  -->
<!-- ``LEFT JOIN`` -->
<!-- ~~~~~~~~~~~~~ -->
<!-- The left join is the set of rows from an inner join of two relations, plus the -->
<!-- rows from the left relation that are not in the inner join, substituting NULL -->
<!-- values for those attributes that are missing in the inner join. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> left_join(R, lambda left, right: left.name == right.name) -->
<!--  -->
<!-- ``RIGHT JOIN`` -->
<!-- ~~~~~~~~~~~~~~ -->
<!-- The right join follows the same logic as the left join, with the tables -->
<!-- reversed. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> right_join(R, lambda left, right: left.name == right.name) -->
<!--  -->
<!-- Set Operations -->
<!-- -------------- -->
<!--  -->
<!-- ``UNION`` -->
<!-- ~~~~~~~~~ -->
<!-- The `union` of two relations :math:`L` and :math:`R` is defined as: -->
<!--  -->
<!-- .. math:: -->
<!--  -->
<!--    L\cup{R} -->
<!--  -->
<!-- that is, tuples that are in either :math:`L` or :math:`R`. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> union(R) -->
<!--  -->
<!-- ``INTERSECT`` -->
<!-- ~~~~~~~~~~~~~ -->
<!-- The `intersection` of two relations :math:`L` and :math:`R` is defined as: -->
<!--  -->
<!-- .. math:: -->
<!--  -->
<!--    L\cap{R} -->
<!--  -->
<!-- that is, tuples that are in both :math:`L` and :math:`R`. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> intersect(R) -->
<!--  -->
<!-- ``DIFFERENCE`` -->
<!-- ~~~~~~~~~~~~~~ -->
<!-- The `difference` of two relations :math:`L` and :math:`R` is defined as: -->
<!--  -->
<!-- .. math:: -->
<!--  -->
<!--    L - R -->
<!--  -->
<!-- that is, tuples that are in :math:`L` and not in :math:`R`. -->
<!--  -->
<!-- In stupidb this is: -->
<!--  -->
<!-- .. code-block:: python -->
<!--  -->
<!--    >>> L >> difference(R) -->
<!--  -->
<!-- Aggregations -->
<!-- ------------ -->
<!-- StupiDB is focused on creating the right abstractions. Aggregations are no -->
<!-- exception. To that end there is really one goal: -->
<!--  -->
<!-- **Easy creation of custom aggregates, including window functions.** -->
<!--  -->
<!-- The UD(A)F interface is heavily inspired by SQLite's aggregate function -->
<!-- interface, so there isn't anything new here with respect to the API. -->
