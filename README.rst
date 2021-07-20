=======
StupiDB
=======

.. image:: https://img.shields.io/pypi/v/stupidb.svg
        :target: https://pypi.python.org/pypi/stupidb

.. image:: https://github.com/cpcloud/stupidb/actions/workflows/ci.yml/badge.svg?branch=master
        :target: https://github.com/cpcloud/stupidb/actions/workflows/ci.yml

.. image:: https://readthedocs.org/projects/stupidb/badge/?version=latest
        :target: https://stupidb.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

Are you tired of software that's too smart? Try StupiDB, the stupidest database
you'll ever come across.

StupiDB was built to understand how a relational database might be implemented.

RDBMSs like PostgreSQL are extremely complex. It was hard for to me to imagine
what implementing the core of a relational database like PostgreSQL would look
like just by tinkering with and reading the source code, so I decided to write
my own.

* Free software: Apache Software License 2.0
* Documentation: https://stupidb.readthedocs.io.

Features
--------
* Stupid joins
* Idiotic window functions
* Woefully naive set operations
* Sophomoric group bys
* Dumb custom aggregates
* Scales down, to keep expectations low
* Wildly cloud unready
* Worst-in-class performance

Non-Features
------------
* Stupid simple in-memory format: ``Iterable[Mapping[str, Any]]``
* Stupidly clean codebase

Credits
-------
This package was created with Cookiecutter_ and the
`audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
