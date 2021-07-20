.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/cpcloud/stupidb/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

stupidb could always use more documentation, whether as part of the
official stupidb docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at
https://github.com/cpcloud/stupidb/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `stupidb` for local development.

1. Fork the `stupidb` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/stupidb.git

3. `Install nix <https://nixos.org/guides/install-nix.html>`_

4. Enable the ``stupidb`` `cachix`_ cache::

    $ nix run nixpkgs.cachix -c cachix use stupidb

   .. note::

      This step is optional but **highly recommended**. Without the ``stupidb``
      cache, most dependencies will be built from source and it will take a
      very long time to setup your development environment.

      The ``stupidb`` cache is **only** populated from CI, using ``nix-shell
      --pure``. This greatly decreases the likelihood that something
      unintentional makes its way into the cache.

5. Enter a `nix-shell`::

    $ cd stupidb/
    $ nix-shell

   This will take a bit of time, but should be relatively quick if you
   enabled the ``stupidb`` cachix cache.

6. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

7. When you're done making changes, check that your changes pass formatting
   checks and tests::

    $ make format
    $ make check
    $ make test

8. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit
    $ git push origin name-of-your-bugfix-or-feature

9. Submit a pull request using the `gh <https://cli.github.com>`_ CLI tool or
   through the GitHub web UI.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put your
   new functionality into a function with a docstring, and add the feature to
   the list in README.rst.
3. The pull request should work for Python 3.8. Check
   https://github.com/cpcloud/stupidb/actions and make sure that the tests
   pass.

Releasing
---------

Releases are entirely automated using `semantic release`_ conventions.

.. warning::

   Releasing by hand is intentionally not documented. Do **not** release
   anything by hand.

1. Releases are cut for every commit as determined by `python-semantic-release`_.
2. `CHANGELOG.md` is automatically updated with new changes by the
   `python-semantic-release GitHub action`_.
3. Versions are bumped in the necessary places automatically.
4. Docs are updated at https://readthedocs.org/projects/stupidb on every commit
   regardless of whether a release is cut or not.

.. _python-semantic-release: https://python-semantic-release.readthedocs.io
.. _python-semantic-release GitHub Action: https://python-semantic-release.readthedocs.io/en/latest/automatic-releases/github-actions.html
.. _semantic release: https://semantic-release.gitbook.io/semantic-release
.. _cachix: https://cachix.org
