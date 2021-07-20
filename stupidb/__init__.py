# -*- coding: utf-8 -*-

"""Top-level package for stupidb."""

from stupidb.api import *  # noqa: F401,F403

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata  # type: ignore

__version__ = importlib_metadata.version(__name__)

del importlib_metadata
