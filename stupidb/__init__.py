"""Top-level package for stupidb."""

from stupidb.aggregation import Window  # noqa: F401
from stupidb.api import *  # noqa: F401,F403

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

__version__ = importlib_metadata.version(__name__)
