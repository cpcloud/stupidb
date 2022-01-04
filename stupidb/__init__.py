"""Top-level package for stupidb."""

import importlib.metadata

from stupidb.aggregation import Window  # noqa: F401
from stupidb.api import *  # noqa: F401,F403

__version__ = importlib.metadata.version(__name__)

del importlib
