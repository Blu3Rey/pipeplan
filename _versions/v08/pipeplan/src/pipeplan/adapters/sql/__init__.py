"""The SQL adapter family: a relational :class:`DBAdapter` plus its supporting
capability layer (:class:`SqlLoadTarget`), dialect compilers, and load
strategies. This is one adapter family among several -- see ``adapters.file`` and
``adapters.document`` for non-relational peers.
"""

from __future__ import annotations

from . import strategies  # noqa: F401 - registers the load strategies
from .adapter import DBAdapter
from .dialects import AccessDialect, SqlDialect, resolve_dialect
from .target import SqlLoadTarget

__all__ = [
    "DBAdapter",
    "SqlLoadTarget",
    "SqlDialect",
    "AccessDialect",
    "resolve_dialect",
]
