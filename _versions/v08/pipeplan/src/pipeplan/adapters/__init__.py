"""Resource adapters: the only components that perform external I/O.

Adapters are a registry-driven extension point (``pipeplan.adapters``). Bundled
families: relational (``sql``), flat files (``file``), and JSON/NoSQL documents
(``document``). Each registers itself under a kind string; the factory resolves
that kind from config.
"""

from __future__ import annotations

from .base import Adapter, LoadResult, WriteRequest
from .document import DocumentAdapter
from .factory import create_adapter
from .file import FileAdapter
from .rest import RestAdapter
from .sql import DBAdapter

__all__ = [
    "Adapter",
    "WriteRequest",
    "LoadResult",
    "FileAdapter",
    "DBAdapter",
    "DocumentAdapter",
    "RestAdapter",
    "create_adapter",
]
