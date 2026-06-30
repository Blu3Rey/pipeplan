"""Resource adapters: the only components that perform external I/O."""

from __future__ import annotations

from .base import Adapter
from .db import DBAdapter
from .factory import create_adapter
from .file import FileAdapter

__all__ = ["Adapter", "DBAdapter", "FileAdapter", "create_adapter"]
