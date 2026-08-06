"""File-backed adapter: all I/O is pandas file reads/writes.

Supports excel (one collection == one sheet), csv, tsv, json and parquet. Files
own the whole artifact, so ``replace`` rewrites it and ``append`` concatenates;
relational modes (``upsert``/``scd2``/``delete``) are not meaningful for a flat
file and are rejected. Incremental ``since`` filtering is applied in pandas
after the read (files cannot push a predicate down).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import AdapterError
from .base import Adapter

class FileAdapter(Adapter):
    """Read and write dataframes to a single file on disk."""

    SUPPORTED: ClassVar[frozenset[str]] = frozenset(
        {"excel", "xlsx", "xls", "csv", "tsv", "json", "parquet"}
    )

    def __init__(self, config: ResourceConfig) -> None:
        super().__init__(config)
        params = config.params
        # Last Here