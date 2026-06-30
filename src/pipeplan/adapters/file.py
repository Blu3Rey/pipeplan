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

from 