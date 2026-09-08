"""Shared helpers for turning JSON documents into DataFrames and back.

Used by every non-relational adapter that speaks JSON (the ``document`` store and
the ``rest`` adapter): nested documents flatten to dotted columns on read and are
reconstructed on write, with absent (NaN/None) keys dropped so documents stay
schemaless.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def to_frame(docs: list[dict[str, Any]], sep: str = ".") -> pd.DataFrame:
    """Flatten a list of (possibly nested) JSON documents to a DataFrame."""
    if not docs:
        return pd.DataFrame()
    return pd.json_normalize(docs, sep=sep)


def to_document(flat: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """Rebuild a nested document from a flat, dotted-key row, dropping NaN/None."""
    out: dict[str, Any] = {}
    for key, value in flat.items():
        if value is None or (np.isscalar(value) and pd.isna(value)):
            continue
        parts = key.split(sep)
        cursor = out
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = value
    return out


def to_documents(frame: pd.DataFrame, sep: str = ".") -> list[dict[str, Any]]:
    return [to_document(row, sep) for row in frame.to_dict(orient="records")]


def extract_records(body: Any, record_path: str | None, sep: str = ".") -> list[dict[str, Any]]:
    """Pull the list of records out of a decoded JSON response body.

    ``record_path`` is a dotted path to the array (e.g. ``data`` or
    ``result.items``); when omitted, a top-level list is used directly and a
    single object is wrapped in a one-element list.
    """
    node = body
    if record_path:
        for part in record_path.split(sep):
            if isinstance(node, dict):
                node = node.get(part)
            else:
                node = None
                break
    if node is None:
        return []
    if isinstance(node, list):
        return [d for d in node if isinstance(d, dict)]
    if isinstance(node, dict):
        return [node]
    return []


def dig(body: Any, path: str | None, sep: str = ".") -> Any:
    """Follow a dotted path into a decoded JSON body, returning None if absent."""
    if not path:
        return None
    node = body
    for part in path.split(sep):
        if isinstance(node, dict):
            node = node.get(part)
        else:
            return None
    return node