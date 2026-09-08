"""Document (NoSQL / key-value) adapter.

Backs a resource whose records are JSON documents rather than relational rows --
the shape of MongoDB, DynamoDB, Firestore, CouchDB, and plain JSON collections.
It is deliberately *not* built on the SQL capability layer: it demonstrates that
the adapter contract (read -> DataFrame, ``write_batch`` over
:class:`WriteRequest`) is storage-agnostic, and that the load *modes* are a shared
vocabulary each family implements in its own terms.

Storage here is a directory of JSON files (one list-of-documents per collection),
or a single ``.json`` file. Nested documents are flattened to dotted columns on
read (via :func:`pandas.json_normalize`) so downstream transforms see a tabular
view, and reconstructed into nested documents on write. Key-based ``upsert`` and
``delete`` operate per-document by key -- true key-value semantics, no rewrite of
unrelated documents' structure. Writes are atomic (temp file + rename).

A real MongoDB/DynamoDB adapter would subclass the same :class:`Adapter`
contract and swap this JSON-file persistence for a driver; the read/normalize and
mode logic here is the reusable core.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import AdapterError
from ..core.registry import register_adapter
from . import jsonutil
from .base import Adapter, LoadResult, WriteRequest


@register_adapter("document")
class DocumentAdapter(Adapter):
    """Read/write collections of JSON documents as normalised DataFrames."""

    def __init__(self, config: ResourceConfig) -> None:
        super().__init__(config)
        path = config.params.get("path")
        if not path:
            raise AdapterError(f"resource '{self.name}': document adapter requires a 'path'")
        self.root = Path(path)
        self.sep = str(config.params.get("separator", "."))

    # ------------------------------------------------------------------ #
    # location
    # ------------------------------------------------------------------ #

    def _file_for(self, collection: str | None) -> Path:
        if self.root.suffix.lower() == ".json":
            return self.root  # single-collection store; collection ignored
        if collection is None:
            raise AdapterError(
                f"resource '{self.name}': a 'collection' is required unless 'path' "
                f"points at a single .json file"
            )
        return self.root / f"{collection}.json"

    # ------------------------------------------------------------------ #
    # document <-> DataFrame
    # ------------------------------------------------------------------ #

    def _read_docs(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        data = json.loads(text)
        if isinstance(data, dict):
            for value in data.values():  # tolerate {"items": [...]} wrappers
                if isinstance(value, list):
                    return value
            return [data]
        return list(data)

    def _to_frame(self, docs: list[dict[str, Any]]) -> pd.DataFrame:
        return jsonutil.to_frame(docs, self.sep)

    def _to_docs(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        return jsonutil.to_documents(frame, self.sep)

    # ------------------------------------------------------------------ #
    # read
    # ------------------------------------------------------------------ #

    def read(self, collection: str | None, *, since: tuple[str, Any] | None = None) -> pd.DataFrame:
        self._require(Permission.READ)
        frame = self._to_frame(self._read_docs(self._file_for(collection)))
        if since is not None and not frame.empty:
            cursor, min_value = since
            if cursor in frame.columns and min_value is not None:
                try:
                    frame = frame[frame[cursor] > min_value]
                except TypeError:
                    frame = frame[frame[cursor].astype("string") > str(min_value)]
                frame = frame.reset_index(drop=True)
        return frame

    # ------------------------------------------------------------------ #
    # write
    # ------------------------------------------------------------------ #

    def write_batch(self, requests: list[WriteRequest]) -> list[LoadResult]:
        self._require(Permission.WRITE)
        return [self._write_one(r) for r in requests]

    def _write_one(self, req: WriteRequest) -> LoadResult:
        mode = req.mode
        if mode is LoadMode.SCD2:
            raise AdapterError(
                f"resource '{self.name}': the document adapter does not support 'scd2' "
                f"(temporal history is a relational pattern; use a db resource)"
            )
        path = self._file_for(req.collection)
        incoming = req.frame
        existing = self._to_frame(self._read_docs(path))
        keys = req.key

        if mode is LoadMode.REPLACE:
            result, res = incoming, LoadResult(req.collection, "replace", len(incoming),
                                               inserted=len(incoming))
        elif mode is LoadMode.APPEND:
            result = self._concat(existing, incoming)
            res = LoadResult(req.collection, "append", len(incoming), inserted=len(incoming))
        elif mode is LoadMode.UPSERT:
            result, res = self._upsert(existing, incoming, keys, req.collection)
        elif mode is LoadMode.DELETE:
            result, res = self._delete(existing, incoming, keys, req.collection)
        else:  # pragma: no cover - guarded by the enum
            raise AdapterError(f"resource '{self.name}': unsupported mode '{mode}'")

        self._atomic_write(self._to_docs(result), path)
        return res

    # -- mode implementations (vectorised, key-value semantics) ----------- #

    @staticmethod
    def _concat(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
        if existing.empty:
            return incoming.reset_index(drop=True)
        cols = list(dict.fromkeys([*existing.columns, *incoming.columns]))
        return pd.concat(
            [existing.reindex(columns=cols), incoming.reindex(columns=cols)], ignore_index=True
        )

    def _require_keys(self, frame: pd.DataFrame, keys: list[str], collection: str) -> None:
        if not keys:
            raise AdapterError(f"document upsert/delete into '{collection}' requires a key")
        missing = [k for k in keys if k not in frame.columns]
        if missing:
            raise AdapterError(
                f"document load into '{collection}': key column(s) {missing} absent "
                f"from the documents (columns: {list(frame.columns)})"
            )

    def _upsert(self, existing, incoming, keys, collection):
        self._require_keys(incoming, keys, collection)
        if incoming[keys].isna().any().any():
            raise AdapterError(f"document upsert into '{collection}': key contains nulls")
        incoming = incoming.drop_duplicates(subset=keys, keep="last")
        if existing.empty:
            return incoming.reset_index(drop=True), LoadResult(
                collection, "upsert", len(incoming), inserted=len(incoming))
        keep_mask = ~self._key_index(existing, keys).isin(self._key_index(incoming, keys))
        matched = int((~keep_mask).sum())
        result = self._concat(existing[keep_mask], incoming)
        return result, LoadResult(collection, "upsert", len(incoming),
                                  inserted=len(incoming) - matched, updated=matched)

    def _delete(self, existing, incoming, keys, collection):
        self._require_keys(incoming, keys, collection)
        if existing.empty:
            return existing, LoadResult(collection, "delete", len(incoming))
        drop_mask = self._key_index(existing, keys).isin(self._key_index(incoming, keys))
        result = existing[~drop_mask].reset_index(drop=True)
        return result, LoadResult(collection, "delete", len(incoming), deleted=int(drop_mask.sum()))

    @staticmethod
    def _key_index(frame: pd.DataFrame, keys: list[str]) -> pd.Series:
        """A comparable per-row key (scalar for single key, tuple for composite)."""
        if len(keys) == 1:
            return frame[keys[0]]
        return pd.Series(list(map(tuple, frame[keys].to_numpy())), index=frame.index)

    # -- atomic persistence ---------------------------------------------- #

    def _atomic_write(self, docs: list[dict[str, Any]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".json")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            tmp.write_text(json.dumps(docs, indent=2, default=str), encoding="utf-8")
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()
