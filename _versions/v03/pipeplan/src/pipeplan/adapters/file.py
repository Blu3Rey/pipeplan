"""File-backed adapter: all I/O is pandas file reads/writes.

Supports excel (one collection == one sheet), csv, tsv, json and parquet. Files
own the whole artifact, so ``replace`` rewrites it and ``append`` concatenates;
relational modes (``upsert``/``scd2``/``delete``) are not meaningful for a flat
file and are rejected. Incremental ``since`` filtering is applied in pandas
after the read (files cannot push a predicate down).
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import AdapterError
from .base import Adapter, LoadResult, WriteRequest


class FileAdapter(Adapter):
    """Read and write dataframes to a single file on disk."""

    SUPPORTED: ClassVar[frozenset[str]] = frozenset(
        {"excel", "xlsx", "xls", "csv", "tsv", "json", "parquet"}
    )

    def __init__(self, config: ResourceConfig) -> None:
        super().__init__(config)
        params = config.params
        fmt = str(params.get("format", "")).lower()
        if not fmt:
            raise AdapterError(f"resource '{self.name}': file adapter requires a 'format'")
        if fmt not in self.SUPPORTED:
            raise AdapterError(
                f"resource '{self.name}': unsupported file format '{fmt}' "
                f"(supported: {', '.join(sorted(self.SUPPORTED))})"
            )
        path = params.get("path")
        if not path:
            raise AdapterError(f"resource '{self.name}': file adapter requires a 'path'")
        self.format = "excel" if fmt in {"excel", "xlsx", "xls"} else fmt
        self.path = Path(path)
        self.options: dict[str, Any] = dict(params.get("options", {}))

    # ------------------------------------------------------------------ #
    # read
    # ------------------------------------------------------------------ #

    def read(self, collection: str | None, *, since: tuple[str, Any] | None = None) -> pd.DataFrame:
        self._require(Permission.READ)
        if not self.path.exists():
            raise AdapterError(f"resource '{self.name}': file not found for read: {self.path}")
        try:
            frame = self._read_raw(collection)
        except AdapterError:
            raise
        except Exception as exc:  # pragma: no cover - surfaced as AdapterError
            raise AdapterError(f"resource '{self.name}': failed to read {self.path}: {exc}") from exc
        if since is not None:
            cursor, min_value = since
            if cursor in frame.columns and min_value is not None:
                col = frame[cursor]
                try:
                    frame = frame[col > min_value]
                except TypeError:
                    frame = frame[col.astype("string") > str(min_value)]
                frame = frame.reset_index(drop=True)
        return frame

    def _read_raw(self, collection: str | None) -> pd.DataFrame:
        if self.format == "excel":
            if collection is None:
                raise AdapterError(
                    f"resource '{self.name}': excel reads require a sheet name (set 'collection')"
                )
            return pd.read_excel(self.path, sheet_name=collection, **self.options)
        if self.format == "csv":
            return pd.read_csv(self.path, **self.options)
        if self.format == "tsv":
            return pd.read_csv(self.path, sep="\t", **self.options)
        if self.format == "json":
            return self._read_json()
        if self.format == "parquet":
            return pd.read_parquet(self.path, **self.options)
        raise AdapterError(f"resource '{self.name}': unreachable format")  # pragma: no cover

    def _read_json(self) -> pd.DataFrame:
        text = self.path.read_text(encoding="utf-8").strip()
        if not text:
            return pd.DataFrame()
        if text[0] in "[{":
            data = json.loads(text)
            if isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list):
                        return pd.DataFrame(value)
                return pd.DataFrame([data])
            return pd.DataFrame(data)
        return pd.read_json(self.path, lines=True, **self.options)

    # ------------------------------------------------------------------ #
    # write
    # ------------------------------------------------------------------ #

    def write_batch(self, requests: list[WriteRequest]) -> list[LoadResult]:
        # Files are independent artifacts; there is no cross-file transaction.
        return [self._write_one(r) for r in requests]

    def _write_one(self, req: WriteRequest) -> LoadResult:
        self._require(Permission.WRITE)
        frame, collection, mode = req.frame, req.collection, req.mode
        if mode in (LoadMode.SCD2, LoadMode.DELETE, LoadMode.UPSERT):
            raise AdapterError(
                f"resource '{self.name}': mode '{mode.value}' is not supported by the "
                f"file adapter -- flat files have no keys. Use a db resource for "
                f"relational load modes, or 'replace'/'append'."
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        appended = mode is LoadMode.APPEND and self._exists_for_append(collection)
        combined = self._combine(frame, collection) if appended else frame
        self._atomic_write(combined, collection)
        return LoadResult(collection, mode.value, len(frame), inserted=len(frame),
                          created_table=not appended)

    # -- append helpers --------------------------------------------------- #

    def _exists_for_append(self, collection: str) -> bool:
        if self.format == "excel":
            if not self.path.exists():
                return False
            try:
                pd.read_excel(self.path, sheet_name=collection, nrows=0)
                return True
            except (ValueError, KeyError):
                return False
        return self.path.exists()

    def _combine(self, frame: pd.DataFrame, collection: str) -> pd.DataFrame:
        existing = self._read_existing(collection)
        # Align columns by name so appends never misalign positionally.
        cols = list(dict.fromkeys([*existing.columns, *frame.columns]))
        existing = existing.reindex(columns=cols)
        frame = frame.reindex(columns=cols)
        return pd.concat([existing, frame], ignore_index=True)

    def _read_existing(self, collection: str) -> pd.DataFrame:
        if self.format == "excel":
            return pd.read_excel(self.path, sheet_name=collection)
        if self.format in {"csv", "tsv"}:
            sep = "\t" if self.format == "tsv" else ","
            return pd.read_csv(self.path, sep=sep)
        if self.format == "json":
            return self._read_json()
        if self.format == "parquet":
            return pd.read_parquet(self.path)
        raise AdapterError(f"resource '{self.name}': cannot read existing {self.format}")

    # -- atomic write ----------------------------------------------------- #

    def _atomic_write(self, frame: pd.DataFrame, collection: str) -> None:
        """Write to a temp file in the same directory, then atomically replace,
        so a crash mid-write never corrupts the existing artifact."""
        try:
            if self.format == "excel":
                self._write_excel_atomic(frame, collection)
                return
            suffix = self.path.suffix or ".tmp"
            fd, tmp_name = tempfile.mkstemp(dir=str(self.path.parent), suffix=suffix)
            os.close(fd)
            tmp = Path(tmp_name)
            try:
                if self.format in {"csv", "tsv"}:
                    sep = "\t" if self.format == "tsv" else ","
                    frame.to_csv(tmp, sep=sep, index=False)
                elif self.format == "json":
                    tmp.write_text(
                        json.dumps(frame.to_dict(orient="records"), indent=2, default=str),
                        encoding="utf-8",
                    )
                elif self.format == "parquet":
                    frame.to_parquet(tmp, index=False)
                os.replace(tmp, self.path)
            finally:
                if tmp.exists():
                    tmp.unlink()
        except AdapterError:
            raise
        except Exception as exc:  # pragma: no cover - surfaced as AdapterError
            raise AdapterError(f"resource '{self.name}': failed to write {self.path}: {exc}") from exc

    def _write_excel_atomic(self, frame: pd.DataFrame, sheet: str) -> None:
        # Preserve other sheets: build the full workbook then replace the file.
        sheets: dict[str, pd.DataFrame] = {}
        if self.path.exists():
            try:
                sheets = pd.read_excel(self.path, sheet_name=None)
            except Exception:  # pragma: no cover - unreadable workbook
                sheets = {}
        sheets[sheet] = frame
        fd, tmp_name = tempfile.mkstemp(dir=str(self.path.parent), suffix=".xlsx")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            with pd.ExcelWriter(tmp, engine="openpyxl", mode="w") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
            os.replace(tmp, self.path)
        finally:
            if tmp.exists():
                tmp.unlink()
