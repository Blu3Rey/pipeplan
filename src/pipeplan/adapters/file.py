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

    def write(
        self,
        frame: pd.DataFrame,
        collection: str,
        *,
        mode: LoadMode,
        key: str | list[str] | None = None,
        chunksize: int | None = None,
        partition_by: list[str] | None = None,
        scd: dict[str, Any] | None = None,
    ) -> None:
        self._require(Permission.WRITE)
        if mode in (LoadMode.SCD2, LoadMode.DELETE):
            raise AdapterError(
                f"resource '{self.name}': mode '{mode.value}' is not supported by the "
                f"file adapter (use a db resource for relational load modes)"
            )
        effective = LoadMode.REPLACE if mode is LoadMode.UPSERT else mode
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self.format == "excel":
                self._write_excel(frame, collection, effective)
            elif self.format in {"csv", "tsv"}:
                self._write_delimited(frame, effective, chunksize)
            elif self.format == "json":
                self._write_json(frame, effective)
            elif self.format == "parquet":
                self._write_parquet(frame, effective)
        except AdapterError:
            raise
        except Exception as exc:  # pragma: no cover - surfaced as AdapterError
            raise AdapterError(f"resource '{self.name}': failed to write {self.path}: {exc}") from exc

    def _write_excel(self, frame: pd.DataFrame, sheet: str, mode: LoadMode) -> None:
        file_exists = self.path.exists()
        if mode is LoadMode.APPEND and file_exists:
            try:
                existing = pd.read_excel(self.path, sheet_name=sheet)
                frame = pd.concat([existing, frame], ignore_index=True)
            except (ValueError, KeyError):
                pass
        if file_exists:
            with pd.ExcelWriter(self.path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
                frame.to_excel(w, sheet_name=sheet, index=False)
        else:
            with pd.ExcelWriter(self.path, engine="openpyxl", mode="w") as w:
                frame.to_excel(w, sheet_name=sheet, index=False)

    def _write_delimited(self, frame: pd.DataFrame, mode: LoadMode, chunksize: int | None) -> None:
        sep = "\t" if self.format == "tsv" else ","
        if mode is LoadMode.APPEND and self.path.exists():
            frame.to_csv(self.path, sep=sep, index=False, mode="a", header=False, chunksize=chunksize)
        else:
            frame.to_csv(self.path, sep=sep, index=False, mode="w", chunksize=chunksize)

    def _write_json(self, frame: pd.DataFrame, mode: LoadMode) -> None:
        if mode is LoadMode.APPEND and self.path.exists():
            existing = self._read_json()
            frame = pd.concat([existing, frame], ignore_index=True)
        records = frame.to_dict(orient="records")
        self.path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")

    def _write_parquet(self, frame: pd.DataFrame, mode: LoadMode) -> None:
        if mode is LoadMode.APPEND and self.path.exists():
            existing = pd.read_parquet(self.path)
            frame = pd.concat([existing, frame], ignore_index=True)
        frame.to_parquet(self.path, index=False)
