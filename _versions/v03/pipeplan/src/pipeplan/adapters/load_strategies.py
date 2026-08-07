"""Load strategies -- the pluggable policy layer for the load phase.

Each write mode is a strategy registered under its name in ``LOAD_STRATEGIES``
(entry-point group ``pipeplan.load_strategies``), so third parties can add modes
(``merge``, soft-delete, ``scd4`` ...) without touching the adapter. A strategy
receives a :class:`~pipeplan.adapters.sql.SqlLoadTarget` bound to the batch's
transaction plus the :class:`~pipeplan.adapters.base.WriteRequest`, and returns a
:class:`~pipeplan.adapters.base.LoadResult`.

All strategies are set-based (no per-row Python), use explicit column lists
(never ``SELECT *``), stage into uniquely-named temp tables, and run inside the
caller's transaction so the whole batch is atomic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from ..core.exceptions import AdapterError
from ..core.registry import register_load_strategy
from .base import LoadResult, WriteRequest
from .sql import SqlLoadTarget


class LoadStrategy(ABC):
    @abstractmethod
    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult: ...

    # -- shared helpers --------------------------------------------------- #

    @staticmethod
    def _require_columns(req: WriteRequest, cols: list[str], role: str) -> None:
        missing = [c for c in cols if c not in req.frame.columns]
        if missing:
            raise AdapterError(
                f"load into '{req.collection}': {role} column(s) {missing} "
                f"not present in the frame (columns: {list(req.frame.columns)})"
            )

    @staticmethod
    def _cols_sql(target: SqlLoadTarget, cols: list[str]) -> str:
        return ", ".join(target.q(c) for c in cols)

    @staticmethod
    def _key_join(target: SqlLoadTarget, table: str, alias: str, keys: list[str]) -> str:
        return " AND ".join(f"{target.q(table)}.{target.q(k)} = {alias}.{target.q(k)}" for k in keys)


@register_load_strategy("replace")
class ReplaceStrategy(LoadStrategy):
    """Make the target equal the frame. Partition-scoped when ``partition_by`` is
    set: only the frame's partitions are replaced. Preserves table schema by
    truncating rather than dropping."""

    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult:
        frame, table = req.frame, req.collection
        created = False
        if not target.has_table(table):
            target.create_table(table, frame, req.contract)
            created = True
            target.append(frame, table, req.chunksize)
            return LoadResult(table, "replace", len(frame), inserted=len(frame), created_table=True)

        if req.partition_by:
            self._require_columns(req, req.partition_by, "partition_by")
            staging = target.stage(frame[req.partition_by].drop_duplicates(), f"part_{table}")
            try:
                join = self._key_join(target, table, "src", req.partition_by)
                deleted = target.execute(
                    f"DELETE FROM {target.q(table)} "
                    f"WHERE EXISTS (SELECT 1 FROM {target.q(staging)} AS src WHERE {join})"
                )
            finally:
                target.drop(staging)
            target.append(frame, table, req.chunksize)
            return LoadResult(table, "replace", len(frame), inserted=len(frame), deleted=deleted)

        target.truncate(table)
        target.append(frame, table, req.chunksize)
        return LoadResult(table, "replace", len(frame), inserted=len(frame), created_table=created)


@register_load_strategy("append")
class AppendStrategy(LoadStrategy):
    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult:
        created = False
        if not target.has_table(req.collection):
            target.create_table(req.collection, req.frame, req.contract)
            created = True
        target.append(req.frame, req.collection, req.chunksize)
        return LoadResult(req.collection, "append", len(req.frame),
                          inserted=len(req.frame), created_table=created)


@register_load_strategy("upsert")
class UpsertStrategy(LoadStrategy):
    """Idempotent on ``key``: delete matching keys then insert the frame's rows,
    with an explicit column list (order-independent)."""

    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult:
        if not req.key:
            raise AdapterError(f"upsert into '{req.collection}' requires a key")
        self._require_columns(req, req.key, "key")
        frame, table = req.frame, req.collection
        if not target.has_table(table):
            target.create_table(table, frame, req.contract)
            target.append(frame, table, req.chunksize)
            return LoadResult(table, "upsert", len(frame), inserted=len(frame), created_table=True)

        cols = list(frame.columns)
        staging = target.stage(frame, f"up_{table}")
        try:
            join = self._key_join(target, table, "src", req.key)
            deleted = target.execute(
                f"DELETE FROM {target.q(table)} "
                f"WHERE EXISTS (SELECT 1 FROM {target.q(staging)} AS src WHERE {join})"
            )
            col_sql = self._cols_sql(target, cols)
            inserted = target.execute(
                f"INSERT INTO {target.q(table)} ({col_sql}) "
                f"SELECT {col_sql} FROM {target.q(staging)}"
            )
        finally:
            target.drop(staging)
        return LoadResult(table, "upsert", len(frame),
                          inserted=inserted or len(frame), deleted=deleted)


@register_load_strategy("delete")
class DeleteStrategy(LoadStrategy):
    """Remove target rows whose key matches any row in the frame."""

    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult:
        if not req.key:
            raise AdapterError(f"delete from '{req.collection}' requires a key")
        self._require_columns(req, req.key, "key")
        table = req.collection
        if not target.has_table(table):
            return LoadResult(table, "delete", len(req.frame))
        staging = target.stage(req.frame[req.key].drop_duplicates(), f"del_{table}")
        try:
            join = self._key_join(target, table, "src", req.key)
            deleted = target.execute(
                f"DELETE FROM {target.q(table)} "
                f"WHERE EXISTS (SELECT 1 FROM {target.q(staging)} AS src WHERE {join})"
            )
        finally:
            target.drop(staging)
        return LoadResult(table, "delete", len(req.frame), deleted=deleted)


@register_load_strategy("scd2")
class Scd2Strategy(LoadStrategy):
    """Type-2 history. Only current rows' key+tracked columns are read (to detect
    change); superseded versions are closed with a set-based UPDATE and new
    versions inserted -- unchanged rows are never rewritten. Effective dates come
    from the run's logical timestamp (``as_of``), not wall-clock."""

    def apply(self, target: SqlLoadTarget, req: WriteRequest) -> LoadResult:
        scd = req.scd or {}
        keys = req.key
        track = list(scd.get("track", []))
        if not keys:
            raise AdapterError(f"scd2 into '{req.collection}' requires a key")
        self._require_columns(req, keys, "key")
        self._require_columns(req, track, "track")
        col_from = scd.get("effective_from", "valid_from")
        col_to = scd.get("effective_to", "valid_to")
        col_flag = scd.get("current_flag", "is_current")
        as_of = req.as_of if req.as_of is not None else pd.Timestamp.now().normalize()

        frame, table = req.frame, req.collection

        def _stamp(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df[col_from] = as_of
            df[col_to] = pd.NaT
            df[col_flag] = True
            return df

        if not target.has_table(table):
            opened = _stamp(frame)
            target.create_table(table, opened, req.contract)
            target.append(opened, table, req.chunksize)
            return LoadResult(table, "scd2", len(frame), inserted=len(frame), created_table=True)

        # Read only the *current* rows' key + tracked columns to detect change.
        proj = self._cols_sql(target, keys + track)
        current = pd.read_sql(
            f"SELECT {proj} FROM {target.q(table)} WHERE {target.q(col_flag)} = 1", target.conn
        ) if track else pd.DataFrame(columns=keys)

        merged = frame[keys + track].merge(
            current, on=keys, how="left", suffixes=("", "__cur"), indicator=True
        )
        is_new = merged["_merge"] == "left_only"
        changed = pd.Series(False, index=merged.index)
        for col in track:
            cur = f"{col}__cur"
            if cur in merged.columns:
                a, b = merged[col], merged[cur]
                differs = (a != b) & ~(a.isna() & b.isna())
                changed = changed | differs.fillna(False)
        affected = merged.loc[is_new | (changed & ~is_new), keys].drop_duplicates()
        if affected.empty:
            return LoadResult(table, "scd2", len(frame))  # idempotent no-op

        staging_keys = target.stage(affected, f"scd_{table}")
        try:
            join = self._key_join(target, table, "src", keys)
            updated = target.execute(
                f"UPDATE {target.q(table)} SET {target.q(col_to)} = :ts, {target.q(col_flag)} = 0 "
                f"WHERE {target.q(col_flag)} = 1 AND EXISTS "
                f"(SELECT 1 FROM {target.q(staging_keys)} AS src WHERE {join})",
                {"ts": as_of.to_pydatetime()},
            )
        finally:
            target.drop(staging_keys)

        new_versions = _stamp(frame.merge(affected, on=keys, how="inner"))
        inserted = target.append(new_versions, table, req.chunksize)
        return LoadResult(table, "scd2", len(frame), inserted=inserted, updated=updated)
