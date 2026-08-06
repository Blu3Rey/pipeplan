"""Database-backed adapter: SQLAlchemy + ODBC, never file I/O.

Per the manifesto, local databases (MS Access, SQLite) and server databases
(PostgreSQL) are all reached through ``pd.read_sql`` / ``DataFrame.to_sql`` over
a SQLAlchemy engine rather than by reading their files directly.

Tolerant of two real-world inconsistencies: a JDBC-style ``jdbc:`` URI prefix
(stripped) and the backend being named with either an ``engine`` or ``format``
key.

Load modes implemented set-based (no per-row Python): ``replace`` (optionally
partition-scoped), ``append``, ``upsert``, ``delete`` and ``scd2`` Type-2 history.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
from sqlalchemy import create_engine, inspect as sa_inspect, text
from sqlalchemy.engine import Engine

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import AdapterError
from .base import Adapter


def _normalise_uri(uri: str) -> str:
    cleaned = uri.strip()
    if cleaned.lower().startswith("jdbc:"):
        cleaned = cleaned[len("jdbc:") :]
    return cleaned


class DBAdapter(Adapter):
    """Read and write dataframes to a relational database via SQLAlchemy."""

    SUPPORTED: ClassVar[frozenset[str]] = frozenset(
        {"postgres", "postgresql", "sqlite", "access", "mssql", "mysql"}
    )

    def __init__(self, config: ResourceConfig) -> None:
        super().__init__(config)
        params = config.params
        backend = str(params.get("engine") or params.get("format") or "").lower()
        if not backend:
            raise AdapterError(
                f"resource '{self.name}': db adapter requires an 'engine' or 'format'"
            )
        if backend not in self.SUPPORTED:
            raise AdapterError(
                f"resource '{self.name}': unsupported db backend '{backend}' "
                f"(supported: {', '.join(sorted(self.SUPPORTED))})"
            )
        self.backend = "postgres" if backend == "postgresql" else backend
        self.params = params
        self._engine: Engine | None = None

    # ------------------------------------------------------------------ #
    # engine construction (lazy)
    # ------------------------------------------------------------------ #

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._make_engine()
        return self._engine

    def _make_engine(self) -> Engine:
        params = self.params
        try:
            if self.backend == "sqlite":
                path = params.get("path") or params.get("database")
                if not path:
                    raise AdapterError(f"resource '{self.name}': sqlite requires a 'path'")
                return create_engine(f"sqlite:///{path}")
            if self.backend == "postgres":
                uri = params.get("uri") or params.get("url")
                if uri:
                    url = _normalise_uri(str(uri))
                    if url.startswith("postgresql://"):
                        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
                    return create_engine(url)
                return create_engine(self._url_from_parts("postgresql+psycopg2"))
            if self.backend in {"mysql", "mssql"}:
                uri = params.get("uri") or params.get("url")
                if uri:
                    return create_engine(_normalise_uri(str(uri)))
                driver = "mysql+pymysql" if self.backend == "mysql" else "mssql+pyodbc"
                return create_engine(self._url_from_parts(driver))
            if self.backend == "access":
                return self._make_access_engine()
        except AdapterError:
            raise
        except Exception as exc:  # pragma: no cover - driver/env specific
            raise AdapterError(f"resource '{self.name}': could not create engine: {exc}") from exc
        raise AdapterError(f"resource '{self.name}': unreachable backend")  # pragma: no cover

    def _url_from_parts(self, driver: str) -> str:
        p = self.params
        user = p.get("user", "")
        password = p.get("password", "")
        host = p.get("host", "localhost")
        port = p.get("port", "")
        database = p.get("database", "")
        auth = f"{user}:{password}@" if user else ""
        netloc = f"{host}:{port}" if port else host
        return f"{driver}://{auth}{netloc}/{database}"

    def _make_access_engine(self) -> Engine:
        from urllib.parse import quote_plus

        uri = self.params.get("uri") or self.params.get("path")
        if not uri:
            raise AdapterError(f"resource '{self.name}': access requires a 'uri' or 'path'")
        uri = _normalise_uri(str(uri))
        if uri.lower().startswith(("access+pyodbc://", "access://")):
            return create_engine(uri)
        if uri.lower().endswith((".mdb", ".accdb")):
            conn = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" f"DBQ={uri};"
        else:
            conn = uri
        return create_engine(f"access+pyodbc:///?odbc_connect={quote_plus(conn)}")

    # ------------------------------------------------------------------ #
    # small helpers (also used by the watermark store)
    # ------------------------------------------------------------------ #

    def has_table(self, table: str) -> bool:
        return sa_inspect(self.engine).has_table(table)

    def scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        with self.engine.connect() as conn:
            return conn.execute(text(sql), params or {}).scalar()

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql), params or {})

    # ------------------------------------------------------------------ #
    # read (with optional incremental pushdown)
    # ------------------------------------------------------------------ #

    def read(self, collection: str | None, *, since: tuple[str, Any] | None = None) -> pd.DataFrame:
        self._require(Permission.READ)
        if collection is None:
            raise AdapterError(
                f"resource '{self.name}': db reads require a table name (set 'collection')"
            )
        query = self.params.get("query")
        try:
            with self.engine.connect() as conn:
                if query:
                    frame = pd.read_sql(text(query), conn)
                    if since is not None:
                        cursor, min_value = since
                        if cursor in frame.columns and min_value is not None:
                            frame = frame[frame[cursor] > min_value].reset_index(drop=True)
                    return frame
                if since is not None and since[1] is not None:
                    cursor, min_value = since
                    sql = f'SELECT * FROM "{collection}" WHERE "{cursor}" > :since'
                    return pd.read_sql(text(sql), conn, params={"since": min_value})
                return pd.read_sql(text(f'SELECT * FROM "{collection}"'), conn)
        except Exception as exc:
            raise AdapterError(
                f"resource '{self.name}': failed to read table '{collection}': {exc}"
            ) from exc

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
        keys = [key] if isinstance(key, str) else (list(key) if key else [])
        try:
            if mode is LoadMode.REPLACE:
                if partition_by:
                    self._replace_partitions(frame, collection, partition_by, chunksize)
                else:
                    frame.to_sql(collection, self.engine, if_exists="replace",
                                 index=False, chunksize=chunksize, method="multi")
            elif mode is LoadMode.APPEND:
                frame.to_sql(collection, self.engine, if_exists="append",
                             index=False, chunksize=chunksize, method="multi")
            elif mode is LoadMode.UPSERT:
                self._upsert(frame, collection, keys, chunksize)
            elif mode is LoadMode.DELETE:
                self._delete(frame, collection, keys, chunksize)
            elif mode is LoadMode.SCD2:
                self._scd2(frame, collection, keys, scd or {}, chunksize)
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(
                f"resource '{self.name}': failed to write table '{collection}': {exc}"
            ) from exc

    # -- partitioned replace ---------------------------------------------- #

    def _replace_partitions(self, frame, collection, partition_by, chunksize) -> None:
        inspector = sa_inspect(self.engine)
        if not inspector.has_table(collection):
            frame.to_sql(collection, self.engine, if_exists="append",
                         index=False, chunksize=chunksize, method="multi")
            return
        with self.engine.begin() as conn:
            staging = f"__pp_part_{collection}"
            distinct = frame[partition_by].drop_duplicates()
            distinct.to_sql(staging, conn, if_exists="replace", index=False, method="multi")
            try:
                join = " AND ".join(f'"{collection}"."{c}" = src."{c}"' for c in partition_by)
                conn.execute(text(
                    f'DELETE FROM "{collection}" '
                    f'WHERE EXISTS (SELECT 1 FROM "{staging}" AS src WHERE {join})'
                ))
            finally:
                conn.execute(text(f'DROP TABLE IF EXISTS "{staging}"'))
            frame.to_sql(collection, conn, if_exists="append",
                         index=False, chunksize=chunksize, method="multi")

    # -- upsert ------------------------------------------------------------ #

    def _upsert(self, frame, collection, keys, chunksize) -> None:
        if not keys:
            raise AdapterError(f"resource '{self.name}': upsert into '{collection}' requires a key")
        inspector = sa_inspect(self.engine)
        with self.engine.begin() as conn:
            if not inspector.has_table(collection):
                frame.to_sql(collection, conn, if_exists="append",
                             index=False, chunksize=chunksize, method="multi")
                return
            staging = f"__pipeplan_stage_{collection}"
            frame.to_sql(staging, conn, if_exists="replace",
                         index=False, chunksize=chunksize, method="multi")
            try:
                join = " AND ".join(f'"{collection}"."{k}" = src."{k}"' for k in keys)
                conn.execute(text(
                    f'DELETE FROM "{collection}" '
                    f'WHERE EXISTS (SELECT 1 FROM "{staging}" AS src WHERE {join})'
                ))
                conn.execute(text(f'INSERT INTO "{collection}" SELECT * FROM "{staging}"'))
            finally:
                conn.execute(text(f'DROP TABLE IF EXISTS "{staging}"'))

    # -- delete ------------------------------------------------------------ #

    def _delete(self, frame, collection, keys, chunksize) -> None:
        if not keys:
            raise AdapterError(f"resource '{self.name}': delete from '{collection}' requires a key")
        inspector = sa_inspect(self.engine)
        if not inspector.has_table(collection):
            return
        with self.engine.begin() as conn:
            staging = f"__pp_del_{collection}"
            frame[keys].drop_duplicates().to_sql(
                staging, conn, if_exists="replace", index=False, method="multi"
            )
            try:
                join = " AND ".join(f'"{collection}"."{k}" = src."{k}"' for k in keys)
                conn.execute(text(
                    f'DELETE FROM "{collection}" '
                    f'WHERE EXISTS (SELECT 1 FROM "{staging}" AS src WHERE {join})'
                ))
            finally:
                conn.execute(text(f'DROP TABLE IF EXISTS "{staging}"'))

    # -- scd2 (Type-2 history) -------------------------------------------- #

    def _scd2(self, frame, collection, keys, scd, chunksize) -> None:
        if not keys:
            raise AdapterError(f"resource '{self.name}': scd2 into '{collection}' requires a key")
        track = list(scd.get("track", []))
        col_from = scd.get("effective_from", "valid_from")
        col_to = scd.get("effective_to", "valid_to")
        col_flag = scd.get("current_flag", "is_current")
        now = pd.Timestamp.now().normalize()

        incoming = frame.copy()
        inspector = sa_inspect(self.engine)
        if not inspector.has_table(collection):
            incoming[col_from] = now
            incoming[col_to] = pd.NaT
            incoming[col_flag] = True
            incoming.to_sql(collection, self.engine, if_exists="replace",
                            index=False, chunksize=chunksize, method="multi")
            return

        existing = pd.read_sql(text(f'SELECT * FROM "{collection}"'), self.engine)
        for col in (col_from, col_to):
            if col in existing.columns:
                existing[col] = pd.to_datetime(existing[col], errors="coerce")
        is_current = existing[col_flag].astype("boolean").fillna(False)
        current = existing[is_current]
        historical = existing[~is_current]

        merged = incoming.merge(
            current[keys + track], on=keys, how="left", suffixes=("", "__cur"), indicator=True
        )
        is_new = merged["_merge"] == "left_only"
        changed = pd.Series(False, index=merged.index)
        for col in track:
            cur_col = f"{col}__cur"
            if cur_col in merged.columns:
                changed = changed | (merged[col].astype("string") != merged[cur_col].astype("string"))
        changed = changed & (~is_new)
        affected_keys = merged.loc[is_new | changed, keys].drop_duplicates()

        # Close out superseded current rows for changed keys.
        closed = current.merge(affected_keys, on=keys, how="inner")
        if not closed.empty:
            closed = closed.copy()
            closed[col_to] = now
            closed[col_flag] = False
        kept_current = current.merge(affected_keys, on=keys, how="left", indicator=True)
        kept_current = kept_current[kept_current["_merge"] == "left_only"].drop(columns="_merge")

        # New open versions for new + changed keys.
        new_versions = incoming.merge(affected_keys, on=keys, how="inner").copy()
        new_versions[col_from] = now
        new_versions[col_to] = pd.NaT
        new_versions[col_flag] = True

        rebuilt = pd.concat(
            [historical, kept_current, closed, new_versions], ignore_index=True, sort=False
        )
        rebuilt.to_sql(collection, self.engine, if_exists="replace",
                       index=False, chunksize=chunksize, method="multi")

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
