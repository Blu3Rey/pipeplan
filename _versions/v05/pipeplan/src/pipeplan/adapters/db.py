"""Database-backed adapter: SQLAlchemy + ODBC, never file I/O.

Per the manifesto, local databases (MS Access, SQLite) and server databases
(PostgreSQL) are all reached through ``pd.read_sql`` / ``DataFrame.to_sql`` over
a SQLAlchemy engine rather than by reading their files directly.

Tolerant of two real-world inconsistencies: a JDBC-style ``jdbc:`` URI prefix
(stripped) and the backend being named with either an ``engine`` or ``format``
key.

Writes are delegated to pluggable load strategies (see ``load_strategies``)
running against a :class:`~pipeplan.adapters.sql.SqlLoadTarget`. A whole batch
executes in one transaction, so a multi-table load is atomic.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
from sqlalchemy import create_engine, inspect as sa_inspect, text
from sqlalchemy.engine import Engine

from ..config.models import Permission, ResourceConfig
from ..core.exceptions import AdapterError
from ..core.registry import LOAD_STRATEGIES
from . import load_strategies as _load_strategies  # noqa: F401  (registers strategies)
from .base import Adapter, LoadResult, WriteRequest
from .sql import SqlLoadTarget

# Python 3.12 deprecates sqlite3's default datetime adapters; register explicit
# ISO adapters so date/datetime writes stay clean and unambiguous.
import datetime as _dt
import sqlite3 as _sqlite3

_sqlite3.register_adapter(_dt.datetime, lambda v: v.isoformat(sep=" "))
_sqlite3.register_adapter(_dt.date, lambda v: v.isoformat())


def _normalise_uri(uri: str) -> str:
    cleaned = uri.strip()
    if cleaned.lower().startswith("jdbc:"):
        cleaned = cleaned[len("jdbc:") :]
    return cleaned


def _enable_sqlite_transactional_ddl(engine: Engine) -> None:
    """Make pysqlite honour transactions around DDL.

    By default the stdlib ``sqlite3`` driver emits an implicit COMMIT before DDL
    (CREATE/DROP), which would let table creation escape a load batch's
    transaction and break atomicity. This is the documented SQLAlchemy recipe:
    take over BEGIN emission so DDL rolls back with everything else.
    """
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _sqlite_disable_autobegin(dbapi_connection, _record):  # pragma: no cover - driver glue
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _sqlite_emit_begin(conn):  # pragma: no cover - driver glue
        conn.exec_driver_sql("BEGIN")


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
                engine = create_engine(f"sqlite:///{path}")
                _enable_sqlite_transactional_ddl(engine)
                return engine
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
    # write (delegated to strategies, atomic per batch)
    # ------------------------------------------------------------------ #

    def write_batch(self, requests: list[WriteRequest]) -> list[LoadResult]:
        self._require(Permission.WRITE)
        if not requests:
            return []
        results: list[LoadResult] = []
        try:
            with self.engine.begin() as conn:
                target = SqlLoadTarget(conn, resource=self.name)
                for req in requests:
                    strategy_cls = self._strategy_for(req.mode.value)
                    target.default_chunksize = req.chunksize
                    results.append(strategy_cls().apply(target, req))
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(
                f"resource '{self.name}': load batch failed and was rolled back: {exc}"
            ) from exc
        return results

    @staticmethod
    def _strategy_for(mode: str):
        try:
            return LOAD_STRATEGIES.get(mode)
        except Exception as exc:
            raise AdapterError(f"no load strategy registered for mode '{mode}'") from exc

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
