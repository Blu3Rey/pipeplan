"""A thin, dialect-aware capability layer over a live SQL connection.

Load strategies (replace / append / upsert / delete / scd2) are written against
this interface rather than against a raw engine, so the policy of *how* a mode
mutates a table is decoupled from the storage mechanics (quoting, staging, DDL,
batching). Everything here runs on a single ``Connection`` supplied by the
adapter, which is what makes a whole load batch atomic.
"""

from __future__ import annotations

import uuid
from typing import Any

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    Table,
    Text,
    inspect as sa_inspect,
    text,
)
from sqlalchemy.engine import Connection

from .dialects import resolve_dialect

# contract dtype -> SQLAlchemy column type (dialect-compiled by SQLAlchemy).
_CONTRACT_TYPES = {
    "integer": BigInteger, "int": BigInteger, "bigint": BigInteger,
    "float": Float, "double": Float, "number": Float, "numeric": Float,
    "string": Text, "str": Text, "text": Text,
    "boolean": Boolean, "bool": Boolean,
    "date": Date, "datetime": DateTime, "timestamp": DateTime,
}


def _frame_type(series: pd.Series):
    dtype = series.dtype
    if pd.api.types.is_integer_dtype(dtype):
        return BigInteger
    if pd.api.types.is_float_dtype(dtype):
        return Float
    if pd.api.types.is_bool_dtype(dtype):
        return Boolean
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    return Text


def to_sql_safe(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalise pandas nullable/extension NA to Python ``None`` so every driver
    writes a real SQL NULL rather than a ``<NA>`` sentinel."""
    out = frame.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.api.extensions.ExtensionDtype):
            out[col] = out[col].astype(object).where(out[col].notna(), None)
    return out


class SqlLoadTarget:
    """Capability object handed to load strategies for one connection."""

    def __init__(self, conn: Connection, *, resource: str, chunksize: int | None = None) -> None:
        self.conn = conn
        self.resource = resource
        self.default_chunksize = chunksize
        self.dialect = conn.dialect.name
        self._preparer = conn.dialect.identifier_preparer
        # The dialect compiler owns every SQL string; ANSI is the fallback.
        self.sql = resolve_dialect(self.dialect)(self.q)

    # -- identifiers ------------------------------------------------------ #

    def q(self, ident: str) -> str:
        return self._preparer.quote(ident)

    # -- introspection ---------------------------------------------------- #

    def has_table(self, table: str) -> bool:
        return sa_inspect(self.conn).has_table(table)

    def columns(self, table: str) -> list[str]:
        return [c["name"] for c in sa_inspect(self.conn).get_columns(table)]

    def primary_key(self, table: str) -> list[str]:
        pk = sa_inspect(self.conn).get_pk_constraint(table)
        return list(pk.get("constrained_columns") or [])

    def unique_column_sets(self, table: str) -> list[set[str]]:
        """Every column-set with a uniqueness guarantee (PK + unique constraints
        + unique indexes) -- the candidate conflict targets for a native upsert."""
        insp = sa_inspect(self.conn)
        sets: list[set[str]] = []
        pk = insp.get_pk_constraint(table).get("constrained_columns") or []
        if pk:
            sets.append(set(pk))
        try:
            for uc in insp.get_unique_constraints(table):
                cols = uc.get("column_names")
                if cols:
                    sets.append(set(cols))
        except NotImplementedError:  # pragma: no cover - dialect dependent
            pass
        for ix in insp.get_indexes(table):
            if ix.get("unique") and ix.get("column_names"):
                sets.append({c for c in ix["column_names"] if c})
        return sets

    def supports_native_upsert(self, table: str, keys: list[str]) -> bool:
        """A native upsert is safe only when the dialect provides one *and* the
        key columns are backed by a unique/primary-key constraint to conflict on."""
        if not self.sql.has_native_upsert:
            return False
        key_set = set(keys)
        return any(key_set == cols for cols in self.unique_column_sets(table))

    def native_upsert_sql(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        return self.sql.native_upsert(table, cols, keys)

    def count_matching_keys(self, table: str, staging: str, keys: list[str]) -> int:
        sql = self.sql.count_matching(table, keys).format(staging=self.q(staging))
        val = self.conn.execute(text(sql)).scalar()
        return int(val or 0)

    # -- simulated upsert / scd2 statements (delegated to the dialect) ----- #

    def update_matched_sql(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        return self.sql.update_matched(table, cols, keys)

    def insert_missing_sql(self, table: str, cols: list[str], keys: list[str]) -> str:
        return self.sql.insert_missing(table, cols, keys)

    def delete_matching_sql(self, table: str, keys: list[str]) -> str:
        return self.sql.delete_matching(table, keys)

    def select_current_sql(self, table: str, cols: list[str], flag_col: str) -> str:
        return self.sql.select_current(table, cols, flag_col)

    def close_current_sql(self, table: str, to_col: str, flag_col: str, keys: list[str]) -> str:
        return self.sql.close_current(table, to_col, flag_col, keys)

    # -- DDL -------------------------------------------------------------- #

    def create_table(self, table: str, frame: pd.DataFrame, contract: Any = None) -> None:
        """Create ``table`` with typed columns.

        When a schema contract is supplied its dtypes, nullability and primary
        key drive the DDL; otherwise column types are inferred from the frame.
        This keeps the warehouse schema governed by the declared contract rather
        than by pandas' per-run inference.
        """
        metadata = MetaData()
        col_specs = getattr(contract, "columns", {}) or {}
        pk_cols = set(getattr(contract, "primary_key", []) or [])
        columns = []
        for name in frame.columns:
            spec = col_specs.get(name)
            if spec is not None and spec.dtype and spec.dtype.lower() in _CONTRACT_TYPES:
                col_type = _CONTRACT_TYPES[spec.dtype.lower()]
            else:
                col_type = _frame_type(frame[name])
            nullable = True if spec is None else spec.nullable
            columns.append(Column(name, col_type(), nullable=nullable, primary_key=name in pk_cols))
        Table(table, metadata, *columns).create(self.conn)

    def truncate(self, table: str) -> None:
        self.conn.execute(text(self.sql.truncate(table)))

    def drop(self, table: str) -> None:
        self.conn.execute(text(f"DROP TABLE IF EXISTS {self.q(table)}"))

    # -- data ------------------------------------------------------------- #

    def append(self, frame: pd.DataFrame, table: str, chunksize: int | None = None) -> int:
        safe = to_sql_safe(frame)
        # Postgres bulk COPY is dramatically faster than INSERT; fall back to a
        # portable executemany on any other dialect or if COPY is unavailable.
        if self.dialect == "postgresql" and self._try_copy(safe, table):
            return len(frame)
        # method=None (driver executemany) avoids the single-giant-INSERT that
        # method="multi" builds, which trips SQLite's bind-parameter cap.
        safe.to_sql(table, self.conn, if_exists="append", index=False,
                    chunksize=chunksize or self.default_chunksize)
        return len(frame)

    def _try_copy(self, frame: pd.DataFrame, table: str) -> bool:
        """Attempt a psycopg2 COPY. Returns False (caller falls back) on any issue
        so a driver/type edge case can never fail an otherwise-valid load."""
        try:  # pragma: no cover - requires a live postgres + psycopg2
            import io

            raw = self.conn.connection.dbapi_connection
            if raw.__class__.__module__.split(".")[0] not in ("psycopg2", "psycopg"):
                return False
            buffer = io.StringIO()
            frame.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)
            cols = ", ".join(self.q(c) for c in frame.columns)
            sql = (
                f"COPY {self.q(table)} ({cols}) FROM STDIN "
                f"WITH (FORMAT csv, NULL '\\N')"
            )
            with raw.cursor() as cur:
                cur.copy_expert(sql, buffer)
            return True
        except Exception:  # pragma: no cover - any failure -> portable path
            return False

    def stage(self, frame: pd.DataFrame, base: str) -> str:
        name = f"__pp_{base}_{uuid.uuid4().hex[:8]}"
        to_sql_safe(frame).to_sql(name, self.conn, if_exists="replace", index=False,
                                  chunksize=self.default_chunksize)
        return name

    def read_table(self, table: str) -> pd.DataFrame:
        return pd.read_sql(text(f"SELECT * FROM {self.q(table)}"), self.conn)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        result = self.conn.execute(text(sql), params or {})
        return result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0
