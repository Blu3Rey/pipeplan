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
        # DELETE (not TRUNCATE DDL) so it participates in the surrounding txn on
        # every dialect and preserves the table's schema/constraints.
        self.conn.execute(text(f"DELETE FROM {self.q(table)}"))

    def drop(self, table: str) -> None:
        self.conn.execute(text(f"DROP TABLE IF EXISTS {self.q(table)}"))

    # -- data ------------------------------------------------------------- #

    def append(self, frame: pd.DataFrame, table: str, chunksize: int | None = None) -> int:
        safe = to_sql_safe(frame)
        # method=None (driver executemany) avoids the single-giant-INSERT that
        # method="multi" builds, which trips SQLite's bind-parameter cap.
        safe.to_sql(table, self.conn, if_exists="append", index=False,
                    chunksize=chunksize or self.default_chunksize)
        return len(frame)

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
