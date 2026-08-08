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

    #: dialects for which a native INSERT-or-update construct is emitted.
    _NATIVE_UPSERT = frozenset({"sqlite", "postgresql", "mysql", "mssql"})

    def supports_native_upsert(self, table: str, keys: list[str]) -> bool:
        """A native upsert is safe only when the dialect supports one *and* the
        key columns are backed by a unique/primary-key constraint to conflict on."""
        if self.dialect not in self._NATIVE_UPSERT:
            return False
        key_set = set(keys)
        return any(key_set == cols for cols in self.unique_column_sets(table))

    def native_upsert_sql(self, table: str, cols: list[str], keys: list[str]) -> str:
        """Build the dialect-native merge/upsert statement reading from staging."""
        non_key = [c for c in cols if c not in keys]
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        t, s = self.q(table), "src"
        staging_ref = f"{{staging}} AS {s}"  # filled by caller

        if self.dialect in ("sqlite", "postgresql"):
            conflict = ", ".join(self.q(k) for k in keys)
            if non_key:
                setters = ", ".join(f"{self.q(c)} = excluded.{self.q(c)}" for c in non_key)
                action = f"DO UPDATE SET {setters}"
            else:
                action = "DO NOTHING"
            # WHERE true disambiguates the upsert clause from the SELECT's FROM
            # (required by SQLite; harmless on Postgres).
            return (
                f"INSERT INTO {t} ({col_sql}) SELECT {select_sql} FROM {staging_ref} "
                f"WHERE true ON CONFLICT ({conflict}) {action}"
            )
        if self.dialect == "mysql":
            if non_key:
                setters = ", ".join(f"{self.q(c)} = VALUES({self.q(c)})" for c in non_key)
            else:  # no-op update to absorb the duplicate
                setters = f"{self.q(keys[0])} = {self.q(keys[0])}"
            return (
                f"INSERT INTO {t} ({col_sql}) SELECT {select_sql} FROM {staging_ref} "
                f"WHERE true ON DUPLICATE KEY UPDATE {setters}"
            )
        # mssql MERGE
        on = " AND ".join(f"tgt.{self.q(k)} = src.{self.q(k)}" for k in keys)
        insert_cols = ", ".join(self.q(c) for c in cols)
        insert_vals = ", ".join(f"src.{self.q(c)}" for c in cols)
        matched = ""
        if non_key:
            setters = ", ".join(f"tgt.{self.q(c)} = src.{self.q(c)}" for c in non_key)
            matched = f"WHEN MATCHED THEN UPDATE SET {setters} "
        return (
            f"MERGE INTO {t} AS tgt USING {staging_ref} ON ({on}) "
            f"{matched}"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )

    def count_matching_keys(self, table: str, staging: str, keys: list[str]) -> int:
        join = " AND ".join(f"{self.q(table)}.{self.q(k)} = src.{self.q(k)}" for k in keys)
        val = self.conn.execute(text(
            f"SELECT COUNT(*) FROM {self.q(table)} "
            f"WHERE EXISTS (SELECT 1 FROM {self.q(staging)} AS src WHERE {join})"
        )).scalar()
        return int(val or 0)

    # -- simulated upsert (update matched in place, insert missing) ------- #

    def update_matched_sql(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        """A portable ``UPDATE`` that sets each incoming non-key column on rows
        whose key matches staging, leaving target-only columns untouched. Returns
        ``None`` when there are no non-key columns (nothing to update).

        Uses SQL-92 correlated subqueries so it runs on every dialect (including
        pre-3.33 SQLite that lacks ``UPDATE ... FROM``); the ``EXISTS`` guard
        ensures only matched rows are touched, so no column is ever nulled by a
        non-match.
        """
        non_key = [c for c in cols if c not in keys]
        if not non_key:
            return None
        t = self.q(table)
        corr = " AND ".join(f"{t}.{self.q(k)} = src.{self.q(k)}" for k in keys)
        setters = ", ".join(
            f"{self.q(c)} = (SELECT src.{self.q(c)} FROM {{staging}} AS src WHERE {corr})"
            for c in non_key
        )
        return (
            f"UPDATE {t} SET {setters} "
            f"WHERE EXISTS (SELECT 1 FROM {{staging}} AS src WHERE {corr})"
        )

    def insert_missing_sql(self, table: str, cols: list[str], keys: list[str]) -> str:
        """A portable ``INSERT ... WHERE NOT EXISTS`` for keys absent from target."""
        t = self.q(table)
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        corr = " AND ".join(f"{t}.{self.q(k)} = src.{self.q(k)}" for k in keys)
        return (
            f"INSERT INTO {t} ({col_sql}) SELECT {select_sql} FROM {{staging}} AS src "
            f"WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {corr})"
        )

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
