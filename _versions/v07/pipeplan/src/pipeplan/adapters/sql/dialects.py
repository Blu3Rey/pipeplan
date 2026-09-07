"""SQL dialect compilers.

Every SQL string the load phase emits is produced here, not inlined in the
capability layer or the strategies. A :class:`SqlDialect` owns the *spelling* of
each set-based operation; :class:`~pipeplan.adapters.sql.SqlLoadTarget` owns the
*mechanics* (connection, staging, introspection) and the load strategies own the
*policy* (which operations, in what order). Because the divergent SQL is isolated
behind one small interface, supporting a niche backend is a self-contained
subclass -- it never touches the mainstream code paths.

Compilers are chosen per connection by SQLAlchemy dialect name from the
``SQL_DIALECTS`` registry (entry-point group ``pipeplan.sql_dialects``), falling
back to the portable ANSI compiler for anything unregistered. Third parties can
register a compiler for their own backend out-of-tree.

Statements that read from a staging table use a ``{staging}`` placeholder that the
caller fills with the (quoted) staging name.
"""

from __future__ import annotations

from typing import Callable

from ...core.registry import SQL_DIALECTS, register_sql_dialect

Quote = Callable[[str], str]


class SqlDialect:
    """Portable ANSI/SQL-92 compiler. The default for any unrecognised backend.

    Overridable knobs: ``has_native_upsert`` (whether :meth:`native_upsert`
    returns a statement) and the boolean literals used for flag columns.
    """

    name = "ansi"
    has_native_upsert: bool = False
    true_literal: str = "1"
    false_literal: str = "0"

    def __init__(self, quote: Quote) -> None:
        self.q = quote

    # -- key-matching predicates ------------------------------------------ #

    def _match(self, table: str, keys: list[str], alias: str = "src") -> str:
        return " AND ".join(f"{self.q(table)}.{self.q(k)} = {alias}.{self.q(k)}" for k in keys)

    # -- statements ------------------------------------------------------- #

    def truncate(self, table: str) -> str:
        # DELETE, not TRUNCATE DDL, so it joins the surrounding transaction on
        # every dialect and preserves the table's schema/constraints.
        return f"DELETE FROM {self.q(table)}"

    def delete_matching(self, table: str, keys: list[str]) -> str:
        return (
            f"DELETE FROM {self.q(table)} "
            f"WHERE EXISTS (SELECT 1 FROM {{staging}} AS src WHERE {self._match(table, keys)})"
        )

    def count_matching(self, table: str, keys: list[str]) -> str:
        return (
            f"SELECT COUNT(*) FROM {self.q(table)} "
            f"WHERE EXISTS (SELECT 1 FROM {{staging}} AS src WHERE {self._match(table, keys)})"
        )

    def update_matched(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        """UPDATE non-key columns of matched rows, via SQL-92 correlated
        subqueries (valid everywhere, including pre-3.33 SQLite)."""
        non_key = [c for c in cols if c not in keys]
        if not non_key:
            return None
        corr = self._match(table, keys)
        setters = ", ".join(
            f"{self.q(c)} = (SELECT src.{self.q(c)} FROM {{staging}} AS src WHERE {corr})"
            for c in non_key
        )
        return (
            f"UPDATE {self.q(table)} SET {setters} "
            f"WHERE EXISTS (SELECT 1 FROM {{staging}} AS src WHERE {corr})"
        )

    def insert_missing(self, table: str, cols: list[str], keys: list[str]) -> str:
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        return (
            f"INSERT INTO {self.q(table)} ({col_sql}) SELECT {select_sql} FROM {{staging}} AS src "
            f"WHERE NOT EXISTS (SELECT 1 FROM {self.q(table)} WHERE {self._match(table, keys)})"
        )

    def native_upsert(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        """A single-statement upsert, or ``None`` if the backend has none."""
        return None

    # -- SCD2 helpers ----------------------------------------------------- #

    def select_current(self, table: str, cols: list[str], flag_col: str) -> str:
        col_sql = ", ".join(self.q(c) for c in cols)
        return f"SELECT {col_sql} FROM {self.q(table)} WHERE {self.q(flag_col)} = {self.true_literal}"

    def close_current(self, table: str, to_col: str, flag_col: str, keys: list[str]) -> str:
        return (
            f"UPDATE {self.q(table)} SET {self.q(to_col)} = :ts, {self.q(flag_col)} = {self.false_literal} "
            f"WHERE {self.q(flag_col)} = {self.true_literal} "
            f"AND EXISTS (SELECT 1 FROM {{staging}} AS src WHERE {self._match(table, keys)})"
        )


# --------------------------------------------------------------------------- #
# mainstream backends -- each contributes only its native-upsert spelling
# --------------------------------------------------------------------------- #


class _OnConflictDialect(SqlDialect):
    """SQLite / PostgreSQL: ``INSERT ... ON CONFLICT``."""

    has_native_upsert = True

    def native_upsert(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        non_key = [c for c in cols if c not in keys]
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        conflict = ", ".join(self.q(k) for k in keys)
        if non_key:
            setters = ", ".join(f"{self.q(c)} = excluded.{self.q(c)}" for c in non_key)
            action = f"DO UPDATE SET {setters}"
        else:
            action = "DO NOTHING"
        # WHERE true disambiguates the upsert clause from the SELECT's FROM
        # (required by SQLite; harmless on Postgres).
        return (
            f"INSERT INTO {self.q(table)} ({col_sql}) SELECT {select_sql} FROM {{staging}} AS src "
            f"WHERE true ON CONFLICT ({conflict}) {action}"
        )


@register_sql_dialect("sqlite")
class SqliteDialect(_OnConflictDialect):
    name = "sqlite"


@register_sql_dialect("postgresql")
class PostgresDialect(_OnConflictDialect):
    name = "postgresql"


@register_sql_dialect("mysql")
class MySqlDialect(SqlDialect):
    name = "mysql"
    has_native_upsert = True

    def native_upsert(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        non_key = [c for c in cols if c not in keys]
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        if non_key:
            setters = ", ".join(f"{self.q(c)} = VALUES({self.q(c)})" for c in non_key)
        else:  # no-op update to absorb the duplicate key
            setters = f"{self.q(keys[0])} = {self.q(keys[0])}"
        return (
            f"INSERT INTO {self.q(table)} ({col_sql}) SELECT {select_sql} FROM {{staging}} AS src "
            f"WHERE true ON DUPLICATE KEY UPDATE {setters}"
        )


@register_sql_dialect("mssql")
class MssqlDialect(SqlDialect):
    name = "mssql"
    has_native_upsert = True

    def native_upsert(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        non_key = [c for c in cols if c not in keys]
        on = " AND ".join(f"tgt.{self.q(k)} = src.{self.q(k)}" for k in keys)
        insert_cols = ", ".join(self.q(c) for c in cols)
        insert_vals = ", ".join(f"src.{self.q(c)}" for c in cols)
        matched = ""
        if non_key:
            setters = ", ".join(f"tgt.{self.q(c)} = src.{self.q(c)}" for c in non_key)
            matched = f"WHEN MATCHED THEN UPDATE SET {setters} "
        return (
            f"MERGE INTO {self.q(table)} AS tgt USING {{staging}} AS src ON ({on}) "
            f"{matched}"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )


# --------------------------------------------------------------------------- #
# niche backend -- self-contained, touches no shared code
# --------------------------------------------------------------------------- #


@register_sql_dialect("access")
class AccessDialect(SqlDialect):
    """Microsoft Access (Jet/ACE).

    Access rejects the correlated-subquery ``UPDATE``/``DELETE`` forms the ANSI
    compiler uses ("operation must use an updateable query") and needs the
    ``INNER JOIN`` action-query forms instead. Booleans are ``-1``/``0``. It has
    no single-statement upsert, so it inherits the portable update-then-insert
    simulation (with the JOIN-based UPDATE below). Bracket quoting is handled by
    SQLAlchemy's identifier preparer.
    """

    name = "access"
    has_native_upsert = False
    true_literal = "-1"
    false_literal = "0"

    def _on(self, table: str, keys: list[str]) -> str:
        return " AND ".join(f"{self.q(table)}.{self.q(k)} = src.{self.q(k)}" for k in keys)

    def delete_matching(self, table: str, keys: list[str]) -> str:
        return (
            f"DELETE {self.q(table)}.* FROM {self.q(table)} "
            f"INNER JOIN {{staging}} AS src ON {self._on(table, keys)}"
        )

    def update_matched(self, table: str, cols: list[str], keys: list[str]) -> str | None:
        non_key = [c for c in cols if c not in keys]
        if not non_key:
            return None
        setters = ", ".join(f"{self.q(table)}.{self.q(c)} = src.{self.q(c)}" for c in non_key)
        return (
            f"UPDATE {self.q(table)} INNER JOIN {{staging}} AS src "
            f"ON {self._on(table, keys)} SET {setters}"
        )

    def insert_missing(self, table: str, cols: list[str], keys: list[str]) -> str:
        # Left-join anti-join, which Access executes reliably as an append query.
        col_sql = ", ".join(self.q(c) for c in cols)
        select_sql = ", ".join(f"src.{self.q(c)}" for c in cols)
        first_key = self.q(keys[0])
        return (
            f"INSERT INTO {self.q(table)} ({col_sql}) SELECT {select_sql} "
            f"FROM {{staging}} AS src LEFT JOIN {self.q(table)} ON {self._on(table, keys)} "
            f"WHERE {self.q(table)}.{first_key} IS NULL"
        )

    def close_current(self, table: str, to_col: str, flag_col: str, keys: list[str]) -> str:
        return (
            f"UPDATE {self.q(table)} INNER JOIN {{staging}} AS src ON {self._on(table, keys)} "
            f"SET {self.q(table)}.{self.q(to_col)} = :ts, "
            f"{self.q(table)}.{self.q(flag_col)} = {self.false_literal} "
            f"WHERE {self.q(table)}.{self.q(flag_col)} = {self.true_literal}"
        )


def resolve_dialect(name: str) -> type[SqlDialect]:
    """Return the compiler class for a SQLAlchemy dialect name (ANSI fallback)."""
    try:
        return SQL_DIALECTS.get(name)
    except Exception:
        return SqlDialect