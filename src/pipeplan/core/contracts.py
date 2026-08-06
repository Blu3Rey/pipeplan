"""Schema contracts and data-quality expectations.

A :class:`DataframeContract` declares the shape a dataframe must have — column
dtypes, nullability, uniqueness, allowed values, value checks, primary/foreign
keys, and whether unexpected columns are tolerated. Attaching one to a task's
input/output moves a whole class of errors from deep-in-the-run surprises to
declarative, fail-fast validation.

:class:`Expectation` covers softer data-quality assertions (row counts, null
rates, value bounds) that can be configured to *warn* rather than *fail*.

Both are Pydantic models (so they validate at config-parse time) but their
checking logic runs vectorised against a live ``pd.DataFrame``.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Literal

import pandas as pd
from pandas.api import types as pdt
from pydantic import BaseModel, ConfigDict, Field

from .exceptions import ContractError, ExpectationError

logger = logging.getLogger("pipeplan.contracts")

# --------------------------------------------------------------------------- #
# dtype family matching
# --------------------------------------------------------------------------- #

_DTYPE_CHECKS = {
    "integer": pdt.is_integer_dtype,
    "int": pdt.is_integer_dtype,
    "bigint": pdt.is_integer_dtype,
    "float": pdt.is_float_dtype,
    "double": pdt.is_float_dtype,
    "number": pdt.is_numeric_dtype,
    "numeric": pdt.is_numeric_dtype,
    "string": lambda s: pdt.is_string_dtype(s) or pdt.is_object_dtype(s),
    "str": lambda s: pdt.is_string_dtype(s) or pdt.is_object_dtype(s),
    "text": lambda s: pdt.is_string_dtype(s) or pdt.is_object_dtype(s),
    "boolean": pdt.is_bool_dtype,
    "bool": pdt.is_bool_dtype,
    "date": pdt.is_datetime64_any_dtype,
    "datetime": pdt.is_datetime64_any_dtype,
    "timestamp": pdt.is_datetime64_any_dtype,
    "category": lambda s: isinstance(s.dtype, pd.CategoricalDtype),
}

_COMPARATORS = {
    "==": lambda s, v: s == v,
    "!=": lambda s, v: s != v,
    ">": lambda s, v: s > v,
    ">=": lambda s, v: s >= v,
    "<": lambda s, v: s < v,
    "<=": lambda s, v: s <= v,
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- #
# schema contract
# --------------------------------------------------------------------------- #


class ColumnSchema(StrictModel):
    dtype: str | None = None
    nullable: bool = True
    unique: bool = False
    allowed: list[Any] | None = None
    checks: list[dict[str, Any]] = Field(default_factory=list)


class ForeignKey(StrictModel):
    column: str
    references: str  # "<dataframe>.<column>"


class DataframeContract(StrictModel):
    name: str | None = None
    description: str | None = None
    strict: bool = False
    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    columns: dict[str, ColumnSchema] = Field(default_factory=dict)

    def validate_frame(
        self,
        df: pd.DataFrame,
        *,
        label: str,
        state: Any = None,
    ) -> None:
        """Raise :class:`ContractError` if ``df`` violates this contract."""
        problems: list[str] = []
        cols = set(df.columns)

        for col_name, spec in self.columns.items():
            if col_name not in cols:
                problems.append(f"missing required column '{col_name}'")
                continue
            series = df[col_name]
            problems.extend(self._check_column(col_name, series, spec))

        if self.strict:
            extra = cols - set(self.columns)
            if extra:
                problems.append(f"unexpected column(s) {sorted(extra)} (strict schema)")

        for key in self.primary_key:
            if key not in cols:
                problems.append(f"primary key column '{key}' is absent")
        if self.primary_key and all(k in cols for k in self.primary_key):
            pk = df[self.primary_key]
            if pk.isnull().any().any():
                problems.append(f"primary key {self.primary_key} contains nulls")
            if pk.duplicated().any():
                dupes = int(pk.duplicated().sum())
                problems.append(f"primary key {self.primary_key} has {dupes} duplicate row(s)")

        if state is not None:
            problems.extend(self._check_foreign_keys(df, state))

        if problems:
            joined = "; ".join(problems)
            raise ContractError(f"contract violation for {label}: {joined}")

    def _check_column(self, name: str, series: pd.Series, spec: ColumnSchema) -> list[str]:
        out: list[str] = []
        if spec.dtype:
            check = _DTYPE_CHECKS.get(spec.dtype.lower())
            if check is None:
                out.append(f"column '{name}': unknown dtype '{spec.dtype}' in contract")
            elif not check(series):
                out.append(f"column '{name}': expected {spec.dtype}, got {series.dtype}")
        if not spec.nullable and series.isnull().any():
            n = int(series.isnull().sum())
            out.append(f"column '{name}': {n} null(s) but nullable=false")
        if spec.unique:
            non_null = series.dropna()
            if non_null.duplicated().any():
                out.append(f"column '{name}': values are not unique")
        if spec.allowed is not None:
            bad = ~series.dropna().isin(spec.allowed)
            if bad.any():
                out.append(
                    f"column '{name}': {int(bad.sum())} value(s) outside allowed set {spec.allowed}"
                )
        for check in spec.checks:
            for op, value in check.items():
                cmp = _COMPARATORS.get(op)
                if cmp is None:
                    out.append(f"column '{name}': unknown check operator '{op}'")
                    continue
                violations = ~cmp(series.dropna(), value)
                if violations.any():
                    out.append(
                        f"column '{name}': {int(violations.sum())} value(s) fail check {op} {value}"
                    )
        return out

    def _check_foreign_keys(self, df: pd.DataFrame, state: Any) -> list[str]:
        out: list[str] = []
        for fk in self.foreign_keys:
            if fk.column not in df.columns:
                out.append(f"foreign key column '{fk.column}' is absent")
                continue
            ref_frame, _, ref_col = fk.references.partition(".")
            if not state.has(ref_frame):
                continue  # referenced frame not materialised yet -> skip silently
            ref = state.get(ref_frame, copy=False)
            if ref_col not in ref.columns:
                out.append(f"foreign key target '{fk.references}' column not found")
                continue
            present = df[fk.column].dropna()
            orphans = ~present.isin(ref[ref_col])
            if orphans.any():
                out.append(
                    f"foreign key '{fk.column}' has {int(orphans.sum())} value(s) "
                    f"missing from {fk.references}"
                )
        return out


# --------------------------------------------------------------------------- #
# expectations
# --------------------------------------------------------------------------- #


class ExpectationAssertion(StrictModel):
    column: str | None = None
    not_null: bool | None = None
    unique: bool | None = None
    min: float | None = None
    max: float | None = None
    allowed: list[Any] | None = None
    row_count: dict[str, Any] | None = None
    null_rate_below: float | None = None


class Expectation(StrictModel):
    name: str
    assert_: ExpectationAssertion = Field(alias="assert")
    on_failure: Literal["warn", "fail"] = "fail"

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    def evaluate(self, df: pd.DataFrame, *, task: str) -> str | None:
        """Return a violation message, or ``None`` if the expectation holds."""
        a = self.assert_
        col = a.column
        if a.row_count is not None:
            for op, value in a.row_count.items():
                cmp = _COMPARATORS.get(op)
                if cmp is None:
                    return f"expectation '{self.name}': unknown row_count operator '{op}'"
                if not bool(cmp(pd.Series([len(df)]), value).iloc[0]):
                    return f"expectation '{self.name}': row_count {len(df)} fails {op} {value}"
        if col is not None:
            if col not in df.columns:
                return f"expectation '{self.name}': column '{col}' absent"
            series = df[col]
            if a.not_null and series.isnull().any():
                return f"expectation '{self.name}': column '{col}' has nulls"
            if a.unique and series.dropna().duplicated().any():
                return f"expectation '{self.name}': column '{col}' not unique"
            if a.min is not None and (series.dropna() < a.min).any():
                return f"expectation '{self.name}': column '{col}' below min {a.min}"
            if a.max is not None and (series.dropna() > a.max).any():
                return f"expectation '{self.name}': column '{col}' above max {a.max}"
            if a.allowed is not None and (~series.dropna().isin(a.allowed)).any():
                return f"expectation '{self.name}': column '{col}' has values outside {a.allowed}"
            if a.null_rate_below is not None:
                rate = float(series.isnull().mean())
                if rate >= a.null_rate_below:
                    return (
                        f"expectation '{self.name}': null rate {rate:.3f} "
                        f">= {a.null_rate_below}"
                    )
        return None

    def check(self, df: pd.DataFrame, *, task: str) -> None:
        message = self.evaluate(df, task=task)
        if message is None:
            return
        if self.on_failure == "warn":
            logger.warning("[%s] %s", task, message)
        else:
            raise ExpectationError(f"[{task}] {message}")
