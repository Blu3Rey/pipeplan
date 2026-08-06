"""Element-tier transforms: 1:1 vectorised column operations.

Every operation here maps each input row to exactly one output row. There is no
row-by-row Python and no ``DataFrame.apply``; all work goes through the pandas
``str``/numeric vectorised paths.
"""

from __future__ import annotations

import re
from typing import Any, ClassVar, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, model_validator, field_validator

from ..ast.expression import ExpressionNode
from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import register_transform
from .base import Tier, Transform

# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #


def _require_columns(df: pd.DataFrame, columns: list[str], action: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise TransformError(
            f"transform '{action}' references missing column(s) {missing}; "
            f"present columns: {list(df.columns)}"
        )


class _ColumnMapped(Transform):
    """Transforms whose entire ``params`` block is a ``{column: spec}`` mapping."""

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: Any) -> Any:
        if isinstance(data, dict) and "columns" not in data:
            return {"columns": data}
        return data


# --------------------------------------------------------------------------- #
# label
# --------------------------------------------------------------------------- #


@register_transform("label")
class LabelTransform(_ColumnMapped):
    """Rename columns (a pure metadata operation)."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, str]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        return df.rename(columns=self.columns)


# --------------------------------------------------------------------------- #
# map
# --------------------------------------------------------------------------- #


@register_transform("map")
class MapTransform(_ColumnMapped):
    """Replace specific values per column, leaving unmapped values intact."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, dict[Any, Any]]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        _require_columns(df, list(self.columns), "map")
        for column, mapping in self.columns.items():
            series = df[column]
            mask = series.isin(list(mapping.keys()))
            df[column] = series.map(mapping).where(mask, series)
        return df


# --------------------------------------------------------------------------- #
# replace
# --------------------------------------------------------------------------- #

_FLAG_MAP = {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL, "x": re.VERBOSE}


class ReplaceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    regex: str
    swap: str = ""
    flags: str = ""

    def compiled_flags(self) -> int:
        value = 0
        for char in self.flags.lower():
            if char not in _FLAG_MAP:
                raise ValueError(f"unknown regex flag '{char}' in '{self.flags}'")
            value |= _FLAG_MAP[char]
        return value


@register_transform("replace")
class ReplaceTransform(_ColumnMapped):
    """Vectorised regex substitution per column."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, list[ReplaceSpec]]

    @field_validator("columns", mode="before")
    @classmethod
    def _coerce_list(cls, value: object) -> object:
        # if isinstance(value, dict):
        #     return [value]
        for key, val in value.items():
            if isinstance(val, dict):
                value[key] = [val]
        return value

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        _require_columns(df, list(self.columns), "replace")
        for column, specs in self.columns.items():
            # df[column] = (
            #     df[column]
            #     .astype("string")
            #     .str.replace(spec.regex, spec.swap, regex=True, flags=spec.compiled_flags())
            # )
            for spec in specs:
                df[column] = (
                    df[column]
                    .astype("string")
                    .str.replace(spec.regex, spec.swap, regex=True, flags=spec.compiled_flags())
                )
        return df


# --------------------------------------------------------------------------- #
# cast
# --------------------------------------------------------------------------- #

CastType = Literal[
    "integer", "int", "bigint",
    "float", "double", "number",
    "string", "str", "text",
    "boolean", "bool",
    "date", "datetime", "timestamp",
    "category",
]


def _cast_series(series: pd.Series, target: str) -> pd.Series:
    if target in ("integer", "int", "bigint"):
        return pd.to_numeric(series, errors="raise").astype("Int64")
    if target in ("float", "double", "number"):
        return pd.to_numeric(series, errors="raise").astype("float64")
    if target in ("string", "str", "text"):
        return series.astype("string")
    if target in ("boolean", "bool"):
        return series.astype("boolean")
    if target in ("date",):
        return pd.to_datetime(series, errors="raise").dt.normalize()
    if target in ("datetime", "timestamp"):
        return pd.to_datetime(series, errors="raise")
    if target in ("category",):
        return series.astype("category")
    raise TransformError(f"unsupported cast target '{target}'")  # pragma: no cover


def _coerce_lenient(series: pd.Series, target: str) -> pd.Series:
    """Like :func:`_cast_series` but turns un-parseable values into NaN/NaT."""
    if target in ("integer", "int", "bigint"):
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if target in ("float", "double", "number"):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if target in ("date",):
        return pd.to_datetime(series, errors="coerce").dt.normalize()
    if target in ("datetime", "timestamp"):
        return pd.to_datetime(series, errors="coerce")
    # Non-coercible target types fall back to the strict path.
    return _cast_series(series, target)


@register_transform("cast")
class CastTransform(_ColumnMapped):
    """Coerce column dtypes. Bad values fail loudly rather than silently nulling."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    supports_quarantine: ClassVar[bool] = True
    columns: dict[str, CastType]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        _require_columns(df, list(self.columns), "cast")
        for column, target in self.columns.items():
            try:
                df[column] = _cast_series(df[column], target)
            except (ValueError, TypeError) as exc:
                raise TransformError(
                    f"cannot cast column '{column}' to {target}: {exc}"
                ) from exc
        return df

    def apply_safe(self, df, ctx):
        """Coerce with errors->NaN, divert rows that failed to parse."""
        assert df is not None
        _require_columns(df, list(self.columns), "cast")
        out = df.copy()
        bad_mask = pd.Series(False, index=out.index)
        for column, target in self.columns.items():
            before_null = out[column].isna()
            coerced = _coerce_lenient(out[column], target)
            newly_null = coerced.isna() & ~before_null
            bad_mask = bad_mask | newly_null
            out[column] = coerced
        rejected = df.loc[bad_mask].copy()
        good = out.loc[~bad_mask].reset_index(drop=True)
        return good, rejected.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# affix
# --------------------------------------------------------------------------- #


class AffixSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str
    position: Literal["prefix", "suffix"] = "suffix"


@register_transform("affix")
class AffixTransform(_ColumnMapped):
    """Prepend or append a literal to a column's string representation."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, AffixSpec]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        _require_columns(df, list(self.columns), "affix")
        for column, spec in self.columns.items():
            text = df[column].astype("string")
            if spec.position == "prefix":
                df[column] = spec.text + text
            else:
                df[column] = text + spec.text
        return df


# --------------------------------------------------------------------------- #
# normalize
# --------------------------------------------------------------------------- #

NormOp = Literal[
    "nfc", "nfkc", "nfd", "nfkd",
    "strip", "lstrip", "rstrip",
    "upper", "lower", "title", "casefold",
]


def _apply_norm(series: pd.Series, op: str) -> pd.Series:
    text = series.astype("string")
    if op in ("nfc", "nfkc", "nfd", "nfkd"):
        return text.str.normalize(op.upper())
    return getattr(text.str, op)()


@register_transform("normalize")
class NormalizeTransform(_ColumnMapped):
    """Apply an ordered pipeline of string normalisations per column."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, list[NormOp]]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        _require_columns(df, list(self.columns), "normalize")
        for column, ops in self.columns.items():
            series = df[column]
            for op in ops:
                series = _apply_norm(series, op)
            df[column] = series
        return df


# --------------------------------------------------------------------------- #
# derive (compute AST)
# --------------------------------------------------------------------------- #


@register_transform("derive")
class DeriveTransform(Transform):
    """Compute a new (or overwritten) column from a recursive expression tree."""

    tier: ClassVar[Tier] = Tier.ELEMENT
    target: str
    expr: ExpressionNode

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        df[self.target] = self.expr.evaluate(df)
        return df


# --------------------------------------------------------------------------- #
# fillna
# --------------------------------------------------------------------------- #


@register_transform("fillna")
class FillNaTransform(_ColumnMapped):
    """Fill missing values per column with a constant.

    Columns named here that are absent from the frame are created and filled,
    which makes a downstream contract's ``nullable: false`` satisfiable for an
    optional upstream field.
    """

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: dict[str, Any]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        for column, value in self.columns.items():
            if column in df.columns:
                df[column] = df[column].fillna(value)
            else:
                df[column] = value
        return df
