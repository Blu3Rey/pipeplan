"""Set-tier transforms: row-masking and reshaping operations.

These operate on a single dataframe as a *set of rows* -- selecting, ordering,
de-duplicating or aggregating -- without reaching into the shared state.
"""

from __future__ import annotations

from typing import Any, ClassVar, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, model_validator

from ..ast.predicate import PredicateNode
from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import register_transform
from .base import Tier, Transform

# --------------------------------------------------------------------------- #
# filter
# --------------------------------------------------------------------------- #


@register_transform("filter")
class FilterTransform(Transform):
    """Keep rows that satisfy a recursive predicate AST."""

    tier: ClassVar[Tier] = Tier.SET
    root: PredicateNode

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: Any) -> Any:
        # The whole params block *is* the predicate tree.
        if isinstance(data, dict) and "root" not in data:
            return {"root": data}
        return data

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        mask = self.root.evaluate(df)
        return df.loc[mask].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# sort
# --------------------------------------------------------------------------- #


@register_transform("sort")
class SortTransform(Transform):
    """Order rows by one or more columns (``{"col": "asc"|"desc"}``)."""

    tier: ClassVar[Tier] = Tier.SET
    columns: dict[str, Literal["asc", "desc"]]

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: Any) -> Any:
        if isinstance(data, dict) and "columns" not in data:
            return {"columns": data}
        return data

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        by = list(self.columns)
        missing = [c for c in by if c not in df.columns]
        if missing:
            raise TransformError(f"sort references missing column(s) {missing}")
        ascending = [direction == "asc" for direction in self.columns.values()]
        return df.sort_values(by=by, ascending=ascending, kind="stable").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# dedupe
# --------------------------------------------------------------------------- #


@register_transform("dedupe")
class DedupeTransform(Transform):
    """Drop duplicate rows, optionally keyed on a subset of columns."""

    tier: ClassVar[Tier] = Tier.SET
    on: str | list[str] | None = None
    keep: Literal["first", "last", False] = "first"

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        subset = [self.on] if isinstance(self.on, str) else self.on
        if subset:
            missing = [c for c in subset if c not in df.columns]
            if missing:
                raise TransformError(f"dedupe references missing column(s) {missing}")
        return df.drop_duplicates(subset=subset, keep=self.keep).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# group
# --------------------------------------------------------------------------- #


@register_transform("group")
class GroupTransform(Transform):
    """Aggregate rows by key.

    ``agg`` is required (``{"amount": "sum", "score": "mean"}``); grouping keys
    are returned as columns. For key-wise de-duplication, use ``dedupe`` instead
    -- ``group`` always aggregates, so its intent is never ambiguous.
    """

    tier: ClassVar[Tier] = Tier.SET
    by: str | list[str]
    agg: dict[str, str]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        keys = [self.by] if isinstance(self.by, str) else list(self.by)
        missing = [c for c in keys if c not in df.columns]
        if missing:
            raise TransformError(f"group references missing key column(s) {missing}")
        agg_missing = [c for c in self.agg if c not in df.columns]
        if agg_missing:
            raise TransformError(f"group aggregates missing column(s) {agg_missing}")
        grouped = df.groupby(keys, as_index=False, sort=False, dropna=False)
        return grouped.agg(self.agg).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# select / drop (column projection)
# --------------------------------------------------------------------------- #


# @register_transform("select")
# class SelectTransform(Transform):
#     """Keep only the named columns, in the given order."""

#     tier: ClassVar[Tier] = Tier.SET
#     columns: list[str]

#     @model_validator(mode="before")
#     @classmethod
#     def _wrap(cls, data: Any) -> Any:
#         # Accept a bare list as the columns value.
#         if isinstance(data, list):
#             return {"columns": data}
#         return data

#     def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
#         assert df is not None
#         missing = [c for c in self.columns if c not in df.columns]
#         if missing:
#             raise TransformError(f"select references missing column(s) {missing}")
#         return df[self.columns].copy()


# @register_transform("drop")
# class DropTransform(Transform):
#     """Remove the named columns."""

#     tier: ClassVar[Tier] = Tier.SET
#     columns: list[str]

#     @model_validator(mode="before")
#     @classmethod
#     def _wrap(cls, data: Any) -> Any:
#         if isinstance(data, list):
#             return {"columns": data}
#         return data

#     def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
#         assert df is not None
#         return df.drop(columns=[c for c in self.columns if c in df.columns])


# --------------------------------------------------------------------------- #
# window (vectorised ranked / cumulative columns)
# --------------------------------------------------------------------------- #

_WINDOW_FNS = {"row_number", "rank", "dense_rank", "cumsum", "cummax", "cummin", "cumcount"}


class WindowFn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fn: Literal["row_number", "rank", "dense_rank", "cumsum", "cummax", "cummin", "cumcount"]
    column: str | None = None


@register_transform("window")
class WindowTransform(Transform):
    """Add ranked / cumulative columns over partitions, fully vectorised.

    Example::

        partition_by: [cust_id]
        order_by: { order_date: desc }
        add:
          recency_rank: { fn: row_number }
          running_total: { fn: cumsum, column: line_total }
    """

    tier: ClassVar[Tier] = Tier.SET
    partition_by: list[str] = []
    order_by: dict[str, Literal["asc", "desc"]] | None = None
    add: dict[str, WindowFn]

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        work = df
        if self.order_by:
            by = list(self.order_by)
            ascending = [d == "asc" for d in self.order_by.values()]
            work = work.sort_values(by=by, ascending=ascending, kind="stable")
        grouped = work.groupby(self.partition_by, sort=False, dropna=False) if self.partition_by else None
        for new_col, spec in self.add.items():
            work[new_col] = self._compute(work, grouped, spec)
        return work.reset_index(drop=True)

    def _compute(self, work, grouped, spec: WindowFn):
        fn = spec.fn
        if fn in ("row_number", "cumcount"):
            base = grouped.cumcount() if grouped is not None else pd.Series(range(len(work)), index=work.index)
            return base + 1 if fn == "row_number" else base
        if fn in ("rank", "dense_rank"):
            if spec.column is None:
                raise TransformError(f"window '{fn}' requires a 'column'")
            method = "dense" if fn == "dense_rank" else "min"
            if grouped is not None:
                return grouped[spec.column].rank(method=method)
            return work[spec.column].rank(method=method)
        # cumulative numeric ops
        if spec.column is None:
            raise TransformError(f"window '{fn}' requires a 'column'")
        if spec.column not in work.columns:
            raise TransformError(f"window references missing column '{spec.column}'")
        if grouped is not None:
            return grouped[spec.column].transform(fn)
        return getattr(work[spec.column], fn)()
