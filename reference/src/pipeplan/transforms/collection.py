"""Collection-tier transforms: relational operations across dataframes.

These transforms reach into the shared execution state to pull other dataframes
by name. They implement the *implicit piped data rule*: when a ``left`` operand
is omitted, the dataframe currently flowing through the task's step sequence is
used instead.
"""

from __future__ import annotations

from typing import ClassVar, Literal

import numpy as np
import pandas as pd

from ..config.models import PIPE_TOKENS
from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import register_transform
from .base import Tier, Transform

How = Literal["inner", "left", "right", "outer", "cross"]


def _resolve(ref: str, flowing: pd.DataFrame | None, ctx: ExecutionContext, role: str) -> pd.DataFrame:
    """Resolve a collection operand to a concrete dataframe.

    The ``${pipe}`` token binds the dataframe currently flowing through the
    task's step sequence. Any other string is a name looked up in the shared
    state. Operands are mandatory and explicit -- there is no implicit fallback.
    """
    if ref in PIPE_TOKENS:
        if flowing is None:
            raise TransformError(
                f"the '{role}' operand is '{ref}' but no dataframe is flowing "
                f"through this task to bind to"
            )
        return flowing
    return ctx.state.get(ref)


# --------------------------------------------------------------------------- #
# merge
# --------------------------------------------------------------------------- #


@register_transform("merge")
class MergeTransform(Transform):
    """Relational join of two dataframes pulled from the state (or the pipe)."""

    tier: ClassVar[Tier] = Tier.COLLECTION
    left: str
    right: str
    how: How = "inner"
    on: str | list[str] | None = None
    left_on: str | list[str] | None = None
    right_on: str | list[str] | None = None
    suffixes: tuple[str, str] = ("_x", "_y")

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        left_df = _resolve(self.left, df, ctx, "left")
        right_df = _resolve(self.right, df, ctx, "right")
        try:
            return pd.merge(
                left_df,
                right_df,
                how=self.how,
                on=self.on,
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=self.suffixes,
            )
        except (KeyError, ValueError) as exc:
            raise TransformError(f"merge failed: {exc}") from exc


# --------------------------------------------------------------------------- #
# join
# --------------------------------------------------------------------------- #


@register_transform("join")
class JoinTransform(Transform):
    """Like ``merge`` but the left operand defaults to the flowing dataframe."""

    tier: ClassVar[Tier] = Tier.COLLECTION
    left: str
    right: str
    how: How = "left"
    on: str | list[str] | None = None
    left_on: str | list[str] | None = None
    right_on: str | list[str] | None = None
    suffixes: tuple[str, str] = ("_x", "_y")

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        left_df = _resolve(self.left, df, ctx, "left")
        right_df = _resolve(self.right, df, ctx, "right")
        try:
            return pd.merge(
                left_df,
                right_df,
                how=self.how,
                on=self.on,
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=self.suffixes,
            )
        except (KeyError, ValueError) as exc:
            raise TransformError(f"join failed: {exc}") from exc


# --------------------------------------------------------------------------- #
# fuzzy_join
# --------------------------------------------------------------------------- #


def _best_matches(left_keys: np.ndarray, right_keys: np.ndarray, threshold: float) -> dict[str, str]:
    """Map each unique left key to its best right key above ``threshold``.

    Scoring is done only over *unique* keys, then broadcast back to the full
    frame by the caller -- the mandated extract-unique / score / broadcast
    pattern, so cost scales with cardinality, not row count.
    """
    left_str = [str(k) for k in left_keys]
    right_str = [str(k) for k in right_keys]

    try:
        from rapidfuzz import fuzz, process

        # cdist returns a len(left) x len(right) score matrix in C, fully vectorised.
        scores = process.cdist(left_str, right_str, scorer=fuzz.ratio)
        best_idx = scores.argmax(axis=1)
        best_score = scores[np.arange(len(left_str)), best_idx]
        return {
            left_str[i]: right_str[best_idx[i]]
            for i in range(len(left_str))
            if best_score[i] >= threshold * 100
        }
    except ImportError:
        from difflib import SequenceMatcher

        mapping: dict[str, str] = {}
        for lk in left_str:
            best_key, best = None, 0.0
            for rk in right_str:
                ratio = SequenceMatcher(None, lk, rk).ratio()
                if ratio > best:
                    best_key, best = rk, ratio
            if best_key is not None and best >= threshold:
                mapping[lk] = best_key
        return mapping


@register_transform("fuzzy_join")
class FuzzyJoinTransform(Transform):
    """Approximate string join.

    Unique keys are extracted from both sides, scored, and the winning matches
    are broadcast back onto the full frames before an exact merge on the
    resolved key. Uses ``rapidfuzz`` when installed, with a stdlib fallback.
    """

    tier: ClassVar[Tier] = Tier.COLLECTION
    left: str = "${pipe}"
    right: str
    left_on: str
    right_on: str
    how: How = "left"
    threshold: float = 0.85
    suffixes: tuple[str, str] = ("_x", "_y")

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        left_df = _resolve(self.left, df, ctx, "left").copy()
        right_df = _resolve(self.right, df, ctx, "right")
        if self.left_on not in left_df.columns:
            raise TransformError(f"fuzzy_join: left key '{self.left_on}' not found")
        if self.right_on not in right_df.columns:
            raise TransformError(f"fuzzy_join: right key '{self.right_on}' not found")

        left_keys = left_df[self.left_on].dropna().unique()
        right_keys = right_df[self.right_on].dropna().unique()
        mapping = _best_matches(left_keys, right_keys, self.threshold)

        bridge = "__fuzzy_key__"
        left_df[bridge] = left_df[self.left_on].astype("string").map(mapping)
        try:
            merged = pd.merge(
                left_df,
                right_df,
                how=self.how,
                left_on=bridge,
                right_on=self.right_on,
                suffixes=self.suffixes,
            )
        except (KeyError, ValueError) as exc:
            raise TransformError(f"fuzzy_join failed: {exc}") from exc
        return merged.drop(columns=[bridge])


# --------------------------------------------------------------------------- #
# union (vertical concatenation)
# --------------------------------------------------------------------------- #


@register_transform("union")
class UnionTransform(Transform):
    """Stack rows from several dataframes (a vertical concat).

    Each entry in ``frames`` is a state dataframe name or the ``${pipe}`` token.
    With ``dedupe: true`` exact-duplicate rows are collapsed after stacking.
    """

    tier: ClassVar[Tier] = Tier.COLLECTION
    frames: list[str]
    dedupe: bool = False
    ignore_index: bool = True

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        if not self.frames:
            raise TransformError("union requires at least one frame")
        resolved = [_resolve(name, df, ctx, "frames") for name in self.frames]
        out = pd.concat(resolved, ignore_index=self.ignore_index, sort=False)
        if self.dedupe:
            out = out.drop_duplicates().reset_index(drop=True)
        return out
