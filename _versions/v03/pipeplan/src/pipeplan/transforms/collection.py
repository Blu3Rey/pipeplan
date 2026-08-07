"""Collection-tier transforms: combining dataframes.

Two orthogonal axes of combination, each a single, clearly-named operation:

* **Horizontal** -- ``join`` (alias ``merge``): widen records by attaching
  attributes from another frame, matched on key(s). Wraps a vectorised
  ``pd.merge`` with the usual ``how`` strategies.
* **Vertical** -- ``concat`` (aliases ``append``, ``union``): lengthen a frame by
  stacking the records of several frames that share a schema.

``fuzzy_join`` is a specialised horizontal join on approximate string keys.

Operands are explicit. A frame is named in the shared state, or the ``${pipe}``
token binds the frame currently flowing through the task; there is no implicit
fallback.
"""

from __future__ import annotations

from typing import ClassVar, Literal

import numpy as np
import pandas as pd

from ..config.models import PIPE_TOKENS
from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import TRANSFORMS, register_transform
from .base import Tier, Transform

How = Literal["inner", "left", "right", "outer", "cross"]


def _resolve(ref: str, flowing: pd.DataFrame | None, ctx: ExecutionContext, role: str) -> pd.DataFrame:
    """Resolve a collection operand to a concrete dataframe."""
    if ref in PIPE_TOKENS:
        if flowing is None:
            raise TransformError(
                f"the '{role}' operand is '{ref}' but no dataframe is flowing "
                f"through this task to bind to"
            )
        return flowing
    return ctx.state.get(ref)


# --------------------------------------------------------------------------- #
# join  (horizontal: add attributes to records)        alias: merge
# --------------------------------------------------------------------------- #


@register_transform("join")
class JoinTransform(Transform):
    """Combine two frames *horizontally*: attach the right frame's attributes to
    the left frame's records, matched on key(s).

    ``how`` defaults to ``left`` -- the natural choice for "enrich existing
    records": every left record is kept and gains the matched right columns.
    Use ``inner`` to keep only matches, ``outer`` for a full union of keys,
    ``right`` to keep all right records, or ``cross`` for a cartesian product.

    ``merge`` is registered as a synonym for this same operation.
    """

    tier: ClassVar[Tier] = Tier.COLLECTION
    left: str
    right: str
    how: How = "left"
    on: str | list[str] | None = None
    left_on: str | list[str] | None = None
    right_on: str | list[str] | None = None
    suffixes: tuple[str, str] = ("_x", "_y")
    relationship: Literal["1:1", "1:m", "m:1", "m:m"] | None = None

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        left_df = _resolve(self.left, df, ctx, "left")
        right_df = _resolve(self.right, df, ctx, "right")
        validate = {"1:1": "one_to_one", "1:m": "one_to_many",
                    "m:1": "many_to_one", "m:m": "many_to_many"}.get(self.relationship or "")
        try:
            return pd.merge(
                left_df,
                right_df,
                how=self.how,
                on=self.on,
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=self.suffixes,
                validate=validate or None,
            )
        except (KeyError, ValueError, pd.errors.MergeError) as exc:
            raise TransformError(f"join failed: {exc}") from exc


# `merge` is the same horizontal operation under a familiar name.
TRANSFORMS.register("merge", JoinTransform)


# --------------------------------------------------------------------------- #
# concat  (vertical: add records under shared attributes)  aliases: append, union
# --------------------------------------------------------------------------- #


@register_transform("concat")
class ConcatTransform(Transform):
    """Combine frames *vertically*: stack their records into one longer frame.

    Each entry in ``frames`` is a state dataframe name or the ``${pipe}`` token.
    Column handling via ``columns``:

    * ``outer`` (default) -- keep the union of all columns; cells absent from a
      given frame are filled with NA (a forgiving append).
    * ``inner`` -- keep only the columns common to *every* frame (a strict
      append guaranteeing no NA is introduced by misaligned schemas).

    With ``dedupe: true`` exact-duplicate rows are collapsed after stacking.

    ``append`` and ``union`` are registered as synonyms for this operation.
    """

    tier: ClassVar[Tier] = Tier.COLLECTION
    frames: list[str]
    columns: Literal["outer", "inner"] = "outer"
    dedupe: bool = False
    ignore_index: bool = True

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        if not self.frames:
            raise TransformError("concat requires at least one frame")
        resolved = [_resolve(name, df, ctx, "frames") for name in self.frames]

        if self.columns == "inner":
            common = set.intersection(*(set(f.columns) for f in resolved))
            if not common:
                raise TransformError("concat columns='inner' but the frames share no columns")
            ordered = [c for c in resolved[0].columns if c in common]
            resolved = [f[ordered] for f in resolved]

        out = pd.concat(resolved, ignore_index=self.ignore_index, sort=False)
        if self.dedupe:
            out = out.drop_duplicates().reset_index(drop=True)
        return out


# Familiar synonyms for the same vertical operation.
TRANSFORMS.register("append", ConcatTransform)
TRANSFORMS.register("union", ConcatTransform)


# --------------------------------------------------------------------------- #
# fuzzy_join  (specialised horizontal join on approximate string keys)
# --------------------------------------------------------------------------- #


def _best_matches(left_keys: np.ndarray, right_keys: np.ndarray, threshold: float) -> dict[str, str]:
    """Map each unique left key to its best right key above ``threshold``.

    Scoring is over *unique* keys then broadcast back to the full frame by the
    caller -- the mandated extract-unique / score / broadcast pattern, so cost
    scales with cardinality, not row count.
    """
    left_str = [str(k) for k in left_keys]
    right_str = [str(k) for k in right_keys]

    try:
        from rapidfuzz import fuzz, process

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
    """Approximate horizontal join: match keys by string similarity, then merge.

    Unique keys are extracted from both sides, scored, and the winning matches
    broadcast back onto the full frames before an exact merge on the resolved
    key. Uses ``rapidfuzz`` when installed, with a stdlib fallback.
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
