"""Collection-tier change-data-capture: the ``compare_diff`` transform.

``compare_diff`` performs **delta processing** (Change Delta Capture) between two
snapshots of the same logical dataset:

* ``source`` -- the *new* / incoming snapshot (state name or ``${pipe}``).
* ``target`` -- the *previous* / baseline snapshot (state name or ``${pipe}``).

It classifies every business key into one of four change types and emits a single
dataframe of the changed rows, each tagged in an operation column (``_op`` by
default):

============  ===========================================================
operation     meaning
============  ===========================================================
``insert``    key present in ``source`` but not ``target`` (carries source values)
``update``    key present in both, but a compared column differs (source values)
``delete``    key present in ``target`` but not ``source`` (carries target values)
``unchanged`` key present in both with no compared column differing (source values)
============  ===========================================================

By default only ``insert``/``update``/``delete`` are emitted; ``unchanged`` is
opt-in via ``emit``.

Design constraints honoured
---------------------------
* **Fully vectorised.** Identity classification is an outer key merge with an
  indicator; change detection is a null-safe element-wise comparison (or an
  optional row hash) over the *common keys* only -- never a Python row loop and
  never ``.apply``.
* **Explicit pipe rule.** ``source`` and ``target`` are mandatory operands and
  bind the flowing frame only via the explicit ``${pipe}`` token, exactly like
  ``merge``/``join`` -- there is no implicit fallback.
* **Pristine core.** Registered through ``@register_transform`` like every other
  built-in; the engine never special-cases it.
"""

from __future__ import annotations

from typing import ClassVar, Literal

import numpy as np
import pandas as pd
from pydantic import Field, field_validator, model_validator

from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import register_transform
from .base import Tier, Transform
from .collection import _resolve  # canonical ${pipe}/state operand resolver

# Internal suffixes used while aligning the two snapshots for comparison. They
# never leak into the output (only the comparison block uses them), so a private,
# collision-unlikely pair is fine.
_SRC_SUFFIX = "__cdc_src"
_TGT_SUFFIX = "__cdc_tgt"

# Canonical output ordering of change types, independent of the order the user
# lists them in ``emit`` -- keeps output deterministic.
_CANONICAL_ORDER: tuple[str, ...] = ("insert", "update", "delete", "unchanged")

ChangeType = Literal["insert", "update", "delete", "unchanged"]


@register_transform("compare_diff")
class CompareDiffTransform(Transform):
    """Change-data-capture delta between a new and a previous snapshot.

    Parameters
    ----------
    source, target
        Operands for the new and previous snapshots. Each is either a dataframe
        name in the execution state or the ``${pipe}`` token.
    key
        Business key column(s) identifying a row's identity across snapshots.
    compare
        Columns whose values decide ``update`` vs ``unchanged``. Defaults to all
        non-key columns common to both snapshots.
    ignore
        Columns excluded from comparison (applied after ``compare``).
    change_detection
        ``"exact"`` (default) does a null-safe value comparison and tolerates
        equivalent dtypes (``1 == 1.0``). ``"hash"`` compares a per-row hash of
        the compared block -- faster on very wide frames, but assumes the two
        snapshots share column dtypes.
    emit
        Which change types to include in the output. Defaults to
        ``["insert", "update", "delete"]``.
    op_column
        Name of the emitted operation column. Defaults to ``"_op"``.
    op_labels
        Override the literal written into ``op_column`` per change type, e.g.
        ``{"insert": "I", "update": "U", "delete": "D"}``.
    duplicate_keys
        How to handle non-unique keys within a snapshot. ``"error"`` (default)
        raises; ``"keep_first"`` / ``"keep_last"`` deduplicate per snapshot first.
    """

    tier: ClassVar[Tier] = Tier.COLLECTION

    source: str
    target: str
    key: str | list[str]
    compare: list[str] | None = None
    ignore: list[str] = Field(default_factory=list)
    change_detection: Literal["exact", "hash"] = "exact"
    emit: list[ChangeType] = Field(default_factory=lambda: ["insert", "update", "delete"])
    op_column: str = "_op"
    op_labels: dict[ChangeType, str] = Field(default_factory=dict)
    duplicate_keys: Literal["error", "keep_first", "keep_last"] = "error"

    # ----------------------------------------------------------------------- #
    # config-time validation
    # ----------------------------------------------------------------------- #

    @field_validator("key")
    @classmethod
    def _key_nonempty(cls, v: str | list[str]) -> str | list[str]:
        if isinstance(v, list) and not v:
            raise ValueError("compare_diff 'key' must name at least one column")
        return v

    @field_validator("emit")
    @classmethod
    def _emit_valid(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("compare_diff 'emit' must list at least one change type")
        if len(set(v)) != len(v):
            raise ValueError(f"compare_diff 'emit' has duplicates: {v}")
        return v

    @model_validator(mode="after")
    def _labels_known(self) -> "CompareDiffTransform":
        unknown = set(self.op_labels) - set(_CANONICAL_ORDER)
        if unknown:
            raise ValueError(f"compare_diff 'op_labels' has unknown change types: {sorted(unknown)}")
        return self

    # ----------------------------------------------------------------------- #
    # helpers
    # ----------------------------------------------------------------------- #

    @property
    def _keys(self) -> list[str]:
        return [self.key] if isinstance(self.key, str) else list(self.key)

    def _label(self, change: ChangeType) -> str:
        return self.op_labels.get(change, change)

    @staticmethod
    def _key_index(frame: pd.DataFrame, keys: list[str]) -> pd.Index:
        """A hashable, ``.isin``-able index over the key column(s)."""
        if len(keys) == 1:
            return pd.Index(frame[keys[0]])
        return pd.MultiIndex.from_frame(frame[keys])

    def _dedupe(self, frame: pd.DataFrame, keys: list[str], role: str) -> pd.DataFrame:
        dup = frame.duplicated(subset=keys, keep=False)
        if not dup.any():
            return frame
        if self.duplicate_keys == "error":
            n = int(dup.sum())
            raise TransformError(
                f"compare_diff '{role}' snapshot has {n} row(s) with duplicate "
                f"key(s) {keys}; delta semantics require unique keys "
                f"(set duplicate_keys='keep_first'|'keep_last' to override)"
            )
        keep = "first" if self.duplicate_keys == "keep_first" else "last"
        return frame.drop_duplicates(subset=keys, keep=keep)

    def _compare_columns(self, source: pd.DataFrame, target: pd.DataFrame, keys: list[str]) -> list[str]:
        common = [c for c in source.columns if c in set(target.columns)]
        if self.compare is not None:
            missing = [c for c in self.compare if c not in set(common)]
            if missing:
                raise TransformError(
                    f"compare_diff 'compare' names column(s) {missing} that are "
                    f"not common to both snapshots"
                )
            candidate = list(self.compare)
        else:
            candidate = common
        skip = set(keys) | set(self.ignore)
        return [c for c in candidate if c not in skip]

    def _changed_mask(self, joined: pd.DataFrame, compare_cols: list[str]) -> np.ndarray:
        """Null-safe per-row 'has changed' mask over the aligned comparison block."""
        n = len(joined)
        if not compare_cols:
            # Nothing to compare -> rows present in both are 'unchanged'.
            return np.zeros(n, dtype=bool)

        if self.change_detection == "hash":
            left = joined[[f"{c}{_SRC_SUFFIX}" for c in compare_cols]].set_axis(compare_cols, axis=1)
            right = joined[[f"{c}{_TGT_SUFFIX}" for c in compare_cols]].set_axis(compare_cols, axis=1)
            h_left = pd.util.hash_pandas_object(left, index=False).to_numpy()
            h_right = pd.util.hash_pandas_object(right, index=False).to_numpy()
            return h_left != h_right

        changed = np.zeros(n, dtype=bool)
        for col in compare_cols:  # small fixed set of columns; each op is vectorised
            a = joined[f"{col}{_SRC_SUFFIX}"]
            b = joined[f"{col}{_TGT_SUFFIX}"]
            both_na = (a.isna() & b.isna()).to_numpy()
            eq = (a == b).fillna(False).to_numpy(dtype=bool)  # NA-comparisons -> not-equal
            changed |= (~eq) & (~both_na)
        return changed

    # ----------------------------------------------------------------------- #
    # execution
    # ----------------------------------------------------------------------- #

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        keys = self._keys
        source = _resolve(self.source, df, ctx, "source")
        target = _resolve(self.target, df, ctx, "target")

        for role, frame in (("source", source), ("target", target)):
            missing = [k for k in keys if k not in frame.columns]
            if missing:
                raise TransformError(
                    f"compare_diff key column(s) {missing} absent from the '{role}' snapshot"
                )

        if self.op_column in set(source.columns) | set(target.columns):
            raise TransformError(
                f"compare_diff op_column '{self.op_column}' collides with an existing "
                f"column; choose another name"
            )

        source = self._dedupe(source, keys, "source")
        target = self._dedupe(target, keys, "target")

        compare_cols = self._compare_columns(source, target, keys)

        # --- identity classification (vectorised key membership) ------------ #
        src_ix = self._key_index(source, keys)
        tgt_ix = self._key_index(target, keys)
        in_target = np.asarray(src_ix.isin(tgt_ix))   # source rows that also exist in target
        in_source = np.asarray(tgt_ix.isin(src_ix))   # target rows that also exist in source

        # --- change detection over the common keys only -------------------- #
        joined = source[keys + compare_cols].merge(
            target[keys + compare_cols],
            on=keys,
            how="inner",
            suffixes=(_SRC_SUFFIX, _TGT_SUFFIX),
        )
        changed = self._changed_mask(joined, compare_cols)
        changed_ix = self._key_index(joined.loc[changed], keys)
        is_update = in_target & np.asarray(src_ix.isin(changed_ix))

        # --- per-row operation labels -------------------------------------- #
        # source rows are insert (new), update (changed) or unchanged.
        src_op = np.where(
            ~in_target,
            "insert",
            np.where(is_update, "update", "unchanged"),
        )
        source_tagged = source.assign(**{self.op_column: src_op})
        # target rows missing from source are deletes.
        delete_rows = target.loc[~in_source].assign(**{self.op_column: "delete"})

        # --- assemble the requested change types, deterministic order ------- #
        union_cols = list(source.columns) + [c for c in target.columns if c not in set(source.columns)]
        emit = set(self.emit)
        pieces: list[pd.DataFrame] = []
        for change in _CANONICAL_ORDER:
            if change not in emit:
                continue
            if change == "delete":
                part = delete_rows
            else:
                part = source_tagged.loc[source_tagged[self.op_column] == change]
            if part.empty:
                continue
            part = part.drop(columns=[self.op_column]).reindex(columns=union_cols)
            part.insert(0, self.op_column, self._label(change))
            pieces.append(part)

        if pieces:
            out = pd.concat(pieces, ignore_index=True, copy=False)
        else:
            out = pd.DataFrame(columns=[self.op_column, *union_cols])
        return out