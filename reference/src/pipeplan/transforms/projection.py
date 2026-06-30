"""Element-tier column projection: the ``select`` and ``drop`` transforms.

These alter a dataset's *column* shape — keeping or removing named columns — and
therefore live in the **Element** tier (1:1 column-axis operations), alongside
``cast``/``label``/``map``. They never mask rows, so they are not Set-tier ops.

Both accept the bare-list shorthand enabled by ``TransformStep.with_`` (a step may
write ``with: [a, b, c]`` instead of ``with: { columns: [a, b, c] }``).

Missing-column policy differs by intent, and the asymmetry is deliberate:

* ``select`` is a hard requirement — if a requested column is absent the desired
  output literally cannot be produced, so it raises by default.
* ``drop`` is declarative and idempotent — the goal state is "column not present",
  which is already satisfied when the column is absent, so it is lenient by
  default.

Either default is overridable with ``ignore_missing``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
from pydantic import field_validator, model_validator

from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import register_transform
from .base import Tier, Transform


class _Projection(Transform):
    """Shared base for column-projection transforms.

    Carries the ``columns`` list, the bare-list shorthand, and validation that the
    list is non-empty and free of duplicates. Subclasses set the tier (Element)
    and implement :meth:`apply`.
    """

    tier: ClassVar[Tier] = Tier.ELEMENT
    columns: list[str]
    ignore_missing: bool

    @model_validator(mode="before")
    @classmethod
    def _accept_bare_list(cls, data: Any) -> Any:
        # Allow ``with: [a, b, c]`` as sugar for ``with: {columns: [a, b, c]}``.
        if isinstance(data, list):
            return {"columns": data}
        return data

    @field_validator("columns")
    @classmethod
    def _nonempty_unique(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one column must be named")
        seen: set[str] = set()
        dupes: list[str] = []
        for col in value:
            if col in seen and col not in dupes:
                dupes.append(col)
            seen.add(col)
        if dupes:
            raise ValueError(f"duplicate column(s) named: {dupes}")
        return value

    def _check_missing(self, df: pd.DataFrame, verb: str) -> list[str]:
        present = set(df.columns)
        missing = [c for c in self.columns if c not in present]
        if missing and not self.ignore_missing:
            raise TransformError(
                f"{verb} references missing column(s) {missing}; set "
                f"ignore_missing=true to skip them"
            )
        return missing


@register_transform("select")
class SelectTransform(_Projection):
    """Keep only the named columns, in the order given (a projection + reorder).

    Raises on any missing column unless ``ignore_missing`` is set, in which case
    absent names are silently skipped and the surviving columns keep the
    requested order. Returns a copy, so downstream steps never alias the input.
    """

    ignore_missing: bool = False

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        self._check_missing(df, "select")
        keep = [c for c in self.columns if c in df.columns]
        return df.loc[:, keep].copy()


@register_transform("drop")
class DropTransform(_Projection):
    """Remove the named columns, preserving the order of those that remain.

    Lenient by default: dropping an already-absent column is a no-op (idempotent).
    Set ``ignore_missing=false`` to make an absent target an error instead.
    """

    ignore_missing: bool = True

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        assert df is not None
        self._check_missing(df, "drop")
        present = [c for c in self.columns if c in df.columns]
        return df.drop(columns=present)