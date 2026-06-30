"""Transform base class and factory.

Each transform is a Pydantic model: the ``params`` block of a step validates
*directly* into the transform's fields, so parameter schema validation and
business logic live in one place. Transforms declare which tier they belong to
(Element / Set / Collection) per the architecture manifesto.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar

import pandas as pd
from pydantic import BaseModel, ConfigDict

from ..core.context import ExecutionContext
from ..core.exceptions import TransformError
from ..core.registry import TRANSFORMS


class Tier(str, Enum):
    ELEMENT = "element"  # 1:1 vectorised column ops
    SET = "set"  # row-masking / reshaping ops
    COLLECTION = "collection"  # multi-dataframe relational ops


class Transform(BaseModel):
    """Base class for every transform action."""

    model_config = ConfigDict(extra="forbid")

    tier: ClassVar[Tier]
    #: whether this transform can divert failing rows instead of aborting.
    supports_quarantine: ClassVar[bool] = False

    def apply(self, df: pd.DataFrame | None, ctx: ExecutionContext) -> pd.DataFrame:
        raise NotImplementedError  # pragma: no cover - abstract

    def apply_safe(
        self, df: pd.DataFrame | None, ctx: ExecutionContext
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply, diverting un-processable rows to a second 'rejected' frame.

        The default implementation does not understand row-level failure, so it
        simply applies normally and returns an empty rejected frame. Transforms
        that can isolate bad rows (e.g. ``cast``) override this.
        """
        result = self.apply(df, ctx)
        empty = result.iloc[0:0].copy()
        return result, empty


def build_transform(action: str, params: dict[str, Any]) -> Transform:
    """Resolve ``action`` to a transform class and validate ``params`` into it."""
    cls = TRANSFORMS.get(action)
    try:
        return cls.model_validate(params)
    except Exception as exc:
        raise TransformError(
            f"invalid parameters for transform '{action}': {exc}"
        ) from exc
