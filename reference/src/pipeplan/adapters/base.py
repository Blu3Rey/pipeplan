"""Resource adapter abstraction.

An adapter mediates all I/O with one external system. The two concrete
implementations are :class:`FileAdapter` (pandas file I/O) and
:class:`DBAdapter` (SQLAlchemy + ODBC). Permission enforcement (the resource's
``allow`` list) lives here so it cannot be bypassed by a task.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import PermissionDeniedError


class Adapter(ABC):
    def __init__(self, config: ResourceConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name or "<resource>"

    def _require(self, permission: Permission) -> None:
        if not self.config.permits(permission):
            allowed = ", ".join(p.value for p in self.config.allow) or "<none>"
            raise PermissionDeniedError(
                f"resource '{self.name}' does not allow '{permission.value}' "
                f"(allowed: {allowed})"
            )

    @abstractmethod
    def read(
        self,
        collection: str | None,
        *,
        since: tuple[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Read a collection. ``since=(cursor, min_value)`` requests only rows
        whose ``cursor`` exceeds ``min_value`` (incremental extract)."""
        ...

    @abstractmethod
    def write(
        self,
        frame: pd.DataFrame,
        collection: str,
        *,
        mode: LoadMode,
        key: str | list[str] | None = None,
        chunksize: int | None = None,
        partition_by: list[str] | None = None,
        scd: dict[str, Any] | None = None,
    ) -> None:
        ...
