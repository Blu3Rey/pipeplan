"""Resource adapter abstraction.

An adapter mediates all I/O with one external system. The two concrete
implementations are :class:`FileAdapter` (pandas file I/O) and :class:`DBAdapter`
(SQLAlchemy). Permission enforcement (the resource's ``allow`` list) lives here so
it cannot be bypassed by a task.

Writes are expressed as :class:`WriteRequest` values and executed via
:meth:`Adapter.write_batch`, which is the unit of atomicity: a db adapter runs an
entire batch inside one transaction, so a multi-table load either fully lands or
fully rolls back. :meth:`Adapter.write` is the single-request convenience wrapper.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from ..config.models import LoadMode, Permission, ResourceConfig
from ..core.exceptions import PermissionDeniedError


@dataclass(slots=True)
class WriteRequest:
    """One write instruction within a load task."""

    frame: pd.DataFrame
    collection: str
    mode: LoadMode
    key: list[str] = field(default_factory=list)
    chunksize: int | None = None
    partition_by: list[str] = field(default_factory=list)
    scd: dict[str, Any] | None = None
    contract: Any = None          # DataframeContract used for typed DDL on create
    as_of: pd.Timestamp | None = None  # logical run timestamp (SCD effective date)


@dataclass(slots=True)
class LoadResult:
    """Outcome of a single write, surfaced for logging / notification."""

    collection: str
    mode: str
    rows_in: int
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    created_table: bool = False

    def summary(self) -> str:
        parts = [f"{self.rows_in} in"]
        if self.inserted:
            parts.append(f"{self.inserted} inserted")
        if self.updated:
            parts.append(f"{self.updated} updated")
        if self.deleted:
            parts.append(f"{self.deleted} deleted")
        if self.created_table:
            parts.append("table created")
        return f"{self.collection} [{self.mode}]: " + ", ".join(parts)


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
    def write_batch(self, requests: list[WriteRequest]) -> list[LoadResult]:
        """Execute a batch of writes. For transactional adapters this is atomic:
        all requests commit together or none do."""
        ...

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
        contract: Any = None,
        as_of: pd.Timestamp | None = None,
    ) -> LoadResult:
        """Convenience single-write wrapper over :meth:`write_batch`."""
        keys = [key] if isinstance(key, str) else (list(key) if key else [])
        request = WriteRequest(
            frame=frame, collection=collection, mode=mode, key=keys,
            chunksize=chunksize, partition_by=list(partition_by or []),
            scd=scd, contract=contract, as_of=as_of,
        )
        return self.write_batch([request])[0]
