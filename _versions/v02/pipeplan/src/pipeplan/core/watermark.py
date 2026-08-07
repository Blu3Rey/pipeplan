"""Incremental watermark store.

Persists the high-water mark of each incremental extract between runs so the
next run only pulls rows whose cursor advanced. Backed by a small table on a
db-adapter resource (declared via ``orchestration.watermark_store``).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("pipeplan.watermark")


class WatermarkStore:
    def __init__(self, adapter: Any, table: str = "pipeplan_watermarks") -> None:
        self._adapter = adapter
        self._table = table
        self._ensured = False

    def _ensure(self) -> None:
        if self._ensured:
            return
        # A single text-valued store keeps it dialect-portable; callers coerce.
        self._adapter.execute(
            f'CREATE TABLE IF NOT EXISTS "{self._table}" '
            f'(pipeline TEXT, task TEXT, cursor TEXT, value TEXT, '
            f'updated_at TEXT, PRIMARY KEY (pipeline, task, cursor))'
        )
        self._ensured = True

    def get(self, pipeline: str, task: str, cursor: str) -> Any:
        try:
            self._ensure()
            return self._adapter.scalar(
                f'SELECT value FROM "{self._table}" '
                f'WHERE pipeline = :p AND task = :t AND cursor = :c',
                {"p": pipeline, "t": task, "c": cursor},
            )
        except Exception:  # pragma: no cover - store unavailable -> full read
            logger.warning("watermark get failed for %s/%s; treating as cold start", pipeline, task)
            return None

    def set(self, pipeline: str, task: str, cursor: str, value: Any) -> None:
        if value is None:
            return
        import datetime as _dt

        self._ensure()
        self._adapter.execute(
            f'DELETE FROM "{self._table}" WHERE pipeline = :p AND task = :t AND cursor = :c',
            {"p": pipeline, "t": task, "c": cursor},
        )
        self._adapter.execute(
            f'INSERT INTO "{self._table}" (pipeline, task, cursor, value, updated_at) '
            f'VALUES (:p, :t, :c, :v, :u)',
            {
                "p": pipeline, "t": task, "c": cursor,
                "v": str(value), "u": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            },
        )
