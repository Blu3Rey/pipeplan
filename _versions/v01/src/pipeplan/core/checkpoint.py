"""Checkpoint store for resumable runs.

When ``orchestration.checkpoint.enabled`` is set, each task's output frame is
written to a parquet file keyed by a fingerprint of the task name plus the
hashes of its input frames. On a re-run with ``reuse: true``, a task whose
inputs are unchanged restores its output from the checkpoint instead of
recomputing -- the basis for cheap resume-after-failure.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeplan.checkpoint")


def frame_fingerprint(df: pd.DataFrame) -> str:
    """A stable content hash of a dataframe (shape + row hashes)."""
    try:
        row_hashes = pd.util.hash_pandas_object(df, index=False).values
        digest = hashlib.sha1(row_hashes.tobytes())
        digest.update(",".join(map(str, df.columns)).encode("utf-8"))
        digest.update(str(df.shape).encode("utf-8"))
        return digest.hexdigest()[:16]
    except Exception:  # pragma: no cover - unhashable content
        return hashlib.sha1(str(df.shape).encode("utf-8")).hexdigest()[:16]


class CheckpointStore:
    def __init__(self, root: str | Path, *, reuse: bool = True) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.reuse = reuse

    def key(self, task: str, input_fingerprints: list[str]) -> str:
        h = hashlib.sha1(task.encode("utf-8"))
        for fp in sorted(input_fingerprints):
            h.update(fp.encode("utf-8"))
        return f"{task}.{h.hexdigest()[:12]}"

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.parquet"

    def load(self, key: str) -> pd.DataFrame | None:
        if not self.reuse:
            return None
        path = self._path(key)
        if path.exists():
            try:
                logger.info("checkpoint hit: %s", key)
                return pd.read_parquet(path)
            except Exception:  # pragma: no cover - corrupt checkpoint
                logger.warning("failed to read checkpoint %s; recomputing", key)
        return None

    def save(self, key: str, df: pd.DataFrame) -> None:
        try:
            df.to_parquet(self._path(key), index=False)
        except Exception:  # pragma: no cover - non-parquetable frame
            logger.warning("failed to checkpoint %s", key)
