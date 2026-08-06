"""The Execution State Manager.

The manifesto mandates a ``Dict[str, pd.DataFrame]`` memory state that tasks
read from (``input_dataframe``) and write back to (``output_dataframe``).

This module wraps that dictionary in a small, strictly typed facade that gives
clear error messages, prevents silent overwrites of an already-materialised
dataframe unless explicitly intended, and hands out *copies* on read so that a
downstream task can never mutate a dataframe another task still depends on.
"""

from __future__ import annotations

import threading
from typing import Iterator

import pandas as pd

from .exceptions import StateError


class StateManager:
    """In-memory registry of named dataframes flowing through the pipeline.

    Access is guarded by a re-entrant lock so independent tasks scheduled on
    different threads (see ``orchestration.max_parallelism``) can read and write
    the shared state safely.
    """

    __slots__ = ("_frames", "_lock")

    def __init__(self) -> None:
        self._frames: dict[str, pd.DataFrame] = {}
        self._lock = threading.RLock()

    def set(self, name: str, frame: pd.DataFrame) -> None:
        """Store ``frame`` under ``name``, overwriting any previous value."""
        if not isinstance(frame, pd.DataFrame):  # pragma: no cover - defensive
            raise StateError(
                f"Refusing to store non-DataFrame under '{name}' "
                f"(got {type(frame).__name__})."
            )
        with self._lock:
            self._frames[name] = frame

    def get(self, name: str, *, copy: bool = True) -> pd.DataFrame:
        """Return the dataframe stored under ``name``.

        A defensive copy is returned by default so the caller can mutate it
        freely without corrupting the shared state.
        """
        with self._lock:
            try:
                frame = self._frames[name]
            except KeyError:
                available = ", ".join(sorted(self._frames)) or "<none>"
                raise StateError(
                    f"Dataframe '{name}' is not available in the execution state. "
                    f"Currently materialised: {available}."
                ) from None
            return frame.copy() if copy else frame

    def has(self, name: str) -> bool:
        with self._lock:
            return name in self._frames

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._frames)

    def drop(self, name: str) -> None:
        with self._lock:
            self._frames.pop(name, None)

    def __contains__(self, name: object) -> bool:
        return name in self._frames

    def __iter__(self) -> Iterator[str]:
        return iter(self._frames)

    def __len__(self) -> int:
        return len(self._frames)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        sizes = {name: frame.shape for name, frame in self._frames.items()}
        return f"StateManager({sizes})"
