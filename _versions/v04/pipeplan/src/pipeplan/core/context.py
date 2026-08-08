"""The execution context shared across a single pipeline run.

Carries the shared dataframe state plus everything a task/adapter needs at
runtime: provisioned adapters, resolved run parameters, the active partition
value, secret values (for log redaction), and handles to the watermark and
checkpoint stores.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .state import StateManager

if TYPE_CHECKING:
    from ..adapters.base import Adapter
    from ..config.models import PipelineConfig
    from .checkpoint import CheckpointStore
    from .notify import Notifier
    from .secrets import SecretProvider
    from .watermark import WatermarkStore


@dataclass(slots=True)
class ExecutionContext:
    config: "PipelineConfig"
    state: StateManager = field(default_factory=StateManager)
    adapters: dict[str, "Adapter"] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    partition_value: Any = None
    secret_values: set[str] = field(default_factory=set)
    secret_provider: "SecretProvider | None" = None
    watermark_store: "WatermarkStore | None" = None
    checkpoint_store: "CheckpointStore | None" = None
    notifier: "Notifier | None" = None
    run_id: str = "run"
    #: watermarks staged by incremental extracts, flushed only on a clean run.
    pending_watermarks: dict[tuple[str, str], object] = field(default_factory=dict)

    @property
    def timezone(self) -> str:
        return self.config.timezone

    def stage_watermark(self, task: str, cursor: str, value: object) -> None:
        """Record a new high-water mark to be committed after the run succeeds."""
        key = (task, cursor)
        prior = self.pending_watermarks.get(key)
        # Keep the max across steps of the same task.
        try:
            if prior is None or (value is not None and value > prior):
                self.pending_watermarks[key] = value
        except TypeError:
            self.pending_watermarks[key] = value

    def flush_watermarks(self) -> None:
        """Persist staged watermarks. Called only after a fully successful run so
        a failed load never advances the cursor (which would drop rows next run)."""
        if self.watermark_store is None:
            return
        for (task, cursor), value in self.pending_watermarks.items():
            self.watermark_store.set(self.config.pipeline_id, task, cursor, value)
        self.pending_watermarks.clear()
