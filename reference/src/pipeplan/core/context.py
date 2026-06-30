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

    @property
    def timezone(self) -> str:
        return self.config.timezone
