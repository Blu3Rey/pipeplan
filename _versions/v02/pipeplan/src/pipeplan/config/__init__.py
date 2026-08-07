"""Configuration schema, fragment loader, and interpolation."""

from __future__ import annotations

from .loader import load_config
from .models import (
    AdapterKind,
    ExtractTask,
    LoadMode,
    LoadTask,
    OnFailure,
    Permission,
    PipelineConfig,
    ResourceConfig,
    Stage,
    TransformTask,
    ValidationMode,
)

__all__ = [
    "load_config",
    "PipelineConfig",
    "ResourceConfig",
    "Stage",
    "AdapterKind",
    "Permission",
    "LoadMode",
    "OnFailure",
    "ValidationMode",
    "ExtractTask",
    "TransformTask",
    "LoadTask",
]
