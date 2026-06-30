"""PipePlan -- a declarative, configuration-driven batch ETL framework.

Public API
----------
``load_config``  -- parse and validate a blueprint JSON into a ``PipelineConfig``.
``run_pipeline`` -- orchestrate a validated config end to end.
``Orchestrator`` -- the control-flow engine (for finer-grained control).

Importing this package registers all built-in transforms and expression
functions as a side effect, so a freshly loaded config can be run immediately.
"""

from __future__ import annotations

# Importing the transforms package triggers registration of every built-in
# transform via its decorators -- do this eagerly so the registry is populated.
from . import transforms as _transforms  # noqa: F401
from .config.loader import load_config
from .config.models import PipelineConfig
from .core.context import ExecutionContext
from .core.exceptions import (
    AdapterError,
    ConfigError,
    ContractError,
    DependencyError,
    ExpectationError,
    ExpressionError,
    InterpolationError,
    PermissionDeniedError,
    PipePlanError,
    RegistryError,
    SecretError,
    StateError,
    TransformError,
)
from .orchestrator import Orchestrator, run_pipeline

__version__ = "1.0.0"

__all__ = [
    "__version__",
    "load_config",
    "run_pipeline",
    "Orchestrator",
    "PipelineConfig",
    "ExecutionContext",
    "PipePlanError",
    "ConfigError",
    "DependencyError",
    "StateError",
    "RegistryError",
    "AdapterError",
    "PermissionDeniedError",
    "TransformError",
    "ExpressionError",
    "ContractError",
    "ExpectationError",
    "InterpolationError",
    "SecretError",
]
