"""Core runtime: state, registry, context and the exception hierarchy."""

from __future__ import annotations

from .context import ExecutionContext
from .exceptions import (
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
from .registry import EXPRESSIONS, TRANSFORMS, register_expression, register_transform
from .state import StateManager

__all__ = [
    "ExecutionContext",
    "StateManager",
    "TRANSFORMS",
    "EXPRESSIONS",
    "register_transform",
    "register_expression",
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
