"""Central exception hierarchy for PipePlan.

Every failure mode in the framework raises a subclass of :class:`PipePlanError`
so callers can catch the whole family or narrow to a specific category.
"""

from __future__ import annotations


class PipePlanError(Exception):
    """Base class for all PipePlan errors."""


class ConfigError(PipePlanError):
    """Raised when a pipeline configuration is structurally invalid."""


class DependencyError(ConfigError):
    """Raised when the task dependency graph is invalid (cycles, missing nodes)."""


class InterpolationError(ConfigError):
    """Raised when a ``${ns:ref}`` token cannot be resolved."""


class StateError(PipePlanError):
    """Raised when a requested dataframe is missing from the execution state."""


class RegistryError(PipePlanError):
    """Raised when a transform / expression cannot be resolved from the registry."""


class AdapterError(PipePlanError):
    """Raised when a resource adapter cannot read or write."""


class PermissionDeniedError(AdapterError):
    """Raised when a resource is used for an operation outside its ``allow`` list."""


class TransformError(PipePlanError):
    """Raised when a transform step fails to execute."""


class ExpressionError(PipePlanError):
    """Raised when a filter / compute AST node is malformed or cannot evaluate."""


class ContractError(PipePlanError):
    """Raised when a dataframe violates its declared schema contract."""


class ExpectationError(PipePlanError):
    """Raised when a hard data-quality expectation fails."""


class SecretError(ConfigError):
    """Raised when a secret cannot be resolved from the configured provider."""
