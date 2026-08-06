"""Built-in transforms.

Importing this package registers every built-in transform with the global
registry. Third-party transforms are discovered separately via the
``pipeplan.transforms`` entry-point group.
"""

from __future__ import annotations

from . import collection, element, set_ops  # noqa: F401  (import for side effects)
from .base import Tier, Transform, build_transform
from . import cdc # noqa: F401 (registers compare_diff)
from . import projection # noqa: F401

__all__ = ["Tier", "Transform", "build_transform"]
