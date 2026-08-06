"""Factory that builds the right :class:`Adapter` for a resource config."""

from __future__ import annotations

from ..config.models import AdapterKind, ResourceConfig
from ..core.exceptions import AdapterError
from .base import Adapter
from .db import DBAdapter
from .file import FileAdapter

_REGISTRY: dict[AdapterKind, type[Adapter]] = {
    AdapterKind.FILE: FileAdapter,
    AdapterKind.DB: DBAdapter,
}


def create_adapter(resource: ResourceConfig) -> Adapter:
    """Instantiate the adapter implementation for ``resource``."""
    try:
        cls = _REGISTRY[resource.adapter]
    except KeyError:  # pragma: no cover - guarded by the enum
        raise AdapterError(
            f"resource '{resource.name}': no adapter for kind '{resource.adapter}'"
        ) from None
    return cls(resource)
