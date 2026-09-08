"""Factory that builds the right :class:`Adapter` for a resource config.

Adapter kinds are resolved from the ``ADAPTERS`` registry (entry-point group
``pipeplan.adapters``), so a new resource family -- relational, document, a REST
API, an object store -- is added by registering an adapter, never by editing this
factory. Built-in families are imported here so they self-register.
"""

from __future__ import annotations

from ..config.models import ResourceConfig
from ..core.exceptions import AdapterError
from ..core.registry import ADAPTERS
from .base import Adapter

# Import built-in adapter families for their registration side effects.
from . import file as _file  # noqa: F401,E402
from . import document as _document  # noqa: F401,E402
from .sql import adapter as _sql_adapter  # noqa: F401,E402
from .rest import adapter as _rest_adapter  # noqa: F401,E402


def create_adapter(resource: ResourceConfig) -> Adapter:
    """Instantiate the adapter implementation for ``resource``."""
    kind = resource.adapter
    try:
        cls = ADAPTERS.get(kind)
    except Exception:
        available = ", ".join(sorted(ADAPTERS.names())) or "<none>"
        raise AdapterError(
            f"resource '{resource.name}': no adapter registered for kind '{kind}' "
            f"(available: {available})"
        ) from None
    return cls(resource)
