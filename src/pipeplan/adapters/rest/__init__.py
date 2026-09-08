"""The REST adapter family: a REST API as an extract/load resource, driven by a
declarative, safety-first endpoint map.
"""

from __future__ import annotations

from .adapter import RestAdapter
from .config import RestConfig
from .transport import RateLimiter, RequestsTransport, Response, Transport, send

__all__ = [
    "RestAdapter",
    "RestConfig",
    "Transport",
    "Response",
    "RequestsTransport",
    "RateLimiter",
    "send"
]