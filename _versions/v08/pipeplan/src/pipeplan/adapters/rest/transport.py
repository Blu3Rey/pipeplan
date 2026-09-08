"""HTTP transport for the REST adapter.

The adapter talks to a :class:`Transport` protocol rather than to ``requests``
directly, so tests inject a deterministic mock and no network is required. The
default :class:`RequestsTransport` lazily imports ``requests`` (an optional
dependency). :class:`RateLimiter` enforces a request budget and :func:`send`
wraps a transport with retry + ``Retry-After`` handling.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ...core.durations import parse_seconds
from ...core.exceptions import AdapterError


@dataclass
class Response:
    status_code: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


@runtime_checkable
class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json: Any = None,
        timeout: float | None = None,
    ) -> Response: ...


class RequestsTransport:
    """Default transport backed by the ``requests`` library."""

    def __init__(self) -> None:
        try:
            import requests  # noqa: F401
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise AdapterError(
                "the rest adapter needs the 'requests' package "
                "(pip install 'pipeplan[rest]')"
            ) from exc

    def request(self, method, url, *, headers=None, params=None, json=None, timeout=None):  # pragma: no cover - needs network
        import requests

        resp = requests.request(
            method, url, headers=headers, params=params, json=json, timeout=timeout
        )
        try:
            body = resp.json()
        except ValueError:
            body = resp.text or None
        return Response(resp.status_code, body, dict(resp.headers))


class RateLimiter:
    """Thread-safe sliding-window limiter: at most ``n`` requests per window."""

    def __init__(self, n: int, per_seconds: float) -> None:
        self.n = n
        self.per = per_seconds
        self._lock = threading.Lock()
        self._events: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= self.per:
                    self._events.popleft()
                if len(self._events) < self.n:
                    self._events.append(now)
                    return
                sleep_for = self.per - (now - self._events[0])
            time.sleep(max(sleep_for, 0.001))


def send(
    transport: Transport,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None,
    params: dict[str, Any] | None,
    json: Any,
    timeout: float,
    retry_attempts: int,
    retry_backoff: float,
    retry_on: set[int],
    limiter: RateLimiter | None = None,
    label: str = "",
) -> Response:
    """Send one request, honouring the rate limit and retrying transient failures."""
    delay = 1.0
    last: Response | None = None
    for attempt in range(1, retry_attempts + 1):
        if limiter is not None:
            limiter.acquire()
        try:
            resp = transport.request(
                method, url, headers=headers, params=params, json=json, timeout=timeout
            )
        except Exception as exc:  # network/transport failure -> retry
            last = Response(0, str(exc))
            if attempt >= retry_attempts:
                raise AdapterError(f"{label or method + ' ' + url} failed: {exc}") from exc
            time.sleep(delay)
            delay *= retry_backoff
            continue
        if resp.status_code not in retry_on or attempt >= retry_attempts:
            return resp
        # Retryable status: respect Retry-After if present.
        retry_after = resp.headers.get("Retry-After") if resp.headers else None
        wait = parse_seconds(retry_after) if retry_after else delay
        time.sleep(wait)
        delay *= retry_backoff
        last = resp
    return last if last is not None else Response(0, "no response")  # pragma: no cover
