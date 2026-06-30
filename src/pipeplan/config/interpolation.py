"""Namespaced, type-aware interpolation for blueprints.

Tokens look like ``${namespace:reference}``. Four namespaces are resolved at
load time:

* ``${env:NAME}``     -- process environment
* ``${var:name}``     -- the blueprint's ``vars`` block
* ``${param:name}``   -- a typed run parameter
* ``${secret:path}``  -- the configured secret provider (value is redacted in logs)

``${pipe}`` has no namespace separator and is deliberately left untouched — it is
a *runtime* operand resolved by collection transforms, not a load-time value.

Two substitution modes:

* **Whole-value**: a string that is exactly one token (``"${var:regions}"``) is
  replaced by the resolver's raw value, so a token can yield a list or mapping,
  not just a string.
* **Embedded**: a token inside a larger string (``"db://${secret:u}@host"``) is
  replaced by ``str(value)``.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Mapping, MutableSet

from ..core.exceptions import InterpolationError

_TOKEN = re.compile(r"\$\{(\w+):([^}]*)\}")
_FULL = re.compile(r"^\$\{(\w+):([^}]*)\}$")

Resolver = Callable[[str], Any]

def resolve(
    data: Any,
    resolvers: Mapping[str, Resolver],
    *,
    strict: bool,
    secret_sink: MutableSet[str] | None = None,
) -> Any:
    """Recursively resolve ``${ns:ref}`` tokens in a parsed structure."""
    if isinstance(data, dict):
        return {k: resolve(v, resolvers, strict=strict, secret_sink=secret_sink) for k, v in data.items()}
    if isinstance(data, list):
        return [resolve(v, resolvers, strict=strict, secret_sink=secret_sink) for v in data]
    if isinstance(data, str):
        return _resolve_string(data, resolvers, strict=strict, secret_sink=secret_sink)
    return data

def _lookup(ns: str, ref: str, resolvers: Mapping[str, Resolver], *, strict: bool) -> Any:
    resolver = resolvers.get(ns)
    if resolver is None:
        if strict:
            raise InterpolationError(f"unknown interpolation namespace '${{{ns}:...}}'")
        return _MISSING
    try:
        return resolver(ref)
    except KeyError:
        if strict:
            raise InterpolationError(f"cannot resolve ${{{ns}:{ref}}}") from None
        return _MISSING

class _Missing:
    pass

_MISSING = _Missing()

def _resolve_string(
    value: str,
    resolvers: Mapping[str, Resolver],
    *,
    strict: bool,
    secret_sink: MutableSet[str] | None,
) -> Any:
    full = _FULL.match(value)
    if full:
        ns, ref = full.group(1), full.group(2)
        resolved = _lookup(ns, ref, resolvers, strict=strict)
        if resolved is _MISSING:
            return value    # leave token intact (non-strict)
        if ns == "secret" and secret_sink is not None and isinstance(resolved, str):
            secret_sink.add(resolved)
        return resolved
    
    def _sub(match: re.Match[str]) -> str:
        ns, ref = match.group(1), match.group(2)
        resolved = _lookup(ns, ref, resolvers, strict=strict)
        if resolved is _MISSING:
            return match.group(0)
        if ns == "secret" and secret_sink is not None:
            secret_sink.add(str(resolved))
        return str(resolved)
    
    return _TOKEN.sub(_sub, value)