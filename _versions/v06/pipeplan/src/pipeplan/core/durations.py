"""Parse human duration strings like ``30s``, ``10m``, ``2h``, ``7d``."""

from __future__ import annotations

import re
from datetime import timedelta

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*$", re.IGNORECASE)


def parse_seconds(value: str | float | int | None) -> float:
    """Return a number of seconds from a number or a duration string."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    match = _PATTERN.match(value)
    if not match:
        # Bare numeric string falls back to seconds.
        try:
            return float(value)
        except ValueError:
            raise ValueError(f"invalid duration '{value}' (use e.g. '30s', '10m', '2h', '7d')")
    amount, unit = match.group(1), match.group(2).lower()
    return float(amount) * _UNITS[unit]


def parse_timedelta(value: str | float | int | None) -> timedelta:
    return timedelta(seconds=parse_seconds(value))
