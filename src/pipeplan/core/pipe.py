"""Runtime resolution of the ``${pipe:...}`` operand.

``${pipe}`` (bare) is the whole frame flowing through a task -- bound by
collection transforms. ``${pipe:column}`` references a *column* of that frame and
is resolved here, at execution time, against the live dataframe. It is never
touched by load-time interpolation.

Two shapes, chosen explicitly by the author:

* ``${pipe:col}``           -> the column as a ``pd.Series`` (horizontal/aligned).
* ``${pipe:col|reducer}``   -> the column collapsed across rows (vertical):
  a list (``unique``/``list``/``values``/``set``) or a scalar (``min``/``max``/
  ``mean``/``median``/``sum``/``std``/``var``/``count``/``nunique``/``first``/
  ``last``).

The reducer is the sole disambiguator between horizontal and vertical use, so an
operation never has to infer the direction of flow.
"""

from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd

from .exceptions import ExpressionError

_PIPE_RE = re.compile(r"^\$\{pipe(?::([^}]*))?\}$")

# Reducers that collapse a column to a list of values (membership use).
_LIST_REDUCERS: dict[str, Callable[[pd.Series], list]] = {
    "unique": lambda s: list(pd.unique(s.dropna())),
    "set": lambda s: list(pd.unique(s.dropna())),
    "list": lambda s: list(s),
    "values": lambda s: list(s),
}

# Reducers that collapse a column to a single scalar.
_SCALAR_REDUCERS: dict[str, Callable[[pd.Series], Any]] = {
    "min": lambda s: s.min(),
    "max": lambda s: s.max(),
    "mean": lambda s: s.mean(),
    "median": lambda s: s.median(),
    "sum": lambda s: s.sum(),
    "std": lambda s: s.std(),
    "var": lambda s: s.var(),
    "count": lambda s: int(s.count()),
    "nunique": lambda s: int(s.nunique(dropna=True)),
    "first": lambda s: s.iloc[0] if len(s) else None,
    "last": lambda s: s.iloc[-1] if len(s) else None,
}

def is_pipe_ref(value: Any) -> bool:
    """True if ``value`` is a ``${pipe...}`` token string."""
    return isinstance(value, str) and _PIPE_RE.match(value) is not None

def resolve_pipe_token(token: str, df: pd.DataFrame) -> Any:
    """Resolve a full ``${pipe:col|reducer}`` token against ``df``."""
    match = _PIPE_RE.match(token)
    if match is None:   # pragma: no cover - guarded by is_pipe_ref
        raise ExpressionError(f"not a pipe token: {token!r}")
    ref = match.group(1)
    if ref is None or ref == "":
        raise ExpressionError(
            "${pipe} refers to the whole flowing frame; use ${pipe:column} "
            "(optionally ${pipe:column|reducer}) to reference a column's values"
        )
    return resolve_pipe_ref(ref, df)

def resolve_pipe_ref(ref: str, df: pd.DataFrame) -> Any:
    """Resolve the inner ``col`` or ``col|reducer`` reference against ``df``."""
    column, _, reducer = ref.partition("|")
    column = column.strip()
    reducer = reducer.strip()
    if column not in df.columns:
        raise ExpressionError(
            f"${{pipe:{ref}}} references unknown column '{column}'; "
            f"available columns: {list(df.columns)}"
        )
    series = df[column]
    if not reducer:
        return series   # aligned Series (horizontal)
    if reducer in _LIST_REDUCERS:
        return _LIST_REDUCERS[reducer](series)
    if reducer in _SCALAR_REDUCERS:
        try:
            return _SCALAR_REDUCERS[reducer](series)
        except (TypeError, ValueError) as exc:
            raise ExpressionError(
                f"${{pipe:{ref}}}: reducer '{reducer}' cannot be applied to "
                f"column '{column}' (dtype {series.dtype}): {exc}"
            ) from exc
    known = sorted([*_LIST_REDUCERS, *_SCALAR_REDUCERS])
    raise ExpressionError(f"${{pipe:{ref}}}: unknown reducer '{reducer}' (known: {known})")