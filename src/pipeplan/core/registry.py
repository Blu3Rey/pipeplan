"""Plugin registry.

The core stays pristine: every transform and every expression function is
discovered through a registry rather than hard-wired ``if/elif`` ladders.

Two discovery mechanisms are supported:

* **In-tree built-ins** register themselves at import time via the
  :func:`register_transform` / :func:`register_expression` decorators.
* **Third-party plugins** are discovered through ``importlib.metadata``
  entry points declared in a distribution's ``pyproject.toml``::

      [project.entry-points."pipeplan.transforms"]
      my_action = "my_pkg.module:MyTransform"

      [project.entry-points."pipeplan.expressions"]
      geodistance = "my_pkg.module:geodistance"

Entry points are loaded lazily and cached, so importing the framework does not
pay the cost of scanning installed distributions until a lookup actually misses
the in-tree registry.
"""

from __future__ import annotations

from importlib import metadata
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from .exceptions import RegistryError

if TYPE_CHECKING:   # avoid import cycles at runtime
    from ..transforms.base import Transform

T = TypeVar("T")

TRANSFORM_GROUP = "pipeplan.transforms"
EXPRESSION_GROUP = "pipeplan.expressions"
NOTIFIER_GROUP = "pipeplan.notifiers"
SECRET_PROVIDER_GROUP = "pipeplan.secret_providers"

class Registry(Generic[T]):
    """A name -> object registry backed by an entry-point group."""

    def __init__(self, entry_point_group: str, kind: str) -> None:
        self._group = entry_point_group
        self._kind = kind
        self._items: dict[str, T] = {}
        self._entry_points_loaded = False
    
    def register(self, name: str, obj: T | None = None) -> Callable[[T], T] | T:
        """Register ``obj`` under ``name``.

        Usable directly (``registry.register("x", obj)``) or as a decorator
        (``@registry.register("x")``).
        """

        def _do(target: T) -> T:
            if name in self._items:
                raise RegistryError(
                    f"{self._kind} '{name}' is already registered "
                    f"({self._items[name]!r}); refusing to shadow it."
                )
            self._items[name] = target
            return target
        
        if obj is not None:
            return _do(obj)
        return _do
    
    def _load_entry_points(self) -> None:
        if self._entry_points_loaded:
            return
        self._entry_points_loaded = True
        try:
            eps = metadata.entry_points(group=self._group)
        except TypeError:   # pragma: no cover - Python < 3.10 fallback
            eps = metadata.entry_points().get(self._group, [])  # type: ignore[attr-defined]
        for ep in eps:
            if ep.name in self._items:
                continue
            try:
                self._items[ep.name] = ep.load()
            except Exception as exc:    # pragma: no cover - depends on env
                raise RegistryError(
                    f"Failed to load {self._kind} plugin '{ep.name}' "
                    f"from entry point {ep.value!r}: {exc}"
                ) from exc
    
    def get(self, name: str) -> T:
        if name not in self._items:
            self._load_entry_points()
        try:
            return self._items[name]
        except KeyError:
            known = ", ".join(sorted(self._items)) or "<none>"
            raise RegistryError(
                f"Unknown {self._kind} '{name}'. Registered {self._kind}s: {known}. "
                f"Custom ones can be exposed via the '{self._group}' entry-point group."
            ) from None
    
    def names(self) -> list[str]:
        self._load_entry_points()
        return sorted(self._items)

TRANSFORMS: "Registry[type[Transform]]" = Registry(TRANSFORM_GROUP, "transform")
EXPRESSIONS: "Registry[Callable[..., object]]" = Registry(EXPRESSION_GROUP, "expression")
NOTIFIERS: "Registry[Callable[[], object]]" = Registry(NOTIFIER_GROUP, "notifier")

def register_notifier(name: str) -> Callable[[Callable[[], object]], Callable[[], object]]:
    """Register a notifier factory (zero-arg callable returning a Notifier)."""
    return NOTIFIERS.register(name) # type: ignore[return-value]

def register_transform(name: str) -> Callable[[type["Transform"]], type["Transform"]]:
    """Class decorator that registers a :class:`Transform` under ``name``."""
    return TRANSFORMS.register(name)    # type: ignore[return-value]

def register_expression(name: str) -> Callable[[Callable[..., object]], Callable[..., object]]:
    """Decorator that registers a scalar/vectorised expression function."""
    return EXPRESSIONS.register(name)   # type: ignore[return-value]