"""Load and assemble a v1 blueprint into a validated :class:`PipelineConfig`.

Pipeline:

1. **Read** the root document (YAML or JSON, by extension/content).
2. **Imports** -- recursively load and deep-merge each fragment listed in
   ``imports`` (resources, schemas, vars, tasks may be spread across files).
3. **Templates & defaults** -- apply ``extends`` task templates, then fold the
   global ``defaults`` block into every task.
4. **Parameters** -- coerce/validate provided run parameters against the typed
   ``parameters`` schema.
5. **Interpolate** -- resolve ``${env|var|param|secret:...}`` over the parsed
   structure (``${pipe}`` is preserved for runtime).
6. **Validate** -- construct the Pydantic model.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from ..core.exceptions import ConfigError
from ..core.secrets import EnvSecretProvider, SecretProvider
from . import interpolation
from .models import PipelineConfig

_TASK_DEFAULT_KEYS = ("retry", "on_failure", "checkpoint", "validate")


# --------------------------------------------------------------------------- #
# parsing & imports
# --------------------------------------------------------------------------- #


def _read_document(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        return _parse_yaml(text, path)
    if path.suffix.lower() == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{path} is not valid JSON: {exc}") from exc
    # Unknown extension: try YAML (a superset of JSON).
    return _parse_yaml(text, path)


def _parse_yaml(text: str, path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency declared
        raise ConfigError("PyYAML is required to read YAML blueprints") from exc
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"{path}: top-level document must be a mapping")
    return loaded


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in overlay.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        elif key in base and isinstance(base[key], list) and isinstance(value, list):
            base[key] = base[key] + value
        else:
            base[key] = value
    return base


def _assemble(path: Path, _seen: set[Path] | None = None) -> dict[str, Any]:
    """Read a document and deep-merge all of its (recursive) imports."""
    _seen = _seen or set()
    resolved = path.resolve()
    if resolved in _seen:
        raise ConfigError(f"circular import detected at {resolved}")
    _seen.add(resolved)

    doc = _read_document(path)
    imports = doc.pop("imports", []) or []
    merged: dict[str, Any] = {}
    for rel in imports:
        fragment_path = (path.parent / rel).resolve()
        if not fragment_path.exists():
            raise ConfigError(f"{path}: imported fragment not found: {rel}")
        _deep_merge(merged, _assemble(fragment_path, _seen))
    # The importing document's own keys win over imported fragments.
    _deep_merge(merged, doc)
    return merged


# --------------------------------------------------------------------------- #
# templates & defaults
# --------------------------------------------------------------------------- #


def _apply_templates_and_defaults(doc: dict[str, Any]) -> None:
    templates = doc.get("templates", {}) or {}
    defaults = doc.get("defaults", {}) or {}
    tasks = doc.get("tasks", {}) or {}

    for name, task in tasks.items():
        if not isinstance(task, dict):
            continue
        # 1. extends: shallow-merge the template under the task (task wins).
        template_name = task.pop("extends", None)
        if template_name is not None:
            template = templates.get(template_name)
            if template is None:
                raise ConfigError(f"task '{name}' extends unknown template '{template_name}'")
            for key, value in template.items():
                task.setdefault(key, value)
        # 2. defaults: fill task-level operational keys when not set.
        for key in _TASK_DEFAULT_KEYS:
            if key in defaults and key not in task:
                task[key] = defaults[key]


# --------------------------------------------------------------------------- #
# parameters
# --------------------------------------------------------------------------- #


def _coerce_param(name: str, spec: dict[str, Any], value: Any) -> Any:
    ptype = spec.get("type", "string")
    try:
        if ptype == "integer":
            return int(value)
        if ptype == "float":
            return float(value)
        if ptype == "boolean":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("1", "true", "yes", "on")
        if ptype in ("date", "datetime"):
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            return str(value)
        return str(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"parameter '{name}': cannot coerce {value!r} to {ptype}: {exc}")


def _resolve_parameters(doc: dict[str, Any], provided: Mapping[str, Any]) -> dict[str, Any]:
    specs = doc.get("parameters", {}) or {}
    resolved: dict[str, Any] = {}
    for name, spec in specs.items():
        spec = spec or {}
        if name in provided:
            value = provided[name]
        elif spec.get("default") is not None:
            value = spec["default"]
        elif spec.get("required"):
            raise ConfigError(f"required parameter '{name}' was not provided")
        else:
            continue
        value = _coerce_param(name, spec, value)
        allowed = spec.get("allowed")
        if allowed is not None and value not in allowed:
            raise ConfigError(f"parameter '{name}'={value!r} not in allowed set {allowed}")
        resolved[name] = value
    # Allow undeclared params through too (validated only if a spec exists).
    for name, value in provided.items():
        resolved.setdefault(name, value)
    return resolved


# --------------------------------------------------------------------------- #
# public API
# --------------------------------------------------------------------------- #


def load_config(
    source: str | Path | Mapping[str, Any],
    *,
    params: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    secret_provider: SecretProvider | None = None,
    strict: bool = False,
) -> PipelineConfig:
    """Parse, assemble, interpolate and validate a pipeline configuration.

    Parameters
    ----------
    source: path to a YAML/JSON root blueprint, or an already-parsed mapping.
    params: run parameters (validated against the ``parameters`` block).
    env: environment for ``${env:...}`` (defaults to ``os.environ``).
    secret_provider: resolves ``${secret:...}`` (defaults to env-backed).
    strict: when True, an unresolved token aborts loading.
    """
    if isinstance(source, Mapping):
        doc: dict[str, Any] = json.loads(json.dumps(dict(source)))  # deep copy
    else:
        path = Path(source)
        if not path.exists():
            raise ConfigError(f"blueprint not found: {path}")
        doc = _assemble(path)

    _apply_templates_and_defaults(doc)

    environment = dict(os.environ if env is None else env)
    provider = secret_provider or EnvSecretProvider(environment)
    resolved_params = _resolve_parameters(doc, params or {})

    secret_sink: set[str] = set()

    # Resolve the vars block first (it may reference env/param/secret), then
    # expose a var resolver for the rest of the document.
    base_resolvers = {
        "env": lambda ref: environment[ref],
        "param": lambda ref: resolved_params[ref],
        "secret": provider.get,
    }
    raw_vars = doc.get("vars", {}) or {}
    resolved_vars = interpolation.resolve(
        raw_vars, base_resolvers, strict=strict, secret_sink=secret_sink
    )
    doc["vars"] = resolved_vars

    full_resolvers = dict(base_resolvers)
    full_resolvers["var"] = lambda ref: resolved_vars[ref]

    doc = interpolation.resolve(doc, full_resolvers, strict=strict, secret_sink=secret_sink)

    try:
        config = PipelineConfig.model_validate(doc)
    except Exception as exc:  # pydantic ValidationError -> friendly ConfigError
        raise ConfigError(f"invalid pipeline configuration: {exc}") from exc

    config._runtime_params = resolved_params
    config._secret_values = secret_sink
    return config
