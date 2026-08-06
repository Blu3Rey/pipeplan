"""Secret resolution for ``${secret:path}`` interpolation.

A :class:`SecretProvider` turns an opaque path (e.g. ``pg/password``) into a
secret value. The default :class:`EnvSecretProvider` maps the path onto an
environment variable so the framework runs out of the box; production
deployments register a provider backed by Vault / AWS Secrets Manager / etc.
through the ``pipeplan.secret_providers`` entry-point group.

Resolved secret values are never written to logs: adapters and error handlers
redact them (see :func:`redact`).
"""

from __future__ import annotations

import os
import re
from typing import Mapping, Protocol, runtime_checkable

from .exceptions import SecretError


@runtime_checkable
class SecretProvider(Protocol):
    """Resolves a secret path to its value."""

    def get(self, path: str) -> str: ...


class EnvSecretProvider:
    """Map ``a/b-c`` -> environment variable ``A_B_C``."""

    def __init__(self, environ: Mapping[str, str] | None = None, prefix: str = "") -> None:
        self._environ = os.environ if environ is None else environ
        self._prefix = prefix

    def get(self, path: str) -> str:
        var = self._prefix + re.sub(r"[^0-9A-Za-z]+", "_", path).upper()
        try:
            return self._environ[var]
        except KeyError:
            raise SecretError(
                f"secret '{path}' is unavailable (looked up env var '{var}')"
            ) from None


class MappingSecretProvider:
    """Resolve secrets from an in-memory mapping (handy for tests / local runs)."""

    def __init__(self, secrets: Mapping[str, str]) -> None:
        self._secrets = dict(secrets)

    def get(self, path: str) -> str:
        try:
            return self._secrets[path]
        except KeyError:
            raise SecretError(f"secret '{path}' is not defined") from None


def redact(text: str, secrets: set[str]) -> str:
    """Replace any known secret value in ``text`` with ``***``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text
