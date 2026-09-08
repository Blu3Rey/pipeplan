"""Declarative configuration for the REST adapter.

Parsed from a resource's ``params`` and validated up front, so a malformed
endpoint map, an unknown auth type, or a bad HTTP method fails at provision time
rather than against a live API. The endpoint map is the safety core: only
declared operations are callable, each with a pinned method and a path template
that can't escape ``base_url``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class AuthConfig(StrictModel):
    type: Literal["none", "bearer", "api_key", "basic"] = "none"
    token: str | None = None                 # bearer
    header: str = "X-API-Key"                 # api_key (header placement)
    key: str | None = None                    # api_key value
    param: str | None = None                  # api_key (query-param placement)
    user: str | None = None                   # basic
    password: str | None = None               # basic

    @model_validator(mode="after")
    def _check(self) -> "AuthConfig":
        if self.type == "bearer" and not self.token:
            raise ValueError("auth type 'bearer' requires a 'token'")
        if self.type == "api_key" and not self.key:
            raise ValueError("auth type 'api_key' requires a 'key'")
        if self.type == "basic" and not (self.user and self.password):
            raise ValueError("auth type 'basic' requires 'user' and 'password'")
        return self


class RateLimit(StrictModel):
    requests: int = Field(gt=0)
    per: str = "1s"


class RetryConfig(StrictModel):
    attempts: int = Field(default=3, ge=1)
    backoff: float = Field(default=2.0, ge=1.0)
    on_status: list[int] = Field(default_factory=lambda: [429, 500, 502, 503, 504])


class Pagination(StrictModel):
    style: Literal["none", "page", "offset", "cursor", "link"] = "none"
    param: str | None = None          # page number / offset / cursor query param
    size_param: str | None = None     # page-size / limit query param
    size: int | None = None
    start: int = 1                    # first page number / starting offset
    next: str | None = None           # cursor: dotted path to the next cursor
    max_pages: int = Field(default=1000, ge=1)

    @model_validator(mode="after")
    def _check(self) -> "Pagination":
        if self.style in ("page", "offset", "cursor") and not self.param:
            raise ValueError(f"pagination style '{self.style}' requires a 'param'")
        if self.style == "cursor" and not self.next:
            raise ValueError("pagination style 'cursor' requires 'next' (path to next cursor)")
        return self


class Operation(StrictModel):
    method: str
    path: str
    record_path: str | None = None    # read: dotted path to the records array
    since_param: str | None = None    # read: incremental query param

    @field_validator("method", mode="before")
    @classmethod
    def _upper(cls, v: str) -> str:
        return str(v).upper()

    @field_validator("method")
    @classmethod
    def _known(cls, v: str) -> str:
        if v not in _METHODS:
            raise ValueError(f"unknown HTTP method '{v}' (allowed: {sorted(_METHODS)})")
        return v


class Endpoint(StrictModel):
    key: str | list[str] | None = None
    read: Operation | None = None
    create: Operation | None = None
    update: Operation | None = None
    delete: Operation | None = None
    replace: Operation | None = None

    @property
    def keys(self) -> list[str]:
        if self.key is None:
            return []
        return [self.key] if isinstance(self.key, str) else list(self.key)

    @model_validator(mode="after")
    def _key_required_for_keyed_ops(self) -> "Endpoint":
        if (self.update or self.delete) and not self.key:
            raise ValueError("endpoints with 'update'/'delete' operations require a 'key'")
        return self


class RestConfig(StrictModel):
    base_url: str
    auth: AuthConfig = Field(default_factory=AuthConfig)
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float = 30.0
    concurrency: int = Field(default=4, ge=1)
    dry_run: bool = False
    continue_on_error: bool = False
    idempotency_header: str = "Idempotency-Key"
    rate_limit: RateLimit | None = None
    retry: RetryConfig = Field(default_factory=RetryConfig)
    pagination: Pagination = Field(default_factory=Pagination)
    endpoints: dict[str, Endpoint] = Field(min_length=1)

    @field_validator("base_url")
    @classmethod
    def _https(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v.rstrip("/")
