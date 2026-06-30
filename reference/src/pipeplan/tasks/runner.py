"""Task runners -- the data-flow engine.

Each runner takes a validated task plus the shared :class:`ExecutionContext` and
executes it against the in-memory dataframe state. Control flow (ordering,
parallelism, dependencies) is the orchestrator's job.

The *explicit piped data rule* lives in the transform runner: steps run in
sequence and the dataframe produced by one becomes the dataframe "flowing" into
the next, which collection transforms bind to via the ``${pipe}`` token.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, TypeVar

import pandas as pd

from ..adapters.base import Adapter
from ..config.models import (
    ExtractTask,
    IncrementalStrategy,
    LoadTask,
    RetryPolicy,
    TransformTask,
    ValidationMode,
)
from ..core.context import ExecutionContext
from ..core.durations import parse_seconds, parse_timedelta
from ..core.exceptions import ContractError, PipePlanError, TransformError
from ..transforms.base import build_transform

logger = logging.getLogger("pipeplan.runner")

T = TypeVar("T")


def _with_retry(policy: RetryPolicy | None, label: str, fn: Callable[[], T]) -> T:
    if policy is None:
        return fn()
    delay = parse_seconds(policy.delay)
    last: Exception | None = None
    for attempt in range(1, policy.attempts + 1):
        try:
            return fn()
        except PipePlanError as exc:  # only retry framework-level failures
            last = exc
            if attempt >= policy.attempts:
                break
            if delay > 0:
                time.sleep(delay)
            delay *= policy.backoff
    raise TransformError(f"{label} failed after {policy.attempts} attempt(s): {last}") from last


def _adapter(ctx: ExecutionContext, name: str, label: str) -> Adapter:
    try:
        return ctx.adapters[name]
    except KeyError:  # pragma: no cover - orchestrator provisions these
        raise TransformError(f"{label}: adapter for resource '{name}' was not provisioned") from None


def _validate_contract(
    df: pd.DataFrame, schema_name: str | None, label: str, ctx: ExecutionContext, mode: ValidationMode
) -> None:
    if schema_name is None or mode is ValidationMode.OFF:
        return
    contract = ctx.config.schemas.get(schema_name)
    if contract is None:  # pragma: no cover - validated at config time
        return
    try:
        contract.validate_frame(df, label=label, state=ctx.state)
    except ContractError as exc:
        if mode is ValidationMode.WARN:
            logger.warning("%s", exc)
        else:
            raise


# --------------------------------------------------------------------------- #
# extract
# --------------------------------------------------------------------------- #


def run_extract(task: ExtractTask, ctx: ExecutionContext) -> None:
    def _do() -> None:
        adapter = _adapter(ctx, task.resource, f"extract '{task.name}'")
        incr = task.incremental
        cursor = incr.cursor if incr else None
        since = _resolve_since(task, ctx) if incr and incr.strategy is IncrementalStrategy.WATERMARK else None
        if incr and incr.strategy is IncrementalStrategy.CDC:
            logger.warning("extract '%s': CDC strategy not implemented; reading in full", task.name)

        for step in task.steps:
            frame = adapter.read(step.collection, since=since)
            if not isinstance(frame, pd.DataFrame):  # pragma: no cover - defensive
                raise TransformError(
                    f"extract '{task.name}': adapter '{adapter.name}' returned "
                    f"{type(frame).__name__}, not a DataFrame"
                )
            ctx.state.set(step.output, frame)
            _validate_contract(frame, step.contract, f"{task.name}:{step.output}", ctx, task.validation)
            if cursor and ctx.watermark_store is not None and cursor in frame.columns and not frame.empty:
                high = frame[cursor].max()
                ctx.watermark_store.set(ctx.config.pipeline_id, task.name, cursor, high)

    _with_retry(task.retry, f"extract '{task.name}'", _do)


def _resolve_since(task: ExtractTask, ctx: ExecutionContext) -> tuple[str, Any] | None:
    incr = task.incremental
    if incr is None or incr.cursor is None:
        return None
    last = None
    if ctx.watermark_store is not None:
        last = ctx.watermark_store.get(ctx.config.pipeline_id, task.name, incr.cursor)
    if last is None:
        last = incr.initial
    if last is None:
        return None
    # Apply a safety lookback window when the cursor is a timestamp.
    if incr.lookback:
        try:
            as_ts = pd.Timestamp(last) - parse_timedelta(incr.lookback)
            # Bind as a plain string: portable across drivers and correct for
            # ISO date/datetime text columns.
            last = as_ts.strftime("%Y-%m-%d") if as_ts.normalize() == as_ts else as_ts.isoformat()
        except (ValueError, TypeError):
            pass
    return (incr.cursor, last)


# --------------------------------------------------------------------------- #
# transform
# --------------------------------------------------------------------------- #


def run_transform(task: TransformTask, ctx: ExecutionContext) -> None:
    def _do() -> None:
        flowing: pd.DataFrame | None = ctx.state.get(task.input) if task.input else None
        if flowing is not None:
            _validate_contract(flowing, task.input_contract, f"{task.name}:input", ctx, task.validation)

        for index, step in enumerate(task.steps):
            transform = build_transform(step.action, step.with_)
            label = f"transform '{task.name}' step #{index + 1} ('{step.action}')"
            try:
                if step.on_error == "quarantine":
                    if not transform.supports_quarantine:
                        raise TransformError(
                            f"{label}: on_error='quarantine' is not supported by '{step.action}'"
                        )
                    good, rejected = transform.apply_safe(flowing, ctx)
                    if not rejected.empty:
                        ctx.state.set(f"{task.output}__rejected", rejected)
                        logger.warning("%s: quarantined %d row(s)", label, len(rejected))
                    flowing = good
                else:
                    flowing = transform.apply(flowing, ctx)
            except PipePlanError as exc:
                if step.on_error == "skip":
                    logger.warning("%s: skipped after error: %s", label, exc)
                    continue
                raise
            except Exception as exc:
                if step.on_error == "skip":
                    logger.warning("%s: skipped after error: %s", label, exc)
                    continue
                raise TransformError(f"{label} failed: {exc}") from exc
            if not isinstance(flowing, pd.DataFrame):  # pragma: no cover - defensive
                raise TransformError(f"{label} did not return a DataFrame")

        if flowing is None:  # pragma: no cover - guarded by min_length=1
            raise TransformError(f"transform '{task.name}' produced no dataframe")

        _validate_contract(flowing, task.output_contract, f"{task.name}:output", ctx, task.validation)
        for expectation in task.expectations:
            expectation.check(flowing, task=task.name)
        ctx.state.set(task.output, flowing)

    _with_retry(task.retry, f"transform '{task.name}'", _do)


# --------------------------------------------------------------------------- #
# load
# --------------------------------------------------------------------------- #


def run_load(task: LoadTask, ctx: ExecutionContext) -> None:
    def _do() -> None:
        adapter = _adapter(ctx, task.resource, f"load '{task.name}'")
        for step in task.steps:
            frame = ctx.state.get(step.input, copy=False)
            _validate_contract(frame, step.input_contract, f"{task.name}:{step.collection}", ctx, task.validation)
            scd = step.scd.model_dump() if step.scd is not None else None
            adapter.write(
                frame,
                step.collection,
                mode=step.mode,
                key=step.key,
                chunksize=step.write.chunksize,
                partition_by=step.write.partition_by or None,
                scd=scd,
            )

    _with_retry(task.retry, f"load '{task.name}'", _do)
